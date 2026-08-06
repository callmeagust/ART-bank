import asyncio
from datetime import datetime, timezone, timedelta
import html
import logging
import math
import os
import random
import re
import shutil
import sqlite3
import string
import uuid
import zipfile

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)
import aiosqlite
from aiohttp import web  # اضافه شده برای ایجاد Web Service در Render

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

SUPER_ADMIN_1 = 8490505070
SUPER_ADMIN_2 = 475473068  # ادمین دوم با دسترسی کامل

# لیست سوپرادمین‌ها (از دیتابیس بارگذاری و به‌روزرسانی می‌شود)
SUPER_ADMINS: set[int] = {SUPER_ADMIN_1, SUPER_ADMIN_2}

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "atr_bank.db"
BACKUP_DIR = "backups"
MAX_BALANCE_LIMIT = 1000000000  # سقف ۱ میلیارد آتر
USERS_PER_PAGE = 10  # تعداد کاربران در هر صفحه پنل مدیریت

# ⚙️ آیدی عددی کانال خصوصی بکاپ تلگرام شما
BACKUP_CHANNEL_ID = -1003971216432

# --- 🏛 سیستم مالی جدید: خزانه مرکزی، بانک آترامنتوم و وام پویا ---
# طبق درخواست، این دو متغیر به‌صورت جداگانه تعریف می‌شوند تا منطق مالی خزانه
# (که صرفاً یک حساب در جدول users است) از منطق دسترسی مدیریتی (سوپرادمین) تفکیک شود.
# از نظر عددی هر دو برابر با آیدی سوپرادمین اول هستند، اما در کد برای دو منظور متفاوت استفاده می‌شوند.
TREASURY_USER_ID = 8490505070
SUPER_ADMIN_ID = 8490505070

BANK_SAVINGS_CAP = 4000  # سقف سپرده‌گذاری در بانک آترامنتوم (آتر)
BANK_GRACE_PERIOD_HOURS = 24  # مهلت طلایی پرداخت اقساط قبل از جریمه دیرکرد
IRAN_TZ = timezone(timedelta(hours=3, minutes=30))  # UTC+3:30

# قفل هم‌روندی ناهمگام برای ایمن‌سازی تراکنش‌های مالی در برابر Race Condition
# همان قفل سراسری برای تراکنش‌های خزانه/بانک/وام هم استفاده می‌شود تا از قفل‌شدگی متقابل
# (Deadlock) بین چند قفل مجزا جلوگیری شود؛ تمام عملیات مالی این ماژول‌ها هم زیر همین قفل انجام می‌گیرند.
db_lock = asyncio.Lock()


# --- وب‌سرور سبک برای ساخت Web Service در Render ---
async def handle_health_check(request):
    return web.Response(text="Atr Bank Bot is Running Successfully!")


async def start_dummy_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"🌐 Dummy Web Server started on port {port}")


class TxForm(StatesGroup):
    waiting_for_to_user = State()
    waiting_for_amount = State()
    waiting_for_confirm = State()


class AdminConfirmForm(StatesGroup):
    waiting_for_confirm = State()
    waiting_for_frozen_ack = State()


class ResetForm(StatesGroup):
    waiting_for_confirm = State()


class TreasuryConfirmForm(StatesGroup):
    waiting_for_confirm = State()
    waiting_for_frozen_ack = State()


class OpsConfirmForm(StatesGroup):
    """تأییدیه دو مرحله‌ای (پیش‌نمایش + تأیید/لغو) برای دستورات فروشگاه و مدیریت گروه‌ها."""
    waiting_for_confirm = State()


# --- FSM States جدید برای بخش فروشگاه و پستی ---
class AddProductForm(StatesGroup):
    waiting_for_photo = State()
    waiting_for_title = State()
    waiting_for_description = State()
    waiting_for_price = State()
    waiting_for_stock_type = State()
    waiting_for_stock = State()
    waiting_for_needs_courier = State()


class ProductEditForm(StatesGroup):
    waiting_for_new_price = State()
    waiting_for_new_stock = State()


class RequestShopForm(StatesGroup):
    waiting_for_channel = State()


# --- FSM States جدید برای بانک آترامنتوم و وام پویا ---
class BankForm(StatesGroup):
    waiting_for_deposit_amount = State()
    waiting_for_withdraw_amount = State()


class LoanForm(StatesGroup):
    waiting_for_amount = State()
    waiting_for_installments = State()
    waiting_for_method = State()
    waiting_for_guarantor = State()
    waiting_for_guarantor_confirm = State()


# =====================================================================================
# ⏳ سیستم لغو خودکار عملیات‌های نیمه‌کاره (Input Timeout Auto-Cancel)
# =====================================================================================
# اگر کاربر وارد یک فرآیندی شد که نیاز به دریافت ورودی (پیام یا انتخاب دکمه) دارد، اما
# ظرف ۱ دقیقه ادامه نداد: عملیات به‌صورت خودکار لغو، State مربوطه به‌طور کامل پاک و هیچ
# اطلاعات ناقصی ذخیره نمی‌شود؛ کاربر می‌تواند بدون هیچ محدودیتی فرآیند را از نو آغاز کند.
INPUT_TIMEOUT_SECONDS = 60
_pending_input_timeouts: dict[tuple[int, int], asyncio.Task] = {}


def _timeout_key(chat_id: int, user_id: int) -> tuple[int, int]:
    return (chat_id, user_id)


def cancel_input_timeout(chat_id: int, user_id: int) -> None:
    """لغو تایمر عدم‌فعالیت در صورتی که کاربر ورودی معتبر ارسال کرده یا فرآیند پایان یافته باشد."""
    task = _pending_input_timeouts.pop(_timeout_key(chat_id, user_id), None)
    if task and not task.done():
        task.cancel()


async def _run_input_timeout(state: FSMContext, chat_id: int, user_id: int, expected_state, on_timeout) -> None:
    try:
        await asyncio.sleep(INPUT_TIMEOUT_SECONDS)
        current_state = await state.get_state()
        if current_state != expected_state:
            return  # کاربر قبلاً ادامه داده یا فرآیند به‌طریق دیگری پایان یافته است
        await state.clear()
        try:
            await on_timeout()
        except Exception as e:
            logging.error(f"❌ خطا در لغو خودکار عملیات نیمه‌کاره: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        _pending_input_timeouts.pop(_timeout_key(chat_id, user_id), None)


def schedule_input_timeout(state: FSMContext, chat_id: int, user_id: int, expected_state, on_timeout) -> None:
    """
    فعال‌سازی تایمر لغو خودکار برای یک State در حال انتظار ورودی.
    expected_state: خروجی state.get_state() بلافاصله پس از set_state (رشته نام State).
    on_timeout: تابع async بدون آرگومان که هنگام وقوع Timeout اجرا می‌شود (ادیت/ارسال پیام اطلاع‌رسانی).
    """
    cancel_input_timeout(chat_id, user_id)
    task = asyncio.create_task(_run_input_timeout(state, chat_id, user_id, expected_state, on_timeout))
    _pending_input_timeouts[_timeout_key(chat_id, user_id)] = task


async def _default_timeout_notice(bot: Bot, chat_id: int, message_id) -> None:
    """پیام پیش‌فرض اطلاع‌رسانی لغو خودکار برای فرآیندهایی که پیام اصلی ثابتی برای Edit ندارند."""
    text = (
        "⏳ <b>عملیات لغو شد</b>\n\n"
        "به دلیل عدم دریافت پاسخ در بازه ۱ دقیقه، این عملیات به‌صورت خودکار لغو شد.\n"
        "می‌توانید فرآیند را مجدداً از ابتدا آغاز کنید."
    )
    if message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML")
            return
        except Exception:
            pass
    try:
        await bot.send_message(chat_id, text, parse_mode="HTML")
    except Exception:
        pass


# =====================================================================================
# ⚠️ ابزارهای مشترک هشدار «مقصد فریز است» برای دستورات مالی تک‌نفره و گروهی
# (بدون رد کردن عملیات؛ صرفاً یک مرحله هشدار/تأیید اضافه اضافه می‌شود)
# =====================================================================================
FROZEN_WARN_PAGE_SIZE = 10


def _frozen_target_card_text(u, user_id: int) -> str:
    """کارت اطلاعاتی هشدار برای یک مقصد فریزشده (نام، یوزرنیم، آیدی، گروه)."""
    safe_name = html.escape(u["full_name"] or "ناشناس")
    safe_username = f"@{html.escape(u['username'])}" if u["username"] else "—"
    safe_group = html.escape(u["group_name"] or "Default")
    return (
        "⚠️ <b>هشدار: حساب مقصد فریز (Freeze) است!</b>\n\n"
        f"👤 نام: <b>{safe_name}</b>\n"
        f"🔗 یوزرنیم: {safe_username}\n"
        f"🆔 آیدی: <code>{user_id}</code>\n"
        f"🏷 گروه: {safe_group}\n\n"
        "آیا با وجود فریز بودن حساب مقصد، مایل به ادامه عملیات هستید؟"
    )


def _group_frozen_warning_page(normal_count: int, frozen_members, page: int, g_name: str, per_amount: int):
    """
    متن و کیبورد صفحه هشدار فریز عملیات گروهی را می‌سازد: آمار کامل (تعداد فعال/فریز،
    گروه، مبلغ فردی، مجموع هر دو حالت) + لیست صفحه‌بندی‌شده اعضای فریز (حداکثر ۱۰ نفر
    در هر صفحه) + دو دکمه انتخاب مسیر اجرا (شامل کردن یا رد کردن فریزها).
    """
    frozen_count = len(frozen_members)
    total_pages = max(1, math.ceil(frozen_count / FROZEN_WARN_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * FROZEN_WARN_PAGE_SIZE
    page_items = frozen_members[start:start + FROZEN_WARN_PAGE_SIZE]

    safe_g_name = html.escape(g_name)
    total_all = (normal_count + frozen_count) * per_amount
    total_normal_only = normal_count * per_amount

    text = (
        f"⚠️ <b>هشدار: در گروه «{safe_g_name}» کاربر فریز وجود دارد</b>\n\n"
        f"👤 تعداد اعضای فعال (بدون فریز): <code>{normal_count}</code>\n"
        f"❄️ تعداد اعضای فریز: <code>{frozen_count}</code>\n"
        f"👥 گروه/دسته‌بندی: <b>{safe_g_name}</b>\n"
        f"💰 مبلغ فردی: <code>₳ {per_amount}</code>\n"
        f"🧮 مجموع (در صورت شامل کردن فریزها): <code>₳ {total_all}</code>\n"
        f"🧮 مجموع (در صورت رد کردن فریزها): <code>₳ {total_normal_only}</code>\n\n"
        f"❄️ <b>لیست کاربران فریز:</b>\n"
    )
    for m in page_items:
        safe_name = html.escape(m["full_name"] or "ناشناس")
        safe_username = f"@{html.escape(m['username'])}" if m["username"] else "—"
        text += f"👤 <b>{safe_name}</b> | {safe_username} | <code>{m['user_id']}</code>\n"
    if total_pages > 1:
        text += f"\n📄 صفحه {page + 1} از {total_pages}"

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ صفحه قبل", callback_data=f"gfwarn_page_{page - 1}"))
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton(text=f"📄 {page + 1}/{total_pages}", callback_data="gfwarn_noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="صفحه بعد ➡️", callback_data=f"gfwarn_page_{page + 1}"))

    rows = []
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="💰 انتقال پول به کاربران فریز‌شده", callback_data="gfwarn_include")])
    rows.append([InlineKeyboardButton(text="🚫 رد کردن کاربران فریز‌شده", callback_data="gfwarn_exclude")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


class AntiSpamMiddleware(BaseMiddleware):

    def __init__(self, limit=1.5):
        self.limit = limit
        self.users = {}
        super().__init__()

    async def __call__(self, handler, event, data):
        user_id = event.from_user.id
        now = datetime.now().timestamp()
        if user_id in self.users:
            if now - self.users[user_id] < self.limit:
                if isinstance(event, Message):
                    await event.reply(
                        "⚠️ لطفاً از اسپم خودداری کنید! کمی آرام‌تر."
                    )
                return
        self.users[user_id] = now
        return await handler(event, data)


# --- ساخت دیتابیس و جدول‌ها ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance INTEGER DEFAULT 0, 
                is_admin BOOLEAN DEFAULT FALSE,
                is_frozen BOOLEAN DEFAULT FALSE,
                group_name TEXT DEFAULT 'Default'
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                tx_id TEXT PRIMARY KEY,
                timestamp TEXT,
                from_user INTEGER,
                to_user INTEGER,
                amount INTEGER,
                reason TEXT,
                status TEXT DEFAULT 'SUCCESS'
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_name TEXT PRIMARY KEY
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS group_links (
                code TEXT PRIMARY KEY,
                group_name TEXT,
                expires_at TEXT,
                created_at TEXT,
                FOREIGN KEY(group_name) REFERENCES groups(group_name) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS super_admins (
                user_id INTEGER PRIMARY KEY
            )
        """)

        # --- جداول جدید ساختار فروشگاهی و پستی ---
        await db.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                val REAL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS shops (
                shop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                channel_id TEXT,
                channel_title TEXT,
                status TEXT DEFAULT 'PENDING'
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id INTEGER,
                photo_id TEXT,
                title TEXT,
                description TEXT,
                price INTEGER,
                stock_type TEXT, -- 'SINGLE', 'LIMITED', 'UNLIMITED'
                stock_qty INTEGER DEFAULT 1,
                needs_courier BOOLEAN DEFAULT FALSE,
                channel_msg_id INTEGER
            )
        """)

        # --- 🏷 مهاجرت (Migration) سیستم کدگذاری یکتای محصولات ---
        # (بدون حذف یا تغییر هیچ‌کدام از ستون‌ها یا داده‌های قبلی محصولات)
        try:
            await db.execute("ALTER TABLE products ADD COLUMN product_code TEXT")
        except Exception:
            pass  # ستون از قبل وجود دارد
        try:
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code)"
            )
        except Exception:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS couriers (
                user_id INTEGER PRIMARY KEY
            )
        """)

        # --- 🏦 مهاجرت (Migration) ستون‌های بانک آترامنتوم و وام پویا در جدول users ---
        # (بدون حذف هیچ‌کدام از داده‌ها یا ستون‌های قبلی)
        for col_name, col_type in [
            ("bank_savings", "INTEGER DEFAULT 0"),
            ("frozen_balance", "INTEGER DEFAULT 0"),
            ("last_bank_claim", "DATETIME DEFAULT NULL"),
            ("last_daily_profit", "INTEGER DEFAULT 0"),
        ]:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # ستون از قبل وجود دارد
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code_10 TEXT UNIQUE,
                buyer_id INTEGER,
                shop_id INTEGER,
                product_id INTEGER,
                price INTEGER,
                courier_fee INTEGER,
                courier_id INTEGER DEFAULT 0,
                status TEXT DEFAULT 'PENDING', -- 'PENDING', 'DISPATCHED', 'DELIVERED'
                product_title TEXT,
                product_desc TEXT,
                product_photo_id TEXT
            )
        """)

        # --- 📦 مهاجرت (Migration) ستون تاریخ ثبت سفارش، برای نمایش «تاریخ دریافت» در /my_assets ---
        try:
            await db.execute("ALTER TABLE orders ADD COLUMN created_at TEXT")
        except Exception:
            pass  # ستون از قبل وجود دارد

        # --- 🏛 جداول جدید سیستم مالی: بانک آترامنتوم و وام پویا ---
        await db.execute("""
            CREATE TABLE IF NOT EXISTS loans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                guarantor_id INTEGER,
                total_amount INTEGER,
                interest_rate REAL,
                total_repayment INTEGER,
                installments_count INTEGER,
                status TEXT,          -- PENDING_GUARANTOR, PENDING_GUARANTOR_FINAL, PENDING_ADMIN, ACTIVE, REJECTED, CANCELLED, PAID, FAILED
                loan_type TEXT,       -- COLLATERAL, GUARANTOR
                created_at TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS loan_installments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                loan_id INTEGER,
                installment_number INTEGER,
                amount INTEGER,
                due_date TIMESTAMP,
                status TEXT,          -- PENDING, PAID, DEFAULTED
                paid_at TIMESTAMP
            )
        """)
        # ستون‌های کمکی برای محاسبه صحیح جریمه دیرکرد بدون از دست دادن مبلغ پایه قسط
        for col_name, col_type in [
            ("base_amount", "INTEGER"),
            ("penalty_amount", "INTEGER DEFAULT 0"),
            ("last_reminder_stage", "TEXT DEFAULT ''"),
        ]:
            try:
                await db.execute(f"ALTER TABLE loan_installments ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # ستون از قبل وجود دارد

        # --- 🔒 مهاجرت (Migration) ستون مبلغ وثیقه ثابت‌شده وام ---
        # طبق سیستم جدید وثیقه، هیچ مبلغی در لحظه ثبت درخواست قفل نمی‌شود؛ مبلغ وثیقه فقط
        # محاسبه و روی خود وام ذخیره می‌شود تا اگر سوپرادمین بعداً نرخ وثیقه را تغییر دهد،
        # وام‌های در حال بررسی/فعال قبلی هنگام تأیید یا آزادسازی دچار مغایرت نشوند.
        for col_name, col_type in [
            ("collateral_amount", "INTEGER DEFAULT 0"),
        ]:
            try:
                await db.execute(f"ALTER TABLE loans ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # ستون از قبل وجود دارد

        # --- 🤝 مهاجرت (Migration) ستون‌های وثیقه وام ضامنی (گیرنده و ضامن) ---
        # طبق سیستم جدید وام ضامنی، وثیقه هر دو طرف (گیرنده و ضامن) در لحظه ثبت درخواست
        # محاسبه و روی خود وام ذخیره می‌شود، اما قفل واقعی (frozen_balance) فقط در لحظه
        # تأیید نهایی سوپرادمین انجام می‌گیرد؛ دقیقاً مطابق همان منطق وام وثیقه‌ای.
        for col_name, col_type in [
            ("borrower_collateral_amount", "INTEGER DEFAULT 0"),
            ("guarantor_collateral_amount", "INTEGER DEFAULT 0"),
        ]:
            try:
                await db.execute(f"ALTER TABLE loans ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # ستون از قبل وجود دارد

        # --- مهاجرت (Migration) ستون‌های اسنپ‌شات محصول برای دیتابیس‌های قدیمی ---
        # این ستون‌ها باعث می‌شوند حذف فروشگاه/محصول، اطلاعات سفارش‌های قبلی خریداران را از بین نبرد
        for col_name, col_type in [
            ("product_title", "TEXT"),
            ("product_desc", "TEXT"),
            ("product_photo_id", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE orders ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # ستون از قبل وجود دارد

        # درج تنظیمات پیش‌فرض نرخ مالیات و نرخ پست در صورت عدم وجود
        default_settings = [
            ("shop_seller_pct", 51.0),
            ("shop_bank_pct", 40.0),
            ("shop_burn_pct", 9.0),
            ("courier_pct", 61.0),
            ("courier_bank_pct", 30.0),
            ("courier_burn_pct", 9.0),
            ("tier1_pct", 8.0),   # تا 99 آتر
            ("tier2_pct", 10.0),  # 100 تا 999 آتر
            ("tier3_pct", 12.0),  # 1000 آتر به بالا

            # --- 🏦 تنظیمات پیش‌فرض بانک آترامنتوم ---
            ("bank_daily_rate", 1.23),   # درصد سود روزانه پیش‌فرض
            # سهم سود روزانه بانک که از خزانه مرکزی کسر می‌شود (باقی‌مانده به‌صورت خلق پول تأمین می‌گردد)
            ("bank_treasury_profit_pct", 45.0),

            # --- 💳 تنظیمات پیش‌فرض وام پویا ---
            ("min_loan_amount", 2000),
            ("max_loan_amount", 25000),
            ("min_loan_interest", 2.0),
            ("max_loan_interest", 5.0),
            ("allowed_installments", "2,3"),
            ("collateral_rate", 0.17),
            ("required_balance_rate", 0.40),
            ("late_penalty_rate", 0.0085),
            ("loan_guarantor_balance_rate_borrower", 0.20),
            ("loan_guarantor_balance_rate_guarantor", 0.20),
            ("loan_guarantor_collateral_rate_borrower", 0.08),
            ("loan_guarantor_collateral_rate_guarantor", 0.09),
        ]
        for key, val in default_settings:
            await db.execute("INSERT OR IGNORE INTO system_settings (key, val) VALUES (?, ?)", (key, val))

        # فقط گروه پیش‌فرض Default ثبت می‌شود
        for g in ["Default"]:
            await db.execute(
                "INSERT OR IGNORE INTO groups (group_name) VALUES (?)", (g,)
            )

        # ثبت سوپرادمین‌های پایه
        for sa_id in [SUPER_ADMIN_1, SUPER_ADMIN_2]:
            await db.execute(
                "INSERT OR IGNORE INTO super_admins (user_id) VALUES (?)", (sa_id,)
            )

        # --- 🏛 ثبت حساب خزانه مرکزی در جدول users (در صورت عدم وجود) ---
        await db.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, full_name, balance)
            VALUES (?, ?, ?, 0)
            """,
            (TREASURY_USER_ID, "Treasury", "🏛 خزانه مرکزی آترامنتوم"),
        )

        await db.commit()

        # بارگذاری لیست سوپرادمین‌ها از دیتابیس
        await load_super_admins(db)


async def load_super_admins(db=None):
    """بارگذاری لیست سوپرادمین‌ها از دیتابیس به متغیر سراسری"""
    global SUPER_ADMINS
    close_after = False
    if db is None:
        db = await aiosqlite.connect(DB_PATH)
        close_after = True
    try:
        async with db.execute("SELECT user_id FROM super_admins") as cur:
            rows = await cur.fetchall()
            SUPER_ADMINS = {row[0] for row in rows}
            # همیشه دو آیدی پایه را نگه دار
            SUPER_ADMINS.add(SUPER_ADMIN_1)
            SUPER_ADMINS.add(SUPER_ADMIN_2)
    finally:
        if close_after:
            await db.close()


async def sync_user(user_id: int, username: str, full_name: str = "Unknown"):
    async with aiosqlite.connect(DB_PATH) as db:
        username_clean = username if username else "بدون آیدی"
        full_name_clean = full_name if full_name else "ناشناس"
        await db.execute(
            """
            INSERT INTO users (user_id, username, full_name, balance) VALUES (?, ?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET username = ?, full_name = ?
        """,
            (user_id, username_clean, full_name_clean, username_clean, full_name_clean),
        )
        await db.commit()


async def get_user_data(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT balance, is_admin, is_frozen, username, full_name, group_name, "
            "bank_savings, frozen_balance, last_bank_claim, last_daily_profit FROM"
            " users WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            return await cursor.fetchone()


async def get_setting(key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT val FROM system_settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0.0


async def set_setting(key: str, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO system_settings (key, val) VALUES (?, ?)", (key, value)
        )
        await db.commit()


# --- 🏛 توابع کمکی خزانه مرکزی (Central Treasury) ---
# تمام توابع زیر باید درون یک کانکشن دیتابیس باز (db) و ترجیحاً زیر db_lock فراخوانی شوند
# تا از race condition و منفی شدن موجودی خزانه جلوگیری شود. هیچ‌کدام commit داخلی انجام نمی‌دهند
# مگر آن‌که صراحتاً commit شوند توسط فراخواننده، تا بخشی از یک تراکنش بزرگ‌تر باقی بمانند.

async def get_treasury_balance() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT balance FROM users WHERE user_id = ?", (TREASURY_USER_ID,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def treasury_credit(db, amount: int, reason: str, related_user: int = 0):
    """واریز درآمد به خزانه مرکزی (فروشگاه، پستچی و سایر ورودی‌های سیستم)."""
    if amount <= 0:
        return
    await db.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (amount, TREASURY_USER_ID),
    )
    tx_id = f"TRZ-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
    await db.execute(
        "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            tx_id,
            datetime.now(timezone.utc).isoformat(),
            related_user,
            TREASURY_USER_ID,
            amount,
            reason,
        ),
    )


async def treasury_debit(db, amount: int, reason: str, related_user: int = 0) -> bool:
    """
    کسر از خزانه مرکزی (سود بانکی، اصل وام و ...).
    قانون عدم خلق پول: در صورت ناکافی بودن موجودی خزانه، هیچ تغییری اعمال نشده و False برگردانده می‌شود.
    """
    if amount <= 0:
        return True
    async with db.execute(
        "SELECT balance FROM users WHERE user_id = ?", (TREASURY_USER_ID,)
    ) as cur:
        row = await cur.fetchone()
    treasury_balance = row[0] if row else 0
    if treasury_balance < amount:
        return False
    await db.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (amount, TREASURY_USER_ID),
    )
    tx_id = f"TRZ-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
    await db.execute(
        "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            tx_id,
            datetime.now(timezone.utc).isoformat(),
            TREASURY_USER_ID,
            related_user,
            amount,
            reason,
        ),
    )
    return True


user_router = Router()
admin_router = Router()
shop_router = Router()

user_router.message.middleware(AntiSpamMiddleware())
shop_router.message.middleware(AntiSpamMiddleware())


# --- دستور همگانی /cancel ---
# این هندلر عمداً بلافاصله بعد از تعریف روترها و قبل از هر هندلر دیگری ثبت می‌شود
# تا در هر مرحله از هر فرآیندی (انتقال آتر، ساخت فروشگاه، ثبت محصول، ریست و ...)
# با اولویت بالاتر از هندلرهای مخصوص هر state اجرا شده و آن عملیات را لغو کند.
@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return await message.reply("ℹ️ در حال حاضر هیچ عملیات درحال‌انجامی برای لغو کردن وجود ندارد.")
    cancel_input_timeout(message.chat.id, message.from_user.id)
    await state.clear()
    await message.reply("✅ عملیات جاری با موفقیت لغو شد. می‌توانید از ابتدا شروع کنید.")


async def check_admin_filter(message: Message) -> bool:
    if is_super_admin(message.from_user.id):
        return True
    u = await get_user_data(message.from_user.id)
    return u and u["is_admin"]


def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS


def is_private(message: Message) -> bool:
    return message.chat.type == "private"


# --- ایجاد پوشه بکاپ ---
os.makedirs(BACKUP_DIR, exist_ok=True)


def create_zip_backup(prefix="manual"):
    if not os.path.exists(DB_PATH):
        return None
    timestamp = datetime.now(IRAN_TZ).strftime("%Y%m%d_%H%M%S")
    zip_name = f"{prefix}_backup_{timestamp}.zip"
    zip_path = os.path.join(BACKUP_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(DB_PATH, arcname="atr_bank.db")
    return zip_path


# --- سیستم بکاپ‌گیری و بازیابی خودکار تلگرامی ---
async def restore_db_from_telegram(bot: Bot):
    if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0:
        logging.info("✅ فایل دیتابیس موجود است.")
        return

    logging.info("⚠️ دیتابیس روی سرور یافت نشد! در حال دانلود آخرین بکاپ از کانال تلگرام...")
    try:
        async for message in bot.get_chat_history(BACKUP_CHANNEL_ID, limit=15):
            if message.document and message.document.file_name.endswith(".db"):
                file_info = await bot.get_file(message.document.file_id)
                await bot.download_file(file_info.file_path, DB_PATH)
                logging.info("🎉 دیتابیس با موفقیت از کانال تلگرام بازیابی شد!")
                return
        logging.info("ℹ️ هیچ فایل بکاپی در کانال یافت نشد. دیتابیس جدید ساخته خواهد شد.")
    except Exception as e:
        logging.error(f"❌ خطا در بازیابی دیتابیس از تلگرام: {e}")


async def auto_backup_loop(bot: Bot):
    while True:
        await asyncio.sleep(900)  # ارسال بکاپ خودکار هر ۱۵ دقیقه
        if os.path.exists(DB_PATH):
            try:
                await bot.send_document(
                    chat_id=BACKUP_CHANNEL_ID,
                    document=FSInputFile(DB_PATH),
                    caption=f"<b>📦 بکاپ خودکار دیتابیس</b>\n⏰ {datetime.now(IRAN_TZ).strftime('%Y-%m-%d %H:%M:%S')} (به وقت ایران)",
                    parse_mode="HTML"
                )
                logging.info("✅ بکاپ خودکار به کانال تلگرام ارسال شد.")
            except Exception as e:
                logging.error(f"❌ خطا در ارسال بکاپ خودکار به تلگرام: {e}")


# --- ساخت صفحه کاربران ---
async def get_users_page(page: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) as total FROM users") as cur:
            total_users = (await cur.fetchone())["total"]

        total_pages = max(1, (total_users + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * USERS_PER_PAGE

        async with db.execute(
            "SELECT user_id, username, full_name, balance, group_name, is_frozen, is_admin FROM users LIMIT ? OFFSET ?",
            (USERS_PER_PAGE, offset),
        ) as cur:
            users = await cur.fetchall()

    text = f"👥 <b>لیست کاربران (صفحه {page} از {total_pages})</b>\n"
    text += f"📊 کل کاربران: <code>{total_users}</code> نفر\n\n"

    for idx, u in enumerate(users, start=offset + 1):
        safe_full_name = html.escape(u['full_name'] or 'ناشناس')
        safe_group_name = html.escape(u['group_name'] or 'Default')
        text += (
            f"<b>{idx}. {safe_full_name}</b>\n"
            f"شماره حساب: <code>{u['user_id']}</code>\n"
            f"موجودی: <code>₳ {u['balance']}</code>\n"
            f"گروه: <b>{safe_group_name}</b>\n"
            f"------------------------------\n"
        )

    buttons = []
    nav_row = []

    if page > 1:
        nav_row.append(InlineKeyboardButton(text="➡️ قبلی", callback_data=f"users_page_{page - 1}"))
    
    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="users_noop"))

    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="بعدی ⬅️", callback_data=f"users_page_{page + 1}"))

    if nav_row:
        buttons.append(nav_row)

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    return text, kb


# --- ساخت صفحه کاربران فریزشده (مطابق دقیق ساختار /users؛ فقط فیلتر is_frozen و فیلدهای نمایشی متفاوت) ---
async def get_frozen_users_page(page: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) as total FROM users WHERE is_frozen = 1") as cur:
            total_users = (await cur.fetchone())["total"]

        total_pages = max(1, (total_users + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * USERS_PER_PAGE

        async with db.execute(
            "SELECT user_id, username, full_name, group_name FROM users WHERE is_frozen = 1 LIMIT ? OFFSET ?",
            (USERS_PER_PAGE, offset),
        ) as cur:
            users = await cur.fetchall()

    text = f"🥶 <b>لیست کاربران فریزشده (صفحه {page} از {total_pages})</b>\n"
    text += f"📊 کل کاربران فریزشده: <code>{total_users}</code> نفر\n\n"

    for idx, u in enumerate(users, start=offset + 1):
        safe_full_name = html.escape(u['full_name'] or 'ناشناس')
        safe_group_name = html.escape(u['group_name'] or 'Default')
        safe_username = f"@{html.escape(u['username'])}" if u['username'] else "ندارد"
        text += (
            f"<b>{idx}. {safe_full_name}</b>\n"
            f"آیدی عددی: <code>{u['user_id']}</code>\n"
            f"یوزرنیم: {safe_username}\n"
            f"گروه: <b>{safe_group_name}</b>\n"
            f"------------------------------\n"
        )

    buttons = []
    nav_row = []

    if page > 1:
        nav_row.append(InlineKeyboardButton(text="➡️ قبلی", callback_data=f"frozen_users_page_{page - 1}"))

    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="frozen_users_noop"))

    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="بعدی ⬅️", callback_data=f"frozen_users_page_{page + 1}"))

    if nav_row:
        buttons.append(nav_row)

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    return text, kb


# --- دستورات کاربران ---


@user_router.message(Command("start"))
async def cmd_start(message: Message):
    if not is_private(message):
        return

    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            already_exists = await cur.fetchone() is not None

    await sync_user(user_id, username, full_name)

    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        payload = args[1].strip()

        if payload.upper().startswith("G"):
            if payload.startswith("G_") or payload.startswith("g_"):
                code = payload[2:].strip()
            else:
                code = payload[1:].strip()

            if not code:
                return await message.reply("❌ کد دعوت نامعتبر است.")

            try:
                async with db_lock:
                    async with aiosqlite.connect(DB_PATH) as db:
                        db.row_factory = aiosqlite.Row

                        link_data = None
                        try:
                            async with db.execute(
                                "SELECT group_name, expires_at FROM group_links WHERE code = ?",
                                (code,),
                            ) as cur:
                                link_data = await cur.fetchone()
                        except Exception:
                            async with db.execute(
                                "SELECT group_name FROM group_links WHERE code = ?",
                                (code,),
                            ) as cur:
                                row = await cur.fetchone()
                                if row:
                                    link_data = {
                                        "group_name": row[0] if not hasattr(row, "keys") else row["group_name"],
                                        "expires_at": None,
                                    }

                        if not link_data:
                            safe_code = html.escape(code)
                            return await message.reply(
                                f"❌ لینک دعوت نامعتبر است.\n"
                                f"کد دریافتی: <code>{safe_code}</code>",
                                parse_mode="HTML",
                            )

                        if hasattr(link_data, "keys"):
                            group_name = link_data["group_name"]
                            expires_val = link_data["expires_at"] if "expires_at" in link_data.keys() else None
                        else:
                            group_name = link_data.get("group_name")
                            expires_val = link_data.get("expires_at")

                        if expires_val:
                            try:
                                expires = datetime.fromisoformat(str(expires_val))
                                if datetime.now(timezone.utc) > expires:
                                    return await message.reply("❌ لینک دعوت منقضی شده است.")
                            except Exception:
                                pass

                        u = await get_user_data(user_id)
                        if u and u["is_frozen"]:
                            return await message.reply("❌ حساب شما فریز است.")

                        await db.execute(
                            "UPDATE users SET group_name = ? WHERE user_id = ?",
                            (group_name, user_id),
                        )
                        await db.commit()

                        safe_g_name = html.escape(group_name)
                        return await message.reply(
                            f"🎉 شما با موفقیت عضو گروه <b>{safe_g_name}</b> شدید.",
                            parse_mode="HTML",
                        )
            except Exception as e:
                logging.error(f"start group link error: {e}")
                safe_err_type = html.escape(type(e).__name__)
                safe_err = html.escape(str(e))
                safe_code = html.escape(code)
                return await message.reply(
                    f"❌ خطا در عضویت گروه:\n<code>{safe_err_type}: {safe_err}</code>\n"
                    f"کد: <code>{safe_code}</code>",
                    parse_mode="HTML",
                )

    if not already_exists:
        await message.reply(
            f"به بانک جادویی Atramentum خوش اومدید.\n"
            f"شماره حساب: <code>{user_id}</code>",
            parse_mode="HTML",
        )
    else:
        await message.reply(
            f"شماره حساب: <code>{user_id}</code>",
            parse_mode="HTML",
        )


def _profile_text(u, user_id: int) -> str:
    status_text = "❄️ فریز شده" if u["is_frozen"] else "🟢 فعال"
    safe_name = html.escape(u['full_name'] or "ناشناس")
    return (
        f"👤 نام: {safe_name}\n"
        f"🆔 شماره حساب: <code>{user_id}</code>\n"
        f"💰 موجودی: <code>₳ {u['balance']}</code>\n"
        f"⚡ وضعیت حساب: {status_text}"
    )


def _profile_main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏦 بانک آترامنتوم", callback_data="prof_bank")],
        [InlineKeyboardButton(text="📦 لیست دارایی‌ها", callback_data="prof_assets")],
        [InlineKeyboardButton(text="❓ راهنمایی", callback_data="prof_help")],
    ])


def _profile_help_back_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔙 برگشت به پروفایل", callback_data="prof_home"),
    ]])


@user_router.message(Command("profile"))
@user_router.message(Command("balance"))
@user_router.message(F.text == "پروفایل")
async def cmd_profile(message: Message):
    user_id = message.from_user.id
    await sync_user(user_id, message.from_user.username, message.from_user.full_name)
    u = await get_user_data(user_id)

    if not u:
        return await message.reply("❌ حساب شما یافت نشد.")

    await message.reply(
        _profile_text(u, user_id),
        reply_markup=_profile_main_buttons(),
        parse_mode="HTML",
    )


@user_router.callback_query(F.data == "prof_home")
async def cb_prof_home(callback: CallbackQuery):
    """کلید «🔙 برگشت به پروفایل»: پیام را ویرایش کرده و به حالت اول صفحه /profile بازمی‌گرداند."""
    user_id = callback.from_user.id
    u = await get_user_data(user_id)
    if not u:
        await callback.answer("❌ حساب شما یافت نشد.", show_alert=True)
        return
    try:
        await callback.message.edit_text(
            _profile_text(u, user_id),
            reply_markup=_profile_main_buttons(),
            parse_mode="HTML",
        )
    except Exception:
        pass
    await callback.answer()


@user_router.callback_query(F.data == "prof_help")
async def cb_prof_help(callback: CallbackQuery):
    """کلید «❓ راهنمایی»: در گروه فقط لیست دستورات عمومی (بدون درنظرگرفتن سطح دسترسی یا ادمین
    بودن کاربر) و در پیوی راهنمای کامل و شخصی‌سازی‌شده نمایش داده می‌شود."""
    group_only = not is_private(callback.message)
    txt = await _build_help_text(callback.from_user.id, group_only=group_only)
    try:
        await callback.message.edit_text(
            txt,
            reply_markup=_profile_help_back_buttons(),
            parse_mode="HTML",
        )
    except Exception:
        pass
    await callback.answer()


@user_router.callback_query(F.data == "prof_bank")
async def cb_prof_bank(callback: CallbackQuery):
    """کلید «🏦 بانک آترامنتوم»: پیام پروفایل را به ساختار بانک تبدیل می‌کند — در پیوی نمای کامل
    حساب بانکی (موجودی، سپرده، سود، وام) و در گروه/سوپرگروه نمای خلاصه؛ در هر دو حالت دکمه‌های
    عملیاتی اصلی بانک (واریز/برداشت/وام/مدیریت) کاملاً فعال هستند و دکمه «🔙 برگشت به پروفایل»
    در انتهای کیبورد قرار می‌گیرد."""
    user_id = callback.from_user.id
    panel_type = "full" if is_private(callback.message) else "panel"
    rendered = await _bank_render(user_id, panel_type, with_back=True)
    if not rendered:
        await callback.answer("❌ حساب شما یافت نشد.", show_alert=True)
        return
    text, kb = rendered
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


PROF_ASSETS_PAGE_SIZE = 10


async def _fetch_profile_assets(user_id: int):
    """دارایی‌های قطعی (سفارش‌های تحویل‌شده) و سفارش‌های در حال جریان (در انتظار ارسال یا در حال
    ارسال) کاربر را واکشی می‌کند. طبق اولویت‌بندی درخواستی، دارایی‌های قطعی همیشه در صدر لیست
    قرار می‌گیرند و پس از آن‌ها سفارش‌های در حال جریان می‌آیند."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? AND status = 'DELIVERED' ORDER BY order_id DESC",
            (user_id,),
        ) as cur:
            delivered = await cur.fetchall()
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? AND status IN ('PENDING', 'DISPATCHED') ORDER BY order_id DESC",
            (user_id,),
        ) as cur2:
            in_progress = await cur2.fetchall()
    return list(delivered) + list(in_progress), len(delivered)


def _render_profile_assets_page(items, delivered_count: int, page: int):
    """متن شماره‌گذاری‌شده (۱۰تایی) و کیبورد دکمه‌های شماره‌ای ۲ستونه + ناوبری صفحه را می‌سازد."""
    total = len(items)
    total_pages = max(1, math.ceil(total / PROF_ASSETS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * PROF_ASSETS_PAGE_SIZE
    page_items = items[start:start + PROF_ASSETS_PAGE_SIZE]

    txt = f"📦 <b>لیست دارایی‌ها و سفارش‌ها (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"مجموع موارد: <code>{total}</code>\n\n"

    if not page_items:
        txt += "ℹ️ هیچ دارایی یا سفارشی یافت نشد."

    item_buttons = []
    for offset, o in enumerate(page_items):
        idx = offset + 1
        is_asset = (start + offset) < delivered_count
        emoji = "🟢" if is_asset else "🟡"
        safe_title = html.escape(o["product_title"] or "محصول حذف‌شده")
        txt += f"{idx}. {emoji} {safe_title}\n"
        item_buttons.append(
            InlineKeyboardButton(text=f"محصول {idx}", callback_data=f"av_{o['order_id']}")
        )

    # چیدمان منظم ۲ ستونه برای دکمه‌های شماره‌ای
    kb_rows = [item_buttons[i:i + 2] for i in range(0, len(item_buttons), 2)]

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️ صفحه قبل", callback_data=f"ap_{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 صفحه {page + 1} از {total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="صفحه بعد ▶️", callback_data=f"ap_{page + 1}"))
    kb_rows.append(nav_row)

    kb_rows.append([InlineKeyboardButton(text="🔙 برگشت به پروفایل", callback_data="prof_home")])
    return txt, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _safe_show_text(callback: CallbackQuery, text: str, kb) -> None:
    """پیام فعلی را با متن/کیبورد داده‌شده به‌روزرسانی می‌کند؛ اگر پیام فعلی رسانه‌ای (مثلاً صفحه
    جزئیات محصول با عکس) باشد و edit_text ممکن نباشد، پیام قبلی حذف و پیام متنی جدید ارسال
    می‌شود تا ناوبری بین لیست دارایی‌ها و جزئیات محصول بدون باگ کار کند."""
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        return
    except Exception:
        pass
    try:
        await callback.message.delete()
    except Exception:
        pass
    try:
        await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@user_router.callback_query(F.data == "prof_assets")
async def cb_prof_assets(callback: CallbackQuery):
    """کلید «📦 لیست دارایی‌ها»: پیام پروفایل را به لیست صفحه‌بندی‌شدهٔ دارایی‌های قطعی و
    سفارش‌های در حال جریان کاربر تبدیل می‌کند."""
    items, delivered_count = await _fetch_profile_assets(callback.from_user.id)
    if not items:
        await callback.answer("ℹ️ هیچ دارایی یا سفارشی یافت نشد.", show_alert=True)
        return
    txt, kb = _render_profile_assets_page(items, delivered_count, 0)
    await _safe_show_text(callback, txt, kb)
    await callback.answer()


@user_router.callback_query(F.data.startswith("ap_"))
async def cb_profile_assets_page(callback: CallbackQuery):
    """ناوبری صفحه‌بندی لیست دارایی‌ها (📄 صفحه قبل/بعد)."""
    try:
        page = int(callback.data[len("ap_"):])
    except ValueError:
        await callback.answer()
        return
    items, delivered_count = await _fetch_profile_assets(callback.from_user.id)
    if not items:
        await callback.answer("ℹ️ هیچ دارایی یا سفارشی یافت نشد.", show_alert=True)
        return
    txt, kb = _render_profile_assets_page(items, delivered_count, page)
    await _safe_show_text(callback, txt, kb)
    await callback.answer()


@user_router.callback_query(F.data.startswith("av_"))
async def cb_profile_asset_detail(callback: CallbackQuery):
    """دکمه شماره‌ای هر آیتم: جزئیات کامل همان دارایی/سفارش (عکس، عنوان، توضیحات، قیمت، نام
    فروشگاه و وضعیت ارسال) را نمایش می‌دهد. ابتدا سعی می‌شود پیام فعلی (متن یا عکس) با
    editMessageMedia/editMessageText به‌روزرسانی شود؛ در صورت بروز خطا یا عدم پشتیبانی، پیام
    قبلی حذف و پیام جدید ارسال می‌شود."""
    try:
        order_id = int(callback.data[len("av_"):])
    except ValueError:
        await callback.answer()
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE order_id = ? AND buyer_id = ?", (order_id, callback.from_user.id)
        ) as cur:
            order = await cur.fetchone()
        if not order:
            await callback.answer("❌ این آیتم یافت نشد.", show_alert=True)
            return

        shop_name = "نامشخص"
        async with db.execute("SELECT channel_title FROM shops WHERE shop_id = ?", (order["shop_id"],)) as cur_s:
            shop_row = await cur_s.fetchone()
            if shop_row and shop_row["channel_title"]:
                shop_name = shop_row["channel_title"]

    safe_title = html.escape(order["product_title"] or "محصول حذف‌شده")
    safe_desc = html.escape(order["product_desc"] or "-")
    safe_shop = html.escape(shop_name)
    status_txt = _order_status_label(order["status"])

    caption = (
        f"🛍 <b>{safe_title}</b>\n\n"
        f"📝 توضیحات: {safe_desc}\n"
        f"💰 قیمت: <code>₳ {order['price']}</code>\n"
        f"🏪 فروشگاه: {safe_shop}\n"
        f"🔐 کد پیگیری: <code>{order['code_10']}</code>\n"
        f"⚡ وضعیت: {status_txt}"
    )
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔙 برگشت به لیست دارایی‌ها", callback_data="prof_assets"),
    ]])
    photo_id = order["product_photo_id"]

    edited = False
    try:
        if photo_id:
            # تبدیل پیام (متن یا عکس فعلی) به عکس+کپشن جدید
            await callback.message.edit_media(
                media=InputMediaPhoto(media=photo_id, caption=caption, parse_mode="HTML"),
                reply_markup=back_kb,
            )
        else:
            # محصول عکس ندارد؛ صرفاً در صورتی که پیام فعلی هم متنی باشد قابل ادیت است
            await callback.message.edit_text(caption, reply_markup=back_kb, parse_mode="HTML")
        edited = True
    except Exception:
        edited = False

    if not edited:
        try:
            await callback.message.delete()
        except Exception:
            pass
        try:
            if photo_id:
                await callback.message.answer_photo(photo=photo_id, caption=caption, reply_markup=back_kb, parse_mode="HTML")
            else:
                await callback.message.answer(caption, reply_markup=back_kb, parse_mode="HTML")
        except Exception:
            pass

    await callback.answer()


@user_router.callback_query(F.data == "ignore")
async def cb_ignore_noop(callback: CallbackQuery):
    await callback.answer()


# --- سیستم انتقال آتر (روش‌های درخواستی جدید) ---

async def process_transfer_request(message: Message, state: FSMContext, to_user_id: int, amount: int):
    """تابع کمکی برای شروع تأیید انتقال"""
    from_user = message.from_user.id
    u = await get_user_data(from_user)

    if not u or u["is_frozen"]:
        return await message.reply("❌ حساب شما مسدود (فریز) است.")
    transferable = max(0, u["balance"] - u["frozen_balance"])
    if amount <= 0 or amount > MAX_BALANCE_LIMIT or to_user_id == from_user or transferable < amount:
        return await message.reply("❌ پارامترهای تراکنش یا موجودی نامعتبر است.")

    target = await get_user_data(to_user_id)
    if not target:
        return await message.reply("❌ کاربر مقصد در ربات عضویت ندارد.")
    if target["balance"] + amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ خطا: سقف گنجایش مقصد.")

    target_name = target["full_name"] if target["full_name"] else str(to_user_id)
    safe_target_name = html.escape(target_name)

    await state.update_data(
        to_user_id=to_user_id, amount=amount, target_name=target_name, from_user=from_user
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ تایید", callback_data="tx_yes"),
            InlineKeyboardButton(text="❌ انصراف", callback_data="tx_no"),
        ]]
    )
    confirm_msg = await message.reply(
        f"⚠️ تأییدیه انتقال آتر\n"
        f"دریافت‌کننده: {safe_target_name} (<code>{to_user_id}</code>)\n"
        f"مبلغ: <code>₳ {amount}</code>\n"
        f"آیا مطمئن هستید؟",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.set_state(TxForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, from_user, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@user_router.message(Command("transfer"))
@user_router.message(F.text.startswith("انتقال آتر"))
async def cmd_transfer(message: Message, state: FSMContext):
    from_user = message.from_user.id
    await sync_user(from_user, message.from_user.username, message.from_user.full_name)
    u = await get_user_data(from_user)
    if u and u["is_frozen"]:
        return await message.reply("❌ حساب شما مسدود (فریز) است.")

    text = message.text.strip()
    
    # حذف کلمات ابتدایی دستور
    if text.startswith("/transfer"):
        text = text[len("/transfer"):].strip()
    elif text.startswith("انتقال آتر"):
        text = text[len("انتقال آتر"):].strip()

    # راهنما در صورت عدم وارد کردن آرگومان
    if not text and not message.reply_to_message:
        return await message.reply(
            "📖 <b>روش‌های انتقال آتر:</b>\n\n"
            "1️⃣ <b>انتقال با آیدی عددی:</b>\n"
            "<code>انتقال آتر 123456789 500</code>\n\n"
            "2️⃣ <b>انتقال با نام کاربری:</b>\n"
            "<code>انتقال آتر @username 500</code>\n\n"
            "3️⃣ <b>انتقال با ریپلای روی پیام فرد:</b>\n"
            "ریپلای روی پیام شخص و نوشتن: <code>انتقال آتر 500</code>",
            parse_mode="HTML"
        )

    # حالت سوم: ریپلای روی پیام فرد
    if message.reply_to_message and message.reply_to_message.from_user:
        try:
            amount = int(text)
            to_user_id = message.reply_to_message.from_user.id
            return await process_transfer_request(message, state, to_user_id, amount)
        except ValueError:
            return await message.reply("❌ مبلغ وارد شده نامعتبر است. مبلغ باید یک عدد باشد.")

    # حالت اول و دوم: بر اساس آیدی عددی یا یوزرنیم
    parts = text.split()
    if len(parts) < 2:
        return await message.reply(
            "❌ فرمت دستور اشتباه است.\nمثال: <code>انتقال آتر @username 100</code> یا <code>انتقال آتر 123456 100</code>",
            parse_mode="HTML"
        )

    target_raw = parts[0]
    try:
        amount = int(parts[1])
    except ValueError:
        return await message.reply("❌ مبلغ باید عدد باشد.")

    to_user_id = None
    if target_raw.startswith("@"):
        username = target_raw[1:]
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT user_id FROM users WHERE username = ?",
                (username,),
            ) as cur:
                row = await cur.fetchone()
                if row:
                    to_user_id = row["user_id"]
        if not to_user_id:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT user_id FROM users WHERE username LIKE ?",
                    (f"%{username}%",),
                ) as cur:
                    row = await cur.fetchone()
                    if row:
                        to_user_id = row["user_id"]
        if not to_user_id:
            return await message.reply("❌ کاربری با این آیدی یافت نشد.")
    else:
        try:
            to_user_id = int(target_raw)
        except ValueError:
            return await message.reply("❌ شماره حساب (آیدی عددی) یا نام کاربری وارد شده نامعتبر است.")

    return await process_transfer_request(message, state, to_user_id, amount)


@user_router.callback_query(TxForm.waiting_for_confirm, F.data == "tx_yes")
async def confirm_transfer_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    from_user = data.get("from_user") or callback.from_user.id
    if callback.from_user.id != from_user:
        return await callback.answer("❌ فقط انتقال‌دهنده می‌تواند تأیید کند.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, from_user)
    await state.clear()
    to_user_id = data["to_user_id"]
    amount = data["amount"]
    target_name = data.get("target_name", "کاربر مقصد")
    safe_target_name = html.escape(target_name)

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?",
                (from_user,),
            ) as cur:
                s = await cur.fetchone()
            async with db.execute(
                "SELECT balance FROM users WHERE user_id = ?", (to_user_id,)
            ) as cur2:
                r = await cur2.fetchone()

            s_transferable = max(0, s["balance"] - s["frozen_balance"]) if s else 0
            if (
                not s
                or s["is_frozen"]
                or s_transferable < amount
                or not r
                or (r["balance"] + amount > MAX_BALANCE_LIMIT)
            ):
                return await callback.message.edit_text(
                    "❌ خطا در هم‌روندی یا وضعیت حساب؛ تراکنش لغو شد."
                )

            tx_id = (
                f"TX-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
            )
            await db.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                (amount, from_user),
            )
            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (amount, to_user_id),
            )
            await db.execute(
                """
                INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason)
                VALUES (?, ?, ?, ?, ?, 'انتقال کاربر به کاربر')
            """,
                (
                    tx_id,
                    datetime.now(timezone.utc).isoformat(),
                    from_user,
                    to_user_id,
                    amount,
                ),
            )
            await db.commit()

    await callback.message.edit_text(
        f"✅ تراکنش با موفقیت انجام شد!\n"
        f"به نام: <b>{safe_target_name}</b>\n"
        f"شناسه: <code>{tx_id}</code>\n"
        f"مبلغ: <code>₳ {amount}</code>",
        parse_mode="HTML",
    )

    sender_data = await get_user_data(from_user)
    sender_name = sender_data["full_name"] if sender_data else str(from_user)
    safe_sender_name = html.escape(sender_name)

    try:
        await callback.bot.send_message(
            from_user,
            f"📤 <b>رسید انتقال</b>\n\n"
            f"شما <code>₳ {amount}</code> به <b>{safe_target_name}</b> (<code>{to_user_id}</code>) انتقال دادید.\n"
            f"🔖 شناسه تراکنش: <code>{tx_id}</code>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        await callback.bot.send_message(
            to_user_id,
            f"📥 <b>رسید دریافت</b>\n\n"
            f"شما <code>₳ {amount}</code> از <b>{safe_sender_name}</b> (<code>{from_user}</code>) دریافت کردید.\n"
            f"🔖 شناسه تراکنش: <code>{tx_id}</code>",
            parse_mode="HTML",
        )
    except Exception:
        pass


@user_router.callback_query(TxForm.waiting_for_confirm, F.data == "tx_no")
async def cancel_transfer_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    from_user = data.get("from_user") or callback.from_user.id
    if callback.from_user.id != from_user:
        return await callback.answer("❌ فقط انتقال‌دهنده می‌تواند لغو کند.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, from_user)
    await state.clear()
    await callback.message.edit_text("❌ انتقال وجه لغو شد.")


# --- بخش مدیریت و ادمین ---


@admin_router.message(Command("users"))
async def cmd_users(message: Message):
    if not is_private(message) or not await check_admin_filter(message):
        return

    text, kb = await get_users_page(1)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("users_page_"))
async def cb_users_page(callback: CallbackQuery):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)

    page = int(callback.data.split("_")[2])
    text, kb = await get_users_page(page)
    
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@admin_router.callback_query(F.data == "users_noop")
async def cb_users_noop(callback: CallbackQuery):
    await callback.answer()


@admin_router.message(Command("frozen_users"))
async def cmd_frozen_users(message: Message):
    if not is_private(message) or not await check_admin_filter(message):
        return

    text, kb = await get_frozen_users_page(1)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("frozen_users_page_"))
async def cb_frozen_users_page(callback: CallbackQuery):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)

    page = int(callback.data.split("_")[3])
    text, kb = await get_frozen_users_page(page)

    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@admin_router.callback_query(F.data == "frozen_users_noop")
async def cb_frozen_users_noop(callback: CallbackQuery):
    await callback.answer()


# =====================================================================================
# 🛡 سیستم تأییدیه دو مرحله‌ای عمومی (پیش‌نمایش + تأیید/لغو) برای دستورات فروشگاه
# و مدیریت گروه‌ها: /delete، /create_group، /add_group، /extend_group، /renew_group،
# /rename_group، /move_group، /remove_group.
# هیچ‌کدام از این دستورات با ارسال مستقیم توسط کاربر اجرا نمی‌شوند؛ ابتدا یک کارت
# پیش‌نمایش با دو دکمه شیشه‌ای نمایش داده می‌شود و فقط با کلیک روی «✅ تأیید و اجرای
# نهایی» عملیات واقعی (تغییر دیتابیس) انجام می‌گیرد.
# =====================================================================================

def _build_ops_confirm_dialog(preview_text: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید و اجرای نهایی", callback_data="ops_confirm_yes"),
        InlineKeyboardButton(text="❌ لغو عملیات", callback_data="ops_confirm_no"),
    ]])


async def _start_ops_confirmation(message: Message, state: FSMContext, op_action: str, preview_text: str, **extra) -> None:
    """ذخیره پارامترهای عملیات در State و نمایش کارت پیش‌نمایش (بدون هیچ تغییری در دیتابیس)."""
    await state.update_data(op_action=op_action, op_requester_id=message.from_user.id, **extra)
    kb = _build_ops_confirm_dialog(preview_text)
    confirm_msg = await message.reply(preview_text, reply_markup=kb, parse_mode="HTML")
    await state.set_state(OpsConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


async def _execute_ops_confirmed_action(callback: CallbackQuery, data: dict) -> None:
    """اجرای واقعی عملیات (تغییر دیتابیس) پس از کلیک «✅ تأیید و اجرای نهایی»؛ منطق هر عملیات
    دقیقاً همان منطق اصلی دستور مربوطه است، فقط خروجی به‌جای message.reply روی همان پیام
    پیش‌نمایش ادیت می‌شود."""
    action = data["op_action"]
    bot = callback.bot

    if action == "delete_product":
        product_id = data["op_product_id"]
        channel_id = data["op_channel_id"]
        channel_msg_id = data["op_channel_msg_id"]
        product_title = data["op_product_title"]

        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT order_id, buyer_id, price, courier_fee FROM orders WHERE product_id = ? AND status = 'DISPATCHED'",
                    (product_id,)
                ) as cur_ord:
                    active_orders = await cur_ord.fetchall()

                for o in active_orders:
                    total_frozen = o["price"] + o["courier_fee"]
                    await db.execute(
                        "UPDATE users SET balance = balance + ?, frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                        (total_frozen, total_frozen, o["buyer_id"])
                    )
                    await db.execute("UPDATE orders SET status = 'CANCELLED' WHERE order_id = ?", (o["order_id"],))

                cancelled_orders = len(active_orders)

                await db.execute("DELETE FROM products WHERE product_id = ?", (product_id,))
                await db.commit()

        try:
            await bot.delete_message(chat_id=channel_id, message_id=channel_msg_id)
        except Exception:
            pass

        for o in active_orders:
            try:
                await bot.send_message(
                    o["buyer_id"],
                    f"❌ محصول «<b>{html.escape(product_title)}</b>» توسط فروشنده حذف شد و سفارش شما لغو گردید.\n"
                    f"🔓 مبلغ فریزشده این سفارش (<code>₳ {o['price'] + o['courier_fee']}</code>) به‌طور کامل به موجودی قابل‌استفاده شما بازگشت.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

        note = f"\n\n⚠️ توجه: <code>{cancelled_orders}</code> سفارش در حال ارسال مربوط به این محصول لغو شد و مبلغ فریزشده هرکدام به‌طور کامل به خریدار بازگشت (بدون هیچ تغییر مالی در حساب شما یا اشخاص دیگر)." if cancelled_orders else ""
        await callback.message.edit_text(
            f"🗑 محصول «<b>{html.escape(product_title)}</b>» با موفقیت از فروشگاه شما حذف شد.{note}",
            parse_mode="HTML",
        )
        return

    if action == "create_group":
        g_name = data["op_group_name"]
        safe_g_name = html.escape(g_name)
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT 1 FROM groups WHERE group_name = ?", (g_name,))
            exists = await cursor.fetchone()
            if exists:
                return await callback.message.edit_text(f"ℹ️ گروه <b>{safe_g_name}</b> از قبل وجود دارد.", parse_mode="HTML")
            await db.execute("INSERT INTO groups (group_name) VALUES (?)", (g_name,))
            await db.commit()
        await callback.message.edit_text(
            f"✅ گروه <b>{safe_g_name}</b> با موفقیت به لیست گروه‌ها اضافه شد.\n(هیچ لینکی ساخته نشد)",
            parse_mode="HTML",
        )
        return

    if action == "add_group":
        g_name = data["op_group_name"]
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO groups (group_name) VALUES (?)", (g_name,))
                await db.execute("INSERT INTO group_links (code, group_name) VALUES (?, ?)", (code, g_name))
                await db.commit()
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=G_{code}"
        safe_g_name = html.escape(g_name)
        await callback.message.edit_text(
            f"✅ <b>گروه مجازی «{safe_g_name}»</b> با موفقیت در سیستم ربات ایجاد شد.\n\n"
            f"🔗 <b>لینک عضویت اختصاصی:</b>\n{link}\n\n"
            f"📌 توجه: این گروه صرفاً یک برچسب درون ربات است و ارتباطی با گروه‌های تلگرام ندارد.\n"
            f"کاربران با کلیک روی لینک فوق، به این گروه در ربات ملحق میشوند.",
            parse_mode="HTML",
        )
        return

    if action == "extend_group":
        g_name = data["op_group_name"]
        extra_days = data["op_days"]
        safe_g_name = html.escape(g_name)
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    "SELECT code FROM group_links WHERE group_name = ? ORDER BY rowid DESC LIMIT 1",
                    (g_name,),
                )
                row = await cursor.fetchone()
                if not row:
                    return await callback.message.edit_text(f"❌ گروهی با نام {safe_g_name} یا لینکی برای آن پیدا نشد.", parse_mode="HTML")

                old_code = row[0]
                try:
                    await db.execute("ALTER TABLE group_links ADD COLUMN expires_at TEXT")
                except Exception:
                    pass

                new_expires = (datetime.now(timezone.utc) + timedelta(days=extra_days)).isoformat()
                await db.execute("UPDATE group_links SET expires_at = ? WHERE code = ?", (new_expires, old_code))
                await db.commit()

        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=G_{old_code}"
        await callback.message.edit_text(
            f"✅ لینک گروه <b>{safe_g_name}</b> به مدت {extra_days} روز تمدید شد.\n\n🔗 لینک:\n{link}",
            parse_mode="HTML",
        )
        return

    if action == "renew_group":
        g_name = data["op_group_name"]
        days = data["op_days"]
        safe_g_name = html.escape(g_name)
        new_code = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute("SELECT 1 FROM groups WHERE group_name = ?", (g_name,))
                if not await cursor.fetchone():
                    return await callback.message.edit_text(f"❌ گروهی با نام {safe_g_name} پیدا نشد.", parse_mode="HTML")

                try:
                    await db.execute("ALTER TABLE group_links ADD COLUMN expires_at TEXT")
                except Exception:
                    pass
                try:
                    await db.execute("ALTER TABLE group_links ADD COLUMN created_at TEXT")
                except Exception:
                    pass

                await db.execute(
                    "INSERT INTO group_links (code, group_name, expires_at) VALUES (?, ?, ?)",
                    (new_code, g_name, expires_at),
                )
                await db.commit()

        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=G_{new_code}"
        await callback.message.edit_text(
            f"✅ لینک جدید برای گروه <b>{safe_g_name}</b> ساخته شد.\nمدت اعتبار: {days} روز\n\n🔗 لینک جدید:\n{link}",
            parse_mode="HTML",
        )
        return

    if action == "rename_group":
        old_n = data["op_old_name"]
        new_n = data["op_new_name"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO groups (group_name) VALUES (?)", (new_n,))
            await db.execute("UPDATE users SET group_name = ? WHERE group_name = ?", (new_n, old_n))
            await db.execute("DELETE FROM groups WHERE group_name = ?", (old_n,))
            await db.commit()
        await callback.message.edit_text("🔄 تغییر نام با موفقیت اعمال شد.")
        return

    if action == "move_group":
        t_id = data["op_user_id"]
        g_name = data["op_group_name"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET group_name = ? WHERE user_id = ?", (g_name, t_id))
            await db.commit()
        await callback.message.edit_text("👑 کاربر به گروه جدید منتقل شد.")
        return

    if action == "remove_group":
        t_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET group_name = 'Default' WHERE user_id = ?", (t_id,))
            await db.commit()
        await callback.message.edit_text("✅ کاربر به گروه پیش‌فرض برگردانده شد.")
        return

    if action == "remove_shop":
        shop_id = data["op_shop_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM shops WHERE shop_id = ?", (shop_id,))
            await db.execute("DELETE FROM products WHERE shop_id = ?", (shop_id,))
            await db.commit()
        await callback.message.edit_text(f"🗑 فروشگاه شماره {shop_id} و محصولات آن حذف شدند.")
        return

    if action == "add_courier":
        courier_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO couriers (user_id) VALUES (?)", (courier_id,))
            await db.commit()
        await callback.message.edit_text(f"🚚 کاربر <code>{courier_id}</code> به لیست پستچی‌های مجاز اضافه شد.", parse_mode="HTML")
        return

    if action == "remove_courier":
        courier_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM couriers WHERE user_id = ?", (courier_id,))
            await db.commit()
        await callback.message.edit_text(f"🔥 کاربر <code>{courier_id}</code> از لیست پستچی‌ها حذف شد.", parse_mode="HTML")
        return

    if action == "promote":
        target_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (target_id,))
            await db.commit()
        await callback.message.edit_text("👑 کاربر به سطح ادمین ارتقا یافت.")
        return

    if action == "demote":
        target_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_admin = 0 WHERE user_id = ?", (target_id,))
            await db.commit()
        await callback.message.edit_text("🔥 دسترسی ادمینی کاربر سلب شد.")
        return

    if action == "add_super":
        new_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO super_admins (user_id) VALUES (?)", (new_id,))
            await db.commit()
            await load_super_admins(db)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (new_id,))
            await db.commit()
        await callback.message.edit_text(f"✅ کاربر <code>{new_id}</code> به سوپرادمین‌ها اضافه شد.", parse_mode="HTML")
        return

    if action == "remove_super":
        rem_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM super_admins WHERE user_id = ?", (rem_id,))
            await db.commit()
            await load_super_admins(db)
        await callback.message.edit_text(f"✅ کاربر <code>{rem_id}</code> از سوپرادمین‌ها حذف شد.", parse_mode="HTML")
        return

    if action == "freeze":
        target_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_frozen = 1 WHERE user_id = ?", (target_id,))
            await db.commit()
        await callback.message.edit_text("❄️ حساب کاربر فریز شد.")
        return

    if action == "unfreeze":
        target_id = data["op_user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_frozen = 0 WHERE user_id = ?", (target_id,))
            await db.commit()
        await callback.message.edit_text("🟢 حساب کاربر فعال شد.")
        return

    if action == "undo_tx":
        tx_id = data["op_tx_id"]
        reason = data["op_reason"]

        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                await db.execute("BEGIN IMMEDIATE")

                async with db.execute(
                    "SELECT from_user, to_user, amount, status FROM audit_logs WHERE tx_id = ?", (tx_id,)
                ) as cur:
                    tx = await cur.fetchone()

                if not tx:
                    await db.execute("ROLLBACK")
                    return await callback.message.edit_text("❌ تراکنشی با این شناسه یافت نشد.")
                if tx["status"] == "REFUNDED":
                    await db.execute("ROLLBACK")
                    return await callback.message.edit_text("❌ این تراکنش قبلاً باطل شده است.")

                f_user, t_user, amount = tx["from_user"], tx["to_user"], tx["amount"]

                if t_user != 0:
                    async with db.execute("SELECT balance FROM users WHERE user_id = ?", (t_user,)) as cur_t:
                        target = await cur_t.fetchone()
                    if not target or target["balance"] < amount:
                        await db.execute("ROLLBACK")
                        return await callback.message.edit_text("❌ خطا: موجودی گیرنده برای برگشت زدن کافی نیست.")

                    await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, t_user))

                if f_user != 0:
                    await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, f_user))

                new_tx_id = f"TX-REV-{str(uuid.uuid4()).upper()[:10]}"
                await db.execute("UPDATE audit_logs SET status = 'REFUNDED' WHERE tx_id = ?", (tx_id,))
                await db.execute(
                    """
                    INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'SUCCESS')
                """,
                    (
                        new_tx_id,
                        datetime.now(timezone.utc).isoformat(),
                        t_user,
                        f_user,
                        amount,
                        f"برگشت تراکنش {tx_id}: {reason}",
                    ),
                )

                await db.commit()

        await callback.message.edit_text(
            f"🔄 تراکنش با موفقیت معکوس شد.\n🔖 شناسه برگشتی: <code>{new_tx_id}</code>",
            parse_mode="HTML",
        )
        return


@admin_router.callback_query(OpsConfirmForm.waiting_for_confirm, F.data == "ops_confirm_yes")
async def cb_ops_confirm_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    requester_id = data.get("op_requester_id")
    if callback.from_user.id != requester_id:
        return await callback.answer("❌ فقط خودتان می‌توانید تأیید کنید.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, requester_id)
    await state.clear()
    await callback.answer()
    await _execute_ops_confirmed_action(callback, data)


@admin_router.callback_query(OpsConfirmForm.waiting_for_confirm, F.data == "ops_confirm_no")
async def cb_ops_confirm_no(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    requester_id = data.get("op_requester_id")
    if callback.from_user.id != requester_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, requester_id)
    await state.clear()
    try:
        await callback.message.edit_text("❌ عملیات توسط کاربر لغو شد.")
    except Exception:
        pass
    await callback.answer()


@admin_router.message(Command("create_group"))
async def cmd_create_group(message: Message, state: FSMContext):
    """فقط گروه را به لیست اضافه می‌کند (بدون ساخت لینک)"""
    if not is_private(message) or not await check_admin_filter(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply(
            "⚠️ راهنما:\n"
            "/create_group [نام_گروه]\n\n"
            "مثال:\n"
            "/create_group ترم۱"
        )

    g_name = args[1].strip()
    if not g_name:
        return await message.reply("❌ نام گروه نمی‌تواند خالی باشد.")

    safe_g_name = html.escape(g_name)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM groups WHERE group_name = ?", (g_name,))
        exists = await cursor.fetchone()
    if exists:
        return await message.reply(f"ℹ️ گروه <b>{safe_g_name}</b> از قبل وجود دارد.", parse_mode="HTML")

    preview_text = (
        "🏢 <b>تأیید ساخت گروه</b>\n"
        f"آیا از ایجاد گروه جدید با نام «{safe_g_name}» اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "create_group", preview_text, op_group_name=g_name)


@admin_router.message(Command("add_group"))
async def cmd_add_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply(
            "⚠️ راهنما:\n"
            "/add_group [نام_گروه_مجازی]\n\n"
            "این دستور یک <b>گروه مجازی</b> درون ربات ایجاد میکند (نه یک گروه تلگرامی).\n"
            "کاربران با استفاده از لینک تولیدشده میتوانند به این گروه در ربات بپیوندند.",
            parse_mode="HTML"
        )

    g_name = args[1].strip()
    if not g_name:
        return await message.reply("❌ نام گروه نمی‌تواند خالی باشد.")

    safe_g_name = html.escape(g_name)
    preview_text = (
        "🔗 <b>تأیید ساخت گروه و ساخت لینک</b>\n"
        f"آیا می‌خواهید گروه مجازی «{safe_g_name}» ایجاد شده و لینک دعوت یکتا برای آن تولید شود؟"
    )
    await _start_ops_confirmation(message, state, "add_group", preview_text, op_group_name=g_name)


@admin_router.message(Command("extend_group"))
async def cmd_extend_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply(
            "راهنما: /extend_group [نام_گروه] [تعداد_روز]\n"
            "مثال: /extend_group ترم۱ 2"
        )

    g_name = args[1].strip()
    days_str = args[2].strip()

    if not days_str.isdigit() or int(days_str) <= 0:
        return await message.reply("❌ تعداد روز باید عدد مثبت باشد.")

    extra_days = int(days_str)
    safe_g_name = html.escape(g_name)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT code FROM group_links WHERE group_name = ? ORDER BY rowid DESC LIMIT 1", (g_name,)
        )
        row = await cursor.fetchone()
    if not row:
        return await message.reply(f"❌ گروهی با نام {safe_g_name} یا لینکی برای آن پیدا نشد.", parse_mode="HTML")

    preview_text = (
        "⏳ <b>تأیید تمدید لینک</b>\n"
        f"آیا از افزودن {extra_days} روز به مهلت اعتبار لینک گروه «{safe_g_name}» اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "extend_group", preview_text, op_group_name=g_name, op_days=extra_days)


@admin_router.message(Command("renew_group"))
async def cmd_renew_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply(
            "راهنما: /renew_group [نام_گروه] [تعداد_روز]\n"
            "مثال: /renew_group ترم۱ 30"
        )

    g_name = args[1].strip()
    days_str = args[2].strip()

    if not days_str.isdigit() or int(days_str) <= 0:
        return await message.reply("❌ تعداد روز باید عدد مثبت باشد.")

    days = int(days_str)
    safe_g_name = html.escape(g_name)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM groups WHERE group_name = ?", (g_name,))
        row = await cursor.fetchone()
    if not row:
        return await message.reply(f"❌ گروهی با نام {safe_g_name} پیدا نشد.", parse_mode="HTML")

    preview_text = (
        "🔄 <b>هشدار ابطال و بازسازی لینک</b>\n"
        f"با این کار لینک فعلی گروه «{safe_g_name}» باطل شده و لینک جدیدی با اعتبار {days} روز ساخته می‌شود. آیا تأیید می‌کنید؟"
    )
    await _start_ops_confirmation(message, state, "renew_group", preview_text, op_group_name=g_name, op_days=days)


@admin_router.message(Command("rename_group"))
async def cmd_rename_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return
    args = message.text.split()
    if len(args) < 3:
        return await message.reply("استفاده: <code>/rename_group [قدیمی] [جدید]</code>", parse_mode="HTML")
    old_n, new_n = args[1], args[2]
    safe_old_n = html.escape(old_n)
    safe_new_n = html.escape(new_n)
    preview_text = (
        "✏️ <b>تأیید تغییر نام گروه</b>\n"
        f"آیا از تغییر نام گروه از «{safe_old_n}» به «{safe_new_n}» اطمینان دارید؟ "
        "(این تغییر برای تمام اعضای این گروه نیز اعمال می‌شود)"
    )
    await _start_ops_confirmation(message, state, "rename_group", preview_text, op_old_name=old_n, op_new_name=new_n)


GROUPS_PAGE_SIZE = 10


async def _fetch_groups_overview():
    """برای هر گروه، تعداد اعضا و وضعیت آخرین لینک دعوت را محاسبه می‌کند."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT group_name FROM groups ORDER BY group_name") as cur:
            groups = await cur.fetchall()

        result = []
        for g in groups:
            g_name = g["group_name"]
            async with db.execute("SELECT COUNT(*) FROM users WHERE group_name = ?", (g_name,)) as cur_c:
                member_count = (await cur_c.fetchone())[0]

            try:
                async with db.execute(
                    "SELECT expires_at FROM group_links WHERE group_name = ? ORDER BY rowid DESC LIMIT 1", (g_name,)
                ) as cur_l:
                    link_row = await cur_l.fetchone()
            except Exception:
                link_row = None

            if not link_row:
                link_status = "⚪️ بدون لینک"
            else:
                expires_val = link_row[0]
                if not expires_val:
                    link_status = "🟢 فعال (بدون انقضا)"
                else:
                    try:
                        expires = datetime.fromisoformat(str(expires_val))
                        link_status = "🟢 فعال" if datetime.now(timezone.utc) <= expires else "🔴 منقضی‌شده"
                    except Exception:
                        link_status = "🟢 فعال"

            result.append({"name": g_name, "member_count": member_count, "link_status": link_status})
        return result


def _render_groups_page(groups_info, page: int):
    total = len(groups_info)
    total_pages = max(1, math.ceil(total / GROUPS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * GROUPS_PAGE_SIZE
    page_items = groups_info[start:start + GROUPS_PAGE_SIZE]

    txt = f"🏢 <b>لیست گروه‌های مجازی ثبت‌شده (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"مجموع گروه‌ها: <code>{total}</code> گروه\n\n"
    for g in page_items:
        safe_name = html.escape(g["name"])
        txt += f"🔹 <b>{safe_name}</b> | اعضا: <code>{g['member_count']}</code> نفر | لینک: {g['link_status']}\n"

    kb = _build_pagination_keyboard(page, total_pages, "groups_page", refresh_data="groups_refresh")
    return txt, kb


@admin_router.message(Command("groups"))
async def cmd_groups(message: Message):
    if not is_private(message) or not await check_admin_filter(message):
        return
    groups_info = await _fetch_groups_overview()
    if not groups_info:
        return await message.reply("ℹ️ هیچ گروهی ثبت نشده است.")
    txt, kb = _render_groups_page(groups_info, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "groups_page_noop")
async def cb_groups_noop(callback: CallbackQuery):
    await callback.answer()


@admin_router.callback_query(F.data.startswith("groups_page_"))
async def cb_groups_page(callback: CallbackQuery):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    page = int(callback.data.split("_")[2])
    groups_info = await _fetch_groups_overview()
    if not groups_info:
        return await callback.answer("ℹ️ هیچ گروهی ثبت نشده است.", show_alert=True)
    txt, kb = _render_groups_page(groups_info, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@admin_router.callback_query(F.data == "groups_refresh")
async def cb_groups_refresh(callback: CallbackQuery):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    page = _extract_current_page(callback.message.text or "")
    groups_info = await _fetch_groups_overview()
    if not groups_info:
        return await callback.answer("ℹ️ هیچ گروهی ثبت نشده است.", show_alert=True)
    txt, kb = _render_groups_page(groups_info, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer("🔄 لیست به‌روزرسانی شد.")


GROUP_USERS_PAGE_SIZE = 10
# آخرین گروهی که هر ادمین با /group_users مشاهده کرده (برای پیمایش صفحات و به‌روزرسانی، بدون
# نیاز به رمزگذاری نام گروه در callback_data که ممکن است حاوی فاصله/کاراکتر خاص باشد).
_group_users_context: dict[int, str] = {}


def _render_group_users_page(g_name: str, members, page: int):
    total = len(members)
    total_pages = max(1, math.ceil(total / GROUP_USERS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * GROUP_USERS_PAGE_SIZE
    page_items = members[start:start + GROUP_USERS_PAGE_SIZE]

    safe_g_name = html.escape(g_name)
    txt = f"👥 <b>اعضای گروه «{safe_g_name}» (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"تعداد کل اعضا: <code>{total}</code> نفر\n\n"
    for idx, u in enumerate(page_items, start=start + 1):
        safe_full_name = html.escape(u["full_name"] or "ناشناس")
        txt += f"<b>{idx}.</b> {safe_full_name} | <code>{u['user_id']}</code>\n"

    kb = _build_pagination_keyboard(page, total_pages, "gu_page", refresh_data="gu_refresh")
    return txt, kb


@admin_router.message(Command("group_users"))
async def cmd_group_users(message: Message):
    if not is_private(message) or not await check_admin_filter(message):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/group_users [نام_گروه]</code>", parse_mode="HTML")
    g_name = args[1]
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, full_name FROM users WHERE group_name = ? ORDER BY user_id", (g_name,)
        ) as cur:
            members = await cur.fetchall()
    if not members:
        return await message.reply("عضوی یافت نشد.")

    _group_users_context[message.from_user.id] = g_name
    txt, kb = _render_group_users_page(g_name, members, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "gu_page_noop")
async def cb_group_users_noop(callback: CallbackQuery):
    await callback.answer()


async def _group_users_reload(callback: CallbackQuery, page: int):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    g_name = _group_users_context.get(callback.from_user.id)
    if not g_name:
        return await callback.answer("❌ ابتدا دستور /group_users [نام_گروه] را اجرا کنید.", show_alert=True)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, full_name FROM users WHERE group_name = ? ORDER BY user_id", (g_name,)
        ) as cur:
            members = await cur.fetchall()
    if not members:
        return await callback.answer("عضوی یافت نشد.", show_alert=True)

    txt, kb = _render_group_users_page(g_name, members, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("gu_page_"))
async def cb_group_users_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    await _group_users_reload(callback, page)
    await callback.answer()


@admin_router.callback_query(F.data == "gu_refresh")
async def cb_group_users_refresh(callback: CallbackQuery):
    page = _extract_current_page(callback.message.text or "")
    await _group_users_reload(callback, page)
    await callback.answer("🔄 لیست به‌روزرسانی شد.")


@admin_router.message(Command("move_group"))
async def cmd_move_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return
    args = message.text.split()
    if len(args) < 3:
        return await message.reply("استفاده: <code>/move_group [آیدی] [گروه]</code>", parse_mode="HTML")
    try:
        t_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد صحیح باشد.")
    g_name = args[2]
    safe_g_name = html.escape(g_name)
    preview_text = (
        "🔀 <b>تأیید جابه‌جایی کاربر</b>\n"
        f"آیا از انتقال کاربر با شناسه {t_id} به گروه «{safe_g_name}» اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "move_group", preview_text, op_user_id=t_id, op_group_name=g_name)


@admin_router.message(Command("remove_group"))
async def cmd_remove_group(message: Message, state: FSMContext):
    if not is_private(message) or not await check_admin_filter(message):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/remove_group [آیدی]</code>", parse_mode="HTML")
    try:
        t_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد صحیح باشد.")
    preview_text = (
        "↩️ <b>تأیید بازنشانی گروه کاربر</b>\n"
        f"آیا می‌خواهید گروه کاربر {t_id} را حذف کرده و وضعیت او را به حالت پیش‌فرض (Default) برگردانید؟"
    )
    await _start_ops_confirmation(message, state, "remove_group", preview_text, op_user_id=t_id)


@admin_router.message(Command("delete_group"))
async def cmd_delete_group(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/delete_group [نام_گروه]</code>", parse_mode="HTML")
    g_name = args[1]
    if g_name == "Default":
        return await message.reply("❌ گروه پیش‌فرض حذف‌شدنی نیست.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET group_name = 'Default' WHERE group_name = ?",
            (g_name,),
        )
        await db.execute("DELETE FROM group_links WHERE group_name = ?", (g_name,))
        await db.execute("DELETE FROM groups WHERE group_name = ?", (g_name,))
        await db.commit()
    await message.reply("🗑 گروه حذف شد و اعضا به Default منتقل شدند.")


def _build_admin_give_take_dialog(data):
    """متن و کیبورد صفحه «تأیید اولیه» مشترک بین /give و /take را می‌سازد (خلاصه عملیات + موجودی فعلی مقصد)."""
    action = data["action"]
    target = data["target"]
    amount = data["amount"]
    safe_target_name = html.escape(data.get("target_name") or "ناشناس")
    safe_reason = html.escape(data["reason"])
    current_balance = data.get("target_balance", 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید اولیه", callback_data="admin_yes"),
        InlineKeyboardButton(text="❌ لغو", callback_data="admin_no"),
    ]])
    if action == "give":
        text = (
            f"💰 <b>تأیید واریز مستقیم</b>\n"
            f"آیا از واریز مبلغ <code>₳ {amount}</code> به حساب کاربر <code>{target}</code> اطمینان دارید؟"
        )
    else:  # take
        text = (
            f"🔻 <b>هشدار کسر مستقیم از حساب</b>\n"
            f"آیا از کسر مبلغ <code>₳ {amount}</code> از حساب کاربر <code>{target}</code> اطمینان دارید؟"
        )
    return text, kb


@admin_router.message(Command("give"))
async def cmd_give(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=3)
    if len(args) < 3:
        return await message.reply(
            "❌ ساختار: <code>/give [آیدی] [مقدار] [دلیل_اختیاری]</code>",
            parse_mode="HTML"
        )
    try:
        target, amount = int(args[1]), int(args[2])
        reason = args[3] if len(args) > 3 else "واریز مدیریت"
    except ValueError:
        return await message.reply("❌ ورودی نامعتبر.")
    if amount <= 0:
        return await message.reply("❌ مقدار باید مثبت باشد.")
    if target == TREASURY_USER_ID:
        return await message.reply(
            "🏛 حساب <code>8490505070</code> صرفاً نقش خزانه مرکزی سیستم را دارد و برای جلوگیری از "
            "دستکاری ناخواسته بودجه خزانه، از طریق دستورات مدیریتی شخصی قابل واریز نیست.",
            parse_mode="HTML",
        )

    target_data = await get_user_data(target)
    if not target_data:
        return await message.reply("❌ کاربر مقصد یافت نشد.")
    if target_data["balance"] + amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ خطا: سقف موجودی مقصد.")

    await state.update_data(
        action="give",
        target=target,
        amount=amount,
        reason=reason,
        target_name=target_data["full_name"],
        target_balance=target_data["balance"],
        admin_id=message.from_user.id,
    )

    if target_data["is_frozen"]:
        # ⚠️ مقصد فریز است: عملیات رد نمی‌شود، فقط قبل از ادامه هشدار داده می‌شود
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ ادامه", callback_data="sfwarn_continue"),
            InlineKeyboardButton(text="❌ لغو", callback_data="sfwarn_cancel"),
        ]])
        confirm_msg = await message.reply(
            _frozen_target_card_text(target_data, target), reply_markup=kb, parse_mode="HTML"
        )
        await state.set_state(AdminConfirmForm.waiting_for_frozen_ack)
    else:
        data = await state.get_data()
        text, kb = _build_admin_give_take_dialog(data)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(AdminConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.message(Command("take"))
async def cmd_take(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=3)
    if len(args) < 3:
        return await message.reply(
            "❌ ساختار: <code>/take [آیدی] [مقدار] [دلیل_اختیاری]</code>",
            parse_mode="HTML"
        )
    try:
        target, amount = int(args[1]), int(args[2])
        reason = args[3] if len(args) > 3 else "کسر مدیریت"
    except ValueError:
        return await message.reply("❌ ورودی نامعتبر.")
    if amount <= 0:
        return await message.reply("❌ مقدار باید مثبت باشد.")
    if target == TREASURY_USER_ID:
        return await message.reply(
            "🏛 حساب <code>8490505070</code> صرفاً نقش خزانه مرکزی سیستم را دارد و برای جلوگیری از "
            "دستکاری ناخواسته بودجه خزانه، از طریق دستورات مدیریتی شخصی قابل کسر نیست.",
            parse_mode="HTML",
        )

    target_data = await get_user_data(target)
    if not target_data:
        return await message.reply("❌ کاربر مقصد یافت نشد.")
    if target_data["balance"] < amount:
        return await message.reply("❌ موجودی ناکافی.")

    await state.update_data(
        action="take",
        target=target,
        amount=amount,
        reason=reason,
        target_name=target_data["full_name"],
        target_balance=target_data["balance"],
        admin_id=message.from_user.id,
    )

    if target_data["is_frozen"]:
        # ⚠️ مقصد فریز است: عملیات رد نمی‌شود، فقط قبل از ادامه هشدار داده می‌شود
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ ادامه", callback_data="sfwarn_continue"),
            InlineKeyboardButton(text="❌ لغو", callback_data="sfwarn_cancel"),
        ]])
        confirm_msg = await message.reply(
            _frozen_target_card_text(target_data, target), reply_markup=kb, parse_mode="HTML"
        )
        await state.set_state(AdminConfirmForm.waiting_for_frozen_ack)
    else:
        data = await state.get_data()
        text, kb = _build_admin_give_take_dialog(data)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(AdminConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


async def _execute_admin_confirmed_action(callback: CallbackQuery, data: dict, admin_id: int) -> None:
    """اجرای واقعی عملیات give/take/treasury_add/treasury_sub/rewardgroup پس از تکمیل مراحل تأیید (ادیت پیام اولیه به نتیجه نهایی)."""
    action = data["action"]

    if action == "rewardgroup":
        # 📦 توزیع گروهی پاداش: هر عضو با تراکنش مستقل (BEGIN/COMMIT جداگانه)، دقیقاً مطابق
        # معماری اصلی این دستور؛ فقط اکنون بر اساس انتخاب کاربر، اعضای فریز هم می‌توانند
        # جزو دریافت‌کنندگان باشند (بدون رد خودکار).
        g_name = data["gop_group"]
        amount = data["gop_amount"]
        reason = data["gop_reason"]
        include_frozen = data.get("gop_include_frozen", False)
        normal_members = data["gop_normal_members"]
        frozen_members = data["gop_frozen_members"] if include_frozen else []
        recipients = list(normal_members) + list(frozen_members)

        safe_g_name = html.escape(g_name)
        success_p, failed_p, total_dist = 0, 0, 0

        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                for u in recipients:
                    try:
                        await db.execute("BEGIN IMMEDIATE")
                        sub_tx_id = f"TX-G-{str(uuid.uuid4()).upper()[:12]}"
                        await db.execute(
                            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                            (amount, u["user_id"]),
                        )
                        await db.execute(
                            """
                            INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason, status)
                            VALUES (?, ?, 0, ?, ?, ?, 'SUCCESS')
                        """,
                            (
                                sub_tx_id,
                                datetime.now(timezone.utc).isoformat(),
                                u["user_id"],
                                amount,
                                f"پاداش گروه [{g_name}]: {reason}",
                            ),
                        )
                        await db.commit()
                        success_p += 1
                        total_dist += amount
                    except Exception:
                        await db.execute("ROLLBACK")
                        failed_p += 1

        await callback.message.edit_text(
            f"📊 <b>گزارش واریز گروهی ({safe_g_name}):</b>\n\n"
            f"✅ موفق: <code>{success_p}</code> کاربر\n"
            f"❌ خطا: <code>{failed_p}</code> کاربر\n"
            f"💰 توزیع شده: <code>₳ {total_dist}</code>",
            parse_mode="HTML",
        )
        return

    target = data.get("target")
    amount = data["amount"]
    reason = data["reason"]
    target_name = data.get("target_name", str(target) if target is not None else "")

    safe_target_name = html.escape(target_name)
    safe_reason = html.escape(reason)

    if action in ("treasury_add", "treasury_sub"):
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                if action == "treasury_add":
                    treasury_balance = await get_treasury_balance()
                    if treasury_balance + amount > MAX_BALANCE_LIMIT:
                        return await callback.message.edit_text("❌ خطا: سقف موجودی خزانه.")
                    await treasury_credit(
                        db, amount, f"افزایش دستی خزانه توسط سوپرادمین: {reason}", related_user=admin_id
                    )
                    await db.commit()
                    result_text = (
                        f"✅ به خزانه مرکزی واریز شد.\n💰 مبلغ: <code>₳ {amount}</code>\n📝 دلیل: {safe_reason}"
                    )
                    notify_text = (
                        f"📢 <b>عملیات سوپرادمین</b>\n\n"
                        f"👑 ادمین: <code>{admin_id}</code>\n"
                        f"➕ افزایش موجودی خزانه مرکزی\n"
                        f"💰 مبلغ: <code>₳ {amount}</code>\n"
                        f"📝 دلیل: {safe_reason}"
                    )
                else:  # treasury_sub
                    ok = await treasury_debit(
                        db, amount, f"کاهش دستی خزانه توسط سوپرادمین: {reason}", related_user=admin_id
                    )
                    await db.commit()
                    if not ok:
                        return await callback.message.edit_text("❌ موجودی خزانه کافی نیست.")
                    result_text = (
                        f"🔥 از خزانه مرکزی کسر شد.\n💰 مبلغ: <code>₳ {amount}</code>\n📝 دلیل: {safe_reason}"
                    )
                    notify_text = (
                        f"📢 <b>عملیات سوپرادمین</b>\n\n"
                        f"👑 ادمین: <code>{admin_id}</code>\n"
                        f"➖ کاهش موجودی خزانه مرکزی\n"
                        f"💰 مبلغ: <code>₳ {amount}</code>\n"
                        f"📝 دلیل: {safe_reason}"
                    )

        await callback.message.edit_text(result_text, parse_mode="HTML")
        for sa_id in SUPER_ADMINS:
            if sa_id != admin_id:
                try:
                    await callback.bot.send_message(sa_id, notify_text, parse_mode="HTML")
                except Exception:
                    pass
        return

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT balance FROM users WHERE user_id = ?", (target,)
            ) as cur:
                u = await cur.fetchone()

            if not u:
                return await callback.message.edit_text("❌ کاربر یافت نشد.")

            # ⚠️ کاربر فریزشده هم دقیقاً مانند کاربر عادی پرداخت/کسر می‌شود (بدون لغو خودکار)
            if action == "give":
                if u["balance"] + amount > MAX_BALANCE_LIMIT:
                    return await callback.message.edit_text("❌ خطا: سقف موجودی.")
                tx_id = f"TX-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (amount, target),
                )
                await db.execute(
                    "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) VALUES (?, ?, 0, ?, ?, ?)",
                    (tx_id, datetime.now(timezone.utc).isoformat(), target, amount, f"واریز مدیریت: {reason}"),
                )
                await db.commit()
                result_text = f"✅ واریز شد.\n👤 به: <b>{safe_target_name}</b> (<code>{target}</code>)\n💰 مبلغ: <code>₳ {amount}</code>\n🔖 شناسه: <code>{tx_id}</code>"
                notify_text = (
                    f"📢 <b>عملیات سوپرادمین</b>\n\n"
                    f"👑 ادمین: <code>{admin_id}</code>\n"
                    f"➕ واریز به: <b>{safe_target_name}</b> (<code>{target}</code>)\n"
                    f"💰 مبلغ: <code>₳ {amount}</code>\n"
                    f"📝 دلیل: {safe_reason}\n"
                    f"🔖 شناسه: <code>{tx_id}</code>"
                )
            else:  # take
                if u["balance"] < amount:
                    return await callback.message.edit_text("❌ موجودی ناکافی.")
                tx_id = f"TX-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
                await db.execute(
                    "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                    (amount, target),
                )
                await db.execute(
                    "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) VALUES (?, ?, ?, 0, ?, ?)",
                    (tx_id, datetime.now(timezone.utc).isoformat(), target, amount, f"کسر مدیریت: {reason}"),
                )
                await db.commit()
                result_text = f"🔥 کسر شد.\n👤 از: <b>{safe_target_name}</b> (<code>{target}</code>)\n💰 مبلغ: <code>₳ {amount}</code>\n🔖 شناسه: <code>{tx_id}</code>"
                notify_text = (
                    f"📢 <b>عملیات سوپرادمین</b>\n\n"
                    f"👑 ادمین: <code>{admin_id}</code>\n"
                    f"➖ کسر از: <b>{safe_target_name}</b> (<code>{target}</code>)\n"
                    f"💰 مبلغ: <code>₳ {amount}</code>\n"
                    f"📝 دلیل: {safe_reason}\n"
                    f"🔖 شناسه: <code>{tx_id}</code>"
                )

    await callback.message.edit_text(result_text, parse_mode="HTML")

    for sa_id in SUPER_ADMINS:
        if sa_id != admin_id:
            try:
                await callback.bot.send_message(sa_id, notify_text, parse_mode="HTML")
            except Exception:
                pass


def _build_admin_final_dialog(data):
    """متن و کیبورد صفحه «تأیید نهایی» مشترک بین /give، /take و /rewardgroup (مرحله آخر پیش از اجرا)."""
    action = data["action"]

    if action == "rewardgroup":
        g_name = data["gop_group"]
        amount = data["gop_amount"]
        include_frozen = data.get("gop_include_frozen", False)
        normal_count = len(data["gop_normal_members"])
        frozen_count = len(data["gop_frozen_members"]) if include_frozen else 0
        recipient_count = normal_count + frozen_count
        total = recipient_count * amount
        safe_g_name = html.escape(g_name)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ تأیید نهایی", callback_data="admin_final_yes"),
            InlineKeyboardButton(text="❌ لغو", callback_data="admin_final_no"),
        ]])
        text = (
            f"⚠️ <b>تأیید نهایی</b>\n\n"
            f"با تأیید نهایی، مبلغ <code>₳ {amount}</code> به <code>{recipient_count}</code> نفر از گروه «<b>{safe_g_name}</b>» "
            f"(مجموعاً <code>₳ {total}</code>) واریز خواهد شد.\n"
            f"این عملیات قابل بازگشت نیست.\n\n"
            f"آیا کاملاً مطمئن هستید؟"
        )
        return text, kb

    target = data["target"]
    amount = data["amount"]
    safe_target_name = html.escape(data.get("target_name") or "ناشناس")
    verb = "واریز" if action == "give" else "کسر"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید نهایی", callback_data="admin_final_yes"),
        InlineKeyboardButton(text="❌ لغو", callback_data="admin_final_no"),
    ]])
    text = (
        f"⚠️ <b>تأیید نهایی</b>\n\n"
        f"با تأیید نهایی، مبلغ <code>₳ {amount}</code> برای <b>{safe_target_name}</b> (<code>{target}</code>) {verb} خواهد شد.\n"
        f"این عملیات قابل بازگشت نیست.\n\n"
        f"آیا کاملاً مطمئن هستید؟"
    )
    return text, kb


@admin_router.callback_query(AdminConfirmForm.waiting_for_confirm, F.data == "admin_yes")
async def admin_confirm_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید تأیید کنید.", show_alert=True)

    action = data["action"]

    if action in ("give", "take", "rewardgroup"):
        # ⚠️ طبق فلوی جدید، پس از «تأیید اولیه» باید «تأیید نهایی» روی همان پیام نمایش داده
        # شود؛ اجرای واقعی فقط پس از تأیید نهایی انجام می‌شود.
        cancel_input_timeout(callback.message.chat.id, admin_id)
        text, kb = _build_admin_final_dialog(data)
        try:
            await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
        current_state = await state.get_state()
        schedule_input_timeout(
            state, callback.message.chat.id, admin_id, current_state,
            lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
        )
        return await callback.answer()

    # سایر اکشن‌ها (treasury_add/treasury_sub) بدون تغییر: همچنان تک‌مرحله‌ای اجرا می‌شوند
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await _execute_admin_confirmed_action(callback, data, admin_id)


@admin_router.callback_query(AdminConfirmForm.waiting_for_confirm, F.data == "admin_final_yes")
async def admin_confirm_final_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید تأیید کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await _execute_admin_confirmed_action(callback, data, admin_id)


@admin_router.callback_query(AdminConfirmForm.waiting_for_confirm, F.data == "admin_final_no")
async def admin_confirm_final_no(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات لغو شد.")


@admin_router.callback_query(AdminConfirmForm.waiting_for_confirm, F.data == "admin_no")
async def admin_confirm_no(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات لغو شد.")


# =====================================================================================
# ⚠️ هندلرهای مشترک «هشدار اولیه فریز» برای دستورات تک‌نفره (give/take/treasury_give/treasury_take)
# =====================================================================================
@admin_router.callback_query(AdminConfirmForm.waiting_for_frozen_ack, F.data == "sfwarn_continue")
async def cb_admin_frozen_continue(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید ادامه دهید.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, admin_id)
    text, kb = _build_admin_give_take_dialog(data)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await state.set_state(AdminConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, admin_id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(AdminConfirmForm.waiting_for_frozen_ack, F.data == "sfwarn_cancel")
async def cb_admin_frozen_cancel(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات لغو شد.")


# =====================================================================================
# ⚠️ ابزار و هندلرهای مشترک هشدار/انتخاب فریز برای عملیات گروهی (rewardgroup و group_salary)
# =====================================================================================
def _build_group_path_dialog(kind: str, g_name: str, amount: int, normal_count: int, frozen_count: int, include_frozen: bool):
    """متن و کیبورد مرحله «تأیید اولیه» عملیات گروهی را می‌سازد (بر اساس مسیر انتخاب‌شده)."""
    safe_g_name = html.escape(g_name)
    if frozen_count == 0:
        total = normal_count * amount
        if kind == "rewardgroup":
            text = (
                f"🎁 <b>تأیید واریز همگانی</b>\n"
                f"آیا از واریز مبلغ <code>₳ {amount}</code> به حساب تمام اعضای گروه «{safe_g_name}» "
                f"(تعداد: {normal_count} نفر) اطمینان دارید؟"
            )
        else:
            text = (
                f"⚠️ <b>تأیید عملیات گروهی</b>\n\n"
                f"👥 گروه/دسته‌بندی: <b>{safe_g_name}</b>\n"
                f"👤 تعداد اعضا: <code>{normal_count}</code>\n"
                f"💰 مبلغ فردی: <code>₳ {amount}</code>\n"
                f"🧮 مجموع: <code>₳ {total}</code>\n\n"
                f"آیا تأیید می‌کنید؟"
            )
    elif include_frozen:
        total = (normal_count + frozen_count) * amount
        text = (
            f"👤 <code>{normal_count}</code> کاربر عادی و <code>{frozen_count}</code> کاربر فریز وجود دارد "
            f"که به آن‌ها پول انتقال داده می‌شود.\n"
            f"💰 مبلغ فردی: <code>₳ {amount}</code> | 🧮 مجموع: <code>₳ {total}</code>\n\n"
            f"آیا تأیید می‌کنید؟"
        )
    else:
        total = normal_count * amount
        text = (
            f"آیا مطمئن هستید که می‌خواهید فقط به کاربران بدون فریز پول انتقال دهید؟\n\n"
            f"👤 تعداد: <code>{normal_count}</code> نفر\n"
            f"💰 مبلغ فردی: <code>₳ {amount}</code> | 🧮 مجموع: <code>₳ {total}</code>"
        )

    if kind == "rewardgroup":
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ تأیید اولیه", callback_data="admin_yes"),
            InlineKeyboardButton(text="❌ لغو", callback_data="admin_no"),
        ]])
    else:  # group_salary
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ تأیید اولیه", callback_data="gsalary_yes"),
            InlineKeyboardButton(text="❌ لغو", callback_data="gsalary_no"),
        ]])
    return text, kb


_GROUP_FROZEN_ACK_STATES = StateFilter(AdminConfirmForm.waiting_for_frozen_ack, TreasuryConfirmForm.waiting_for_frozen_ack)


@admin_router.callback_query(_GROUP_FROZEN_ACK_STATES, F.data == "gfwarn_noop")
async def cb_group_frozen_noop(callback: CallbackQuery):
    await callback.answer()


@admin_router.callback_query(_GROUP_FROZEN_ACK_STATES, F.data.startswith("gfwarn_page_"))
async def cb_group_frozen_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید صفحات را تغییر دهید.", show_alert=True)

    page = int(callback.data[len("gfwarn_page_"):])
    text, kb = _group_frozen_warning_page(
        len(data["gop_normal_members"]), data["gop_frozen_members"], page, data["gop_group"], data["gop_amount"]
    )
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@admin_router.callback_query(_GROUP_FROZEN_ACK_STATES, F.data.in_(["gfwarn_include", "gfwarn_exclude"]))
async def cb_group_frozen_choice(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید انتخاب کنید.", show_alert=True)

    include_frozen = (callback.data == "gfwarn_include")
    kind = data.get("action", "group_salary")
    g_name = data["gop_group"]
    amount = data["gop_amount"]
    normal_count = len(data["gop_normal_members"])
    frozen_count = len(data["gop_frozen_members"])

    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.update_data(gop_include_frozen=include_frozen)

    text, kb = _build_group_path_dialog(kind, g_name, amount, normal_count, frozen_count, include_frozen)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass

    new_state = AdminConfirmForm.waiting_for_confirm if kind == "rewardgroup" else TreasuryConfirmForm.waiting_for_confirm
    await state.set_state(new_state)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, admin_id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.message(Command("rewardgroup"))
async def cmd_reward_group(message: Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")

    args = message.text.split(maxsplit=3)
    if len(args) < 3:
        return await message.reply(
            "استفاده: <code>/rewardgroup [گروه] [مقدار] [دلیل]</code>",
            parse_mode="HTML"
        )

    g_name = args[1]
    try:
        amount = int(args[2])
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است.")
    reason = args[3] if len(args) > 3 else "پاداش گروهی مدیریت"
    if amount <= 0:
        return await message.reply("❌ مقدار نامعتبر است.")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, full_name, username, is_frozen FROM users WHERE group_name = ?",
            (g_name,),
        ) as cur:
            all_members = await cur.fetchall()

    if not all_members:
        return await message.reply("❌ هیچ کاربری در این گروه یافت نشد.")

    normal_members = [m for m in all_members if not m["is_frozen"]]
    frozen_members = [m for m in all_members if m["is_frozen"]]

    await state.update_data(
        action="rewardgroup",
        gop_group=g_name,
        gop_amount=amount,
        gop_reason=reason,
        gop_normal_members=normal_members,
        gop_frozen_members=frozen_members,
        admin_id=message.from_user.id,
    )

    if frozen_members:
        text, kb = _group_frozen_warning_page(len(normal_members), frozen_members, 0, g_name, amount)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(AdminConfirmForm.waiting_for_frozen_ack)
    else:
        text, kb = _build_group_path_dialog("rewardgroup", g_name, amount, len(normal_members), 0, False)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(AdminConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.message(Command("undo"))
async def cmd_undo(message: Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        return await message.reply(
            "استفاده: <code>/undo [شناسه_تراکنش] [دلیل_اختیاری]</code>",
            parse_mode="HTML"
        )

    tx_id = args[1]
    reason = args[2] if len(args) > 2 else "لغو تراکنش توسط مدیریت ارشد"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT from_user, to_user, amount, status FROM audit_logs WHERE tx_id = ?", (tx_id,)
        ) as cur:
            tx = await cur.fetchone()

    if not tx:
        return await message.reply("❌ تراکنشی با این شناسه یافت نشد.")
    if tx["status"] == "REFUNDED":
        return await message.reply("❌ این تراکنش قبلاً باطل شده است.")

    safe_tx_id = html.escape(tx_id)
    preview_text = (
        "🔄 <b>هشدار بازگردانی تراکنش</b>\n"
        f"آیا از لغو و معکوس‌سازی تراکنش {safe_tx_id} مطمئن هستید؟ مبالغ به حساب اولیه بازخواهند گشت."
    )
    await _start_ops_confirmation(message, state, "undo_tx", preview_text, op_tx_id=tx_id, op_reason=reason)


@admin_router.message(Command("economy"))
async def cmd_economy(message: Message):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(user_id), SUM(balance) FROM users"
        ) as cur:
            row = await cur.fetchone()
        async with db.execute(
            "SELECT COALESCE(SUM(bank_savings), 0), COALESCE(SUM(frozen_balance), 0) FROM users"
        ) as cur2:
            bank_row = await cur2.fetchone()
        async with db.execute(
            "SELECT COUNT(*), COALESCE(SUM(total_amount), 0) FROM loans WHERE status = 'ACTIVE'"
        ) as cur3:
            loans_row = await cur3.fetchone()

    treasury_balance = await get_treasury_balance()
    await message.reply(
        f"👥 کل اعضا: <code>{row[0]}</code>\n"
        f"💰 حجم نقدینگی در گردش: <code>₳ {row[1] or 0}</code>\n"
        f"🏛 موجودی خزانه مرکزی: <code>₳ {treasury_balance}</code>\n"
        f"🏦 مجموع سپرده‌های بانکی: <code>₳ {bank_row[0]}</code>\n"
        f"🔒 مجموع وثیقه‌های قفل‌شده: <code>₳ {bank_row[1]}</code>\n"
        f"💳 وام‌های فعال: <code>{loans_row[0]}</code> عدد به مبلغ <code>₳ {loans_row[1]}</code>",
        parse_mode="HTML"
    )


TREASURY_TX_PAGE_SIZE = 10


def _render_treasury_page(treasury_balance: int, txs, page: int):
    total = len(txs)
    total_pages = max(1, math.ceil(total / TREASURY_TX_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * TREASURY_TX_PAGE_SIZE
    page_items = txs[start:start + TREASURY_TX_PAGE_SIZE]

    txt = f"🏛 <b>تاریخچه تراکنش‌های خزانه مرکزی (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"موجودی فعلی خزانه: <code>₳ {treasury_balance}</code>\n\n"
    if not page_items:
        txt += "📜 هیچ تراکنشی ثبت نشده است."
    for tx in page_items:
        direction = "➕ ورودی" if tx["to_user"] == TREASURY_USER_ID else "➖ خروجی"
        safe_reason = html.escape(tx["reason"] or "-")
        txt += (
            f"🔹 <code>{tx['tx_id']}</code> | {direction} | <code>₳ {tx['amount']}</code> | "
            f"{safe_reason} | <code>{tx['timestamp']}</code>\n"
        )

    kb = _build_pagination_keyboard(page, total_pages, "treasury_page", refresh_data="treasury_refresh")
    return txt, kb


async def _fetch_treasury_view():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT balance FROM users WHERE user_id = ?", (TREASURY_USER_ID,)
        ) as cur:
            treasury_user = await cur.fetchone()

        async with db.execute(
            "SELECT tx_id, timestamp, from_user, to_user, amount, reason FROM audit_logs "
            "WHERE from_user = ? OR to_user = ? ORDER BY timestamp DESC",
            (TREASURY_USER_ID, TREASURY_USER_ID)
        ) as cur2:
            txs = await cur2.fetchall()

    treasury_balance = treasury_user["balance"] if treasury_user else 0
    return treasury_balance, txs


@admin_router.message(Command("treasury"))
async def cmd_treasury(message: Message):
    if not is_private(message):
        return
    user_id = message.from_user.id
    if not (is_super_admin(user_id) or user_id == TREASURY_USER_ID):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین‌ها و حساب خزانه مرکزی است.")

    treasury_balance, txs = await _fetch_treasury_view()
    txt, kb = _render_treasury_page(treasury_balance, txs, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "treasury_page_noop")
async def cb_treasury_noop(callback: CallbackQuery):
    await callback.answer()


async def _treasury_reload(callback: CallbackQuery, page: int):
    user_id = callback.from_user.id
    if not (is_super_admin(user_id) or user_id == TREASURY_USER_ID):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    treasury_balance, txs = await _fetch_treasury_view()
    txt, kb = _render_treasury_page(treasury_balance, txs, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("treasury_page_"))
async def cb_treasury_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    await _treasury_reload(callback, page)
    await callback.answer()


@admin_router.callback_query(F.data == "treasury_refresh")
async def cb_treasury_refresh(callback: CallbackQuery):
    page = _extract_current_page(callback.message.text or "")
    await _treasury_reload(callback, page)
    await callback.answer("🔄 لیست به‌روزرسانی شد.")


@admin_router.message(Command("treasury_add"))
async def cmd_treasury_add(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        return await message.reply(
            "❌ ساختار: <code>/treasury_add [مقدار] [دلیل_اختیاری]</code>", parse_mode="HTML"
        )
    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("❌ ورودی نامعتبر.")
    reason = args[2] if len(args) > 2 else "افزایش دستی خزانه توسط سوپرادمین"
    if amount <= 0:
        return await message.reply("❌ مقدار باید مثبت باشد.")

    treasury_balance = await get_treasury_balance()
    if treasury_balance + amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ خطا: سقف موجودی خزانه.")

    safe_reason = html.escape(reason)
    await state.update_data(
        action="treasury_add", amount=amount, reason=reason, admin_id=message.from_user.id,
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ تایید", callback_data="admin_yes"),
            InlineKeyboardButton(text="❌ انصراف", callback_data="admin_no"),
        ]]
    )
    confirm_msg = await message.reply(
        f"🏛 <b>تأیید افزایش موجودی خزانه</b>\n"
        f"مبلغ: <code>₳ {amount}</code> | دلیل: {safe_reason}\n"
        f"آیا این تراکنش ورودی به خزانه مرکزی را تأیید می‌کنید؟",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.set_state(AdminConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.message(Command("treasury_sub"))
async def cmd_treasury_sub(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        return await message.reply(
            "❌ ساختار: <code>/treasury_sub [مقدار] [دلیل_اختیاری]</code>", parse_mode="HTML"
        )
    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("❌ ورودی نامعتبر.")
    reason = args[2] if len(args) > 2 else "کاهش دستی خزانه توسط سوپرادمین"
    if amount <= 0:
        return await message.reply("❌ مقدار باید مثبت باشد.")

    treasury_balance = await get_treasury_balance()
    if treasury_balance < amount:
        return await message.reply("❌ موجودی خزانه کافی نیست.")

    safe_reason = html.escape(reason)
    await state.update_data(
        action="treasury_sub", amount=amount, reason=reason, admin_id=message.from_user.id,
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ تایید", callback_data="admin_yes"),
            InlineKeyboardButton(text="❌ انصراف", callback_data="admin_no"),
        ]]
    )
    confirm_msg = await message.reply(
        f"🚨 <b>هشدار برداشت از خزانه مرکزی</b>\n"
        f"مبلغ: <code>₳ {amount}</code> | دلیل: {safe_reason}\n"
        f"آیا از کسر این مبلغ از خزانه مرکزی اطمینان دارید؟",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.set_state(AdminConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


# =====================================================================================
# 🏛 دستور /treasury_give: انتقال مستقیم و یک‌باره از خزانه مرکزی به یک کاربر
# =====================================================================================
def _build_tgive_dialog(target: int, target_name: str, amount: int, treasury_balance: int, token: str):
    """متن و کیبورد مرحله «تأیید اولیه» برای /treasury_give."""
    safe_target_name = html.escape(target_name or "ناشناس")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید اولیه", callback_data=f"tgive_yes_{token}"),
        InlineKeyboardButton(text="❌ لغو", callback_data=f"tgive_no_{token}"),
    ]])
    text = (
        f"⚠️ <b>تأیید انتقال از خزانه مرکزی</b>\n\n"
        f"👤 گیرنده: <b>{safe_target_name}</b> (<code>{target}</code>)\n"
        f"💰 مبلغ: <code>₳ {amount}</code>\n"
        f"🏛 موجودی فعلی خزانه: <code>₳ {treasury_balance}</code>\n\n"
        f"آیا مطمئن هستید؟"
    )
    return text, kb


def _build_tgive_final_dialog(target: int, amount: int, token: str):
    """متن و کیبورد مرحله «تأیید نهایی» برای /treasury_give."""
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید نهایی", callback_data=f"tgive_final_yes_{token}"),
        InlineKeyboardButton(text="❌ لغو", callback_data=f"tgive_final_no_{token}"),
    ]])
    text = (
        f"⚠️ <b>تأیید نهایی</b>\n\n"
        f"با تأیید نهایی، مبلغ <code>₳ {amount}</code> از خزانه مرکزی به حساب <code>{target}</code> واریز خواهد شد.\n"
        f"این عملیات قابل بازگشت نیست.\n\n"
        f"آیا کاملاً مطمئن هستید؟"
    )
    return text, kb


@admin_router.message(Command("treasury_give"))
async def cmd_treasury_give(message: Message, state: FSMContext):
    if not is_private(message):
        return
    if message.from_user.id != TREASURY_USER_ID:
        return

    text = message.text.strip()
    if text.startswith("/treasury_give"):
        text = text[len("/treasury_give"):].strip()

    # حالت ریپلای روی پیام کاربر: فقط مبلغ لازم است
    if message.reply_to_message and message.reply_to_message.from_user:
        try:
            amount = int(text)
        except ValueError:
            return await message.reply("❌ مبلغ وارد شده نامعتبر است.")
        target = message.reply_to_message.from_user.id
    else:
        parts = text.split()
        if len(parts) < 2:
            return await message.reply(
                "❌ ساختار: <code>/treasury_give [آیدی] [مبلغ]</code>\n"
                "یا با ریپلای روی پیام کاربر: <code>/treasury_give [مبلغ]</code>",
                parse_mode="HTML",
            )
        try:
            target, amount = int(parts[0]), int(parts[1])
        except ValueError:
            return await message.reply("❌ آیدی و مبلغ باید عدد صحیح باشند.")

    if amount <= 0 or amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ مبلغ باید یک عدد صحیح مثبت و کمتر از سقف مجاز باشد.")
    if target == TREASURY_USER_ID:
        return await message.reply("❌ امکان انتقال از خزانه به خودش وجود ندارد.")

    target_data = await get_user_data(target)
    if not target_data:
        return await message.reply("❌ کاربر مقصد یافت نشد.")

    treasury_balance = await get_treasury_balance()
    if treasury_balance < amount:
        return await message.reply("❌ موجودی خزانه کافی نیست.")
    if target_data["balance"] + amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ خطا: سقف موجودی مقصد.")

    # 🔐 شناسه یکتا برای جلوگیری از اجرای تکراری/کلیک روی دکمه‌های قدیمی
    token = uuid.uuid4().hex[:12]
    await state.update_data(
        tgive_target=target, tgive_amount=amount, tgive_token=token,
        tgive_target_name=target_data["full_name"], tgive_treasury_balance=treasury_balance,
    )

    if target_data["is_frozen"]:
        # ⚠️ مقصد فریز است: عملیات رد نمی‌شود، فقط قبل از ادامه هشدار داده می‌شود
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ ادامه", callback_data=f"tgive_frozen_yes_{token}"),
            InlineKeyboardButton(text="❌ لغو", callback_data=f"tgive_frozen_no_{token}"),
        ]])
        confirm_msg = await message.reply(
            _frozen_target_card_text(target_data, target), reply_markup=kb, parse_mode="HTML"
        )
        await state.set_state(TreasuryConfirmForm.waiting_for_frozen_ack)
    else:
        dtext, kb = _build_tgive_dialog(target, target_data["full_name"], amount, treasury_balance, token)
        confirm_msg = await message.reply(dtext, reply_markup=kb, parse_mode="HTML")
        await state.set_state(TreasuryConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_frozen_ack, F.data.startswith("tgive_frozen_yes_"))
async def cb_treasury_give_frozen_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند ادامه دهد.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("tgive_frozen_yes_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    text, kb = _build_tgive_dialog(
        data["tgive_target"], data.get("tgive_target_name"), data["tgive_amount"], data["tgive_treasury_balance"], token
    )
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await state.set_state(TreasuryConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_frozen_ack, F.data.startswith("tgive_frozen_no_"))
async def cb_treasury_give_frozen_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("tgive_frozen_no_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ انتقال از خزانه لغو شد.")


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("tgive_yes_"))
async def cb_treasury_give_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند تأیید کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("tgive_yes_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    # ⚠️ طبق فلوی جدید، پس از «تأیید اولیه» باید «تأیید نهایی» نمایش داده شود؛ اجرا فقط بعد از آن است
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    text, kb = _build_tgive_final_dialog(data["tgive_target"], data["tgive_amount"], token)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("tgive_final_yes_"))
async def cb_treasury_give_final_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند تأیید کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("tgive_final_yes_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()

    target = data["tgive_target"]
    amount = data["tgive_amount"]

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (target,)) as cur_u:
                target_row = await cur_u.fetchone()

            if not target_row or target_row["balance"] + amount > MAX_BALANCE_LIMIT:
                await db.rollback()
                return await callback.message.edit_text("❌ خطا در وضعیت حساب مقصد؛ تراکنش لغو شد.")

            # 🏛 کسر از خزانه (با ثبت خودکار یک ردیف audit_log توسط همین تابع)
            ok = await treasury_debit(
                db, amount, f"[TREASURY_GIVE] انتقال مستقیم خزانه به کاربر {target}", related_user=target
            )
            if not ok:
                await db.rollback()
                return await callback.message.edit_text("❌ موجودی خزانه کافی نیست.")

            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, target))
            await db.commit()

    await callback.message.edit_text(
        f"✅ مبلغ <code>₳ {amount}</code> از خزانه مرکزی به حساب <code>{target}</code> واریز شد.",
        parse_mode="HTML",
    )
    try:
        await callback.bot.send_message(
            target,
            f"🏛 مبلغ <code>₳ {amount}</code> از خزانه مرکزی آترامنتوم به حساب شما واریز شد.",
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("tgive_final_no_"))
async def cb_treasury_give_final_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("tgive_final_no_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ انتقال از خزانه لغو شد.")


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("tgive_no_"))
async def cb_treasury_give_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("tgive_no_"):]
    if not data or token != data.get("tgive_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ انتقال از خزانه لغو شد.")


# =====================================================================================
# 🏛 دستور /treasury_take: انتقال مستقیم و یک‌باره از موجودی آزاد یک کاربر به خزانه مرکزی
# =====================================================================================
def _build_ttake_dialog(target: int, target_name: str, amount: int, target_transferable: int, treasury_balance: int, token: str):
    """متن و کیبورد مرحله «تأیید اولیه» برای /treasury_take."""
    safe_target_name = html.escape(target_name or "ناشناس")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید اولیه", callback_data=f"ttake_yes_{token}"),
        InlineKeyboardButton(text="❌ لغو", callback_data=f"ttake_no_{token}"),
    ]])
    text = (
        f"⚠️ <b>تأیید برداشت به خزانه مرکزی</b>\n\n"
        f"👤 از حساب: <b>{safe_target_name}</b> (<code>{target}</code>)\n"
        f"💰 مبلغ: <code>₳ {amount}</code>\n"
        f"🔓 موجودی آزاد فعلی کاربر: <code>₳ {target_transferable}</code>\n"
        f"🏛 موجودی فعلی خزانه: <code>₳ {treasury_balance}</code>\n\n"
        f"آیا مطمئن هستید؟"
    )
    return text, kb


def _build_ttake_final_dialog(target: int, amount: int, token: str):
    """متن و کیبورد مرحله «تأیید نهایی» برای /treasury_take."""
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید نهایی", callback_data=f"ttake_final_yes_{token}"),
        InlineKeyboardButton(text="❌ لغو", callback_data=f"ttake_final_no_{token}"),
    ]])
    text = (
        f"⚠️ <b>تأیید نهایی</b>\n\n"
        f"با تأیید نهایی، مبلغ <code>₳ {amount}</code> از حساب <code>{target}</code> به خزانه مرکزی منتقل خواهد شد.\n"
        f"این عملیات قابل بازگشت نیست.\n\n"
        f"آیا کاملاً مطمئن هستید؟"
    )
    return text, kb


@admin_router.message(Command("treasury_take"))
async def cmd_treasury_take(message: Message, state: FSMContext):
    if not is_private(message):
        return
    if message.from_user.id != TREASURY_USER_ID:
        return

    text = message.text.strip()
    if text.startswith("/treasury_take"):
        text = text[len("/treasury_take"):].strip()

    # حالت ریپلای روی پیام کاربر: فقط مبلغ لازم است
    if message.reply_to_message and message.reply_to_message.from_user:
        try:
            amount = int(text)
        except ValueError:
            return await message.reply("❌ مبلغ وارد شده نامعتبر است.")
        target = message.reply_to_message.from_user.id
    else:
        parts = text.split()
        if len(parts) < 2:
            return await message.reply(
                "❌ ساختار: <code>/treasury_take [آیدی] [مبلغ]</code>\n"
                "یا با ریپلای روی پیام کاربر: <code>/treasury_take [مبلغ]</code>",
                parse_mode="HTML",
            )
        try:
            target, amount = int(parts[0]), int(parts[1])
        except ValueError:
            return await message.reply("❌ آیدی و مبلغ باید عدد صحیح باشند.")

    if amount <= 0 or amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ مبلغ باید یک عدد صحیح مثبت و کمتر از سقف مجاز باشد.")
    if target == TREASURY_USER_ID:
        return await message.reply("❌ امکان انتقال از خزانه به خودش وجود ندارد.")

    target_data = await get_user_data(target)
    if not target_data:
        return await message.reply("❌ کاربر مقصد یافت نشد.")

    # 🔓 فقط موجودی آزاد (غیر از مبالغ فریزشده مثل وثیقه وام یا سفارش‌های در حال ارسال) قابل‌برداشت است
    target_transferable = max(0, target_data["balance"] - target_data["frozen_balance"])
    if target_transferable < amount:
        return await message.reply(
            f"❌ موجودی آزاد کاربر کافی نیست. موجودی آزاد فعلی: <code>₳ {target_transferable}</code>",
            parse_mode="HTML",
        )

    treasury_balance = await get_treasury_balance()
    if treasury_balance + amount > MAX_BALANCE_LIMIT:
        return await message.reply("❌ خطا: سقف موجودی خزانه.")

    # 🔐 شناسه یکتا برای جلوگیری از اجرای تکراری/کلیک روی دکمه‌های قدیمی
    token = uuid.uuid4().hex[:12]
    await state.update_data(
        ttake_target=target, ttake_amount=amount, ttake_token=token,
        ttake_target_name=target_data["full_name"],
        ttake_target_transferable=target_transferable, ttake_treasury_balance=treasury_balance,
    )

    if target_data["is_frozen"]:
        # ⚠️ مقصد فریز است: عملیات رد نمی‌شود، فقط قبل از ادامه هشدار داده می‌شود
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ ادامه", callback_data=f"ttake_frozen_yes_{token}"),
            InlineKeyboardButton(text="❌ لغو", callback_data=f"ttake_frozen_no_{token}"),
        ]])
        confirm_msg = await message.reply(
            _frozen_target_card_text(target_data, target), reply_markup=kb, parse_mode="HTML"
        )
        await state.set_state(TreasuryConfirmForm.waiting_for_frozen_ack)
    else:
        dtext, kb = _build_ttake_dialog(
            target, target_data["full_name"], amount, target_transferable, treasury_balance, token
        )
        confirm_msg = await message.reply(dtext, reply_markup=kb, parse_mode="HTML")
        await state.set_state(TreasuryConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_frozen_ack, F.data.startswith("ttake_frozen_yes_"))
async def cb_treasury_take_frozen_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند ادامه دهد.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("ttake_frozen_yes_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    text, kb = _build_ttake_dialog(
        data["ttake_target"], data.get("ttake_target_name"), data["ttake_amount"],
        data["ttake_target_transferable"], data["ttake_treasury_balance"], token
    )
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await state.set_state(TreasuryConfirmForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_frozen_ack, F.data.startswith("ttake_frozen_no_"))
async def cb_treasury_take_frozen_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("ttake_frozen_no_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ برداشت به خزانه لغو شد.")


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("ttake_yes_"))
async def cb_treasury_take_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند تأیید کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("ttake_yes_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    # ⚠️ طبق فلوی جدید، پس از «تأیید اولیه» باید «تأیید نهایی» نمایش داده شود؛ اجرا فقط بعد از آن است
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    text, kb = _build_ttake_final_dialog(data["ttake_target"], data["ttake_amount"], token)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("ttake_final_yes_"))
async def cb_treasury_take_final_yes(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند تأیید کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("ttake_final_yes_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()

    target = data["ttake_target"]
    amount = data["ttake_amount"]

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT balance, frozen_balance FROM users WHERE user_id = ?", (target,)
            ) as cur_u:
                target_row = await cur_u.fetchone()

            target_transferable = max(0, target_row["balance"] - target_row["frozen_balance"]) if target_row else 0
            treasury_balance = await get_treasury_balance()

            if not target_row or target_transferable < amount or treasury_balance + amount > MAX_BALANCE_LIMIT:
                await db.rollback()
                return await callback.message.edit_text("❌ خطا در وضعیت حساب؛ تراکنش لغو شد.")

            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, target))
            # 🏛 واریز به خزانه (با ثبت خودکار یک ردیف audit_log توسط همین تابع)
            await treasury_credit(
                db, amount, f"[TREASURY_TAKE] انتقال مستقیم از کاربر {target} به خزانه", related_user=target
            )
            await db.commit()

    await callback.message.edit_text(
        f"✅ مبلغ <code>₳ {amount}</code> از حساب <code>{target}</code> به خزانه مرکزی منتقل شد.",
        parse_mode="HTML",
    )
    try:
        await callback.bot.send_message(
            target,
            f"🏛 مبلغ <code>₳ {amount}</code> از حساب شما به خزانه مرکزی آترامنتوم منتقل شد.",
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("ttake_final_no_"))
async def cb_treasury_take_final_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)
    data = await state.get_data()
    token = callback.data[len("ttake_final_no_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ برداشت به خزانه لغو شد.")


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data.startswith("ttake_no_"))
async def cb_treasury_take_no(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != TREASURY_USER_ID:
        return await callback.answer("❌ فقط حساب خزانه مرکزی می‌تواند لغو کند.", show_alert=True)

    data = await state.get_data()
    token = callback.data[len("ttake_no_"):]
    if not data or token != data.get("ttake_token"):
        return await callback.answer("❌ این دکمه دیگر معتبر نیست.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ برداشت به خزانه لغو شد.")


# =====================================================================================
# 🏛 دستور /group_salary: واریز اتمیک حقوق گروهی از خزانه به اعضای یک گروه
# (با فلوی کامل هشدار فریز ⬅️ تأیید اولیه ⬅️ تأیید نهایی ⬅️ اجرا، تماماً با Edit همان پیام)
# =====================================================================================
@admin_router.message(Command("group_salary"))
async def cmd_group_salary(message: Message, state: FSMContext):
    if not is_private(message):
        return
    if message.from_user.id != TREASURY_USER_ID:
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply(
            "❌ ساختار: <code>/group_salary [گروه] [مبلغ_هر_نفر]</code>", parse_mode="HTML"
        )

    g_name = args[1]
    try:
        per_person = int(args[2])
    except ValueError:
        return await message.reply("❌ مبلغ هر نفر باید عدد صحیح باشد.")
    if per_person <= 0 or per_person > MAX_BALANCE_LIMIT:
        return await message.reply("❌ مبلغ هر نفر باید یک عدد صحیح مثبت و کمتر از سقف مجاز باشد.")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # 🏛 خزانه همیشه از دریافت‌کنندگان حذف است، حتی اگر گروهش با گروه هدف یکی باشد
        async with db.execute(
            "SELECT user_id, full_name, username, balance, is_frozen FROM users WHERE group_name = ? AND user_id != ?",
            (g_name, TREASURY_USER_ID),
        ) as cur:
            all_members = await cur.fetchall()

    if not all_members:
        return await message.reply("❌ هیچ عضوی در این گروه یافت نشد.")

    normal_members = [m for m in all_members if not m["is_frozen"]]
    frozen_members = [m for m in all_members if m["is_frozen"]]

    await state.update_data(
        action="group_salary",
        gop_group=g_name,
        gop_amount=per_person,
        gop_normal_members=normal_members,
        gop_frozen_members=frozen_members,
        admin_id=message.from_user.id,
    )

    if frozen_members:
        text, kb = _group_frozen_warning_page(len(normal_members), frozen_members, 0, g_name, per_person)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(TreasuryConfirmForm.waiting_for_frozen_ack)
    else:
        text, kb = _build_group_path_dialog("group_salary", g_name, per_person, len(normal_members), 0, False)
        confirm_msg = await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(TreasuryConfirmForm.waiting_for_confirm)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data == "gsalary_yes")
async def cb_group_salary_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید تأیید کنید.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, admin_id)
    g_name = data["gop_group"]
    amount = data["gop_amount"]
    include_frozen = data.get("gop_include_frozen", False)
    normal_count = len(data["gop_normal_members"])
    frozen_count = len(data["gop_frozen_members"]) if include_frozen else 0
    recipient_count = normal_count + frozen_count
    total = recipient_count * amount
    safe_g_name = html.escape(g_name)

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید نهایی", callback_data="gsalary_final_yes"),
        InlineKeyboardButton(text="❌ لغو", callback_data="gsalary_final_no"),
    ]])
    try:
        await callback.message.edit_text(
            f"⚠️ <b>تأیید نهایی</b>\n\n"
            f"با تأیید نهایی، مبلغ <code>₳ {amount}</code> به <code>{recipient_count}</code> نفر از گروه «<b>{safe_g_name}</b>» "
            f"(مجموعاً <code>₳ {total}</code>) از خزانه مرکزی واریز خواهد شد.\n"
            f"این عملیات قابل بازگشت نیست.\n\n"
            f"آیا کاملاً مطمئن هستید؟",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception:
        pass
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, admin_id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data == "gsalary_no")
async def cb_group_salary_no(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات لغو شد.")


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data == "gsalary_final_yes")
async def cb_group_salary_final_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید تأیید کنید.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()

    g_name = data["gop_group"]
    per_person = data["gop_amount"]
    include_frozen = data.get("gop_include_frozen", False)
    normal_members = data["gop_normal_members"]
    frozen_members = data["gop_frozen_members"] if include_frozen else []
    members = list(normal_members) + list(frozen_members)
    ids = [m["user_id"] for m in members]

    if not ids:
        return await callback.message.edit_text("❌ هیچ عضوی برای پرداخت یافت نشد.")

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            # 🔄 بازخوانی تازه موجودی اعضا و خزانه بلافاصله قبل از اجرا (محافظت در برابر race condition)
            placeholders = ",".join("?" for _ in ids)
            async with db.execute(
                f"SELECT user_id, balance FROM users WHERE user_id IN ({placeholders})", ids
            ) as cur:
                fresh_members = await cur.fetchall()

            total = len(fresh_members) * per_person

            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (TREASURY_USER_ID,)) as cur_t:
                treasury_row = await cur_t.fetchone()
            treasury_balance = treasury_row["balance"] if treasury_row else 0

            if treasury_balance < total:
                await db.rollback()
                return await callback.message.edit_text("❌ موجودی خزانه کافی نیست؛ عملیات لغو شد.")

            over_cap = [m for m in fresh_members if m["balance"] + per_person > MAX_BALANCE_LIMIT]
            if over_cap:
                await db.rollback()
                return await callback.message.edit_text(
                    f"❌ عملیات لغو شد: موجودی {len(over_cap)} نفر از اعضا از سقف مجاز عبور می‌کند."
                )

            # 🏛 کسر یک‌باره کل مبلغ از خزانه (بدون لاگ کلی؛ لاگ‌ها جداگانه و به ازای هر عضو ثبت می‌شوند)
            await db.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?", (total, TREASURY_USER_ID)
            )

            batch_id = f"GS-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
            ts = datetime.now(timezone.utc).isoformat()
            for idx, m in enumerate(fresh_members, start=1):
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?", (per_person, m["user_id"])
                )
                await db.execute(
                    "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        f"{batch_id}-{idx}", ts, TREASURY_USER_ID, m["user_id"], per_person,
                        f"[GROUP_SALARY] batch_id={batch_id} گروه={g_name}",
                    ),
                )

            await db.commit()

    safe_g_name = html.escape(g_name)
    await callback.message.edit_text(
        f"✅ حقوق گروهی برای <code>{len(fresh_members)}</code> نفر از گروه «<b>{safe_g_name}</b>» با موفقیت واریز شد.\n"
        f"💰 مبلغ هر نفر: <code>₳ {per_person}</code> | مبلغ کل: <code>₳ {total}</code>",
        parse_mode="HTML",
    )


@admin_router.callback_query(TreasuryConfirmForm.waiting_for_confirm, F.data == "gsalary_final_no")
async def cb_group_salary_final_no(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    admin_id = data.get("admin_id")
    if callback.from_user.id != admin_id:
        return await callback.answer("❌ فقط خودتان می‌توانید لغو کنید.", show_alert=True)
    cancel_input_timeout(callback.message.chat.id, admin_id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات لغو شد.")


@admin_router.message(Command("promote"))
async def cmd_promote(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/promote [آیدی]</code>", parse_mode="HTML")
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "🎖 <b>تأیید ارتقا به ادمین</b>\n"
        f"آیا از اعطای اختیارات ادمینی به کاربر {target_id} اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "promote", preview_text, op_user_id=target_id)


@admin_router.message(Command("demote"))
async def cmd_demote(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/demote [آیدی]</code>", parse_mode="HTML")
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "🔻 <b>تأیید سلب مقام ادمین</b>\n"
        f"آیا از حذف دسترسی‌های ادمینی کاربر {target_id} مطمئن هستید؟"
    )
    await _start_ops_confirmation(message, state, "demote", preview_text, op_user_id=target_id)


@admin_router.message(Command("list_admins"))
async def cmd_list_admins(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    txt = "👑 <b>لیست سوپرادمین‌ها:</b>\n"
    for sa_id in sorted(SUPER_ADMINS):
        u = await get_user_data(sa_id)
        name = html.escape(u["full_name"]) if u and u["full_name"] else "ناشناس"
        txt += f"- <b>{name}</b> | <code>{sa_id}</code>\n"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, full_name FROM users WHERE is_admin = 1"
        ) as cur:
            admins = await cur.fetchall()

    txt += "\n👥 <b>لیست ادمین‌های معمولی:</b>\n"
    if admins:
        for a in admins:
            if a["user_id"] not in SUPER_ADMINS:
                safe_name = html.escape(a["full_name"] or "ناشناس")
                txt += f"- <b>{safe_name}</b> | <code>{a['user_id']}</code>\n"
    else:
        txt += "- هیچ ادمین معمولی وجود ندارد.\n"

    await message.reply(txt, parse_mode="HTML")


@admin_router.message(Command("add_super"))
async def cmd_add_super(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/add_super [آیدی]</code>", parse_mode="HTML")
    try:
        new_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")

    if new_id in SUPER_ADMINS:
        return await message.reply("ℹ️ این کاربر از قبل سوپرادمین است.")

    preview_text = (
        "👑 <b>هشدار حساس: اعطای دسترسی سوپرادمین</b>\n"
        f"آیا از افزودن کاربر {new_id} به لیست سوپرادمین‌ها (دسترسی کامل به خزانه و تنظیمات) مطمئن هستید؟"
    )
    await _start_ops_confirmation(message, state, "add_super", preview_text, op_user_id=new_id)


@admin_router.message(Command("remove_super"))
async def cmd_remove_super(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/remove_super [آیدی]</code>", parse_mode="HTML")
    try:
        rem_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")

    if rem_id in (SUPER_ADMIN_1, SUPER_ADMIN_2):
        return await message.reply("❌ سوپرادمین‌های پایه قابل حذف نیستند.")

    if rem_id not in SUPER_ADMINS:
        return await message.reply("ℹ️ این کاربر سوپرادمین نیست.")

    preview_text = (
        "⚠️ <b>تأیید عزل سوپرادمین</b>\n"
        f"آیا از سلب دسترسی سوپرادمین از کاربر {rem_id} اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "remove_super", preview_text, op_user_id=rem_id)


@admin_router.message(Command("freeze"))
async def cmd_freeze(message: Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/freeze [آیدی]</code>", parse_mode="HTML")
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "❄️ <b>هشدار مسدودسازی حساب</b>\n"
        f"آیا از فریز و مسدود کردن تمام فعالیت‌های کاربر {target_id} اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "freeze", preview_text, op_user_id=target_id)


@admin_router.message(Command("unfreeze"))
async def cmd_unfreeze(message: Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/unfreeze [آیدی]</code>", parse_mode="HTML")
    try:
        target_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "🟢 <b>تأیید فعال‌سازی حساب</b>\n"
        f"آیا از خروج حساب کاربر {target_id} از حالت فریز اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "unfreeze", preview_text, op_user_id=target_id)


@admin_router.message(Command("check"))
async def cmd_check(message: Message):
    if not is_super_admin(message.from_user.id):
        return await message.reply("❌ این دستور فقط مخصوص سوپرادمین است.")
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("استفاده: <code>/check [آیدی]</code>", parse_mode="HTML")
    u = await get_user_data(int(args[1]))
    if u:
        status = "❄️ فریز شده" if u["is_frozen"] else "🟢 فعال"
        admin_st = "👑 ادمین" if u["is_admin"] else "👤 کاربر عادی"
        safe_full_name = html.escape(u['full_name'] or 'ناشناس')
        safe_username = html.escape(u['username'] or 'بدون آیدی')
        safe_group_name = html.escape(u['group_name'] or 'Default')
        transferable = max(0, u["balance"] - u["frozen_balance"])
        extra_treasury_line = ""
        if int(args[1]) == TREASURY_USER_ID:
            extra_treasury_line = "\n🏛 <b>این حساب، خزانه مرکزی سیستم است.</b>"
        await message.reply(
            f"🔎 <b>اطلاعات کامل کاربر <code>{args[1]}</code>:</b>\n\n"
            f"👤 نام کامل: {safe_full_name}\n"
            f"🏷 نام کاربری: @{safe_username}\n"
            f"💰 موجودی: <code>₳ {u['balance']}</code>\n"
            f"🔒 وثیقه قفل‌شده: <code>₳ {u['frozen_balance']}</code>\n"
            f"💳 موجودی قابل انتقال: <code>₳ {transferable}</code>\n"
            f"🏦 سپرده بانکی: <code>₳ {u['bank_savings']}</code>\n"
            f"👥 گروه: <b>{safe_group_name}</b>\n"
            f"⚡ وضعیت: {status}\n"
            f"🛡 دسترسی: {admin_st}"
            f"{extra_treasury_line}",
            parse_mode="HTML",
        )


# --- دستور صفر کردن دیتابیس و ری‌استارت ---


@admin_router.message(Command("reset_all"))
async def cmd_reset_all(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="💣 بله، دیتابیس کاملاً پاک شود", callback_data="reset_yes"),
            InlineKeyboardButton(text="❌ انصراف", callback_data="reset_no"),
        ]]
    )
    confirm_msg = await message.reply(
        "⚠️ <b>هشدار بسیار مهم!</b>\n\n"
        "آیا مطمئن هستید؟ این دستور یک <b>ریست کامل سیستم</b> انجام می‌دهد: تمام کاربران، موجودی کیف پول‌ها، "
        "موجودی بانک‌ها، موجودی خزانه مرکزی، موجودی فروشگاه‌ها، وثیقه‌های قفل‌شده، وام‌ها، سفارش‌ها، محصولات، "
        "تراکنش‌ها، تاریخچه‌ها و کلیه داده‌های مالی و وابسته <b>حذف کاملاً دائم</b> می‌شوند.\n\n"
        "⚙️ تنها بخشی که حذف <b>نخواهد</b> شد، تنظیمات مدیریتی و درصدهای موجود در <code>/view_set_all</code> است.\n\n"
        "آیا قصد ادامه دارید؟",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await state.set_state(ResetForm.waiting_for_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, confirm_msg.message_id),
    )


@admin_router.callback_query(ResetForm.waiting_for_confirm, F.data == "reset_yes")
async def cb_reset_yes(callback: CallbackQuery, state: FSMContext):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("❌ عدم دسترسی.", show_alert=True)

    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("⏳ در حال ریست کامل سیستم (به‌جز تنظیمات مدیریتی)...")

    # ⚠️ توجه: به‌جای حذف کامل فایل دیتابیس، فقط جدول‌های داده‌ای/مالی/کاربری پاک می‌شوند
    # تا جدول system_settings (تمام درصدها و تنظیمات قابل مشاهده در /view_set_all) و جدول
    # super_admins (دسترسی‌های مدیریتی) دست‌نخورده باقی بمانند.
    tables_to_wipe = [
        "users",           # تمام کاربران + موجودی کیف پول + موجودی بانک + وثیقه‌ها (ستون‌های همین جدول)
        "audit_logs",      # تراکنش‌ها و تاریخچه‌ها
        "group_links",     # لینک‌های گروه
        "groups",          # گروه‌ها
        "shops",           # فروشگاه‌ها
        "products",        # محصولات
        "couriers",        # پستچی‌ها
        "orders",          # سفارش‌ها
        "loans",           # وام‌ها
        "loan_installments",  # اقساط وام‌ها
    ]

    try:
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                for table in tables_to_wipe:
                    await db.execute(f"DELETE FROM {table}")
                # ریست شمارنده‌های AUTOINCREMENT برای شروع تمیز شناسه‌ها (بدون تأثیر روی تنظیمات)
                try:
                    await db.execute(
                        "DELETE FROM sqlite_sequence WHERE name IN "
                        "('shops','products','orders','loans','loan_installments')"
                    )
                except Exception:
                    pass
                # بازسازی گروه پیش‌فرض
                await db.execute("INSERT OR IGNORE INTO groups (group_name) VALUES ('Default')")
                # بازسازی حساب خزانه مرکزی با موجودی صفر
                await db.execute(
                    """
                    INSERT OR IGNORE INTO users (user_id, username, full_name, balance)
                    VALUES (?, ?, ?, 0)
                    """,
                    (TREASURY_USER_ID, "Treasury", "🏛 خزانه مرکزی آترامنتوم"),
                )
                await db.commit()
                await load_super_admins(db)
    except Exception as e:
        return await callback.message.edit_text(f"❌ خطا در ریست کردن سیستم: {e}")

    await callback.message.edit_text(
        "💥 <b>سیستم با موفقیت ریست کامل شد!</b>\n\n"
        "✅ تمام کاربران، موجودی‌ها، بانک، خزانه، فروشگاه‌ها، وثیقه‌ها، وام‌ها، سفارش‌ها، محصولات، "
        "تراکنش‌ها و تاریخچه‌ها حذف شدند.\n"
        "⚙️ تنظیمات مدیریتی و درصدهای سیستم (<code>/view_set_all</code>) دست‌نخورده باقی ماندند.",
        parse_mode="HTML",
    )


@admin_router.callback_query(ResetForm.waiting_for_confirm, F.data == "reset_no")
async def cb_reset_no(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("❌ عملیات صفر کردن دیتابیس لغو شد.")


# --- دستورات بکاپ‌گیری دستی و بازیابی ---


@admin_router.message(Command("backup_now"))
async def cmd_backup_now(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    zip_path = create_zip_backup("manual")
    if zip_path and os.path.exists(zip_path):
        await message.reply_document(
            FSInputFile(zip_path), caption="<b>📦 فایل بکاپ کامل دیتابیس (ZIP)</b>", parse_mode="HTML"
        )
    else:
        await message.reply("❌ خطا در ایجاد فایل بکاپ.")


@admin_router.message(Command("force_backup"))
async def cmd_force_backup(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    if os.path.exists(DB_PATH):
        try:
            await message.bot.send_document(
                chat_id=BACKUP_CHANNEL_ID,
                document=FSInputFile(DB_PATH),
                caption=f"<b>📦 بکاپ دستی دیتابیس (توسط سوپرادمین)</b>\n⏰ {datetime.now(IRAN_TZ).strftime('%Y-%m-%d %H:%M:%S')} (به وقت ایران)",
                parse_mode="HTML"
            )
            await message.reply("✅ فایل دیتابیس با موفقیت به کانال تلگرام بکاپ ارسال شد.")
        except Exception as e:
            await message.reply(f"❌ خطا در ارسال بکاپ به کانال: {e}")
    else:
        await message.reply("❌ فایل دیتابیس یافت نشد.")


@admin_router.message(Command("restore"))
async def cmd_restore(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply(
            "❌ لطفاً این دستور را در **ریپلای (Reply)** روی یک فایل بکاپ ZIP"
            " یا db ارسال کنید."
        )

    doc = message.reply_to_message.document
    file_info = await message.bot.get_file(doc.file_id)
    download_path = f"temp_restore_{doc.file_name}"

    await message.bot.download_file(file_info.file_path, download_path)

    try:
        if download_path.endswith(".zip"):
            with zipfile.ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall("temp_extract")
            extracted_db = os.path.join("temp_extract", "atr_bank.db")
            if os.path.exists(extracted_db):
                shutil.move(extracted_db, DB_PATH)
                shutil.rmtree("temp_extract")
            else:
                os.remove(download_path)
                return await message.reply("❌ فایل `atr_bank.db` در فایل زیپ یافت نشد.")
        else:
            shutil.move(download_path, DB_PATH)

        if os.path.exists(download_path):
            os.remove(download_path)

        await message.reply(
            "<b>✅ پایگاه‌داده با موفقیت بازیابی شد!</b> ربات آماده به کار است.",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.reply(f"❌ خطا در بازیابی دیتابیس: {e}")


# --- 🛒 بخش جدید: سیستم فروشگاه، پست، و تراکنش‌های مالی جدید ---


# --- ۱. دستورات سوپرادمین برای تنظیم نرخ‌ها و مدیریت نقش‌ها ---

LIST_SHOPS_PAGE_SIZE = 10


def _render_list_shops_page(shops, page: int):
    total = len(shops)
    total_pages = max(1, math.ceil(total / LIST_SHOPS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * LIST_SHOPS_PAGE_SIZE
    page_items = shops[start:start + LIST_SHOPS_PAGE_SIZE]

    txt = f"🏪 <b>لیست فروشگاه‌های سیستم (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"مجموع فروشگاه‌ها: <code>{total}</code> فروشگاه\n\n"
    for s in page_items:
        safe_title = html.escape(s["channel_title"] or "بدون نام")
        st_text = "✅ فعال" if s["status"] == "APPROVED" else "⏳ در انتظار تایید"
        txt += (
            f"🔹 <b>{safe_title}</b> (شناسه: <code>{s['shop_id']}</code>) | "
            f"مالک: <code>{s['owner_id']}</code> | وضعیت: {st_text}\n"
        )

    kb = _build_pagination_keyboard(page, total_pages, "listshops_page", refresh_data="listshops_refresh")
    return txt, kb


@admin_router.message(Command("list_shops"))
async def cmd_list_shops(message: Message):
    """دستور جدید 1: مشاهده لیست کامل فروشگاه‌ها"""
    if not is_private(message) or not await check_admin_filter(message):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT shop_id, owner_id, channel_id, channel_title, status FROM shops") as cur:
            shops = await cur.fetchall()

    if not shops:
        return await message.reply("ℹ️ هیچ فروشگاهی در دیتابیس ثبت نشده است.")

    txt, kb = _render_list_shops_page(shops, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "listshops_page_noop")
async def cb_list_shops_noop(callback: CallbackQuery):
    await callback.answer()


async def _list_shops_reload(callback: CallbackQuery, page: int):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT shop_id, owner_id, channel_id, channel_title, status FROM shops") as cur:
            shops = await cur.fetchall()
    if not shops:
        return await callback.answer("ℹ️ هیچ فروشگاهی در دیتابیس ثبت نشده است.", show_alert=True)
    txt, kb = _render_list_shops_page(shops, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("listshops_page_"))
async def cb_list_shops_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    await _list_shops_reload(callback, page)
    await callback.answer()


@admin_router.callback_query(F.data == "listshops_refresh")
async def cb_list_shops_refresh(callback: CallbackQuery):
    page = _extract_current_page(callback.message.text or "")
    await _list_shops_reload(callback, page)
    await callback.answer("🔄 لیست به‌روزرسانی شد.")



LIST_COURIERS_PAGE_SIZE = 10


def _render_list_couriers_page(couriers, page: int):
    total = len(couriers)
    total_pages = max(1, math.ceil(total / LIST_COURIERS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * LIST_COURIERS_PAGE_SIZE
    page_items = couriers[start:start + LIST_COURIERS_PAGE_SIZE]

    txt = f"🚚 <b>لیست پستچی‌های فعال (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"تعداد پستچی‌ها: <code>{total}</code> نفر\n\n"
    for c in page_items:
        safe_name = html.escape(c["full_name"] or "ناشناس")
        txt += f"🔹 <b>{safe_name}</b> | <code>{c['user_id']}</code> | دسترسی: 🟢 فعال\n"

    kb = _build_pagination_keyboard(page, total_pages, "listcouriers_page", refresh_data="listcouriers_refresh")
    return txt, kb


@admin_router.message(Command("list_couriers"))
async def cmd_list_couriers(message: Message):
    """دستور جدید 2: مشاهده لیست پستچی‌ها به همراه آیدی عددی آن‌ها"""
    if not is_private(message) or not await check_admin_filter(message):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT c.user_id, u.full_name, u.username FROM couriers c LEFT JOIN users u ON c.user_id = u.user_id"
        ) as cur:
            couriers = await cur.fetchall()

    if not couriers:
        return await message.reply("ℹ️ هیچ پستچی در سیستم ثبت نشده است.")

    txt, kb = _render_list_couriers_page(couriers, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "listcouriers_page_noop")
async def cb_list_couriers_noop(callback: CallbackQuery):
    await callback.answer()


async def _list_couriers_reload(callback: CallbackQuery, page: int):
    if not await check_admin_filter(callback):
        return await callback.answer("عدم دسترسی.", show_alert=True)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT c.user_id, u.full_name, u.username FROM couriers c LEFT JOIN users u ON c.user_id = u.user_id"
        ) as cur:
            couriers = await cur.fetchall()
    if not couriers:
        return await callback.answer("ℹ️ هیچ پستچی در سیستم ثبت نشده است.", show_alert=True)
    txt, kb = _render_list_couriers_page(couriers, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("listcouriers_page_"))
async def cb_list_couriers_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    await _list_couriers_reload(callback, page)
    await callback.answer()


@admin_router.callback_query(F.data == "listcouriers_refresh")
async def cb_list_couriers_refresh(callback: CallbackQuery):
    page = _extract_current_page(callback.message.text or "")
    await _list_couriers_reload(callback, page)
    await callback.answer("🔄 لیست به‌روزرسانی شد.")



@admin_router.message(Command("set_shop_rates"))
async def cmd_set_shop_rates(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 4:
        return await message.reply("راهنما: <code>/set_shop_rates [فروشنده] [بانک] [سوخت]</code>\nمثال: <code>/set_shop_rates 51 40 9</code>", parse_mode="HTML")
    try:
        s, b, f = float(args[1]), float(args[2]), float(args[3])
        if abs((s + b + f) - 100.0) > 0.01:
            return await message.reply("❌ مجموع درصدها باید برابر با ۱۰۰ باشد.")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('shop_seller_pct', ?)", (s,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('shop_bank_pct', ?)", (b,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('shop_burn_pct', ?)", (f,))
            await db.commit()
        await message.reply(f"✅ درصدهای فروشگاه با موفقیت تنظیم شد:\nفروشنده: {s}%\nبانک: {b}%\nسوخت: {f}%")
    except ValueError:
        await message.reply("❌ مقادیر وارد شده نامعتبر است.")


@admin_router.message(Command("set_courier_rates"))
async def cmd_set_courier_rates(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 7:
        return await message.reply(
            "راهنما:\n<code>/set_courier_rates [پستچی] [بانک] [سوخت] [بازه۱] [بازه۲] [بازه۳]</code>\n"
            "مثال:\n<code>/set_courier_rates 61 30 9 8 10 12</code>", parse_mode="HTML"
        )
    try:
        c, b, f = float(args[1]), float(args[2]), float(args[3])
        t1, t2, t3 = float(args[4]), float(args[5]), float(args[6])
        if abs((c + b + f) - 100.0) > 0.01:
            return await message.reply("❌ مجموع درصدهای تقسیم پستچی باید ۱۰۰ باشد.")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('courier_pct', ?)", (c,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('courier_bank_pct', ?)", (b,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('courier_burn_pct', ?)", (f,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('tier1_pct', ?)", (t1,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('tier2_pct', ?)", (t2,))
            await db.execute("INSERT OR REPLACE INTO system_settings (key, val) VALUES ('tier3_pct', ?)", (t3,))
            await db.commit()
        await message.reply(
            f"✅ درصدهای پست با موفقیت تنظیم شد:\n"
            f"تقسیم: پستچی {c}% | بانک {b}% | سوخت {f}%\n"
            f"بازه‌های هزینه: تا ۹۹ آتر ({t1}%) | ۱۰۰ تا ۹۹۹ ({t2}%) | ۱۰۰۰ به بالا ({t3}%)"
        )
    except ValueError:
        await message.reply("❌ مقادیر وارد شده نامعتبر است.")


# --- 🏦 دستورات سوپرادمین برای تنظیم بانک آترامنتوم و وام پویا ---

@admin_router.message(Command("set_bank_rate"))
async def cmd_set_bank_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_bank_rate [درصد]</code>\nمثال: <code>/set_bank_rate 1.23</code>",
            parse_mode="HTML"
        )
    try:
        rate = float(args[1])
        if rate < 0:
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است.")
    await set_setting("bank_daily_rate", rate)
    await message.reply(f"✅ نرخ سود روزانه بانک آترامنتوم به <b>{rate}٪</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_bank_treasury_profit_pct"))
async def cmd_set_bank_treasury_profit_pct(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_bank_treasury_profit_pct [درصد سهم خزانه]</code>\n"
            "مثال (۴۵٪ خزانه و ۵۵٪ خلق): <code>/set_bank_treasury_profit_pct 45</code>",
            parse_mode="HTML"
        )
    try:
        pct = float(args[1])
        if not (0 <= pct <= 100):
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است. درصد باید بین ۰ تا ۱۰۰ باشد.")
    await set_setting("bank_treasury_profit_pct", pct)
    await message.reply(
        f"✅ نسبت تأمین سود روزانه بانک تغییر یافت.\n"
        f"🏛 سهم خزانه مرکزی: <b>{pct}٪</b>\n"
        f"✨ سهم خلق پول: <b>{100 - pct}٪</b>",
        parse_mode="HTML"
    )


@admin_router.message(Command("set_min_loan"))
async def cmd_set_min_loan(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/set_min_loan [مبلغ]</code>", parse_mode="HTML")
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است.")
    await set_setting("min_loan_amount", amount)
    await message.reply(f"✅ حداقل مبلغ وام به <code>₳ {amount}</code> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_max_loan"))
async def cmd_set_max_loan(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/set_max_loan [مبلغ]</code>", parse_mode="HTML")
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است.")
    await set_setting("max_loan_amount", amount)
    await message.reply(f"✅ حداکثر مبلغ وام به <code>₳ {amount}</code> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_loan_interest"))
async def cmd_set_loan_interest(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 3:
        return await message.reply(
            "راهنما: <code>/set_loan_interest [حداقل] [حداکثر]</code>\nمثال: <code>/set_loan_interest 2 5</code>",
            parse_mode="HTML"
        )
    try:
        mn, mx = float(args[1]), float(args[2])
        if mn < 0 or mx < mn:
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقادیر نامعتبر است.")
    await set_setting("min_loan_interest", mn)
    await set_setting("max_loan_interest", mx)
    await message.reply(f"✅ بازه سود وام به <b>{mn}٪ تا {mx}٪</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_loan_installments"))
async def cmd_set_loan_installments(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_loan_installments [لیست تعداد اقساط با کاما]</code>\n"
            "مثال: <code>/set_loan_installments 2,3,4</code>",
            parse_mode="HTML"
        )
    raw = args[1].strip().replace(" ", "")
    parts = raw.split(",")
    try:
        nums = [int(p) for p in parts if p]
        if not nums or any(n <= 0 for n in nums):
            raise ValueError
    except ValueError:
        return await message.reply("❌ فرمت نامعتبر است. مثال درست: <code>2,3,4</code>", parse_mode="HTML")
    await set_setting("allowed_installments", ",".join(str(n) for n in nums))
    await message.reply(f"✅ تعداد اقساط مجاز به <b>{raw}</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_collateral_rate"))
async def cmd_set_collateral_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_collateral_rate [نسبت اعشاری]</code>\nمثال (۱۷٪): <code>/set_collateral_rate 0.17</code>",
            parse_mode="HTML"
        )
    try:
        rate = float(args[1])
        if not (0 < rate <= 1):
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار باید بین 0 و 1 باشد (مثال: 0.17).")
    await set_setting("collateral_rate", rate)
    await message.reply(f"✅ نرخ وثیقه وام به <b>{rate * 100:.1f}٪</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_loan_guarantor_balance_rate"))
async def cmd_set_loan_guarantor_balance_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 3:
        return await message.reply(
            "راهنما: <code>/set_loan_guarantor_balance_rate [نرخ_گیرنده] [نرخ_ضامن]</code>\n"
            "مثال (۲۰٪ و ۲۰٪): <code>/set_loan_guarantor_balance_rate 0.20 0.20</code>",
            parse_mode="HTML"
        )
    try:
        rate_borrower = float(args[1])
        rate_guarantor = float(args[2])
        if not (0 < rate_borrower <= 1) or not (0 < rate_guarantor <= 1):
            raise ValueError
    except ValueError:
        return await message.reply("❌ هر دو مقدار باید بین 0 و 1 باشند (مثال: 0.20).")
    await set_setting("loan_guarantor_balance_rate_borrower", rate_borrower)
    await set_setting("loan_guarantor_balance_rate_guarantor", rate_guarantor)
    await message.reply(
        f"✅ نرخ موجودی وام ضامنی تغییر یافت.\n"
        f"👤 نرخ گیرنده: <b>{rate_borrower * 100:.1f}٪</b>\n"
        f"🤝 نرخ ضامن: <b>{rate_guarantor * 100:.1f}٪</b>",
        parse_mode="HTML",
    )


@admin_router.message(Command("set_loan_guarantor_collateral_rate"))
async def cmd_set_loan_guarantor_collateral_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 3:
        return await message.reply(
            "راهنما: <code>/set_loan_guarantor_collateral_rate [نرخ_گیرنده] [نرخ_ضامن]</code>\n"
            "مثال (۸٪ و ۹٪): <code>/set_loan_guarantor_collateral_rate 0.08 0.09</code>",
            parse_mode="HTML"
        )
    try:
        rate_borrower = float(args[1])
        rate_guarantor = float(args[2])
        if not (0 < rate_borrower <= 1) or not (0 < rate_guarantor <= 1):
            raise ValueError
    except ValueError:
        return await message.reply("❌ هر دو مقدار باید بین 0 و 1 باشند (مثال: 0.08).")
    await set_setting("loan_guarantor_collateral_rate_borrower", rate_borrower)
    await set_setting("loan_guarantor_collateral_rate_guarantor", rate_guarantor)
    await message.reply(
        f"✅ نرخ وثیقه وام ضامنی تغییر یافت.\n"
        f"👤 نرخ گیرنده: <b>{rate_borrower * 100:.1f}٪</b>\n"
        f"🤝 نرخ ضامن: <b>{rate_guarantor * 100:.1f}٪</b>",
        parse_mode="HTML",
    )


@admin_router.message(Command("set_req_balance_rate"))
async def cmd_set_req_balance_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_req_balance_rate [نسبت اعشاری]</code>\nمثال (۴۰٪): <code>/set_req_balance_rate 0.40</code>",
            parse_mode="HTML"
        )
    try:
        rate = float(args[1])
        if not (0 < rate <= 1):
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار باید بین 0 و 1 باشد (مثال: 0.40).")
    await set_setting("required_balance_rate", rate)
    await message.reply(f"✅ نرخ موجودی اولیه لازم برای وام وثیقه‌ای به <b>{rate * 100:.1f}٪</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("set_late_penalty_rate"))
async def cmd_set_late_penalty_rate(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "راهنما: <code>/set_late_penalty_rate [نسبت اعشاری روزانه]</code>\nمثال (۰.۸۵٪): <code>/set_late_penalty_rate 0.0085</code>",
            parse_mode="HTML"
        )
    try:
        rate = float(args[1])
        if rate < 0:
            raise ValueError
    except ValueError:
        return await message.reply("❌ مقدار نامعتبر است.")
    await set_setting("late_penalty_rate", rate)
    await message.reply(f"✅ نرخ جریمه دیرکرد روزانه به <b>{rate * 100:.2f}٪</b> تغییر یافت.", parse_mode="HTML")


@admin_router.message(Command("view_set_all"))
async def cmd_view_set_all(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return

    keys = [
        "shop_seller_pct", "shop_bank_pct", "shop_burn_pct",
        "courier_pct", "courier_bank_pct", "courier_burn_pct",
        "tier1_pct", "tier2_pct", "tier3_pct",
        "bank_daily_rate", "bank_treasury_profit_pct",
        "min_loan_amount", "max_loan_amount",
        "min_loan_interest", "max_loan_interest",
        "allowed_installments",
        "collateral_rate", "required_balance_rate", "late_penalty_rate",
        "loan_guarantor_balance_rate_borrower", "loan_guarantor_balance_rate_guarantor",
        "loan_guarantor_collateral_rate_borrower", "loan_guarantor_collateral_rate_guarantor",
    ]
    vals = {k: await get_setting(k) for k in keys}

    def pct(key):
        try:
            return f"{float(vals[key])}"
        except (TypeError, ValueError):
            return str(vals[key])

    def ratio_pct(key):
        try:
            return f"{float(vals[key]) * 100:.2f}"
        except (TypeError, ValueError):
            return str(vals[key])

    txt = (
        "⚙️ <b>تمام تنظیمات و درصدهای سیستم</b>\n"
        "برای هر مورد: نام، مقدار فعلی، بخش مربوطه، کاربرد و دستور تنظیم آورده شده است.\n\n"

        "🏪 <b>بخش: فروشگاه — درصد مالیات فروشگاه</b>\n"
        f"🔸 سهم فروشنده: <code>{pct('shop_seller_pct')}٪</code>\n"
        f"🔸 سهم بانک: <code>{pct('shop_bank_pct')}٪</code>\n"
        f"🔸 سوخت (مالیات فروشگاه): <code>{pct('shop_burn_pct')}٪</code>\n"
        "📝 کاربرد: تقسیم مبلغ هر فروش موفق فروشگاهی بین فروشنده، بانک و سوخت سیستم.\n"
        "⚙️ تنظیم: <code>/set_shop_rates [فروشنده] [بانک] [سوخت]</code>\n\n"

        "🚚 <b>بخش: پست — درصد سهم بانک و درصدهای/بازه‌های پستی</b>\n"
        f"🔸 سهم پستچی: <code>{pct('courier_pct')}٪</code>\n"
        f"🔸 سهم بانک از هزینه پست: <code>{pct('courier_bank_pct')}٪</code>\n"
        f"🔸 سوخت هزینه پست: <code>{pct('courier_burn_pct')}٪</code>\n"
        f"🔸 بازه هزینه پست (تا ۹۹ آتر): <code>{pct('tier1_pct')}٪</code>\n"
        f"🔸 بازه هزینه پست (۱۰۰ تا ۹۹۹ آتر): <code>{pct('tier2_pct')}٪</code>\n"
        f"🔸 بازه هزینه پست (۱۰۰۰ آتر به بالا): <code>{pct('tier3_pct')}٪</code>\n"
        "📝 کاربرد: تقسیم هزینه پست بین پستچی، بانک و سوخت؛ و تعیین درصد هزینه پست بر اساس بازه قیمت محصول.\n"
        "⚙️ تنظیم: <code>/set_courier_rates [پستچی] [بانک] [سوخت] [بازه۱] [بازه۲] [بازه۳]</code>\n\n"

        "🏦 <b>بخش: بانک آترامنتوم — نرخ سود بانک</b>\n"
        f"🔸 نرخ سود روزانه: <code>{pct('bank_daily_rate')}٪</code>\n"
        "📝 کاربرد: نرخ سود روزانه‌ای که به سپرده‌های بانکی کاربران تعلق می‌گیرد.\n"
        "⚙️ تنظیم: <code>/set_bank_rate [درصد]</code>\n\n"

        "🏛 <b>بخش: بانک آترامنتوم — منبع تأمین سود روزانه</b>\n"
        f"🔸 سهم خزانه مرکزی: <code>{pct('bank_treasury_profit_pct')}٪</code>\n"
        f"🔸 سهم خلق پول: <code>{100 - float(vals['bank_treasury_profit_pct']):.2f}٪</code>\n"
        "📝 کاربرد: از هر سود روزانه پرداختی به سپرده‌گذاران، این درصد از خزانه مرکزی کسر و مابقی "
        "به‌صورت خلق پول تأمین می‌شود؛ در صورت کمبود موجودی خزانه، پرداخت کاربر هرگز کامل حذف "
        "(توکن‌سوزی) نمی‌شود و باقیِ سهم خزانه که کسر نشده در خود خزانه باقی می‌ماند.\n"
        "⚙️ تنظیم: <code>/set_bank_treasury_profit_pct [درصد سهم خزانه]</code>\n\n"

        "💳 <b>بخش: وام — حداقل/حداکثر مبلغ وام</b>\n"
        f"🔸 حداقل مبلغ وام: <code>₳ {int(float(vals['min_loan_amount']))}</code>\n"
        f"🔸 حداکثر مبلغ وام: <code>₳ {int(float(vals['max_loan_amount']))}</code>\n"
        "📝 کاربرد: محدوده مبلغی که کاربران می‌توانند برای وام درخواست دهند.\n"
        "⚙️ تنظیم: <code>/set_min_loan [مبلغ]</code> و <code>/set_max_loan [مبلغ]</code>\n\n"

        "📈 <b>بخش: وام — حداقل/حداکثر سود وام</b>\n"
        f"🔸 حداقل سود: <code>{pct('min_loan_interest')}٪</code>\n"
        f"🔸 حداکثر سود: <code>{pct('max_loan_interest')}٪</code>\n"
        "📝 کاربرد: نرخ سود وام به‌صورت پویا و متناسب با مبلغ وام، بین این دو مقدار محاسبه می‌شود.\n"
        "⚙️ تنظیم: <code>/set_loan_interest [حداقل] [حداکثر]</code>\n\n"

        "🔢 <b>بخش: وام — اقساط مجاز</b>\n"
        f"🔸 تعداد اقساط مجاز: <code>{vals['allowed_installments']}</code>\n"
        "📝 کاربرد: گزینه‌های تعداد قسطی که کاربر هنگام درخواست وام می‌تواند از بین آن‌ها انتخاب کند.\n"
        "⚙️ تنظیم: <code>/set_loan_installments [لیست با کاما]</code>\n\n"

        "🔒 <b>بخش: وام — نرخ وثیقه</b>\n"
        f"🔸 نرخ وثیقه: <code>{ratio_pct('collateral_rate')}٪</code>\n"
        "📝 کاربرد: درصدی از مبلغ وام که در لحظه تأیید نهایی سوپرادمین، به‌عنوان وثیقه از موجودی "
        "قابل‌انتقال کاربر کسر و قفل می‌شود.\n"
        "⚙️ تنظیم: <code>/set_collateral_rate [نسبت اعشاری]</code>\n\n"

        "💰 <b>بخش: وام — نرخ موجودی اولیه موردنیاز</b>\n"
        f"🔸 نرخ موجودی اولیه لازم: <code>{ratio_pct('required_balance_rate')}٪</code>\n"
        "📝 کاربرد: حداقل درصدی از مبلغ وام که کاربر باید در موجودی قابل‌انتقال خود داشته باشد "
        "تا بتواند درخواست وام وثیقه‌ای ثبت کند.\n"
        "⚙️ تنظیم: <code>/set_req_balance_rate [نسبت اعشاری]</code>\n\n"

        "⏰ <b>بخش: وام — نرخ جریمه دیرکرد</b>\n"
        f"🔸 نرخ جریمه دیرکرد روزانه: <code>{ratio_pct('late_penalty_rate')}٪</code>\n"
        "📝 کاربرد: درصد جریمه‌ای که به ازای هر روز تأخیر در پرداخت قسط، به مبلغ پایه همان قسط اضافه می‌شود.\n"
        "⚙️ تنظیم: <code>/set_late_penalty_rate [نسبت اعشاری روزانه]</code>\n\n"

        "🤝 <b>بخش: وام ضامنی — نرخ موجودی</b>\n"
        f"🔸 نرخ گیرنده: <code>{ratio_pct('loan_guarantor_balance_rate_borrower')}٪</code>\n"
        f"🔸 نرخ ضامن: <code>{ratio_pct('loan_guarantor_balance_rate_guarantor')}٪</code>\n"
        "📝 کاربرد: درصد موجودی موردنیاز گیرنده و ضامن در وام‌های ضامنی.\n"
        "⚙️ تنظیم: <code>/set_loan_guarantor_balance_rate [نرخ_گیرنده] [نرخ_ضامن]</code>\n\n"

        "🔒 <b>بخش: وام ضامنی — نرخ وثیقه</b>\n"
        f"🔸 نرخ گیرنده: <code>{ratio_pct('loan_guarantor_collateral_rate_borrower')}٪</code>\n"
        f"🔸 نرخ ضامن: <code>{ratio_pct('loan_guarantor_collateral_rate_guarantor')}٪</code>\n"
        "📝 کاربرد: درصد وثیقه موردنیاز گیرنده و ضامن در وام‌های ضامنی.\n"
        "⚙️ تنظیم: <code>/set_loan_guarantor_collateral_rate [نرخ_گیرنده] [نرخ_ضامن]</code>"
    )

    await message.reply(txt, parse_mode="HTML")


SHOP_REQUESTS_PAGE_SIZE = 10


async def _fetch_pending_shop_requests():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM shops WHERE status = 'PENDING' ORDER BY shop_id") as cur:
            return await cur.fetchall()


def _render_shop_requests_page(requests, page: int):
    total = len(requests)
    total_pages = max(1, math.ceil(total / SHOP_REQUESTS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * SHOP_REQUESTS_PAGE_SIZE
    page_items = requests[start:start + SHOP_REQUESTS_PAGE_SIZE]

    txt = f"📥 <b>درخواست‌های ثبت فروشگاه (در انتظار بررسی) (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"درخواست‌های معلق: <code>{total}</code> مورد\n\n"

    buttons = []
    for req in page_items:
        safe_title = html.escape(req["channel_title"] or "بدون نام")
        txt += (
            f"🔹 متقاضی: <code>{req['owner_id']}</code> | فروشگاه پیشنهادی: <b>{safe_title}</b> "
            f"(شناسه: <code>{req['shop_id']}</code>)\n"
        )
        buttons.append([
            InlineKeyboardButton(text=f"✅ تایید #{req['shop_id']}", callback_data=f"approve_shop_{req['shop_id']}"),
            InlineKeyboardButton(text=f"❌ رد #{req['shop_id']}", callback_data=f"reject_shop_{req['shop_id']}"),
        ])

    pag_kb = _build_pagination_keyboard(page, total_pages, "shopreq_page", refresh_data="shopreq_refresh")
    buttons.extend(pag_kb.inline_keyboard)
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    return txt, kb


async def _shop_requests_reload(callback: CallbackQuery, page: int) -> None:
    requests = await _fetch_pending_shop_requests()
    if not requests:
        try:
            await callback.message.edit_text("✅ هیچ درخواست معلقی باقی نمانده است.")
        except Exception:
            pass
        return
    txt, kb = _render_shop_requests_page(requests, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.message(Command("shop_requests"))
async def cmd_shop_requests(message: Message):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    requests = await _fetch_pending_shop_requests()
    if not requests:
        return await message.reply("ℹ️ هیچ درخواست ثبت فروشگاهی وجود ندارد.")
    txt, kb = _render_shop_requests_page(requests, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "shopreq_page_noop")
async def cb_shop_requests_noop(callback: CallbackQuery):
    await callback.answer()


@admin_router.callback_query(F.data.startswith("shopreq_page_"))
async def cb_shop_requests_page(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("عدم دسترسی", show_alert=True)
    page = int(callback.data.split("_")[2])
    await _shop_requests_reload(callback, page)
    await callback.answer()


@admin_router.callback_query(F.data == "shopreq_refresh")
async def cb_shop_requests_refresh(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("عدم دسترسی", show_alert=True)
    page = _extract_current_page(callback.message.text or "")
    await _shop_requests_reload(callback, page)
    await callback.answer("🔄 لیست به‌روزرسانی شد.")


@admin_router.callback_query(F.data.startswith("approve_shop_"))
async def cb_approve_shop(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("عدم دسترسی", show_alert=True)
    shop_id = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shops SET status = 'APPROVED' WHERE shop_id = ?", (shop_id,))
        async with db.execute("SELECT owner_id, channel_title FROM shops WHERE shop_id = ?", (shop_id,)) as cur:
            shop = await cur.fetchone()
        await db.commit()

    page = _extract_current_page(callback.message.text or "")
    await _shop_requests_reload(callback, page)
    await callback.answer("✅ فروشگاه تایید شد.")
    if shop:
        try:
            safe_title = html.escape(shop[1] or '')
            await callback.bot.send_message(
                shop[0],
                f"🎉 درخواست ثبت فروشگاه شما برای کانال/گروه <b>{safe_title}</b> با موفقیت تایید شد!",
                parse_mode="HTML"
            )
        except Exception:
            pass


@admin_router.callback_query(F.data.startswith("reject_shop_"))
async def cb_reject_shop(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("عدم دسترسی", show_alert=True)
    shop_id = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT owner_id FROM shops WHERE shop_id = ?", (shop_id,)) as cur:
            shop = await cur.fetchone()
        await db.execute("DELETE FROM shops WHERE shop_id = ?", (shop_id,))
        await db.commit()

    page = _extract_current_page(callback.message.text or "")
    await _shop_requests_reload(callback, page)
    await callback.answer("❌ درخواست فروشگاه رد شد.")
    if shop:
        try:
            await callback.bot.send_message(shop[0], "❌ متأسفانه درخواست ثبت فروشگاه شما رد شد.")
        except Exception:
            pass



@admin_router.message(Command("remove_shop"))
async def cmd_remove_shop(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/remove_shop [شناسه_فروشگاه]</code>", parse_mode="HTML")
    try:
        shop_id = int(args[1])
    except ValueError:
        return await message.reply("❌ شناسه فروشگاه باید عدد باشد.")
    preview_text = (
        "🚨 <b>هشدار لغو مجوز فروشگاه</b>\n"
        f"آیا از لغو مجوز فروشگاه {shop_id} و غیرفعال‌سازی تمام محصولات مرتبط با آن مطمئن هستید؟"
    )
    await _start_ops_confirmation(message, state, "remove_shop", preview_text, op_shop_id=shop_id)


@admin_router.message(Command("add_courier"))
async def cmd_add_courier(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/add_courier [آیدی_عددی_کاربر]</code>", parse_mode="HTML")
    try:
        courier_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "🚚 <b>تأیید انتصاب پستچی</b>\n"
        f"آیا از اعطای دسترسی‌های پستچی به کاربر <code>{courier_id}</code> اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "add_courier", preview_text, op_user_id=courier_id)


@admin_router.message(Command("remove_courier"))
async def cmd_remove_courier(message: Message, state: FSMContext):
    if not is_private(message) or not is_super_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/remove_courier [آیدی_عددی_کاربر]</code>", parse_mode="HTML")
    try:
        courier_id = int(args[1])
    except ValueError:
        return await message.reply("❌ آیدی باید عدد باشد.")
    preview_text = (
        "⚠️ <b>تأیید سلب دسترسی پستچی</b>\n"
        f"آیا از حذف کاربر <code>{courier_id}</code> از لیست پستچی‌ها اطمینان دارید؟"
    )
    await _start_ops_confirmation(message, state, "remove_courier", preview_text, op_user_id=courier_id)


# --- ۲. دستورات فروشندگان (Shop Owners) ---

@shop_router.message(Command("request_shop"))
async def cmd_request_shop(message: Message, state: FSMContext):
    if not is_private(message):
        return
    prompt = await message.reply("لطفاً آیدی عددی یا یوزرنیم کانال/گروه خود را ارسال کنید (مثال: @mychannel یا -100123456789):")
    await state.set_state(RequestShopForm.waiting_for_channel)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.message(RequestShopForm.waiting_for_channel)
async def process_request_shop_channel(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    channel_raw = message.text.strip()
    def _reschedule():
        current_state = RequestShopForm.waiting_for_channel.state
        schedule_input_timeout(
            state, message.chat.id, message.from_user.id, current_state,
            lambda: _default_timeout_notice(message.bot, message.chat.id, None),
        )

    try:
        chat = await message.bot.get_chat(channel_raw)
        bot_member = await message.bot.get_chat_member(chat.id, message.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            await message.reply("⚠️ ربات در این کانال/گروه ادمین نیست! ابتدا ربات را ادمین کنید و مجدداً تلاش کنید.")
            return _reschedule()
    except Exception as e:
        await message.reply(f"❌ یافتن کانال/گروه با خطا مواجه شد. از ادمین بودن ربات و صحت آیدی مطمئن شوید.\nخطا: {e}")
        return _reschedule()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO shops (owner_id, channel_id, channel_title, status) VALUES (?, ?, ?, 'PENDING')",
            (message.from_user.id, str(chat.id), chat.title)
        )
        await db.commit()

    await state.clear()
    await message.reply("✅ درخواست ثبت فروشگاه ارسال شد و پس از بررسی توسط سوپرادمین تایید خواهد شد.")


@shop_router.message(Command("add_product"))
async def cmd_add_product(message: Message, state: FSMContext):
    if not is_private(message):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM shops WHERE owner_id = ? AND status = 'APPROVED'", (message.from_user.id,)) as cur:
            shops = await cur.fetchall()

    if not shops:
        return await message.reply("❌ شما هیچ فروشگاه تاییدشده‌ای ندارید.")

    await state.update_data(shop_id=shops[0]["shop_id"], channel_id=shops[0]["channel_id"])
    prompt = await message.reply("📸 لطفاً عکس محصول را ارسال کنید:")
    await state.set_state(AddProductForm.waiting_for_photo)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.message(AddProductForm.waiting_for_photo, F.photo)
async def process_product_photo(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    photo_id = message.photo[-1].file_id
    await state.update_data(photo_id=photo_id)
    prompt = await message.reply("🏷 نام محصول را وارد کنید:")
    await state.set_state(AddProductForm.waiting_for_title)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.message(AddProductForm.waiting_for_title)
async def process_product_title(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    await state.update_data(title=message.text.strip())
    prompt = await message.reply("📝 توضیحات محصول را وارد کنید:")
    await state.set_state(AddProductForm.waiting_for_description)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.message(AddProductForm.waiting_for_description)
async def process_product_desc(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    await state.update_data(description=message.text.strip())
    prompt = await message.reply("💰 قیمت محصول (به آتر) را وارد کنید:")
    await state.set_state(AddProductForm.waiting_for_price)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.message(AddProductForm.waiting_for_price)
async def process_product_price(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.reply("❌ قیمت باید یک عدد مثبت باشد.")
        current_state = await state.get_state()
        return schedule_input_timeout(
            state, message.chat.id, message.from_user.id, current_state,
            lambda: _default_timeout_notice(message.bot, message.chat.id, None),
        )
    await state.update_data(price=int(message.text))

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="تکی (۱ عدد)", callback_data="st_SINGLE"),
        InlineKeyboardButton(text="محدود", callback_data="st_LIMITED"),
        InlineKeyboardButton(text="نامحدود", callback_data="st_UNLIMITED"),
    ]])
    prompt = await message.reply("📦 نوع موجودی محصول را انتخاب کنید:", reply_markup=kb)
    await state.set_state(AddProductForm.waiting_for_stock_type)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@shop_router.callback_query(AddProductForm.waiting_for_stock_type, F.data.startswith("st_"))
async def process_product_stock_type(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    st_type = callback.data.split("_")[1]
    await state.update_data(stock_type=st_type)

    if st_type in ("SINGLE", "UNLIMITED"):
        await state.update_data(stock_qty=1 if st_type == "SINGLE" else -1)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="بله (نیازمند پستچی)", callback_data="cour_YES"),
            InlineKeyboardButton(text="خیر (دیجیتالی/مستقیم)", callback_data="cour_NO"),
        ]])
        await callback.message.edit_text("🚚 آیا این محصول نیاز به پستچی دارد؟", reply_markup=kb)
        await state.set_state(AddProductForm.waiting_for_needs_courier)
    else:  # LIMITED
        await callback.message.edit_text("تعداد موجودی را به عدد وارد کنید:")
        await state.set_state(AddProductForm.waiting_for_stock)

    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )


@shop_router.message(AddProductForm.waiting_for_stock)
async def process_product_stock_qty(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.reply("❌ تعداد موجودی باید عدد مثبت باشد.")
        current_state = await state.get_state()
        return schedule_input_timeout(
            state, message.chat.id, message.from_user.id, current_state,
            lambda: _default_timeout_notice(message.bot, message.chat.id, None),
        )
    await state.update_data(stock_qty=int(message.text))

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="بله (نیازمند پستچی)", callback_data="cour_YES"),
        InlineKeyboardButton(text="خیر (دیجیتالی/مستقیم)", callback_data="cour_NO"),
    ]])
    prompt = await message.reply("🚚 آیا این محصول نیاز به پستچی دارد؟", reply_markup=kb)
    await state.set_state(AddProductForm.waiting_for_needs_courier)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


async def _generate_unique_product_code(db) -> str:
    """
    تولید یک کد یکتای غیرتکراری برای محصول (هم‌سبک با کد ۱۰ رقمی امنیتی سفارش‌ها)،
    که پس از ثبت محصول در دیتابیس ذخیره شده و مبنای مدیریت و حذف بعدی محصول قرار می‌گیرد.
    """
    for _ in range(20):
        candidate = "PRD-" + "".join(random.choices(string.digits, k=8))
        async with db.execute(
            "SELECT 1 FROM products WHERE product_code = ?", (candidate,)
        ) as cur:
            if not await cur.fetchone():
                return candidate
    # fallback بسیار بعید (در صورت تصادم مکرر): افزودن مهر زمانی برای تضمین یکتایی
    return "PRD-" + datetime.now(timezone.utc).strftime("%y%m%d%H%M%S")


@shop_router.callback_query(AddProductForm.waiting_for_needs_courier, F.data.startswith("cour_"))
async def process_product_final(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    needs_courier = (callback.data.split("_")[1] == "YES")
    data = await state.get_data()
    await state.clear()

    async with aiosqlite.connect(DB_PATH) as db:
        product_code = await _generate_unique_product_code(db)
        insert_sql = (
            """INSERT INTO products (shop_id, photo_id, title, description, price, stock_type, stock_qty, needs_courier, product_code)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        )
        insert_args = (
            data["shop_id"], data["photo_id"], data["title"], data["description"],
            data["price"], data["stock_type"], data["stock_qty"], needs_courier, product_code,
        )
        try:
            cursor = await db.execute(insert_sql, insert_args)
        except sqlite3.IntegrityError:
            # احتمال بسیار نادر تصادم هم‌زمان کد؛ یک‌بار با کد جدید تلاش مجدد می‌شود
            product_code = await _generate_unique_product_code(db)
            insert_args = insert_args[:-1] + (product_code,)
            cursor = await db.execute(insert_sql, insert_args)
        product_id = cursor.lastrowid
        await db.commit()

    # ارسال بنر محصول به کانال
    kb_buy = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🛍 خرید این محصول", callback_data=f"buy_prod_{product_id}")
    ]])
    
    stock_str = "موجود" if data["stock_type"] == "UNLIMITED" else f"{data['stock_qty']} عدد"
    safe_title = html.escape(data['title'])
    safe_desc = html.escape(data['description'])
    caption = (
        f"🛍 <b>{safe_title}</b>\n\n"
        f"📝 {safe_desc}\n\n"
        f"💰 قیمت: <code>₳ {data['price']}</code>\n"
        f"📦 موجودی: <b>{stock_str}</b>"
    )

    try:
        sent_msg = await callback.bot.send_photo(
            chat_id=data["channel_id"],
            photo=data["photo_id"],
            caption=caption,
            reply_markup=kb_buy,
            parse_mode="HTML"
        )
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE products SET channel_msg_id = ? WHERE product_id = ?", (sent_msg.message_id, product_id))
            await db.commit()
    except Exception as e:
        await callback.message.edit_text(
            f"⚠️ محصول ثبت شد اما بنر در کانال ارسال نشد. مطمئن شوید ربات ادمین کانال است.\n"
            f"🏷 کد محصول: <code>{product_code}</code>\n"
            f"خطا: {e}",
            parse_mode="HTML",
        )
        return

    await callback.message.edit_text(
        f"🎉 محصول با موفقیت ثبت شد و بنر خرید در کانال قرار گرفت.\n"
        f"🏷 کد محصول: <code>{product_code}</code>",
        parse_mode="HTML",
    )



# =====================================================================================
# 📦 دستور /inventory: نمایش لیست محصولات (با صفحه‌بندی) و مدیریت محصول با کد
# =====================================================================================
_PRODUCT_TYPE_LABELS = {"SINGLE": "تکی", "LIMITED": "محدود", "UNLIMITED": "نامحدود"}
INVENTORY_PAGE_SIZE = 10


async def _fetch_owner_products(owner_id: int):
    """تمام محصولات فروشگاه(های) تأییدشده یک فروشنده را برمی‌گرداند (برای لیست و صفحه‌بندی)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT p.* FROM products p JOIN shops s ON p.shop_id = s.shop_id "
            "WHERE s.owner_id = ? AND s.status = 'APPROVED' ORDER BY p.product_id ASC",
            (owner_id,)
        ) as cur:
            return await cur.fetchall()


def _build_pagination_keyboard(page: int, total_pages: int, page_prefix: str, noop_data: str = None, refresh_data: str = None) -> InlineKeyboardMarkup:
    """کیبورد استاندارد صفحه‌بندی: ردیف اول ۳ دکمه (◀️ قبلی | 📄 صفحه X از Y | بعدی ▶️)، و در صورت
    ارسال `refresh_data`، یک ردیف دوم اختیاری شامل دکمه «🔄 به‌روزرسانی لیست».
    `page` صفر-پایه است. در صفحه اول دکمه «قبلی» و در صفحه آخر دکمه «بعدی» پنهان می‌شود (طبق مدیریت لبه‌ها)."""
    if noop_data is None:
        noop_data = f"{page_prefix}_noop"
    row = []
    if page > 0:
        row.append(InlineKeyboardButton(text="◀️ قبلی", callback_data=f"{page_prefix}_{page - 1}"))
    row.append(InlineKeyboardButton(text=f"📄 صفحه {page + 1} از {total_pages}", callback_data=noop_data))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton(text="بعدی ▶️", callback_data=f"{page_prefix}_{page + 1}"))
    keyboard = [row]
    if refresh_data:
        keyboard.append([InlineKeyboardButton(text="🔄 به‌روزرسانی لیست", callback_data=refresh_data)])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _extract_current_page(text: str) -> int:
    """شماره صفحه فعلی (صفر-پایه) را از عنوان پیام صفحه‌بندی‌شده («... صفحه X از Y ...») استخراج می‌کند؛
    برای اینکه دکمه «🔄 به‌روزرسانی لیست» بتواند همان صفحه فعلی را دوباره بارگذاری کند."""
    m = re.search(r"صفحه (\d+) از", text or "")
    if m:
        try:
            return max(0, int(m.group(1)) - 1)
        except ValueError:
            return 0
    return 0


def _render_inventory_page(products, page: int):
    """متن و کیبورد صفحه‌بندی‌شده‌ی لیست محصولات را می‌سازد (حداکثر ۱۰ محصول در هر صفحه)."""
    total = len(products)
    total_pages = max(1, math.ceil(total / INVENTORY_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * INVENTORY_PAGE_SIZE
    page_items = products[start:start + INVENTORY_PAGE_SIZE]

    txt = f"📋 <b>انبار و موجودی فروشگاه (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"تعداد کل محصولات: <code>{total}</code> کد کالا\n\n"
    for p in page_items:
        safe_title = html.escape(p["title"])
        code_display = p["product_code"] or f"#{p['product_id']}"
        stock_display = "نامحدود" if p["stock_type"] == "UNLIMITED" else f"{p['stock_qty']} عدد"
        txt += (
            f"🔹 کد: <code>{code_display}</code> | نام: <b>{safe_title}</b> | "
            f"قیمت: <code>₳ {p['price']}</code> | موجودی: <b>{stock_display}</b>\n"
        )
    txt += "\nℹ️ برای مدیریت هر محصول: <code>/inventory [کد_محصول]</code>"

    kb = _build_pagination_keyboard(page, total_pages, "inv_page", noop_data="inv_noop")
    return txt, kb


async def _get_owned_product(owner_id: int, product_code: str):
    """محصول را از روی کد یکتا واکشی می‌کند؛ فقط اگر متعلق به فروشگاه تأییدشده همان فروشنده باشد چیزی برمی‌گرداند."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, s.owner_id AS shop_owner_id, s.channel_id AS shop_channel_id, s.status AS shop_status
               FROM products p JOIN shops s ON p.shop_id = s.shop_id
               WHERE p.product_code = ?""",
            (product_code,)
        ) as cur:
            row = await cur.fetchone()
    if not row or row["shop_owner_id"] != owner_id or row["shop_status"] != "APPROVED":
        return None
    return row


def _product_management_view(p):
    """متن و کیبورد صفحه مدیریت یک محصول را بر اساس نوع موجودی آن می‌سازد."""
    type_label = _PRODUCT_TYPE_LABELS.get(p["stock_type"], p["stock_type"])
    code_display = p["product_code"] or f"#{p['product_id']}"
    safe_title = html.escape(p["title"])
    stock_display = "نامحدود" if p["stock_type"] == "UNLIMITED" else f"{p['stock_qty']} عدد"

    text = (
        "📦 <b>مدیریت محصول</b>\n\n"
        f"🏷 نام: <b>{safe_title}</b>\n"
        f"🔢 کد: <code>{code_display}</code>\n"
        f"🏷 نوع: <b>{type_label}</b>\n"
        f"💰 قیمت فعلی: <code>₳ {p['price']}</code>\n"
        f"📦 موجودی فعلی: <b>{stock_display}</b>"
    )

    if p["stock_type"] == "SINGLE":
        text += "\n\n⚠️ محصولات تکی قابل ویرایش نیستند."
        return text, None

    buttons = [InlineKeyboardButton(text="💰 تغییر قیمت", callback_data=f"pmgmt_price_{p['product_id']}")]
    if p["stock_type"] == "LIMITED":
        buttons.append(InlineKeyboardButton(text="📦 تغییر موجودی", callback_data=f"pmgmt_stock_{p['product_id']}"))
    kb = InlineKeyboardMarkup(inline_keyboard=[buttons])
    return text, kb


async def _notify_channel_product_updated(bot: Bot, channel_id, channel_msg_id) -> None:
    """پس از به‌روزرسانی اطلاعات محصول، ربات روی همان پست محصول در کانال ریپلای می‌کند."""
    if not channel_id or not channel_msg_id:
        return
    try:
        await bot.send_message(
            chat_id=channel_id,
            text="اطلاعات این محصول به‌روزرسانی شد.",
            reply_to_message_id=channel_msg_id,
        )
    except Exception:
        pass


@shop_router.message(Command("inventory"))
async def cmd_inventory(message: Message):
    if not is_private(message):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM shops WHERE owner_id = ? AND status = 'APPROVED'", (message.from_user.id,)
        ) as cur:
            shops = await cur.fetchall()
    if not shops:
        return await message.reply(
            "❌ شما فروشگاه تأییدشده‌ای ندارید. برای ثبت فروشگاه از <code>/request_shop</code> استفاده کنید.",
            parse_mode="HTML",
        )

    args = message.text.split(maxsplit=1)

    # حالت اول: /inventory بدون آرگومان -> فقط نمایش لیست محصولات (با صفحه‌بندی)
    if len(args) < 2 or not args[1].strip():
        products = await _fetch_owner_products(message.from_user.id)
        if not products:
            return await message.reply("📦 شما هیچ محصولی ثبت نکرده‌اید.")
        txt, kb = _render_inventory_page(products, 0)
        return await message.reply(txt, reply_markup=kb, parse_mode="HTML")

    # حالت دوم: /inventory [کد_محصول] -> صفحه مدیریت همان محصول
    product = await _get_owned_product(message.from_user.id, args[1].strip())
    if not product:
        return await message.reply("❌ محصولی با این کد برای شما یافت نشد.")

    text, kb = _product_management_view(product)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@shop_router.callback_query(F.data == "inv_noop")
async def cb_inventory_noop(callback: CallbackQuery):
    await callback.answer()


@shop_router.callback_query(F.data.startswith("inv_page_"))
async def cb_inventory_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    products = await _fetch_owner_products(callback.from_user.id)
    if not products:
        return await callback.answer("📦 محصولی یافت نشد.", show_alert=True)
    txt, kb = _render_inventory_page(products, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@shop_router.callback_query(F.data.startswith("pmgmt_price_"))
async def cb_product_edit_price(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[2])
    owner_id = callback.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, s.owner_id AS shop_owner_id, s.status AS shop_status
               FROM products p JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur:
            product = await cur.fetchone()

    if not product or product["shop_owner_id"] != owner_id or product["shop_status"] != "APPROVED":
        return await callback.answer("❌ این محصول یافت نشد یا متعلق به شما نیست.", show_alert=True)
    if product["stock_type"] == "SINGLE":
        return await callback.answer("⚠️ محصولات تکی قابل ویرایش نیستند.", show_alert=True)

    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    try:
        await callback.message.edit_text(
            f"💰 قیمت جدید محصول «<b>{html.escape(product['title'])}</b>» را با ریپلای روی همین پیام ارسال کنید:",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await state.update_data(
        pmgmt_product_id=product_id, pmgmt_chat_id=chat_id, pmgmt_msg_id=message_id, pmgmt_owner=owner_id,
    )
    await state.set_state(ProductEditForm.waiting_for_new_price)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, chat_id, owner_id, current_state,
        lambda: _default_timeout_notice(callback.bot, chat_id, message_id),
    )
    await callback.answer()


@shop_router.callback_query(F.data.startswith("pmgmt_stock_"))
async def cb_product_edit_stock(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[2])
    owner_id = callback.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, s.owner_id AS shop_owner_id, s.status AS shop_status
               FROM products p JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur:
            product = await cur.fetchone()

    if not product or product["shop_owner_id"] != owner_id or product["shop_status"] != "APPROVED":
        return await callback.answer("❌ این محصول یافت نشد یا متعلق به شما نیست.", show_alert=True)
    if product["stock_type"] != "LIMITED":
        return await callback.answer("⚠️ تغییر موجودی فقط برای محصولات محدود امکان‌پذیر است.", show_alert=True)

    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    try:
        await callback.message.edit_text(
            f"📦 تعداد موجودی جدید محصول «<b>{html.escape(product['title'])}</b>» را با ریپلای روی همین پیام ارسال کنید:",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await state.update_data(
        pmgmt_product_id=product_id, pmgmt_chat_id=chat_id, pmgmt_msg_id=message_id, pmgmt_owner=owner_id,
    )
    await state.set_state(ProductEditForm.waiting_for_new_stock)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, chat_id, owner_id, current_state,
        lambda: _default_timeout_notice(callback.bot, chat_id, message_id),
    )
    await callback.answer()


@shop_router.message(ProductEditForm.waiting_for_new_price)
async def process_product_new_price(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = message.from_user.id
    if owner_id != data.get("pmgmt_owner"):
        return
    chat_id = data.get("pmgmt_chat_id", message.chat.id)
    msg_id = data.get("pmgmt_msg_id")
    product_id = data.get("pmgmt_product_id")

    if not message.reply_to_message or message.reply_to_message.message_id != msg_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text="⚠️ لطفاً روی همین پیام ریپلای کرده و قیمت جدید را ارسال کنید.",
            )
        except Exception:
            pass
        return

    cancel_input_timeout(chat_id, owner_id)

    async def _fail(note_text: str):
        await state.clear()
        try:
            await message.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=note_text)
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass

    try:
        new_price = int(message.text.strip())
    except (ValueError, AttributeError):
        return await _fail("❌ مقدار وارد شده نامعتبر است. برای تلاش مجدد دوباره از /inventory اقدام کنید.")
    if new_price <= 0:
        return await _fail("❌ قیمت باید مثبت باشد. برای تلاش مجدد دوباره از /inventory اقدام کنید.")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, s.owner_id AS shop_owner_id, s.channel_id AS shop_channel_id, s.status AS shop_status
               FROM products p JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur:
            product = await cur.fetchone()

        if not product or product["shop_owner_id"] != owner_id or product["shop_status"] != "APPROVED":
            return await _fail("❌ این محصول دیگر یافت نشد یا متعلق به شما نیست.")
        if product["stock_type"] == "SINGLE":
            return await _fail("⚠️ محصولات تکی قابل ویرایش نیستند.")

        await db.execute("UPDATE products SET price = ? WHERE product_id = ?", (new_price, product_id))
        await db.commit()

        async with db.execute(
            """SELECT p.*, s.channel_id AS shop_channel_id FROM products p
               JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur2:
            updated_product = await cur2.fetchone()

    await state.clear()
    text, kb = _product_management_view(updated_product)
    text = f"✅ قیمت با موفقیت به‌روزرسانی شد.\n\n{text}"
    try:
        await message.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    try:
        await message.delete()
    except Exception:
        pass

    await _notify_channel_product_updated(message.bot, updated_product["shop_channel_id"], updated_product["channel_msg_id"])


@shop_router.message(ProductEditForm.waiting_for_new_stock)
async def process_product_new_stock(message: Message, state: FSMContext):
    data = await state.get_data()
    owner_id = message.from_user.id
    if owner_id != data.get("pmgmt_owner"):
        return
    chat_id = data.get("pmgmt_chat_id", message.chat.id)
    msg_id = data.get("pmgmt_msg_id")
    product_id = data.get("pmgmt_product_id")

    if not message.reply_to_message or message.reply_to_message.message_id != msg_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text="⚠️ لطفاً روی همین پیام ریپلای کرده و تعداد موجودی جدید را ارسال کنید.",
            )
        except Exception:
            pass
        return

    cancel_input_timeout(chat_id, owner_id)

    async def _fail(note_text: str):
        await state.clear()
        try:
            await message.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=note_text)
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass

    raw = (message.text or "").strip()
    if not raw.isdigit() or int(raw) <= 0:
        return await _fail("❌ مقدار وارد شده نامعتبر است. برای تلاش مجدد دوباره از /inventory اقدام کنید.")
    new_stock = int(raw)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, s.owner_id AS shop_owner_id, s.channel_id AS shop_channel_id, s.status AS shop_status
               FROM products p JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur:
            product = await cur.fetchone()

        if not product or product["shop_owner_id"] != owner_id or product["shop_status"] != "APPROVED":
            return await _fail("❌ این محصول دیگر یافت نشد یا متعلق به شما نیست.")
        if product["stock_type"] != "LIMITED":
            return await _fail("⚠️ تغییر موجودی فقط برای محصولات محدود امکان‌پذیر است.")

        await db.execute("UPDATE products SET stock_qty = ? WHERE product_id = ?", (new_stock, product_id))
        await db.commit()

        async with db.execute(
            """SELECT p.*, s.channel_id AS shop_channel_id FROM products p
               JOIN shops s ON p.shop_id = s.shop_id WHERE p.product_id = ?""",
            (product_id,)
        ) as cur2:
            updated_product = await cur2.fetchone()

    await state.clear()
    text, kb = _product_management_view(updated_product)
    text = f"✅ موجودی با موفقیت به‌روزرسانی شد.\n\n{text}"
    try:
        await message.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    try:
        await message.delete()
    except Exception:
        pass

    await _notify_channel_product_updated(message.bot, updated_product["shop_channel_id"], updated_product["channel_msg_id"])


# =====================================================================================
# 🗑 دستور /delete: حذف محصول (بدون هیچ‌گونه تأثیر بر دارایی/تراکنش مالی هیچ کاربری)
# =====================================================================================
@shop_router.message(Command("delete"))
async def cmd_delete_product(message: Message, state: FSMContext):
    if not is_private(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        return await message.reply("راهنما: <code>/delete [کد_محصول]</code>", parse_mode="HTML")

    product_code = args[1].strip()
    owner_id = message.from_user.id

    # محصول باید وجود داشته باشد، حذف نشده باشد و متعلق به همان فروشنده باشد؛
    # در غیر این صورت هیچ تغییری اعمال نمی‌شود و فقط پیام مناسب نمایش داده می‌شود.
    product = await _get_owned_product(owner_id, product_code)
    if not product:
        return await message.reply("❌ محصولی با این کد برای شما یافت نشد.")

    safe_title = html.escape(product["title"])
    safe_code = html.escape(product_code)
    preview_text = (
        "⚠️ <b>هشدار حذف محصول</b>\n"
        f"آیا از حذف محصول با کد {safe_code} ({safe_title}) اطمینان دارید؟ این عملیات غیرقابل بازگشت است."
    )
    await _start_ops_confirmation(
        message, state, "delete_product", preview_text,
        op_product_id=product["product_id"],
        op_channel_id=product["shop_channel_id"],
        op_channel_msg_id=product["channel_msg_id"],
        op_product_title=product["title"],
    )



@shop_router.message(Command("my_shop"))
async def cmd_my_shop(message: Message):
    if not is_private(message):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM shops WHERE owner_id = ? AND status = 'APPROVED'", (message.from_user.id,)
        ) as cur:
            shops = await cur.fetchall()

        if not shops:
            return await message.reply("❌ شما فروشگاه تأییدشده‌ای ندارید. برای ثبت فروشگاه از <code>/request_shop</code> استفاده کنید.", parse_mode="HTML")

        shop = shops[0]
        async with db.execute(
            "SELECT COUNT(*) as cnt, SUM(price) as total FROM orders WHERE shop_id = ?",
            (shop["shop_id"],)
        ) as cur_o:
            stats = await cur_o.fetchone()

    safe_title = html.escape(shop['channel_title'] or '')
    await message.reply(
        f"🏪 <b>گزارش فروشگاه {safe_title}</b>\n\n"
        f"📊 تعداد کل فروش: <code>{stats['cnt']}</code> عدد\n"
        f"💰 مجموع ارزش سفارشات: <code>₳ {stats['total'] or 0}</code>\n"
        f"⚡ وضعیت فروشگاه: <b>{shop['status']}</b>",
        parse_mode="HTML"
    )


# --- ۳. خرید محصول و محاسبات دقیق مالی آتر (با تاییدیه دو مرحله‌ای) ---

@shop_router.callback_query(F.data.startswith("buy_prod_"))
async def cb_initiate_buy(callback: CallbackQuery):
    """مرحله اول: بررسی اولیه و ارسال پیام تاییدیه به پیوی خریدار (بدون کسر هیچ مبلغی)."""
    buyer_id = callback.from_user.id
    product_id = int(callback.data.split("_")[2])

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products WHERE product_id = ?", (product_id,)) as cur:
            prod = await cur.fetchone()

        if not prod:
            return await callback.answer("❌ محصول پیدا نشد.", show_alert=True)

        if prod["stock_type"] != "UNLIMITED" and prod["stock_qty"] <= 0:
            return await callback.answer("❌ موجودی این محصول به اتمام رسیده است.", show_alert=True)

        if prod["stock_type"] != "UNLIMITED":
            async with db.execute(
                "SELECT 1 FROM orders WHERE buyer_id = ? AND product_id = ? LIMIT 1",
                (buyer_id, product_id)
            ) as cur_chk:
                already_bought = await cur_chk.fetchone()
            if already_bought:
                return await callback.answer(
                    "❌ این محصول محدود است و شما قبلاً یک عدد از آن را خریداری کرده‌اید.",
                    show_alert=True
                )

        async with db.execute("SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (buyer_id,)) as cur_u:
            buyer = await cur_u.fetchone()

    if not buyer or buyer["is_frozen"]:
        return await callback.answer("❌ حساب شما مسدود یا غیرفعال است.", show_alert=True)

    price = prod["price"]
    courier_fee = 0
    if prod["needs_courier"]:
        t1 = await get_setting("tier1_pct")
        t2 = await get_setting("tier2_pct")
        t3 = await get_setting("tier3_pct")
        if price <= 99:
            courier_fee = int(price * (t1 / 100.0))
        elif price <= 999:
            courier_fee = int(price * (t2 / 100.0))
        else:
            courier_fee = int(price * (t3 / 100.0))
    total_cost = price + courier_fee

    buyer_transferable = max(0, buyer["balance"] - buyer["frozen_balance"])
    if buyer_transferable < total_cost:
        return await callback.answer(f"❌ موجودی ناکافی! قیمت محصول: ₳ {price} + هزینه پست: ₳ {courier_fee} = مجموع: ₳ {total_cost}", show_alert=True)

    safe_title = html.escape(prod['title'])
    if prod["needs_courier"]:
        cost_line = f"💰 قیمت محصول: <code>₳ {price}</code>\n🚚 هزینه پست: <code>₳ {courier_fee}</code>\n💳 مجموع پرداختی: <code>₳ {total_cost}</code>\n"
    else:
        cost_line = f"💰 مبلغ پرداختی: <code>₳ {price}</code>\n"

    confirm_txt = (
        f"🛍 <b>تاییدیه خرید</b>\n\n"
        f"📦 محصول: <b>{safe_title}</b>\n"
        f"{cost_line}\n"
        f"❓ آیا از خرید این محصول مطمئن هستید؟ در صورت تایید، مبلغ فوراً از حساب شما کسر می‌شود."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تایید و پرداخت", callback_data=f"confirm_buy_{product_id}"),
        InlineKeyboardButton(text="❌ انصراف", callback_data=f"cancel_buy_{product_id}"),
    ]])

    try:
        await callback.bot.send_message(buyer_id, confirm_txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        return await callback.answer(
            "⚠️ ابتدا ربات را در پیوی (چت خصوصی) استارت کنید، سپس دوباره روی خرید بزنید.",
            show_alert=True
        )

    await callback.answer("📩 جهت تایید نهایی خرید، به پیوی ربات مراجعه کنید.", show_alert=True)


@shop_router.callback_query(F.data.startswith("confirm_buy_"))
async def cb_confirm_buy(callback: CallbackQuery):
    """مرحله دوم: تایید نهایی خریدار در پیوی ربات و اجرای واقعی تراکنش خرید."""
    buyer_id = callback.from_user.id
    product_id = int(callback.data.split("_")[2])

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM products WHERE product_id = ?", (product_id,)) as cur:
                prod = await cur.fetchone()

            if not prod:
                await callback.answer("❌ محصول پیدا نشد یا حذف شده است.", show_alert=True)
                try:
                    await callback.message.edit_text("❌ این محصول دیگر در دسترس نیست.")
                except Exception:
                    pass
                return

            if prod["stock_type"] != "UNLIMITED" and prod["stock_qty"] <= 0:
                await callback.answer("❌ موجودی این محصول به اتمام رسیده است.", show_alert=True)
                try:
                    await callback.message.edit_text("❌ متاسفانه موجودی این محصول قبل از تایید شما به اتمام رسید.")
                except Exception:
                    pass
                return

            # جلوگیری از خرید بیش از یک عدد توسط یک کاربر، فقط برای محصولات محدود/تکی
            # (محصولات نامحدود هیچ محدودیتی در تعداد خرید ندارند)
            if prod["stock_type"] != "UNLIMITED":
                async with db.execute(
                    "SELECT 1 FROM orders WHERE buyer_id = ? AND product_id = ? LIMIT 1",
                    (buyer_id, product_id)
                ) as cur_chk:
                    already_bought = await cur_chk.fetchone()
                if already_bought:
                    await callback.answer(
                        "❌ این محصول محدود است و شما قبلاً یک عدد از آن را خریداری کرده‌اید.",
                        show_alert=True
                    )
                    try:
                        await callback.message.edit_text("❌ شما قبلاً این محصول محدود را خریداری کرده‌اید.")
                    except Exception:
                        pass
                    return

            async with db.execute("SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (buyer_id,)) as cur_u:
                buyer = await cur_u.fetchone()

            if not buyer or buyer["is_frozen"]:
                return await callback.answer("❌ حساب شما مسدود یا غیرفعال است.", show_alert=True)

            price = prod["price"]

            # محاسبه هزینه پست در صورت نیاز به پستچی
            courier_fee = 0
            if prod["needs_courier"]:
                t1 = await get_setting("tier1_pct")
                t2 = await get_setting("tier2_pct")
                t3 = await get_setting("tier3_pct")
                if price <= 99:
                    courier_fee = int(price * (t1 / 100.0))
                elif price <= 999:
                    courier_fee = int(price * (t2 / 100.0))
                else:
                    courier_fee = int(price * (t3 / 100.0))

            total_cost = price + courier_fee

            # 🔒 موجودی قابل‌استفاده (غیر از مبالغ قبلاً فریزشده مثل وثیقه وام یا سفارش‌های دیگر)
            transferable = max(0, buyer["balance"] - buyer["frozen_balance"])
            if transferable < total_cost:
                await callback.answer(f"❌ موجودی ناکافی! قیمت محصول: ₳ {price} + هزینه پست: ₳ {courier_fee} = مجموع: ₳ {total_cost}", show_alert=True)
                try:
                    await callback.message.edit_text("❌ موجودی حساب شما برای این خرید کافی نیست.")
                except Exception:
                    pass
                return

            # مالک فروشگاه (برای پرداخت سهم فروشنده در حالت آنی، و اطلاع‌رسانی در هر دو حالت)
            async with db.execute("SELECT owner_id FROM shops WHERE shop_id = ?", (prod["shop_id"],)) as cur_s:
                shop_owner_id = (await cur_s.fetchone())["owner_id"]

            if prod["needs_courier"]:
                # 🧊 سفارش‌های نیازمند پستچی: مبلغ کل (قیمت + هزینه پست) از balance خریدار
                # کسر و هم‌زمان به frozen_balance او اضافه می‌شود. هیچ پولی بین خریدار/فروشنده/
                # بانک/پستچی جابه‌جا نمی‌شود. تسویه نهایی و واقعی فقط پس از تایید تحویل توسط
                # پستچی (/confirm_dispatch) انجام خواهد شد.
                await db.execute(
                    "UPDATE users SET balance = balance - ?, frozen_balance = frozen_balance + ? WHERE user_id = ?",
                    (total_cost, total_cost, buyer_id)
                )
            else:
                # 📦 محصولات بدون نیاز به پستچی: تحویل فوری و دیجیتالی است، بنابراین تسویه
                # مالی نیز بلافاصله و مطابق منطق قبلی پروژه انجام می‌شود (بدون فریز).
                await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (total_cost, buyer_id))

                s_pct = await get_setting("shop_seller_pct")
                b_pct = await get_setting("shop_bank_pct")

                seller_share = int(price * (s_pct / 100.0))
                bank_share = int(price * (b_pct / 100.0))
                # باقی مانده درصد سوخت (امحا) می‌شود و به حسابی واریز نمی‌شود.

                await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (seller_share, shop_owner_id))
                # 🏛 سهم بانک از فروش، به‌عنوان درآمد سیستم مستقیماً به خزانه مرکزی واریز می‌شود
                await treasury_credit(db, bank_share, f"سهم بانک از فروش محصول #{product_id}", related_user=shop_owner_id)

            # کسر از موجودی انبار
            new_qty = prod["stock_qty"]
            if prod["stock_type"] != "UNLIMITED":
                new_qty -= 1
                await db.execute("UPDATE products SET stock_qty = ? WHERE product_id = ?", (new_qty, product_id))

            # تولید کد ۱۰ رقمی امنیتی
            code_10 = "".join(random.choices(string.digits, k=10))

            # ثبت سفارش (به همراه اسنپ‌شات محصول تا در صورت حذف فروشگاه/محصول، اطلاعات خرید باقی بماند)
            await db.execute(
                """INSERT INTO orders
                   (code_10, buyer_id, shop_id, product_id, price, courier_fee, status, product_title, product_desc, product_photo_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (code_10, buyer_id, prod["shop_id"], product_id, price, courier_fee,
                 "DISPATCHED" if prod["needs_courier"] else "DELIVERED",
                 prod["title"], prod["description"], prod["photo_id"], datetime.now(timezone.utc).isoformat())
            )
            await db.commit()

            # به روزرسانی بنر در کانال
            try:
                async with db.execute("SELECT channel_id FROM shops WHERE shop_id = ?", (prod["shop_id"],)) as cur_ch:
                    channel_id = (await cur_ch.fetchone())["channel_id"]

                stock_str = "موجود" if prod["stock_type"] == "UNLIMITED" else f"{new_qty} عدد"
                safe_title = html.escape(prod['title'])
                safe_desc = html.escape(prod['description'])
                caption = (
                    f"🛍 <b>{safe_title}</b>\n\n"
                    f"📝 {safe_desc}\n\n"
                    f"💰 قیمت: <code>₳ {prod['price']}</code>\n"
                    f"📦 موجودی: <b>{stock_str}</b>"
                )
                kb_buy = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍 خرید این محصول", callback_data=f"buy_prod_{product_id}")]])
                await callback.bot.edit_message_caption(chat_id=channel_id, message_id=prod["channel_msg_id"], caption=caption, reply_markup=kb_buy, parse_mode="HTML")
            except Exception:
                pass

    await callback.answer("🎉 خرید با موفقیت انجام شد!", show_alert=True)

    # ویرایش پیام تاییدیه در پیوی خریدار با جزئیات نهایی خرید
    if prod["needs_courier"]:
        cost_line = f"💰 قیمت محصول: <code>₳ {price}</code>\n🚚 هزینه پست: <code>₳ {courier_fee}</code>\n💳 مجموع پرداختی: <code>₳ {total_cost}</code>\n"
    else:
        cost_line = f"💰 مبلغ پرداختی: <code>₳ {price}</code>\n"
    escrow_note = "\n🧊 مبلغ فوق فریز شد و تا زمان تایید تحویل توسط پستچی، به کسی پرداخت نمی‌شود." if prod["needs_courier"] else ""
    msg_buyer = (
        f"🎉 خرید شما نهایی شد!\n"
        f"🛍 محصول: <b>{html.escape(prod['title'])}</b>\n"
        f"{cost_line}"
        f"🔐 کد امنیتی ۱۰ رقمی شما: <code>{code_10}</code>"
        f"{escrow_note}"
    )
    try:
        await callback.message.edit_text(msg_buyer, parse_mode="HTML")
    except Exception:
        try:
            await callback.bot.send_message(buyer_id, msg_buyer, parse_mode="HTML")
        except Exception:
            pass

    seller_escrow_note = "\n🧊 مبلغ این سفارش تا تایید تحویل توسط پستچی به حساب شما واریز نخواهد شد." if prod["needs_courier"] else ""
    msg_seller = f"🛍 سفارش جدید ثبت شد!\nمحصول: <b>{html.escape(prod['title'])}</b>\n🔐 کد امنیتی ۱۰ رقمی: <code>{code_10}</code>{seller_escrow_note}"
    try:
        await callback.bot.send_message(shop_owner_id, msg_seller, parse_mode="HTML")
    except Exception:
        pass

    if prod["needs_courier"]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM couriers") as cur_c:
                couriers = await cur_c.fetchall()

        msg_courier = (
            f"🚚 <b>سفارش جدید آماده ارسال!</b>\n"
            f"📦 محصول: <b>{html.escape(prod['title'])}</b>\n"
            f"💰 کرایه پست: <code>₳ {courier_fee}</code>\n"
            f"🔐 کد ۱۰ رقمی تحویل: <code>{code_10}</code>\n"
            f"جهت تایید تحویل، دستور زیر را بزنید:\n"
            f"<code>/confirm_dispatch {code_10}</code>"
        )
        for c in couriers:
            try:
                await callback.bot.send_message(c[0], msg_courier, parse_mode="HTML")
            except Exception:
                pass


@shop_router.callback_query(F.data.startswith("cancel_buy_"))
async def cb_cancel_buy(callback: CallbackQuery):
    """انصراف خریدار از خرید در مرحله تاییدیه؛ هیچ مبلغی کسر نشده بود."""
    await callback.answer("❌ خرید لغو شد.")
    try:
        await callback.message.edit_text("❌ خرید توسط شما لغو شد. هیچ مبلغی از حساب شما کسر نشد.")
    except Exception:
        pass


# --- ۴. دستورات پستچی‌ها (Couriers) ---

COURIER_ORDERS_PAGE_SIZE = 10


async def _is_courier_or_admin(user_id: int) -> bool:
    if is_super_admin(user_id):
        return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM couriers WHERE user_id = ?", (user_id,)) as cur:
            return bool(await cur.fetchone())


async def _fetch_dispatched_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT o.*, s.channel_title AS shop_name, u.full_name AS buyer_name "
            "FROM orders o LEFT JOIN shops s ON o.shop_id = s.shop_id "
            "LEFT JOIN users u ON o.buyer_id = u.user_id "
            "WHERE o.status = 'DISPATCHED' ORDER BY o.order_id ASC"
        ) as cur_o:
            return await cur_o.fetchall()


def _render_courier_orders_page(orders, page: int):
    total = len(orders)
    total_pages = max(1, math.ceil(total / COURIER_ORDERS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * COURIER_ORDERS_PAGE_SIZE
    page_items = orders[start:start + COURIER_ORDERS_PAGE_SIZE]

    txt = f"🚚 <b>سفارش‌های آماده ارسال (مخصوص پستچی) (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"تعداد مرسولات معلق: <code>{total}</code> مورد\n\n"
    for o in page_items:
        safe_shop = html.escape(o["shop_name"] or "نامشخص")
        safe_buyer = html.escape(o["buyer_name"] or "نامشخص")
        safe_desc = html.escape(o["product_desc"] or "بدون توضیحات")
        txt += (
            f"🔹 کد: <code>{o['code_10']}</code> | فروشگاه: <b>{safe_shop}</b> | "
            f"گیرنده: <b>{safe_buyer}</b> | توضیحات: {safe_desc}\n"
        )

    kb = _build_pagination_keyboard(page, total_pages, "courierorders_page")
    return txt, kb


@shop_router.message(Command("courier_orders"))
async def cmd_courier_orders(message: Message):
    if not is_private(message):
        return
    if not await _is_courier_or_admin(message.from_user.id):
        return await message.reply("❌ شما دسترسی پستچی ندارید.")

    orders = await _fetch_dispatched_orders()
    if not orders:
        return await message.reply("📦 هیچ سفارشی منتظر ارسال نیست.")

    txt, kb = _render_courier_orders_page(orders, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@shop_router.callback_query(F.data == "courierorders_page_noop")
async def cb_courier_orders_noop(callback: CallbackQuery):
    await callback.answer()


@shop_router.callback_query(F.data.startswith("courierorders_page_"))
async def cb_courier_orders_page(callback: CallbackQuery):
    if not await _is_courier_or_admin(callback.from_user.id):
        return await callback.answer("❌ شما دسترسی پستچی ندارید.", show_alert=True)

    page = int(callback.data.split("_")[2])
    orders = await _fetch_dispatched_orders()
    if not orders:
        return await callback.answer("📦 هیچ سفارشی منتظر ارسال نیست.", show_alert=True)
    txt, kb = _render_courier_orders_page(orders, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@shop_router.message(Command("confirm_dispatch"))
async def cmd_confirm_dispatch(message: Message):
    if not is_private(message):
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/confirm_dispatch [کد_۱۰_رقمی]</code>", parse_mode="HTML")

    code_10 = args[1].strip()
    courier_id = message.from_user.id

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM couriers WHERE user_id = ?", (courier_id,)) as cur:
                if not await cur.fetchone() and not is_super_admin(courier_id):
                    return await message.reply("❌ شما در لیست پستچی‌های مجاز قرار ندارید.")

            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM orders WHERE code_10 = ?", (code_10,)) as cur_o:
                order = await cur_o.fetchone()

            if not order:
                return await message.reply("❌ کدهای وارد شده نامعتبر است.")

            if order["status"] == "DELIVERED":
                return await message.reply("⚠️ این سفارش قبلاً تحویل داده شده است.")

            if order["status"] == "CANCELLED":
                return await message.reply("❌ این محصول توسط فروشنده حذف شده و دیگر در دسترس نیست. امکان تحویل این سفارش وجود ندارد.")

            price = order["price"]
            courier_fee = order["courier_fee"]
            total_cost = price + courier_fee

            c_pct = await get_setting("courier_pct")
            s_pct = await get_setting("shop_seller_pct")
            b_pct = await get_setting("shop_bank_pct")

            seller_share = int(price * (s_pct / 100.0))
            bank_share = int(price * (b_pct / 100.0))
            courier_share = int(courier_fee * (c_pct / 100.0))
            # باقی مانده درصد سوخت (امحا) می‌شود و به حسابی واریز نمی‌شود.

            async with db.execute("SELECT owner_id FROM shops WHERE shop_id = ?", (order["shop_id"],)) as cur_s:
                shop_row = await cur_s.fetchone()
            shop_owner_id = shop_row["owner_id"] if shop_row else None

            # 🏛 هزینه پست دریافتی از خریدار به‌عنوان درآمد سیستم به خزانه مرکزی واریز می‌شود
            # (سهم پستچی بلافاصله از همین محل به او پرداخت خواهد شد)
            if courier_fee > 0:
                await treasury_credit(db, courier_fee, f"هزینه پست دریافتی سفارش {order['code_10']}", related_user=order["buyer_id"])

            # 🏛 پرداخت سهم پستچی از خزانه مرکزی (طبق قانون عدم خلق پول)
            paid = await treasury_debit(
                db, courier_share, f"پرداخت سهم پستچی سفارش {order['code_10']}", related_user=courier_id
            )
            if not paid:
                await db.rollback()
                return await message.reply(
                    "❌ عدم امکان پرداخت به دلیل عدم کفایت موجودی خزانه. لطفاً بعداً دوباره تلاش کنید یا با سوپرادمین تماس بگیرید."
                )

            # 🔓 آزادسازی مبلغ فریزشده خریدار (balance او همان لحظه خرید کسر شده بود؛
            # اینجا فقط ردیابی فریز پاک می‌شود، نه balance)
            await db.execute(
                "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                (total_cost, order["buyer_id"])
            )
            # 💰 پرداخت سهم فروشنده (فقط الان، پس از تایید تحویل)
            if shop_owner_id:
                await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (seller_share, shop_owner_id))
            # 🏛 سهم بانک از فروش، به‌عنوان درآمد سیستم مستقیماً به خزانه مرکزی واریز می‌شود
            await treasury_credit(db, bank_share, f"سهم بانک از فروش سفارش {order['code_10']}", related_user=shop_owner_id or 0)

            # واریز سهم خالص به پستچی و تغییر وضعیت سفارش
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (courier_share, courier_id))
            await db.execute("UPDATE orders SET status = 'DELIVERED', courier_id = ? WHERE order_id = ?", (courier_id, order["order_id"]))
            await db.commit()

    await message.reply(f"✅ تحویل سفارش با موفقیت ثبت شد و مبلغ <code>₳ {courier_share}</code> به حساب شما واریز گردید.", parse_mode="HTML")


# --- ۵. خریداران و عمومی ---

MY_ORDERS_PAGE_SIZE = 10


def _order_status_label(status: str) -> str:
    if status == "DELIVERED":
        return "🟢 تحویل شده"
    elif status == "CANCELLED":
        return "❌ این محصول توسط فروشنده حذف شده و دیگر در دسترس نیست"
    return "🚚 در حال ارسال"


def _render_my_orders_page(orders, page: int):
    total = len(orders)
    total_pages = max(1, math.ceil(total / MY_ORDERS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * MY_ORDERS_PAGE_SIZE
    page_items = orders[start:start + MY_ORDERS_PAGE_SIZE]

    txt = f"📦 <b>سفارش‌های من (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"تعداد کل سفارش‌ها: <code>{total}</code> عدد\n\n"
    for o in page_items:
        safe_title = html.escape(o["product_title"] or "محصول حذف‌شده")
        st = _order_status_label(o["status"])
        txt += (
            f"🔹 کد: <code>{o['code_10']}</code> | {safe_title} | "
            f"<code>₳ {o['price']}</code> | {st}\n"
        )

    kb = _build_pagination_keyboard(page, total_pages, "myorders_page")
    return txt, kb


@shop_router.message(Command("my_orders"))
async def cmd_my_orders(message: Message):
    if not is_private(message):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY order_id DESC",
            (message.from_user.id,)
        ) as cur:
            orders = await cur.fetchall()

    if not orders:
        return await message.reply("🛍 شما هیچ سفارشی ثبت نکرده‌اید.")

    txt, kb = _render_my_orders_page(orders, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@shop_router.callback_query(F.data == "myorders_page_noop")
async def cb_my_orders_noop(callback: CallbackQuery):
    await callback.answer()


@shop_router.callback_query(F.data.startswith("myorders_page_"))
async def cb_my_orders_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY order_id DESC",
            (callback.from_user.id,)
        ) as cur:
            orders = await cur.fetchall()
    if not orders:
        return await callback.answer("🛍 سفارشی یافت نشد.", show_alert=True)
    txt, kb = _render_my_orders_page(orders, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


MY_ASSETS_PAGE_SIZE = 10


def _render_my_assets_page(orders, page: int):
    total = len(orders)
    total_pages = max(1, math.ceil(total / MY_ASSETS_PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * MY_ASSETS_PAGE_SIZE
    page_items = orders[start:start + MY_ASSETS_PAGE_SIZE]

    txt = f"🎁 <b>دارایی‌ها و محصولات خریداری‌شده (صفحه {page + 1} از {total_pages})</b>\n"
    txt += f"مجموع دارایی‌ها: <code>{total}</code> آیتم\n\n"
    for o in page_items:
        safe_title = html.escape(o["product_title"] or "محصول حذف‌شده")
        received_at = o["created_at"][:10] if o["created_at"] else "نامشخص"
        txt += (
            f"🔹 <b>{safe_title}</b> | کد: <code>{o['code_10']}</code> | "
            f"تاریخ دریافت: <code>{received_at}</code>\n"
        )

    kb = _build_pagination_keyboard(page, total_pages, "myassets_page")
    return txt, kb


@shop_router.message(Command("my_assets"))
async def cmd_my_assets(message: Message):
    if not is_private(message):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY order_id DESC",
            (message.from_user.id,)
        ) as cur:
            orders = await cur.fetchall()

    if not orders:
        return await message.reply("❌ هیچ دارایی/محصولی یافت نشد.")

    txt, kb = _render_my_assets_page(orders, 0)
    await message.reply(txt, reply_markup=kb, parse_mode="HTML")


@shop_router.callback_query(F.data == "myassets_page_noop")
async def cb_my_assets_noop(callback: CallbackQuery):
    await callback.answer()


@shop_router.callback_query(F.data.startswith("myassets_page_"))
async def cb_my_assets_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY order_id DESC",
            (callback.from_user.id,)
        ) as cur:
            orders = await cur.fetchall()
    if not orders:
        return await callback.answer("❌ هیچ دارایی/محصولی یافت نشد.", show_alert=True)
    txt, kb = _render_my_assets_page(orders, page)
    try:
        await callback.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer()


@shop_router.message(Command("track"))
async def cmd_track(message: Message):
    args = message.text.split()
    if len(args) < 2:
        return await message.reply("راهنما: <code>/track [کد_۱۰_رقمی]</code>", parse_mode="HTML")

    code_10 = args[1].strip()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE code_10 = ?", (code_10,)) as cur:
            order = await cur.fetchone()

    if not order:
        return await message.reply("❌ سفارشی با این کد ۱۰ رقمی پیدا نشد.")

    if order["status"] == "DELIVERED":
        st = "🟢 تحویل داده شده"
    elif order["status"] == "CANCELLED":
        st = "❌ این محصول توسط فروشنده حذف شده و دیگر در دسترس نیست"
    else:
        st = "🚚 در حال ارسال توسط پستچی"
    safe_title = html.escape(order['product_title'] or 'محصول حذف‌شده')
    await message.reply(
        f"🔎 <b>پیگیری سفارش</b>\n\n"
        f"🛍 محصول: <b>{safe_title}</b>\n"
        f"🔐 کد پیگیری: <code>{order['code_10']}</code>\n"
        f"⚡ وضعیت: <b>{st}</b>",
        parse_mode="HTML"
    )


# =====================================================================================
# 🏦 بخش ششم: بانک آترامنتوم (Atramentum Bank)
# =====================================================================================
# 📌 طبق الزامات جدید، تمام مراحل بانک (واریز/برداشت/بازگشت به صفحه اصلی) فقط با ویرایش
# همان پیام اولیه بانک (Edit Message) انجام می‌شود و هیچ پیام جدیدی ارسال نمی‌گردد.
# پیام ریپلای کاربر نیز پس از پردازش حذف می‌شود تا محیط گفتگو مرتب بماند.

def _bank_panel_text(u) -> str:
    total_after_profit = u["bank_savings"] + u["last_daily_profit"]
    return (
        "🏦 <b>بانک آترامنتوم</b>\n\n"
        f"💰 مبلغ کل موجود در بانک: <code>₳ {u['bank_savings']}</code>\n"
        f"📈 آخرین سود روزانه دریافتی: <code>₳ {u['last_daily_profit']}</code>\n"
        f"🧮 موجودی کل بعد از دریافت سود: <code>₳ {total_after_profit}</code>"
    )


def _bank_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📥 واریز پول", callback_data="bank_deposit"),
        InlineKeyboardButton(text="📤 برداشت پول", callback_data="bank_withdraw"),
    ]])


def _bank_full_text(u) -> str:
    transferable = max(0, u["balance"] - u["frozen_balance"])
    return (
        "🏦 <b>حساب بانکی آترامنتوم شما</b>\n\n"
        f"💰 موجودی کل کیف پول: <code>₳ {u['balance']}</code>\n"
        f"🔒 موجودی وثیقه قفل‌شده: <code>₳ {u['frozen_balance']}</code>\n"
        f"💳 موجودی قابل انتقال: <code>₳ {transferable}</code>\n\n"
        f"🏦 سپرده بانکی فعلی: <code>₳ {u['bank_savings']}</code>\n"
        f"📈 آخرین سود روزانه دریافتی: <code>₳ {u['last_daily_profit']}</code>"
    )


def _bank_full_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📥 واریز پول", callback_data="bank_deposit"),
            InlineKeyboardButton(text="📤 برداشت پول", callback_data="bank_withdraw"),
        ],
        [InlineKeyboardButton(text="💳 وام‌های آترامنتوم", callback_data="loan_menu")],
        [InlineKeyboardButton(text="⚙️ مدیریت حساب بانکی", callback_data="bank_manage")],
    ])


def _bank_deposit_prompt_text() -> str:
    return (
        "📥 <b>واریز به بانک آترامنتوم</b>\n\n"
        "لطفاً مبلغ موردنظر برای واریز را با ریپلای روی همین پیام ارسال کنید.\n"
        "⚠️ فقط تا سقف موجودی قابل انتقال شما (موجودی منهای وثیقه قفل‌شده) قابل واریز است "
        f"و سقف کل سپرده بانکی ₳{BANK_SAVINGS_CAP} است."
    )


def _bank_withdraw_prompt_text() -> str:
    return (
        "📤 <b>برداشت از بانک آترامنتوم</b>\n\n"
        "لطفاً مبلغ موردنظر برای برداشت را با ریپلای روی همین پیام ارسال کنید."
    )


def _bank_detect_panel_type(message: Message) -> str:
    """نوع پنل بانکی (کامل یا سریع) را از روی تعداد ردیف‌های دکمه پیام فعلی تشخیص می‌دهد."""
    try:
        rows = message.reply_markup.inline_keyboard
        return "full" if len(rows) > 1 else "panel"
    except Exception:
        return "panel"


def _kb_has_callback(reply_markup, target: str) -> bool:
    """بررسی می‌کند کیبورد فعلی پیام حاوی دکمه‌ای با callback_data مشخص هست یا نه؛ برای تشخیص
    اینکه کاربر از مسیر پروفایل («🔙 برگشت به پروفایل») وارد بخش بانک/وام شده تا همان دکمه در
    زیرمنوهای بعدی هم حفظ شود."""
    try:
        for row in reply_markup.inline_keyboard:
            for btn in row:
                if btn.callback_data == target:
                    return True
    except Exception:
        pass
    return False


def _append_prof_back_row(kb):
    """دکمه «🔙 برگشت به پروفایل» را به‌صورت یک ردیف جدید به کیبورد اضافه می‌کند؛ اگر کیبوردی
    وجود نداشته باشد، کیبورد جدیدی فقط با همین دکمه ساخته می‌شود."""
    back_row = [InlineKeyboardButton(text="🔙 برگشت به پروفایل", callback_data="prof_home")]
    if kb:
        return InlineKeyboardMarkup(inline_keyboard=kb.inline_keyboard + [back_row])
    return InlineKeyboardMarkup(inline_keyboard=[back_row])


async def _bank_render(user_id: int, panel_type: str, with_back: bool = False):
    """متن و کیبورد صفحه اصلی بانک را بر اساس نوع پنل و موجودی به‌روز کاربر می‌سازد.
    در صورت with_back=True، دکمه «🔙 برگشت به پروفایل» به همان ردیف آخر کیبورد اضافه می‌شود
    (بدون افزودن ردیف جدید) تا منطق تشخیص نوع پنل (_bank_detect_panel_type) دست‌نخورده بماند."""
    u = await get_user_data(user_id)
    if not u:
        return None
    if panel_type == "full":
        text, kb = _bank_full_text(u), _bank_full_buttons()
    else:
        text, kb = _bank_panel_text(u), _bank_buttons()
    if with_back:
        rows = [list(row) for row in kb.inline_keyboard]
        rows[-1].append(InlineKeyboardButton(text="🔙 برگشت به پروفایل", callback_data="prof_home"))
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
    return text, kb


async def _bank_edit_main(
    bot: Bot, chat_id: int, message_id: int, user_id: int, panel_type: str, note: str = "", with_back: bool = False
) -> None:
    """پیام اصلی بانک را ویرایش کرده و به صفحه اصلی (با موجودی جدید) بازمی‌گرداند."""
    if not message_id:
        return
    rendered = await _bank_render(user_id, panel_type, with_back=with_back)
    if not rendered:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ حساب شما یافت نشد.")
        except Exception:
            pass
        return
    text, kb = rendered
    if note:
        text = f"{note}\n\n{text}"
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@user_router.message(F.text == "بانک آترامنتوم")
async def cmd_bank_panel(message: Message):
    """این دستور متنی هم در گروه و هم در پیوی فعال است."""
    user_id = message.from_user.id
    await sync_user(user_id, message.from_user.username, message.from_user.full_name)
    u = await get_user_data(user_id)
    if not u:
        return await message.reply("❌ حساب شما یافت نشد.")
    await message.reply(_bank_panel_text(u), reply_markup=_bank_buttons(), parse_mode="HTML")


@user_router.message(Command("bank"))
@user_router.message(Command("atramentum_bank"))
@user_router.message(F.text == "بانک")
async def cmd_bank_full(message: Message):
    """این دستورات صرفاً در پیوی فعال هستند و اطلاعات کامل کیف پول را نمایش می‌دهند."""
    if not is_private(message):
        return
    user_id = message.from_user.id
    await sync_user(user_id, message.from_user.username, message.from_user.full_name)
    rendered = await _bank_render(user_id, "full")
    if not rendered:
        return await message.reply("❌ حساب شما یافت نشد.")
    text, kb = rendered
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@user_router.callback_query(F.data == "bank_manage")
async def cb_bank_manage(callback: CallbackQuery):
    await callback.answer(
        "⚙️ برای واریز/برداشت از دکمه‌های مربوطه و برای وام از بخش «وام‌های آترامنتوم» استفاده کنید.",
        show_alert=True,
    )


@user_router.callback_query(F.data == "bank_deposit")
async def cb_bank_deposit(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    panel_type = _bank_detect_panel_type(callback.message)
    from_profile = _kb_has_callback(callback.message.reply_markup, "prof_home")

    try:
        await callback.message.edit_text(_bank_deposit_prompt_text(), parse_mode="HTML")
    except Exception:
        pass

    await state.update_data(
        bank_user=user_id, bank_chat_id=chat_id, bank_msg_id=message_id, bank_panel_type=panel_type,
        bank_from_profile=from_profile,
    )
    await state.set_state(BankForm.waiting_for_deposit_amount)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, chat_id, user_id, current_state,
        lambda: _bank_edit_main(
            callback.bot, chat_id, message_id, user_id, panel_type,
            note="⏳ عملیات واریز به دلیل عدم دریافت پاسخ در بازه ۱ دقیقه به‌صورت خودکار لغو شد.",
            with_back=from_profile,
        ),
    )
    await callback.answer()


@user_router.callback_query(F.data == "bank_withdraw")
async def cb_bank_withdraw(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    panel_type = _bank_detect_panel_type(callback.message)
    from_profile = _kb_has_callback(callback.message.reply_markup, "prof_home")

    try:
        await callback.message.edit_text(_bank_withdraw_prompt_text(), parse_mode="HTML")
    except Exception:
        pass

    await state.update_data(
        bank_user=user_id, bank_chat_id=chat_id, bank_msg_id=message_id, bank_panel_type=panel_type,
        bank_from_profile=from_profile,
    )
    await state.set_state(BankForm.waiting_for_withdraw_amount)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, chat_id, user_id, current_state,
        lambda: _bank_edit_main(
            callback.bot, chat_id, message_id, user_id, panel_type,
            note="⏳ عملیات برداشت به دلیل عدم دریافت پاسخ در بازه ۱ دقیقه به‌صورت خودکار لغو شد.",
            with_back=from_profile,
        ),
    )
    await callback.answer()


@user_router.message(BankForm.waiting_for_deposit_amount)
async def process_bank_deposit(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id
    if user_id != data.get("bank_user"):
        return
    chat_id = data.get("bank_chat_id", message.chat.id)
    bank_msg_id = data.get("bank_msg_id")
    panel_type = data.get("bank_panel_type", "panel")
    from_profile = data.get("bank_from_profile", False)

    if not message.reply_to_message or message.reply_to_message.message_id != bank_msg_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id, message_id=bank_msg_id,
                text="⚠️ لطفاً روی همین پیام ریپلای کرده و مبلغ عددی را ارسال کنید.\n\n" + _bank_deposit_prompt_text(),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    cancel_input_timeout(chat_id, user_id)

    async def _fail(note_text: str):
        await state.clear()
        await _bank_edit_main(message.bot, chat_id, bank_msg_id, user_id, panel_type, note=note_text, with_back=from_profile)
        try:
            await message.delete()
        except Exception:
            pass

    try:
        amount = int(message.text.strip())
    except (ValueError, AttributeError):
        return await _fail("❌ مبلغ وارد شده نامعتبر است. برای تلاش مجدد، دوباره از منوی بانک اقدام کنید.")
    if amount <= 0:
        return await _fail("❌ مبلغ باید مثبت باشد. برای تلاش مجدد، دوباره از منوی بانک اقدام کنید.")

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT balance, frozen_balance, bank_savings, is_frozen FROM users WHERE user_id = ?",
                (user_id,),
            ) as cur:
                u = await cur.fetchone()
            if not u or u["is_frozen"]:
                return await _fail("❌ حساب شما مسدود (فریز) است.")

            transferable = max(0, u["balance"] - u["frozen_balance"])
            if amount > transferable:
                return await _fail(
                    f"❌ حداکثر مبلغ قابل واریز شما (موجودی قابل انتقال): <code>₳ {transferable}</code>"
                )
            remaining_cap = max(0, BANK_SAVINGS_CAP - u["bank_savings"])
            if amount > remaining_cap:
                return await _fail(
                    f"❌ سقف سپرده‌گذاری بانک <code>₳ {BANK_SAVINGS_CAP}</code> است.\n"
                    f"سقف باقیمانده قابل واریز شما: <code>₳ {remaining_cap}</code>"
                )

            await db.execute(
                "UPDATE users SET balance = balance - ?, bank_savings = bank_savings + ? WHERE user_id = ?",
                (amount, amount, user_id),
            )
            await db.commit()

    await state.clear()
    await _bank_edit_main(
        message.bot, chat_id, bank_msg_id, user_id, panel_type,
        note=f"✅ مبلغ <code>₳ {amount}</code> با موفقیت به حساب بانکی شما واریز شد.",
        with_back=from_profile,
    )
    try:
        await message.delete()
    except Exception:
        pass


@user_router.message(BankForm.waiting_for_withdraw_amount)
async def process_bank_withdraw(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id
    if user_id != data.get("bank_user"):
        return
    chat_id = data.get("bank_chat_id", message.chat.id)
    bank_msg_id = data.get("bank_msg_id")
    panel_type = data.get("bank_panel_type", "panel")
    from_profile = data.get("bank_from_profile", False)

    if not message.reply_to_message or message.reply_to_message.message_id != bank_msg_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id, message_id=bank_msg_id,
                text="⚠️ لطفاً روی همین پیام ریپلای کرده و مبلغ عددی را ارسال کنید.\n\n" + _bank_withdraw_prompt_text(),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    cancel_input_timeout(chat_id, user_id)

    async def _fail(note_text: str):
        await state.clear()
        await _bank_edit_main(message.bot, chat_id, bank_msg_id, user_id, panel_type, note=note_text, with_back=from_profile)
        try:
            await message.delete()
        except Exception:
            pass

    try:
        amount = int(message.text.strip())
    except (ValueError, AttributeError):
        return await _fail("❌ مبلغ وارد شده نامعتبر است. برای تلاش مجدد، دوباره از منوی بانک اقدام کنید.")
    if amount <= 0:
        return await _fail("❌ مبلغ باید مثبت باشد. برای تلاش مجدد، دوباره از منوی بانک اقدام کنید.")

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT bank_savings, is_frozen FROM users WHERE user_id = ?", (user_id,)
            ) as cur:
                u = await cur.fetchone()
            if not u or u["is_frozen"]:
                return await _fail("❌ حساب شما مسدود (فریز) است.")
            if amount > u["bank_savings"]:
                return await _fail(
                    f"❌ موجودی بانکی شما کافی نیست. سپرده فعلی: <code>₳ {u['bank_savings']}</code>"
                )

            await db.execute(
                "UPDATE users SET balance = balance + ?, bank_savings = bank_savings - ? WHERE user_id = ?",
                (amount, amount, user_id),
            )
            await db.commit()

    await state.clear()
    await _bank_edit_main(
        message.bot, chat_id, bank_msg_id, user_id, panel_type,
        note=f"✅ مبلغ <code>₳ {amount}</code> با موفقیت از بانک به کیف پول شما برداشت شد.",
        with_back=from_profile,
    )
    try:
        await message.delete()
    except Exception:
        pass


# --- ⏰ پردازش خودکار شبانه سود بانک (ساعت ۰۰:۰۰ به وقت ایران) ---

async def run_nightly_bank_interest(bot: Bot):
    rate_raw = await get_setting("bank_daily_rate")
    try:
        rate = float(rate_raw) / 100.0
    except (TypeError, ValueError):
        rate = 0.0123
    if rate <= 0:
        return

    # 🏛 نسبت تأمین سود روزانه بانک از خزانه مرکزی؛ باقی‌مانده (خلق پول) بدون کسر از خزانه تأمین می‌شود
    treasury_pct_raw = await get_setting("bank_treasury_profit_pct")
    try:
        treasury_pct = float(treasury_pct_raw)
        if not (0 <= treasury_pct <= 100):
            raise ValueError
    except (TypeError, ValueError):
        treasury_pct = 45.0

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, bank_savings FROM users WHERE bank_savings > 0"
        ) as cur:
            savers = await cur.fetchall()

    for saver in savers:
        user_id = saver["user_id"]
        savings = saver["bank_savings"]
        raw_profit = int(savings * rate)
        if raw_profit <= 0:
            continue

        # سقف سپرده‌گذاری: فقط بخشی از سود که باعث عبور سپرده از سقف نمی‌شود پرداخت می‌شود
        allowed_profit = min(raw_profit, max(0, BANK_SAVINGS_CAP - savings))
        if allowed_profit <= 0:
            continue

        # --- 🏛 تفکیک منبع سود روزانه: بخشی از خزانه مرکزی و بخشی به‌صورت خلق پول ---
        # قانون «بدون توکن‌سوزی»: کل مبلغ allowed_profit همیشه به‌طور کامل به کاربر پرداخت می‌شود.
        # ابتدا سهم خزانه (treasury_pct٪) تلاش می‌شود از خزانه مرکزی کسر شود؛ اگر موجودی خزانه برای
        # این سهم کافی نباشد (به‌جای لغو کل پرداخت که معادل توکن‌سوزی سود کاربر بود)، فقط همان مقداری
        # که واقعاً در خزانه موجود است کسر می‌شود و باقی سهم به‌صورت خلق پول تأمین می‌گردد؛ یعنی هیچ
        # مبلغی بیش از موجودی واقعی از خزانه کسر نمی‌شود و مابقی موجودی خزانه دست‌نخورده در آن باقی می‌ماند.
        treasury_share = int(allowed_profit * (treasury_pct / 100.0))

        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                actual_treasury_debit = 0
                if treasury_share > 0:
                    async with db.execute(
                        "SELECT balance FROM users WHERE user_id = ?", (TREASURY_USER_ID,)
                    ) as cur_t:
                        t_row = await cur_t.fetchone()
                    treasury_balance = t_row[0] if t_row else 0
                    debit_attempt = min(treasury_share, max(0, treasury_balance))
                    if debit_attempt > 0:
                        paid = await treasury_debit(
                            db, debit_attempt,
                            f"سهم خزانه از سود روزانه بانک برای کاربر {user_id}",
                            related_user=user_id,
                        )
                        if paid:
                            actual_treasury_debit = debit_attempt

                # مابقی سود (سهم خزانه‌ای که کسر نشد + سهم خلق‌شده) بدون کسر از خزانه تأمین می‌شود
                minted_share = allowed_profit - actual_treasury_debit
                if minted_share > 0:
                    tx_id = f"MINT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
                    await db.execute(
                        "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            tx_id,
                            datetime.now(timezone.utc).isoformat(),
                            0,
                            user_id,
                            minted_share,
                            f"سهم خلق‌شده از سود روزانه بانک برای کاربر {user_id}",
                        ),
                    )

                await db.execute(
                    "UPDATE users SET bank_savings = bank_savings + ?, last_daily_profit = ?, "
                    "last_bank_claim = ? WHERE user_id = ?",
                    (allowed_profit, allowed_profit, datetime.now(timezone.utc).isoformat(), user_id),
                )
                await db.commit()

        try:
            await bot.send_message(
                user_id,
                "🏦 <b>سود روزانه بانک آترامنتوم</b>\n\n"
                f"💰 سود امروز شما: <code>₳ {allowed_profit}</code>\n"
                f"🏦 موجودی جدید سپرده: <code>₳ {savings + allowed_profit}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass

        # فاصله کوتاه بین کاربران جهت جلوگیری از قفل شدن دیتابیس (db_lock)
        await asyncio.sleep(0.05)


async def bank_interest_loop(bot: Bot):
    while True:
        now_ir = datetime.now(IRAN_TZ)
        next_run = (now_ir + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = max(60.0, (next_run - now_ir).total_seconds())
        await asyncio.sleep(wait_seconds)
        try:
            await run_nightly_bank_interest(bot)
        except Exception as e:
            logging.error(f"❌ خطا در پردازش سود شبانه بانک: {e}")


# =====================================================================================
# 💳 بخش هفتم: سیستم وام پویا و پیشرفته (Atramentum Loans)
# =====================================================================================

async def _get_loan_settings() -> dict:
    keys = [
        "min_loan_amount", "max_loan_amount", "min_loan_interest", "max_loan_interest",
        "allowed_installments", "collateral_rate", "required_balance_rate", "late_penalty_rate",
        "loan_guarantor_balance_rate_borrower", "loan_guarantor_balance_rate_guarantor",
        "loan_guarantor_collateral_rate_borrower", "loan_guarantor_collateral_rate_guarantor",
    ]
    result = {}
    for k in keys:
        result[k] = await get_setting(k)
    return result


def _compute_dynamic_interest(amount: int, settings: dict) -> float:
    """نرخ سود به‌صورت پویا و متناسب با مبلغ وام بین حداقل و حداکثر تعیین‌شده محاسبه می‌شود."""
    min_amt = float(settings.get("min_loan_amount") or 1)
    max_amt = float(settings.get("max_loan_amount") or (min_amt + 1))
    min_int = float(settings.get("min_loan_interest") or 0)
    max_int = float(settings.get("max_loan_interest") or min_int)
    if max_amt <= min_amt:
        return round(min_int, 2)
    ratio = max(0.0, min(1.0, (amount - min_amt) / (max_amt - min_amt)))
    interest = min_int + (max_int - min_int) * ratio
    return round(interest, 2)


async def _create_loan_installments(db, loan_id: int, total_repayment: int, count: int, created_at: datetime):
    """
    اقساط وام را با فاصله دقیق ۱۰ روز از یکدیگر می‌سازد: قسط اول ۱۰ روز پس از لحظه
    واریز موفق وام (created_at)، و هر قسط بعدی = سررسید قسط قبلی + ۱۰ روز.
    این تابع برای هر دو نوع وام (COLLATERAL و GUARANTOR) به‌طور یکسان استفاده می‌شود.
    """
    base_each = total_repayment // count
    remainder = total_repayment - (base_each * count)
    for i in range(1, count + 1):
        amt = base_each + (remainder if i == count else 0)
        due_date = created_at + timedelta(days=10 * i)
        await db.execute(
            """
            INSERT INTO loan_installments
            (loan_id, installment_number, amount, due_date, status, base_amount, penalty_amount, last_reminder_stage)
            VALUES (?, ?, ?, ?, 'PENDING', ?, 0, '')
            """,
            (loan_id, i, amt, due_date.isoformat(), amt),
        )


def _loan_summary_text(
    target_data, amount: int, interest: float, installments: int, total_repayment: int, loan_type: str,
    collateral_amount: int = 0, borrower_collateral: int = 0, guarantor_collateral: int = 0,
) -> str:
    safe_name = html.escape(target_data["full_name"] or "ناشناس")
    type_label = "🔒 وثیقه‌ای" if loan_type == "COLLATERAL" else "🤝 ضامنی"
    lines = [
        f"👤 متقاضی: <b>{safe_name}</b>",
        f"💰 موجودی فعلی: <code>₳ {target_data['balance']}</code>",
        f"💳 مبلغ وام: <code>₳ {amount}</code>",
        f"📈 نرخ سود: <b>{interest}٪</b>",
        f"🔢 تعداد اقساط: <b>{installments}</b>",
        f"🧮 مجموع بازپرداخت: <code>₳ {total_repayment}</code>",
        f"🏷 نوع وام: {type_label}",
    ]
    if loan_type == "COLLATERAL":
        lines.append(f"🔒 مبلغ وثیقه قفل‌شده: <code>₳ {collateral_amount}</code>")
    else:
        lines.append(f"🔒 وثیقه گیرنده: <code>₳ {borrower_collateral}</code>")
        lines.append(f"🔒 وثیقه ضامن: <code>₳ {guarantor_collateral}</code>")
    return "\n".join(lines)


@user_router.message(Command("loan"))
@user_router.message(F.text == "درخواست وام")
async def cmd_loan_start(message: Message, state: FSMContext):
    if not is_private(message):
        return
    user_id = message.from_user.id
    await sync_user(user_id, message.from_user.username, message.from_user.full_name)

    # 🚫 هر کاربر همزمان فقط یک درخواست وام در حال بررسی (Pending) می‌تواند داشته باشد
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM loans WHERE user_id = ? AND status IN ('PENDING_ADMIN', 'PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL') LIMIT 1",
            (user_id,),
        ) as cur:
            has_pending = await cur.fetchone()
    if has_pending:
        return await message.reply(
            "❌ شما در حال حاضر یک درخواست وام در حال بررسی دارید. تا زمان تأیید/رد یا لغو آن، امکان ثبت درخواست جدید وجود ندارد.\n"
            "برای مشاهده و لغو درخواست فعلی: <code>/my_loans</code>",
            parse_mode="HTML",
        )

    settings = await _get_loan_settings()
    await state.update_data(loan_settings=settings)
    prompt = await message.reply(
        "💳 <b>درخواست وام آترامنتوم</b>\n\n"
        f"💰 مبلغ وام باید بین <code>₳ {int(settings['min_loan_amount'])}</code> "
        f"تا <code>₳ {int(settings['max_loan_amount'])}</code> باشد.\n\n"
        "لطفاً مبلغ درخواستی خود را ارسال کنید:",
        parse_mode="HTML",
    )
    await state.set_state(LoanForm.waiting_for_amount)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, user_id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@user_router.callback_query(F.data == "loan_menu")
async def cb_loan_menu(callback: CallbackQuery, state: FSMContext):
    from_profile = _kb_has_callback(callback.message.reply_markup, "prof_home")
    kb_rows = [
        [InlineKeyboardButton(text="➕ درخواست وام جدید", callback_data="loan_new_request")],
        [InlineKeyboardButton(text="📋 وام‌های من", callback_data="loan_my_list")],
    ]
    if from_profile:
        kb_rows.append([InlineKeyboardButton(text="🔙 برگشت به پروفایل", callback_data="prof_home")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    try:
        await callback.message.edit_text("💳 <b>وام‌های آترامنتوم</b>\n\nیکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=kb, parse_mode="HTML")
    except Exception:
        await callback.message.answer("💳 <b>وام‌های آترامنتوم</b>\n\nیکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=kb, parse_mode="HTML")
    await callback.answer()


@user_router.callback_query(F.data == "loan_new_request")
async def cb_loan_new_request(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    # 🚫 هر کاربر همزمان فقط یک درخواست وام در حال بررسی (Pending) می‌تواند داشته باشد
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM loans WHERE user_id = ? AND status IN ('PENDING_ADMIN', 'PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL') LIMIT 1",
            (user_id,),
        ) as cur:
            has_pending = await cur.fetchone()
    if has_pending:
        return await callback.answer(
            "❌ شما در حال حاضر یک درخواست وام در حال بررسی دارید. تا زمان تأیید/رد یا لغو آن، امکان ثبت درخواست جدید وجود ندارد.",
            show_alert=True,
        )

    settings = await _get_loan_settings()
    await state.update_data(loan_settings=settings)
    prompt = await callback.message.answer(
        "💳 <b>درخواست وام آترامنتوم</b>\n\n"
        f"💰 مبلغ وام باید بین <code>₳ {int(settings['min_loan_amount'])}</code> "
        f"تا <code>₳ {int(settings['max_loan_amount'])}</code> باشد.\n\n"
        "لطفاً مبلغ درخواستی خود را ارسال کنید:",
        parse_mode="HTML",
    )
    await state.set_state(LoanForm.waiting_for_amount)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, prompt.message_id),
    )
    await callback.answer()


@user_router.message(LoanForm.waiting_for_amount)
async def loan_process_amount(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    data = await state.get_data()
    settings = data.get("loan_settings") or await _get_loan_settings()

    def _reschedule():
        current_state = LoanForm.waiting_for_amount.state
        schedule_input_timeout(
            state, message.chat.id, message.from_user.id, current_state,
            lambda: _default_timeout_notice(message.bot, message.chat.id, None),
        )

    try:
        amount = int(message.text.strip())
    except (ValueError, AttributeError):
        await message.reply("❌ لطفاً یک عدد صحیح ارسال کنید.")
        return _reschedule()

    min_amt = int(settings["min_loan_amount"])
    max_amt = int(settings["max_loan_amount"])
    if amount < min_amt or amount > max_amt:
        await message.reply(
            f"❌ مبلغ وام باید بین <code>₳ {min_amt}</code> تا <code>₳ {max_amt}</code> باشد.",
            parse_mode="HTML",
        )
        return _reschedule()

    allowed_raw = str(settings.get("allowed_installments") or "2,3")
    allowed_list = [p.strip() for p in allowed_raw.split(",") if p.strip()]

    await state.update_data(loan_amount=amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{n} قسط", callback_data=f"loan_inst_{n}") for n in allowed_list
    ]])
    prompt = await message.reply(
        f"🔢 تعداد اقساط مورد نظر خود را انتخاب کنید (مجاز: {allowed_raw}):",
        reply_markup=kb,
    )
    await state.set_state(LoanForm.waiting_for_installments)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, message.from_user.id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@user_router.callback_query(LoanForm.waiting_for_installments, F.data.startswith("loan_inst_"))
async def loan_process_installments(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    installments = int(callback.data.split("_")[2])
    await state.update_data(loan_installments=installments)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 وثیقه‌ای", callback_data="loan_method_collateral")],
        [InlineKeyboardButton(text="🤝 ضامنی", callback_data="loan_method_guarantor")],
    ])
    try:
        await callback.message.edit_text(
            "🏷 روش دریافت وام را انتخاب کنید:\n\n"
            "🔒 <b>وثیقه‌ای:</b> بخشی از موجودی شما به‌عنوان وثیقه قفل می‌شود.\n"
            "🤝 <b>ضامنی:</b> شخص دیگری به‌عنوان ضامن شما تایید می‌کند.",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception:
        pass
    await state.set_state(LoanForm.waiting_for_method)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


async def _send_loan_request_to_admins(bot: Bot, loan_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur:
            loan = await cur.fetchone()
        target_data = await get_user_data(loan["user_id"])

    treasury_balance = await get_treasury_balance()
    collateral_amount = 0
    borrower_collateral = 0
    guarantor_collateral = 0
    if loan["loan_type"] == "COLLATERAL":
        collateral_amount = loan["collateral_amount"] or 0
    else:
        borrower_collateral = loan["borrower_collateral_amount"] or 0
        guarantor_collateral = loan["guarantor_collateral_amount"] or 0

    summary = _loan_summary_text(
        target_data, loan["total_amount"], loan["interest_rate"], loan["installments_count"],
        loan["total_repayment"], loan["loan_type"], collateral_amount, borrower_collateral, guarantor_collateral
    )
    collateral_note = ""
    if loan["loan_type"] == "COLLATERAL":
        collateral_note = "\n⚠️ وثیقه هنوز قفل نشده و فقط با تأیید شما قفل خواهد شد."
    else:
        collateral_note = "\n⚠️ وثیقه‌های گیرنده و ضامن هنوز قفل نشده‌اند و فقط با تأیید شما قفل خواهند شد."
    text = (
        "💳 <b>درخواست وام جدید</b>\n\n"
        f"{summary}\n\n"
        f"🏛 موجودی فعلی خزانه: <code>₳ {treasury_balance}</code>"
        f"{collateral_note}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🟢 تأیید و واریز وام", callback_data=f"loan_admin_approve_{loan_id}"),
        InlineKeyboardButton(text="🔴 رد درخواست", callback_data=f"loan_admin_reject_{loan_id}"),
    ]])
    for sa_id in SUPER_ADMINS:
        try:
            await bot.send_message(sa_id, text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass


@user_router.callback_query(LoanForm.waiting_for_method, F.data == "loan_method_collateral")
async def loan_method_collateral(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    data = await state.get_data()
    settings = data.get("loan_settings") or await _get_loan_settings()
    amount = data["loan_amount"]
    installments = data["loan_installments"]
    user_id = callback.from_user.id

    required_rate = float(settings["required_balance_rate"])
    collateral_rate = float(settings["collateral_rate"])
    required_balance = int(amount * required_rate)
    collateral_amount = int(amount * collateral_rate)

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (user_id,)
            ) as cur:
                u = await cur.fetchone()

            if not u or u["is_frozen"]:
                await state.clear()
                return await callback.message.edit_text("❌ حساب شما مسدود (فریز) است.")

            transferable = max(0, u["balance"] - u["frozen_balance"])
            if transferable < required_balance:
                await state.clear()
                return await callback.message.edit_text(
                    f"❌ برای دریافت این وام باید حداقل <code>₳ {required_balance}</code> "
                    f"در موجودی آزاد خود داشته باشید.\nموجودی قابل انتقال فعلی شما: <code>₳ {transferable}</code>",
                    parse_mode="HTML",
                )
            if transferable < collateral_amount:
                await state.clear()
                return await callback.message.edit_text(
                    f"❌ موجودی آزاد شما برای وثیقه <code>₳ {collateral_amount}</code> این وام کافی نیست.",
                    parse_mode="HTML",
                )

            interest = _compute_dynamic_interest(amount, settings)
            total_repayment = amount + int(amount * (interest / 100.0))

            # ⚠️ طبق سیستم جدید وثیقه، در لحظه ثبت درخواست هیچ مبلغی قفل نمی‌شود.
            # مبلغ وثیقه صرفاً محاسبه و روی خود وام ذخیره می‌شود؛ قفل واقعی (frozen_balance)
            # فقط در لحظه تأیید نهایی سوپرادمین انجام خواهد شد.
            cur2 = await db.execute(
                """
                INSERT INTO loans
                (user_id, guarantor_id, total_amount, interest_rate, total_repayment,
                 installments_count, status, loan_type, created_at, collateral_amount)
                VALUES (?, 0, ?, ?, ?, ?, 'PENDING_ADMIN', 'COLLATERAL', ?, ?)
                """,
                (
                    user_id, amount, interest, total_repayment, installments,
                    datetime.now(timezone.utc).isoformat(), collateral_amount,
                ),
            )
            loan_id = cur2.lastrowid
            await db.commit()

    await state.clear()
    try:
        await callback.message.edit_text(
            "✅ درخواست وام وثیقه‌ای شما ثبت شد و برای بررسی نهایی برای سوپرادمین ارسال گردید.\n"
            f"🔒 در صورت تأیید سوپرادمین، مبلغ <code>₳ {collateral_amount}</code> به‌عنوان وثیقه از موجودی "
            f"قابل‌انتقال شما کسر و قفل خواهد شد.",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await _send_loan_request_to_admins(callback.bot, loan_id)
    await callback.answer()


@user_router.callback_query(LoanForm.waiting_for_method, F.data == "loan_method_guarantor")
async def loan_method_guarantor(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    try:
        await callback.message.edit_text(
            "🤝 لطفاً آیدی عددی (شماره حساب) ضامن خود را ارسال کنید، یا روی پیام او در ربات ریپلای کرده و آیدی را بنویسید."
        )
    except Exception:
        pass
    await state.set_state(LoanForm.waiting_for_guarantor)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, callback.message.chat.id, callback.from_user.id, current_state,
        lambda: _default_timeout_notice(callback.bot, callback.message.chat.id, callback.message.message_id),
    )
    await callback.answer()


@user_router.message(LoanForm.waiting_for_guarantor)
async def loan_process_guarantor(message: Message, state: FSMContext):
    cancel_input_timeout(message.chat.id, message.from_user.id)
    data = await state.get_data()
    user_id = message.from_user.id

    def _reschedule():
        current_state = LoanForm.waiting_for_guarantor.state
        schedule_input_timeout(
            state, message.chat.id, user_id, current_state,
            lambda: _default_timeout_notice(message.bot, message.chat.id, None),
        )

    guarantor_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        guarantor_id = message.reply_to_message.from_user.id
    else:
        try:
            guarantor_id = int(message.text.strip())
        except (ValueError, AttributeError):
            await message.reply("❌ آیدی عددی نامعتبر است.")
            return _reschedule()

    if guarantor_id == user_id:
        await message.reply("❌ شما نمی‌توانید ضامن خودتان باشید.")
        return _reschedule()

    guarantor_data = await get_user_data(guarantor_id)
    if not guarantor_data:
        await message.reply("❌ کاربری با این آیدی در ربات یافت نشد.")
        return _reschedule()
    if guarantor_data["is_frozen"]:
        await message.reply("❌ حساب ضامن انتخابی مسدود (فریز) است.")
        return _reschedule()

    # 🚫 هر کاربر همزمان فقط یک درخواست وام در حال بررسی (Pending) می‌تواند داشته باشد
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM loans WHERE user_id = ? AND status IN ('PENDING_ADMIN', 'PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL') LIMIT 1",
            (user_id,),
        ) as cur:
            has_pending = await cur.fetchone()
    if has_pending:
        await state.clear()
        return await message.reply(
            "❌ شما در حال حاضر یک درخواست وام در حال بررسی دارید. تا زمان تأیید/رد یا لغو آن، امکان ثبت درخواست جدید وجود ندارد.\n"
            "برای مشاهده و لغو درخواست فعلی: <code>/my_loans</code>",
            parse_mode="HTML",
        )

    settings = data.get("loan_settings") or await _get_loan_settings()
    amount = data["loan_amount"]
    installments = data["loan_installments"]
    interest = _compute_dynamic_interest(amount, settings)
    total_repayment = amount + int(amount * (interest / 100.0))

    # 🤝 نرخ‌های تمکن (موجودی آزاد لازم) و وثیقه برای هر دو طرف وام ضامنی
    balance_rate_borrower = float(settings.get("loan_guarantor_balance_rate_borrower") or 0.20)
    balance_rate_guarantor = float(settings.get("loan_guarantor_balance_rate_guarantor") or 0.20)
    collateral_rate_borrower = float(settings.get("loan_guarantor_collateral_rate_borrower") or 0.08)
    collateral_rate_guarantor = float(settings.get("loan_guarantor_collateral_rate_guarantor") or 0.09)

    required_balance_borrower = int(amount * balance_rate_borrower)
    required_balance_guarantor = int(amount * balance_rate_guarantor)
    borrower_collateral = int(amount * collateral_rate_borrower)
    guarantor_collateral = int(amount * collateral_rate_guarantor)

    # ✅ اعتبارسنجی تمکن گیرنده (حداقل نرخ تنظیم‌شده از موجودی آزاد فعلی او)
    borrower_data = await get_user_data(user_id)
    if not borrower_data or borrower_data["is_frozen"]:
        await state.clear()
        return await message.reply("❌ حساب شما مسدود (فریز) است.")
    borrower_transferable = max(0, borrower_data["balance"] - borrower_data["frozen_balance"])
    if borrower_transferable < required_balance_borrower:
        await message.reply(
            f"❌ برای این وام باید حداقل <code>₳ {required_balance_borrower}</code> در موجودی آزاد خود داشته باشید.\n"
            f"موجودی آزاد فعلی شما: <code>₳ {borrower_transferable}</code>",
            parse_mode="HTML",
        )
        return _reschedule()

    # ✅ اعتبارسنجی تمکن ضامن (حداقل نرخ تنظیم‌شده از موجودی آزاد فعلی او)
    guarantor_transferable = max(0, guarantor_data["balance"] - guarantor_data["frozen_balance"])
    if guarantor_transferable < required_balance_guarantor:
        await message.reply(
            f"❌ موجودی آزاد ضامن انتخابی برای ضمانت این وام کافی نیست.\n"
            f"موجودی آزاد لازم برای ضامن: <code>₳ {required_balance_guarantor}</code>",
            parse_mode="HTML",
        )
        return _reschedule()

    # ⚠️ طبق سیستم جدید، در این مرحله هیچ ثبت یا تغییر مالی‌ای انجام نمی‌شود؛ فقط وثیقه‌ها
    # محاسبه و در State ذخیره می‌شوند تا پس از تأیید نهایی کاربر در صفحه پیش‌نمایش، وام ثبت شود.
    await state.update_data(
        guarantor_id=guarantor_id,
        loan_interest=interest,
        loan_total_repayment=total_repayment,
        borrower_collateral_amount=borrower_collateral,
        guarantor_collateral_amount=guarantor_collateral,
    )

    guarantor_name = html.escape(guarantor_data["full_name"] or str(guarantor_id))
    preview_text = (
        "🔍 <b>پیش‌نمایش درخواست وام ضامنی</b>\n\n"
        f"💳 مبلغ وام: <code>₳ {amount}</code>\n"
        f"📈 نرخ سود: <b>{interest}٪</b>\n"
        f"🔢 تعداد اقساط: <b>{installments}</b>\n"
        f"🧮 مجموع بازپرداخت: <code>₳ {total_repayment}</code>\n"
        f"🤝 ضامن: <b>{guarantor_name}</b> (<code>{guarantor_id}</code>)\n\n"
        f"🔒 وثیقه گیرنده (فقط با تأیید نهایی سوپرادمین قفل می‌شود): <code>₳ {borrower_collateral}</code>\n"
        f"🔒 وثیقه ضامن (فقط با تأیید نهایی سوپرادمین قفل می‌شود): <code>₳ {guarantor_collateral}</code>\n\n"
        f"💰 موجودی آزاد لازم شما: <code>₳ {required_balance_borrower}</code>\n"
        f"💰 موجودی آزاد لازم ضامن: <code>₳ {required_balance_guarantor}</code>\n\n"
        "⚠️ با تأیید، درخواست ضمانت برای ضامن انتخابی ارسال خواهد شد."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید و ارسال به ضامن", callback_data="loan_guarantor_preview_confirm"),
        InlineKeyboardButton(text="❌ انصراف", callback_data="loan_guarantor_preview_cancel"),
    ]])
    prompt = await message.reply(preview_text, reply_markup=kb, parse_mode="HTML")
    await state.set_state(LoanForm.waiting_for_guarantor_confirm)
    current_state = await state.get_state()
    schedule_input_timeout(
        state, message.chat.id, user_id, current_state,
        lambda: _default_timeout_notice(message.bot, message.chat.id, prompt.message_id),
    )


@user_router.callback_query(LoanForm.waiting_for_guarantor_confirm, F.data == "loan_guarantor_preview_cancel")
async def cb_loan_guarantor_preview_cancel(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    # ❌ انصراف: هیچ ثبت وام و هیچ تغییر مالی‌ای تا این مرحله انجام نشده، فقط State پاک می‌شود.
    await state.clear()
    try:
        await callback.message.edit_text("❌ درخواست وام لغو شد. هیچ ثبت یا تغییر مالی‌ای انجام نشد.")
    except Exception:
        pass
    await callback.answer()


@user_router.callback_query(LoanForm.waiting_for_guarantor_confirm, F.data == "loan_guarantor_preview_confirm")
async def cb_loan_guarantor_preview_confirm(callback: CallbackQuery, state: FSMContext):
    cancel_input_timeout(callback.message.chat.id, callback.from_user.id)
    data = await state.get_data()
    user_id = callback.from_user.id
    guarantor_id = data.get("guarantor_id")
    amount = data.get("loan_amount")
    installments = data.get("loan_installments")
    interest = data.get("loan_interest")
    total_repayment = data.get("loan_total_repayment")
    borrower_collateral = data.get("borrower_collateral_amount", 0)
    guarantor_collateral = data.get("guarantor_collateral_amount", 0)

    if not guarantor_id or amount is None or installments is None or interest is None:
        await state.clear()
        return await callback.answer("❌ اطلاعات درخواست نامعتبر شده است. لطفاً دوباره تلاش کنید.", show_alert=True)

    # 🚫 بررسی مجدد Pending بلافاصله پیش از ثبت، برای جلوگیری از ثبت همزمان دو درخواست
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM loans WHERE user_id = ? AND status IN ('PENDING_ADMIN', 'PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL') LIMIT 1",
            (user_id,),
        ) as cur:
            has_pending = await cur.fetchone()
    if has_pending:
        await state.clear()
        try:
            await callback.message.edit_text(
                "❌ شما در حال حاضر یک درخواست وام در حال بررسی دارید. این درخواست ثبت نشد."
            )
        except Exception:
            pass
        return await callback.answer()

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                """
                INSERT INTO loans
                (user_id, guarantor_id, total_amount, interest_rate, total_repayment,
                 installments_count, status, loan_type, created_at,
                 borrower_collateral_amount, guarantor_collateral_amount)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING_GUARANTOR', 'GUARANTOR', ?, ?, ?)
                """,
                (
                    user_id, guarantor_id, amount, interest, total_repayment, installments,
                    datetime.now(timezone.utc).isoformat(), borrower_collateral, guarantor_collateral,
                ),
            )
            loan_id = cur.lastrowid
            await db.commit()

    await state.clear()

    requester_data = await get_user_data(user_id)
    requester_name = html.escape(requester_data["full_name"] or str(user_id))

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ قبول می‌کنم", callback_data=f"guarantor_accept_{loan_id}"),
        InlineKeyboardButton(text="❌ قبول نمی‌کنم", callback_data=f"guarantor_reject_{loan_id}"),
    ]])
    try:
        await callback.bot.send_message(
            guarantor_id,
            f"🤝 کاربر <b>{requester_name}</b> درخواست وام <code>₳ {amount}</code> آتر با سود "
            f"<b>{interest}٪</b> در <b>{installments}</b> قسط کرده است.\n"
            f"🔒 در صورت تأیید نهایی سوپرادمین، مبلغ <code>₳ {guarantor_collateral}</code> از موجودی آزاد شما "
            "به‌عنوان وثیقه قفل خواهد شد.\n\n"
            "آیا حاضر می‌شوید ضامن این شخص شوید؟\n\n"
            "⚠️ نکته: در صورت عدم پرداخت اقساط توسط متقاضی، مبالغ اقساط از موجودی شما کسر خواهد شد.",
            reply_markup=kb,
            parse_mode="HTML",
        )
        try:
            await callback.message.edit_text(
                "✅ درخواست تایید ضمانت برای ضامن انتخابی ارسال شد. پس از تایید ایشان، درخواست شما برای سوپرادمین ارسال خواهد شد."
            )
        except Exception:
            pass
    except Exception:
        try:
            await callback.message.edit_text("❌ امکان ارسال پیام به ضامن انتخابی وجود ندارد (احتمالاً ربات را استارت نکرده است).")
        except Exception:
            pass
    await callback.answer()


@user_router.callback_query(F.data.startswith("guarantor_accept_"))
async def cb_guarantor_accept(callback: CallbackQuery):
    """مرحله ۱ (تأیید اولیه ضامن): وضعیت را به PENDING_GUARANTOR_FINAL می‌برد و هشدار نهایی را نمایش می‌دهد."""
    loan_id = int(callback.data.split("_")[2])
    guarantor_id = callback.from_user.id

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            # 🔒 آپدیت شرطی اتمیک: تنها زمانی اعمال می‌شود که هنوز PENDING_GUARANTOR و متعلق به
            # همین ضامن باشد؛ این کار تضمین می‌کند دو کلیک هم‌زمان (یا کلیک تکراری) فقط یک‌بار اثر کند.
            cur = await db.execute(
                "UPDATE loans SET status = 'PENDING_GUARANTOR_FINAL' "
                "WHERE id = ? AND guarantor_id = ? AND status = 'PENDING_GUARANTOR'",
                (loan_id, guarantor_id),
            )
            updated = cur.rowcount > 0
            await db.commit()
            if not updated:
                return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قبلاً پردازش شده است.", show_alert=True)

            async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur2:
                loan = await cur2.fetchone()

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تأیید نهایی ضمانت", callback_data=f"guarantor_final_accept_{loan_id}"),
        InlineKeyboardButton(text="❌ انصراف", callback_data=f"guarantor_reject_{loan_id}"),
    ]])
    try:
        await callback.message.edit_text(
            "⚠️ <b>هشدار نهایی</b>\n\n"
            f"با تأیید نهایی، مبلغ <code>₳ {loan['guarantor_collateral_amount'] or 0}</code> از موجودی آزاد شما "
            "به‌عنوان وثیقه ضمانت این وام قفل خواهد شد (فقط در لحظه تأیید نهایی سوپرادمین).\n"
            "همچنین در صورت عدم پرداخت اقساط توسط متقاضی، مبالغ اقساط از موجودی شما کسر خواهد شد.\n\n"
            "برای ادامه، تأیید نهایی خود را اعلام کنید.",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception:
        pass
    await callback.answer()


@user_router.callback_query(F.data.startswith("guarantor_final_accept_"))
async def cb_guarantor_final_accept(callback: CallbackQuery):
    """مرحله ۲ (تأیید نهایی ضامن): وضعیت را به PENDING_ADMIN می‌برد و برای سوپرادمین ارسال می‌کند."""
    loan_id = int(callback.data.split("_")[3])
    guarantor_id = callback.from_user.id

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "UPDATE loans SET status = 'PENDING_ADMIN' "
                "WHERE id = ? AND guarantor_id = ? AND status = 'PENDING_GUARANTOR_FINAL'",
                (loan_id, guarantor_id),
            )
            updated = cur.rowcount > 0
            await db.commit()
            if not updated:
                return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قبلاً پردازش شده است.", show_alert=True)

            async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur2:
                loan = await cur2.fetchone()

    try:
        await callback.message.edit_text("✅ ضمانت شما نهایی شد. درخواست برای بررسی نهایی به سوپرادمین ارسال شد.")
    except Exception:
        pass
    await callback.answer()

    await _send_loan_request_to_admins(callback.bot, loan_id)

    try:
        await callback.bot.send_message(
            loan["user_id"],
            "🤝 ضامن شما تأیید نهایی ضمانت را انجام داد. درخواست وام شما برای بررسی نهایی به سوپرادمین ارسال شد.",
        )
    except Exception:
        pass


@user_router.callback_query(F.data.startswith("guarantor_reject_"))
async def cb_guarantor_reject(callback: CallbackQuery):
    """رد ضمانت توسط ضامن، در هر یک از دو مرحله (تأیید اولیه یا تأیید نهایی) قابل انجام است."""
    loan_id = int(callback.data.split("_")[2])
    guarantor_id = callback.from_user.id

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "UPDATE loans SET status = 'REJECTED' WHERE id = ? AND guarantor_id = ? "
                "AND status IN ('PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL')",
                (loan_id, guarantor_id),
            )
            updated = cur.rowcount > 0
            await db.commit()
            if not updated:
                return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قبلاً پردازش شده است.", show_alert=True)

            async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur2:
                loan = await cur2.fetchone()

    try:
        await callback.message.edit_text("❌ شما درخواست ضمانت را رد کردید.")
    except Exception:
        pass
    await callback.answer()

    try:
        await callback.bot.send_message(loan["user_id"], "❌ ضامن انتخابی شما درخواست ضمانت را رد کرد. درخواست وام شما لغو شد.")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("loan_admin_approve_"))
async def cb_loan_admin_approve(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("❌ عدم دسترسی.", show_alert=True)

    loan_id = int(callback.data.split("_")[3])

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur:
                loan = await cur.fetchone()

            if not loan or loan["status"] != "PENDING_ADMIN":
                return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قبلاً پردازش شده است.", show_alert=True)

            paid = await treasury_debit(
                db, loan["total_amount"], f"واریز اصل وام #{loan_id} به کاربر {loan['user_id']}",
                related_user=loan["user_id"],
            )
            if not paid:
                await db.commit()
                try:
                    await callback.message.edit_text(
                        f"{callback.message.html_text}\n\n"
                        "❌ <b>عدم امکان پرداخت به دلیل عدم کفایت موجودی خزانه.</b>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
                return await callback.answer("❌ موجودی خزانه کافی نیست.", show_alert=True)

            # 🔒 طبق سیستم جدید وثیقه، مبلغ وثیقه فقط اکنون (لحظه تأیید نهایی سوپرادمین) از موجودی
            # قابل‌انتقال کاربر کسر و به frozen_balance منتقل می‌شود. اگر کاربر دیگر موجودی کافی
            # نداشته باشد یا حسابش فریز باشد، کل عملیات (از جمله برداشت از خزانه بالا) لغو می‌شود
            # چون تراکنش commit نشده و با بسته‌شدن اتصال به‌طور خودکار rollback خواهد شد.
            if loan["loan_type"] == "COLLATERAL":
                collateral_amount = loan["collateral_amount"] or 0
                async with db.execute(
                    "SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (loan["user_id"],)
                ) as cur_u:
                    borrower = await cur_u.fetchone()

                borrower_transferable = max(0, borrower["balance"] - borrower["frozen_balance"]) if borrower else 0
                if not borrower or borrower["is_frozen"] or borrower_transferable < collateral_amount:
                    return await callback.answer(
                        "❌ موجودی قابل‌انتقال متقاضی برای قفل‌کردن وثیقه کافی نیست یا حساب او فریز است. "
                        "می‌توانید درخواست را رد کنید یا بعداً دوباره تلاش کنید.",
                        show_alert=True,
                    )

                await db.execute(
                    "UPDATE users SET frozen_balance = frozen_balance + ? WHERE user_id = ?",
                    (collateral_amount, loan["user_id"]),
                )

            # 🤝 وام ضامنی: طبق همان منطق، وثیقه گیرنده و ضامن فقط اکنون (لحظه تأیید نهایی
            # سوپرادمین) از موجودی قابل‌انتقال هرکدام کسر و به frozen_balance آن‌ها منتقل
            # می‌شود. اگر موجودی هرکدام کافی نباشد یا حساب هرکدام فریز باشد، کل عملیات
            # (از جمله برداشت از خزانه بالا) به‌دلیل عدم commit به‌طور خودکار rollback می‌شود.
            elif loan["loan_type"] == "GUARANTOR":
                borrower_collateral = loan["borrower_collateral_amount"] or 0
                guarantor_collateral = loan["guarantor_collateral_amount"] or 0
                guarantor_id = loan["guarantor_id"]

                async with db.execute(
                    "SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (loan["user_id"],)
                ) as cur_u:
                    borrower = await cur_u.fetchone()
                async with db.execute(
                    "SELECT balance, frozen_balance, is_frozen FROM users WHERE user_id = ?", (guarantor_id,)
                ) as cur_g:
                    guarantor = await cur_g.fetchone()

                borrower_transferable = max(0, borrower["balance"] - borrower["frozen_balance"]) if borrower else 0
                guarantor_transferable = max(0, guarantor["balance"] - guarantor["frozen_balance"]) if guarantor else 0

                if (
                    not borrower or borrower["is_frozen"] or borrower_transferable < borrower_collateral
                    or not guarantor or guarantor["is_frozen"] or guarantor_transferable < guarantor_collateral
                ):
                    return await callback.answer(
                        "❌ موجودی قابل‌انتقال گیرنده یا ضامن برای قفل‌کردن وثیقه‌ها کافی نیست یا حساب یکی از "
                        "آن‌ها فریز است. می‌توانید درخواست را رد کنید یا بعداً دوباره تلاش کنید.",
                        show_alert=True,
                    )

                await db.execute(
                    "UPDATE users SET frozen_balance = frozen_balance + ? WHERE user_id = ?",
                    (borrower_collateral, loan["user_id"]),
                )
                await db.execute(
                    "UPDATE users SET frozen_balance = frozen_balance + ? WHERE user_id = ?",
                    (guarantor_collateral, guarantor_id),
                )

            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (loan["total_amount"], loan["user_id"]),
            )
            await db.execute("UPDATE loans SET status = 'ACTIVE' WHERE id = ?", (loan_id,))
            await _create_loan_installments(
                db, loan_id, loan["total_repayment"], loan["installments_count"], datetime.now(timezone.utc)
            )
            await db.commit()

    try:
        await callback.message.edit_text(
            f"{callback.message.html_text}\n\n✅ <b>وام تأیید و واریز شد.</b>", parse_mode="HTML"
        )
    except Exception:
        pass
    await callback.answer("✅ وام با موفقیت واریز شد.")

    try:
        await callback.bot.send_message(
            loan["user_id"],
            f"🎉 وام شما به مبلغ <code>₳ {loan['total_amount']}</code> تأیید و به حساب شما واریز شد.\n"
            f"🧮 مجموع بازپرداخت: <code>₳ {loan['total_repayment']}</code> در <b>{loan['installments_count']}</b> قسط.",
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("loan_admin_reject_"))
async def cb_loan_admin_reject(callback: CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        return await callback.answer("❌ عدم دسترسی.", show_alert=True)

    loan_id = int(callback.data.split("_")[3])

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur:
                loan = await cur.fetchone()

            if not loan or loan["status"] != "PENDING_ADMIN":
                return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قبلاً پردازش شده است.", show_alert=True)

            # ⚠️ طبق سیستم جدید وثیقه، هیچ مبلغی پیش از تأیید سوپرادمین قفل نمی‌شود؛
            # بنابراین هنگام رد درخواست هم نیازی به آزادسازی frozen_balance نیست.
            await db.execute("UPDATE loans SET status = 'REJECTED' WHERE id = ?", (loan_id,))
            await db.commit()

    try:
        await callback.message.edit_text(f"{callback.message.html_text}\n\n❌ <b>درخواست رد شد.</b>", parse_mode="HTML")
    except Exception:
        pass
    await callback.answer("❌ درخواست وام رد شد.")

    try:
        await callback.bot.send_message(loan["user_id"], "❌ متأسفانه درخواست وام شما توسط سوپرادمین رد شد.")
    except Exception:
        pass


async def _build_my_loans_view(user_id: int):
    """برمی‌گرداند: (متن، کیبورد یا None) برای نمایش وام‌های فعال/در انتظار کاربر."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM loans WHERE user_id = ? AND status IN ('ACTIVE', 'PENDING_ADMIN', 'PENDING_GUARANTOR', 'PENDING_GUARANTOR_FINAL') ORDER BY id DESC",
            (user_id,),
        ) as cur:
            loans = await cur.fetchall()

    if not loans:
        return "📋 شما در حال حاضر هیچ وام فعال یا در حال بررسی‌ای ندارید.", None

    status_labels = {
        "ACTIVE": "🟢 فعال", "PENDING_ADMIN": "⏳ در انتظار تایید سوپرادمین",
        "PENDING_GUARANTOR": "⏳ در انتظار تایید اولیه ضامن",
        "PENDING_GUARANTOR_FINAL": "⏳ در انتظار تایید نهایی ضامن",
    }
    parts = ["📋 <b>وام‌های شما:</b>\n"]
    kb_rows = []
    for loan in loans:
        parts.append(
            f"\n🔹 وام #{loan['id']} | مبلغ: <code>₳ {loan['total_amount']}</code> | "
            f"وضعیت: {status_labels.get(loan['status'], loan['status'])}"
        )
        if loan["status"] == "ACTIVE":
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT * FROM loan_installments WHERE loan_id = ? AND status = 'PENDING' ORDER BY installment_number LIMIT 1",
                    (loan["id"],),
                ) as cur_i:
                    next_inst = await cur_i.fetchone()
            if next_inst:
                due_dt = datetime.fromisoformat(next_inst["due_date"])
                if due_dt.tzinfo is None:
                    due_dt = due_dt.replace(tzinfo=timezone.utc)
                due = due_dt.astimezone(IRAN_TZ).strftime('%Y-%m-%d')
                parts.append(f"   💳 قسط بعدی: <code>₳ {next_inst['amount']}</code> | سررسید: {due}")
                kb_rows.append([InlineKeyboardButton(
                    text=f"💳 پرداخت قسط #{next_inst['installment_number']} وام #{loan['id']}",
                    callback_data=f"pay_inst_{next_inst['id']}",
                )])
        elif loan["status"] in ("PENDING_ADMIN", "PENDING_GUARANTOR", "PENDING_GUARANTOR_FINAL"):
            kb_rows.append([InlineKeyboardButton(
                text=f"❌ لغو درخواست وام #{loan['id']}",
                callback_data=f"cancel_loan_req_{loan['id']}",
            )])

    text = "".join(parts)
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
    return text, kb


@user_router.message(Command("my_loans"))
async def cmd_my_loans(message: Message):
    if not is_private(message):
        return
    text, kb = await _build_my_loans_view(message.from_user.id)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@user_router.callback_query(F.data == "loan_my_list")
async def cb_my_loans_list(callback: CallbackQuery):
    from_profile = _kb_has_callback(callback.message.reply_markup, "prof_home")
    text, kb = await _build_my_loans_view(callback.from_user.id)
    if from_profile:
        kb = _append_prof_back_row(kb)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


@user_router.callback_query(F.data.startswith("cancel_loan_req_"))
async def cb_cancel_loan_request(callback: CallbackQuery):
    loan_id = int(callback.data[len("cancel_loan_req_"):])
    user_id = callback.from_user.id
    from_profile = _kb_has_callback(callback.message.reply_markup, "prof_home")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM loans WHERE id = ?", (loan_id,)) as cur:
            loan = await cur.fetchone()

    if not loan or loan["user_id"] != user_id or loan["status"] not in ("PENDING_ADMIN", "PENDING_GUARANTOR", "PENDING_GUARANTOR_FINAL"):
        return await callback.answer("❌ این درخواست دیگر معتبر نیست یا قابل لغو نیست.", show_alert=True)

    prev_status = loan["status"]

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            # ⚠️ فقط تغییر وضعیت به CANCELLED؛ رکورد حذف نمی‌شود و چون تا این مرحله هیچ مبلغی
            # (وثیقه یا غیره) قفل/کسر نشده، هیچ آزادسازی یا برگشت مالی لازم نیست.
            await db.execute("UPDATE loans SET status = 'CANCELLED' WHERE id = ?", (loan_id,))
            cancel_tx_id = f"TRZ-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
            await db.execute(
                "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) VALUES (?, ?, ?, 0, 0, ?)",
                (
                    cancel_tx_id, datetime.now(timezone.utc).isoformat(), user_id,
                    f"[LOAN_CANCELLED] لغو درخواست وام #{loan_id} توسط متقاضی",
                ),
            )
            await db.commit()

    text, kb = await _build_my_loans_view(user_id)
    if from_profile:
        kb = _append_prof_back_row(kb)
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await callback.answer("✅ درخواست وام لغو شد.", show_alert=True)

    if prev_status in ("PENDING_GUARANTOR", "PENDING_GUARANTOR_FINAL") and loan["guarantor_id"]:
        try:
            await callback.bot.send_message(
                loan["guarantor_id"], f"ℹ️ درخواست وام #{loan_id} که در انتظار تایید شما بود، توسط متقاضی لغو شد."
            )
        except Exception:
            pass


@user_router.callback_query(F.data.startswith("pay_inst_"))
async def cb_pay_installment(callback: CallbackQuery):
    installment_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT li.*, l.user_id AS loan_user_id, l.guarantor_id, l.installments_count "
                "FROM loan_installments li JOIN loans l ON li.loan_id = l.id WHERE li.id = ?",
                (installment_id,),
            ) as cur:
                inst = await cur.fetchone()

            if not inst or inst["loan_user_id"] != user_id:
                return await callback.answer("❌ این قسط متعلق به شما نیست یا یافت نشد.", show_alert=True)
            if inst["status"] == "MERGED":
                return await callback.answer("این سررسید به قسط جدید منتقل شده است.", show_alert=True)
            if inst["status"] != "PENDING":
                return await callback.answer("این قسط پرداخت شده است، لطفاً منتظر سررسید بعدی باشید.", show_alert=True)

            # ⏳ امکان پرداخت زودتر از موعد سررسید وجود ندارد
            due_date = datetime.fromisoformat(inst["due_date"])
            if due_date.tzinfo is None:
                due_date = due_date.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) < due_date:
                due_date_ir = due_date.astimezone(IRAN_TZ)
                return await callback.answer(
                    f"⏳ سررسید این قسط هنوز نرسیده است. تاریخ سررسید: {due_date_ir.strftime('%Y-%m-%d')}",
                    show_alert=True,
                )

            async with db.execute("SELECT balance, is_frozen FROM users WHERE user_id = ?", (user_id,)) as cur_u:
                u = await cur_u.fetchone()
            if not u or u["is_frozen"]:
                return await callback.answer("❌ حساب شما مسدود است.", show_alert=True)
            if u["balance"] < inst["amount"]:
                return await callback.answer(
                    f"❌ موجودی شما کافی نیست. مبلغ قسط: ₳ {inst['amount']}", show_alert=True
                )

            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (inst["amount"], user_id))
            await treasury_credit(db, inst["amount"], f"بازپرداخت قسط #{inst['installment_number']} وام #{inst['loan_id']}", related_user=user_id)
            await db.execute(
                "UPDATE loan_installments SET status = 'PAID', paid_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), installment_id),
            )

            # بررسی تسویه کامل وام (اقساط ادغام‌شده/MERGED هم به‌عنوان تسویه‌شده در نظر گرفته می‌شوند،
            # چون مبلغشان قبلاً در قسط جدید‌تر ادغام و همراه آن پرداخت شده است)
            async with db.execute(
                "SELECT COUNT(*) FROM loan_installments WHERE loan_id = ? AND status NOT IN ('PAID', 'MERGED')",
                (inst["loan_id"],),
            ) as cur_c:
                remaining = (await cur_c.fetchone())[0]

            fully_paid = remaining == 0
            if fully_paid:
                if inst["guarantor_id"] == 0:
                    # آزادسازی کامل وثیقه در پایان وام وثیقه‌ای (همان مبلغی که در لحظه تأیید سوپرادمین قفل شد)
                    async with db.execute("SELECT collateral_amount FROM loans WHERE id = ?", (inst["loan_id"],)) as cur_l:
                        loan_row = await cur_l.fetchone()
                    collateral_amount = (loan_row[0] or 0) if loan_row else 0
                    await db.execute(
                        "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                        (collateral_amount, user_id),
                    )
                else:
                    # 🤝 آزادسازی کامل وثیقه گیرنده و ضامن در پایان وام ضامنی (همان مبالغی که در
                    # لحظه تأیید سوپرادمین قفل شدند)
                    async with db.execute(
                        "SELECT borrower_collateral_amount, guarantor_collateral_amount FROM loans WHERE id = ?",
                        (inst["loan_id"],),
                    ) as cur_l:
                        loan_row = await cur_l.fetchone()
                    b_collateral = (loan_row[0] or 0) if loan_row else 0
                    g_collateral = (loan_row[1] or 0) if loan_row else 0
                    await db.execute(
                        "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                        (b_collateral, user_id),
                    )
                    await db.execute(
                        "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                        (g_collateral, inst["guarantor_id"]),
                    )
                await db.execute("UPDATE loans SET status = 'PAID' WHERE id = ?", (inst["loan_id"],))
                # 📝 ثبت رویداد تسویه کامل وام در سیستم لاگ
                settle_tx_id = f"TRZ-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
                await db.execute(
                    "INSERT INTO audit_logs (tx_id, timestamp, from_user, to_user, amount, reason) VALUES (?, ?, ?, 0, 0, ?)",
                    (
                        settle_tx_id, datetime.now(timezone.utc).isoformat(), user_id,
                        f"[LOAN_SETTLED] تسویه کامل وام #{inst['loan_id']}",
                    ),
                )

            await db.commit()

    await callback.answer("✅ قسط با موفقیت پرداخت شد.", show_alert=True)
    try:
        await callback.message.edit_text("✅ این قسط با موفقیت پرداخت شد.")
    except Exception:
        pass
    if fully_paid:
        try:
            await callback.bot.send_message(user_id, f"🎉 وام #{inst['loan_id']} شما به‌طور کامل تسویه شد!")
        except Exception:
            pass


# --- ⏰ پردازش خودکار شبانه اقساط: یادآوری، جریمه دیرکرد و کسر خودکار بدهی معوقه ---

async def process_due_installments(bot: Bot):
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT li.*, l.user_id AS loan_user_id, l.guarantor_id, l.total_amount AS loan_total_amount, "
            "l.installments_count AS loan_installments_count "
            "FROM loan_installments li JOIN loans l ON li.loan_id = l.id "
            "WHERE li.status = 'PENDING' AND l.status = 'ACTIVE'"
        ) as cur:
            due_installments = await cur.fetchall()

    for inst in due_installments:
        try:
            due_date = datetime.fromisoformat(inst["due_date"])
            if due_date.tzinfo is None:
                due_date = due_date.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        hours_late = (now - due_date).total_seconds() / 3600.0
        borrower_id = inst["loan_user_id"]

        # 🔔 یادآوری ۲۴ ساعت قبل از سررسید (فقط یک‌بار، صرفاً در پیوی متقاضی)
        # بازه به‌صورت «حداکثر ۲۴ ساعت مانده» در نظر گرفته شده (نه فقط یک بازه یک‌ساعته دقیق)
        # تا در صورت خاموش بودن موقت ربات، پیام پس از روشن شدن مجدد از قلم نیفتد.
        if -24 <= hours_late < 0 and inst["last_reminder_stage"] not in ("REMINDER_24H", "DUE", "GRACE_OVER"):
            try:
                await bot.send_message(
                    borrower_id,
                    "🔔 یادآوری پرداخت قسط\n"
                    "کاربر گرامی، تاریخ پرداخت قسط وام شما تا ۲۴ ساعت آینده فرا می‌رسد. "
                    "لطفاً جهت جلوگیری از اعمال جریمه دیرکرد، نسبت به پرداخت قسط اقدام نمایید.\n"
                    "بانک مرکزی آترامنتوم",
                )
            except Exception:
                pass
            async with db_lock:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE loan_installments SET last_reminder_stage = 'REMINDER_24H' WHERE id = ?", (inst["id"],)
                    )
                    await db.commit()
            continue

        # ⏰ پیام سررسید (از لحظه فرارسیدن سررسید به بعد) + اطلاعات کامل وام + دکمه پرداخت مستقیم
        # بازه به‌صورت «از سررسید به بعد» در نظر گرفته شده (نه فقط بازه یک‌ساعته دقیق) تا در صورت
        # خاموش بودن موقت ربات، پیام سررسید پس از روشن شدن مجدد حتماً ارسال شود.
        if hours_late >= 0 and inst["last_reminder_stage"] not in ("DUE", "GRACE_OVER"):
            # 🔗 انباشت اقساط عقب‌افتاده: اگر قسط(های) قبلی همین وام هنوز پرداخت نشده باشند
            # (مثلاً به دلیل ناکافی بودن موجودی/وثیقه/ضامن در کسر خودکار)، مبلغ آن‌ها به این
            # سررسید جدید اضافه و دیگر به‌صورت مستقل قابل پرداخت نخواهند بود.
            async with aiosqlite.connect(DB_PATH) as db_m:
                db_m.row_factory = aiosqlite.Row
                async with db_m.execute(
                    "SELECT id, installment_number, amount FROM loan_installments "
                    "WHERE loan_id = ? AND status = 'PENDING' AND installment_number < ? ORDER BY installment_number",
                    (inst["loan_id"], inst["installment_number"]),
                ) as cur_prev:
                    overdue_prev = await cur_prev.fetchall()

            carried_over = sum(p["amount"] for p in overdue_prev)
            combined_amount = inst["amount"] + carried_over

            async with db_lock:
                async with aiosqlite.connect(DB_PATH) as db:
                    if overdue_prev:
                        for p in overdue_prev:
                            await db.execute(
                                "UPDATE loan_installments SET status = 'MERGED' WHERE id = ?", (p["id"],)
                            )
                        # ⚠️ base_amount هم باید به‌روزرسانی شود، وگرنه محاسبه جریمه دیرکرد بعدی این
                        # قسط (که بر اساس base_amount انجام می‌شود) بدهی ادغام‌شده را نادیده می‌گیرد.
                        await db.execute(
                            "UPDATE loan_installments SET amount = ?, base_amount = ? WHERE id = ?",
                            (combined_amount, combined_amount, inst["id"]),
                        )
                    await db.execute(
                        "UPDATE loan_installments SET last_reminder_stage = 'DUE' WHERE id = ?", (inst["id"],)
                    )
                    await db.commit()

            async with aiosqlite.connect(DB_PATH) as db_r:
                db_r.row_factory = aiosqlite.Row
                async with db_r.execute(
                    "SELECT COUNT(*) AS paid_count FROM loan_installments WHERE loan_id = ? AND status = 'PAID'",
                    (inst["loan_id"],),
                ) as cur_p:
                    paid_count = (await cur_p.fetchone())["paid_count"]

            total_installments = inst["loan_installments_count"] or 1
            # 🧮 تفکیک تناسبی اصل و سود همین قسط (بر اساس مبلغ اصل کل وام تقسیم‌شده به تعداد اقساط)
            base_principal_each = inst["loan_total_amount"] // total_installments
            remainder_principal = inst["loan_total_amount"] - (base_principal_each * total_installments)
            principal_part = base_principal_each + (remainder_principal if inst["installment_number"] == total_installments else 0)
            interest_part = max(0, inst["base_amount"] - principal_part)

            carried_over_text = ""
            if overdue_prev:
                breakdown_lines = "\n".join(
                    f"   ↳ قسط #{p['installment_number']} (عقب‌افتاده): <code>₳ {p['amount']}</code>"
                    for p in overdue_prev
                )
                carried_over_text = (
                    f"\n⚠️ <b>اقساط عقب‌افتاده به این سررسید اضافه شد:</b>\n{breakdown_lines}\n"
                    f"💳 مبلغ قسط جدید: <code>₳ {inst['amount']}</code>\n"
                )

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💳 پرداخت قسط", callback_data=f"pay_inst_{inst['id']}")
            ]])
            try:
                await bot.send_message(
                    borrower_id,
                    f"⏰ <b>سررسید قسط وام</b>\n\n"
                    f"🔢 شماره قسط: <code>{inst['installment_number']}</code>\n"
                    f"💰 مبلغ اصل قسط: <code>₳ {principal_part}</code>\n"
                    f"📈 سود این قسط: <code>₳ {interest_part}</code>\n"
                    f"✅ اقساط پرداخت‌شده: <code>{paid_count}/{total_installments}</code>\n"
                    f"⏳ اقساط باقی‌مانده: <code>{total_installments - paid_count}</code>\n"
                    f"{carried_over_text}"
                    f"🧮 مبلغ کل قابل پرداخت (شامل جریمه در صورت وجود): <code>₳ {combined_amount}</code>\n\n"
                    f"بانک مرکزی آترامنتوم",
                    reply_markup=kb,
                    parse_mode="HTML",
                )
            except Exception:
                pass
            continue

        if hours_late < BANK_GRACE_PERIOD_HOURS:
            continue  # هنوز در مهلت طلایی است

        days_late = int(hours_late // 24)

        # اعلام اتمام مهلت طلایی و شروع جریمه (فقط یک‌بار)
        if inst["last_reminder_stage"] != "GRACE_OVER" and days_late >= 1:
            late_rate = await get_setting("late_penalty_rate")
            try:
                late_rate = float(late_rate)
            except (TypeError, ValueError):
                late_rate = 0.0085
            penalty = int(inst["base_amount"] * late_rate * days_late)
            new_amount = inst["base_amount"] + penalty

            async with db_lock:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE loan_installments SET amount = ?, penalty_amount = ?, last_reminder_stage = 'GRACE_OVER' WHERE id = ?",
                        (new_amount, penalty, inst["id"]),
                    )
                    await db.commit()
            try:
                await bot.send_message(
                    borrower_id,
                    f"⚠️ مهلت طلایی قسط #{inst['installment_number']} وام #{inst['loan_id']} به پایان رسید.\n"
                    f"💰 جریمه دیرکرد اعمال شد. مبلغ جدید قسط: <code>₳ {new_amount}</code>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            if inst["guarantor_id"]:
                try:
                    await bot.send_message(
                        inst["guarantor_id"],
                        f"⚠️ متقاضی وام #{inst['loan_id']} که شما ضامن او هستید، قسط سررسیدشده را پرداخت نکرده است.",
                    )
                except Exception:
                    pass

        # اولویت کسر اتوماتیک بدهی معوقه، فقط پس از حداقل ۲ روز تاخیر (۱ روز فرصت اضافه پس از پایان مهلت طلایی)
        if days_late < 2:
            continue

        await _auto_collect_overdue_installment(bot, inst["id"])
        await asyncio.sleep(0.05)


async def _auto_collect_overdue_installment(bot: Bot, installment_id: int):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT li.*, l.user_id AS loan_user_id, l.guarantor_id "
                "FROM loan_installments li JOIN loans l ON li.loan_id = l.id WHERE li.id = ?",
                (installment_id,),
            ) as cur:
                inst = await cur.fetchone()

            if not inst or inst["status"] != "PENDING":
                return

            remaining_debt = inst["amount"]
            borrower_id = inst["loan_user_id"]
            guarantor_id = inst["guarantor_id"]

            async with db.execute(
                "SELECT balance, frozen_balance FROM users WHERE user_id = ?", (borrower_id,)
            ) as cur_b:
                borrower = await cur_b.fetchone()

            collected = 0

            # اولویت اول: کسر از وثیقه وام‌گیرنده
            if borrower and borrower["frozen_balance"] > 0 and remaining_debt > 0:
                take = min(borrower["frozen_balance"], remaining_debt)
                await db.execute(
                    "UPDATE users SET frozen_balance = frozen_balance - ? WHERE user_id = ?", (take, borrower_id)
                )
                collected += take
                remaining_debt -= take

            # اولویت دوم: کسر از موجودی آزاد وام‌گیرنده
            # (در این مرحله وثیقه قبلاً در اولویت اول به‌طور کامل مصرف شده، پس کل موجودی، موجودی آزاد است)
            if remaining_debt > 0 and borrower:
                async with db.execute("SELECT balance FROM users WHERE user_id = ?", (borrower_id,)) as cur_bb:
                    fresh_balance = (await cur_bb.fetchone())["balance"]
                take = min(fresh_balance, remaining_debt)
                if take > 0:
                    await db.execute(
                        "UPDATE users SET balance = balance - ? WHERE user_id = ?", (take, borrower_id)
                    )
                    collected += take
                    remaining_debt -= take

            # اولویت سوم: کسر از موجودی آزاد ضامن (در صورت وجود ضامن)
            if remaining_debt > 0 and guarantor_id:
                async with db.execute(
                    "SELECT balance, frozen_balance FROM users WHERE user_id = ?", (guarantor_id,)
                ) as cur_g:
                    guarantor = await cur_g.fetchone()
                if guarantor:
                    g_transferable = max(0, guarantor["balance"] - guarantor["frozen_balance"])
                    take = min(g_transferable, remaining_debt)
                    if take > 0:
                        await db.execute(
                            "UPDATE users SET balance = balance - ? WHERE user_id = ?", (take, guarantor_id)
                        )
                        collected += take
                        remaining_debt -= take

            if collected > 0:
                await treasury_credit(
                    db, collected, f"کسر خودکار بدهی معوقه قسط #{inst['installment_number']} وام #{inst['loan_id']}",
                    related_user=borrower_id,
                )

            if remaining_debt <= 0:
                await db.execute(
                    "UPDATE loan_installments SET status = 'PAID', paid_at = ? WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), installment_id),
                )
                async with db.execute(
                    "SELECT COUNT(*) FROM loan_installments WHERE loan_id = ? AND status != 'PAID'",
                    (inst["loan_id"],),
                ) as cur_c:
                    remaining_count = (await cur_c.fetchone())[0]
                if remaining_count == 0:
                    if not guarantor_id:
                        async with db.execute("SELECT collateral_amount FROM loans WHERE id = ?", (inst["loan_id"],)) as cur_l:
                            loan_row = await cur_l.fetchone()
                        collateral_amount = (loan_row[0] or 0) if loan_row else 0
                        await db.execute(
                            "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                            (collateral_amount, borrower_id),
                        )
                    else:
                        # 🤝 آزادسازی کامل وثیقه گیرنده و ضامن در پایان وام ضامنی (پس از کسر خودکار آخرین قسط)
                        async with db.execute(
                            "SELECT borrower_collateral_amount, guarantor_collateral_amount FROM loans WHERE id = ?",
                            (inst["loan_id"],),
                        ) as cur_l:
                            loan_row = await cur_l.fetchone()
                        b_collateral = (loan_row[0] or 0) if loan_row else 0
                        g_collateral = (loan_row[1] or 0) if loan_row else 0
                        await db.execute(
                            "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                            (b_collateral, borrower_id),
                        )
                        await db.execute(
                            "UPDATE users SET frozen_balance = MAX(0, frozen_balance - ?) WHERE user_id = ?",
                            (g_collateral, guarantor_id),
                        )
                    await db.execute("UPDATE loans SET status = 'PAID' WHERE id = ?", (inst["loan_id"],))
            else:
                # موجودی وثیقه، وام‌گیرنده و ضامن به‌طور کامل کافی نبود
                await db.execute(
                    "UPDATE loan_installments SET amount = ? WHERE id = ?", (remaining_debt, installment_id)
                )

            await db.commit()

    if collected > 0:
        try:
            await bot.send_message(
                borrower_id,
                f"🔻 مبلغ <code>₳ {collected}</code> بابت قسط معوق #{inst['installment_number']} وام #{inst['loan_id']} "
                f"به‌صورت خودکار از حساب/وثیقه شما کسر شد.",
                parse_mode="HTML",
            )
        except Exception:
            pass
    if remaining_debt > 0:
        for sa_id in SUPER_ADMINS:
            try:
                await bot.send_message(
                    sa_id,
                    f"🚨 <b>هشدار نکول وام</b>\n\n"
                    f"وثیقه، موجودی وام‌گیرنده و ضامن برای پرداخت قسط #{inst['installment_number']} "
                    f"وام #{inst['loan_id']} (کاربر <code>{borrower_id}</code>) کافی نبود.\n"
                    f"💰 باقیمانده بدهی وصول‌نشده: <code>₳ {remaining_debt}</code>",
                    parse_mode="HTML",
                )
            except Exception:
                pass


async def loan_management_loop(bot: Bot):
    while True:
        await asyncio.sleep(3600)  # بررسی ساعتی جهت رعایت دقیق مهلت طلایی و جلوگیری از اسپم یادآوری‌ها
        try:
            await process_due_installments(bot)
        except Exception as e:
            logging.error(f"❌ خطا در پردازش اقساط وام: {e}")


# --- راهنمای دستورات ---


async def _build_help_text(user_id: int, group_only: bool = False) -> str:
    """متن راهنما را می‌سازد. در حالت group_only=True فقط لیست دستورات عمومی (بدون درنظرگرفتن
    سطح دسترسی یا ادمین بودن کاربر) بازگردانده می‌شود؛ در غیر این صورت راهنمای کامل و شخصی‌سازی‌شده
    بر اساس نقش واقعی کاربر (فروشگاه تأییدشده، پستچی، ادمین، سوپرادمین) ساخته می‌شود."""
    txt = (
        "📱 <b>راهنمای دستورات کاربران:</b>\n"
        "🔹 <code>/start</code> - شروع و دریافت شماره حساب\n"
        "🔹 <code>/cancel</code> - لغو هر عملیات درحال‌انجام (انتقال، ساخت فروشگاه، ثبت محصول و...)\n"
        "🔹 <code>/profile</code> یا «پروفایل» - مشاهده نام، شماره حساب، موجودی و وضعیت\n"
        "🔹 <code>/transfer</code> یا «انتقال آتر» - انتقال آتر (چند روش مختلف)\n"
        "🔹 <code>/my_orders</code> - مشاهده وضعیت سفارش‌های خریداری‌شده\n"
        "🔹 <code>/my_assets</code> - مشاهده دارایی‌ها و عکس محصولات خریداری‌شده\n"
        "🔹 <code>/track [کد]</code> - پیگیری لحظه‌ای وضعیت سفارش با کد ۱۰ رقمی\n\n"
        "🏦 <b>بانک آترامنتوم:</b>\n"
        "🔹 «بانک آترامنتوم» - پنل سریع بانک (در گروه و پیوی)\n"
        "🔹 <code>/bank</code> یا <code>/atramentum_bank</code> یا «بانک» - حساب بانکی کامل (فقط پیوی)\n\n"
        "💳 <b>وام آترامنتوم:</b>\n"
        "🔹 <code>/loan</code> یا «درخواست وام» - ثبت درخواست وام جدید (فقط پیوی)\n"
        "🔹 <code>/my_loans</code> - مشاهده وام‌های فعال و پرداخت اقساط\n\n"
    )

    # 🏪 دستورات فروشندگان: /add_product همیشه برای همه کاربران نمایش داده می‌شود
    # (حتی قبل از تأیید فروشگاه)، اما /inventory، /my_shop و /delete فقط پس از تأیید فروشگاه
    # توسط مدیریت و فعال شدن دسترسی فروشگاهی نمایش داده می‌شوند.
    txt += (
        "🏪 <b>دستورات فروشندگان:</b>\n"
        "🔹 <code>/request_shop</code> - ارسال درخواست ثبت فروشگاه\n"
        "🔹 <code>/add_product</code> - ثبت محصول جديد با عکس و مشخصات\n"
    )

    if group_only:
        # لیست دستورات عمومی: بدون درنظرگرفتن سطح دسترسی یا ادمین بودن کاربر
        txt += "\n"
        return txt

    is_sa = is_super_admin(user_id)
    u = await get_user_data(user_id)
    is_adm = u and u["is_admin"]

    has_approved_shop = False
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT 1 FROM shops WHERE owner_id = ? AND status = 'APPROVED' LIMIT 1", (user_id,)
        ) as cur:
            has_approved_shop = (await cur.fetchone()) is not None
        async with db.execute("SELECT 1 FROM couriers WHERE user_id = ?", (user_id,)) as cur_c:
            is_courier = (await cur_c.fetchone()) is not None or is_super_admin(user_id)

    if has_approved_shop:
        txt += (
            "🔹 <code>/inventory</code> - مدیریت موجودی انبار\n"
            "🔹 <code>/my_shop</code> - آمار کل و میزان درآمد فروشگاه\n"
            "🔹 <code>/delete [کد_محصول]</code> - حذف محصول از فروشگاه\n"
        )
    txt += "\n"

    # 🚚 دستورات پستچی‌ها: فقط برای کاربرانی که در جدول couriers هستند (یا سوپرادمین) نمایش داده می‌شود
    if is_courier:
        txt += (
            "🚚 <b>دستورات پستچی‌ها:</b>\n"
            "🔹 <code>/courier_orders</code> - مشاهده سفارش‌های آماده ارسال\n"
            "🔹 <code>/confirm_dispatch [کد]</code> - ثبت تحویل نهایی سفارش با کد ۱۰ رقمی\n\n"
        )

    if is_adm or is_sa:
        txt += (
            "👥 <b>دستورات ادمین (فقط پیوی):</b>\n"
            "🔹 <code>/users</code> - لیست کاربران\n"
            "🔹 <code>/frozen_users</code> - لیست کاربران فریز‌شده (صفحه‌بندی ۱۰ نفر)\n"
            "🔹 <code>/groups</code> - لیست گروه‌ها\n"
            "🔹 <code>/group_users [نام]</code> - اعضای یک گروه\n"
            "🔹 <code>/create_group [نام]</code> - فقط اضافه کردن گروه (بدون لینک)\n"
            "🔹 <code>/add_group [نام]</code> - ساخت <b>گروه مجازی</b> + لینک دعوت یکتا (این گروه فقط درون ربات است)\n"
            "🔹 <code>/extend_group [نام] [روز]</code> - تمدید لینک فعلی\n"
            "🔹 <code>/renew_group [نام] [روز]</code> - ساخت لینک جدید با مدت اعتبار\n"
            "🔹 <code>/rename_group [قدیمی] [جدید]</code> - تغییر نام گروه\n"
            "🔹 <code>/move_group [آیدی] [گروه]</code> - تغییر گروه کاربر\n"
            "🔹 <code>/remove_group [آیدی]</code> - برگرداندن به Default\n"
            "🔹 <code>/list_shops</code> - مشاهده لیست کامل فروشگاه‌ها\n"
            "🔹 <code>/list_couriers</code> - مشاهده لیست تمام پستچی‌ها و آیدی عددی آن‌ها\n\n"
        )

    if is_sa:
        txt += (
            "👑 <b>دستورات سوپرادمین (فقط پیوی):</b>\n"
            "🔸 <code>/shop_requests</code> - بررسی درخواست‌های فروشگاه جدید\n"
            "🔸 <code>/remove_shop [آیدی]</code> - حذف یا لغو مجوز فروشگاه\n"
            "🔸 <code>/add_courier [آیدی]</code> - افزودن پستچی جدید\n"
            "🔸 <code>/remove_courier [آیدی]</code> - حذف پستچی\n"
            "🔸 <code>/give [آیدی] [مقدار]</code> - واریز آتر\n"
            "🔸 <code>/take [آیدی] [مقدار]</code> - کسر آتر\n"
            "🔸 <code>/rewardgroup [گروه] [مقدار]</code> - واریز همگانی به یک گروه\n"
            "🔸 <code>/undo [شناسه_تراکنش]</code> - لغو و برگشت تراکنش\n"
            "🔸 <code>/freeze [آیدی]</code> / <code>/unfreeze [آیدی]</code> - مسدود/فعال‌سازی\n"
            "🔸 <code>/promote [آیدی]</code> / <code>/demote [آیدی]</code> - ارتقا/سلب ادمین\n"
            "🔸 <code>/add_super [آیدی]</code> / <code>/remove_super [آیدی]</code> - مدیریت سوپرادمین‌ها\n"
            "🔸 <code>/list_admins</code> - لیست ادمین‌ها و سوپرادمین‌ها\n"
            "🔸 <code>/check [آیدی]</code> - مشاهده اطلاعات کامل حساب\n"
            "🔸 <code>/economy</code> - آمار کل نقدینگی و وضعیت خزانه مرکزی\n\n"
            "🏛 <b>دستورات خزانه:</b>\n"
            "🔸 <code>/treasury</code> - نمایش موجودی و اطلاعات خزانه\n"
            "🔸 <code>/treasury_add [مقدار] [دلیل]</code> - افزایش دستی موجودی خزانه\n"
            "🔸 <code>/treasury_sub [مقدار] [دلیل]</code> - کاهش دستی موجودی خزانه\n"
            "🔸 <code>/treasury_give [آیدی] [مبلغ]</code> - فقط حساب خزانه؛ به کاربر از خزانه پول می‌دهد\n"
            "🔸 <code>/treasury_take [آیدی] [مبلغ]</code> - فقط حساب خزانه؛ از کاربر می‌گیرد و به خزانه اضافه می‌کند\n"
            "🔸 <code>/group_salary [گروه] [مبلغ]</code> - فقط حساب خزانه؛ پرداخت پول گروهی از خزانه\n\n"
            "🔸 <code>/view_set_all</code> - مشاهده تمام تنظیمات و درصدهای سیستم (و دستورهای تغییر هرکدام)\n"
            "🔸 <code>/backup_now</code> - دانلود بکاپ Zip دیتابیس\n"
            "🔸 <code>/force_backup</code> - ارسال فایل دیتابیس به کانال تلگرام\n"
            "🔸 <code>/restore</code> - بازیابی دیتابیس (با ریپلای روی فایل)\n"
            "🔸 <code>/reset_all</code> - ریست کامل سیستم (پاک‌سازی تمام داده‌ها به‌جز تنظیمات مدیریتی)\n"
        )

    return txt


@user_router.message(Command("help"))
@user_router.message(F.text == "راهنمای جامع بانک")
async def cmd_help(message: Message):
    txt = await _build_help_text(message.from_user.id, group_only=False)
    await message.reply(txt, parse_mode="HTML")


# --- نقطه شروع ربات ---


async def main():
    if not BOT_TOKEN:
        logging.error("❌ BOT_TOKEN یافت نشد! لطفا آن را در فایل .env یا Environment Variables تنظیم کنید.")
        return

    # اجرای وب سرور جهت ساخت Web Service در حالت Dummy
    await start_dummy_server()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # بازیابی دیتابیس از تلگرام در صورت نیازمندی سرور (مثل Render بعد از ری‌استارت)
    await restore_db_from_telegram(bot)

    # ایجاد جداول دیتابیس
    await init_db()

    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.include_router(shop_router)

    # شروع حلقه بکاپ‌گیری خودکار تلگرامی
    asyncio.create_task(auto_backup_loop(bot))

    # 🏦 شروع حلقه پردازش سود شبانه بانک آترامنتوم (ساعت ۰۰:۰۰ به وقت ایران)
    asyncio.create_task(bank_interest_loop(bot))

    # 💳 شروع حلقه مدیریت اقساط وام (یادآوری، جریمه دیرکرد و کسر خودکار بدهی معوقه)
    asyncio.create_task(loan_management_loop(bot))

    logging.info("🚀 ربات بانک آتر با موفقیت روشن شد و آماده پردازش است.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 ربات خاموش شد.")
