import os
import logging
import json
import re
import time
import csv
import io
import threading
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
from datetime import datetime, timedelta

# =========================================================
# ===== الإعدادات =====
# =========================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")"
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")"
EXCHANGE_API_KEY = os.environ.get("EXCHANGE_API_KEY", "")"
CREDS_FILE        = "almo-finance-fed54c6df056.json"
SHEET_NAME        = "Almo_v3"
TIMEZONE_OFFSET   = 3        # UTC+3 السعودية
MAX_MSG_LENGTH    = 500      # حد أقصى لطول رسالة GPT
SESSION_TIMEOUT   = 1800     # 30 دقيقة — تنظيف الجلسات المعلقة
DUPLICATE_SECONDS = 10       # منع التكرار خلال 10 ثوانٍ
BACKUP_INTERVAL   = 604800   # أسبوع بالثوانٍ

# =========================================================
# ===== الاتصال بجوجل شيتس =====
# =========================================================
scope           = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds           = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
gc              = gspread.authorize(creds)
sh              = gc.open(SHEET_NAME)
transactions_ws = sh.worksheet("transactions")
debts_ws        = sh.worksheet("debts")
settings_ws     = sh.worksheet("settings")
savings_ws      = sh.worksheet("savings")

client      = OpenAI(api_key=OPENAI_API_KEY)
_write_lock = threading.Lock()
logging.basicConfig(level=logging.INFO)

# =========================================================
# ===== الذاكرة المؤقتة =====
# =========================================================
pending_actions      = {}
undo_buffer          = {}
conversation_history = {}
message_timestamps   = {}
last_registered      = {}
owner_chat_ids       = set()   # لجدولة الـ backup
MAX_HISTORY          = 6

# =========================================================
# ===== التوقيت المحلي — ثابت بغض النظر عن السيرفر =====
# =========================================================
def now_local():
    return datetime.utcnow() + timedelta(hours=TIMEZONE_OFFSET)

# =========================================================
# ===== عرض الأرقام بذكاء — 5.5 تظهر 5.5 مش 6 =====
# =========================================================
def fmt(n):
    try:
        n = float(n)
        return str(int(n)) if n == int(n) else f"{n:.1f}"
    except Exception:
        return str(n)

# =========================================================
# ===== safe_float =====
# =========================================================
def safe_float(value, default=0.0):
    if value is None or str(value).strip() == "":
        return default
    s = str(value).strip()
    s = s.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    s = s.replace("٫", ".").replace("،", "").replace(",", "")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        result = float(s)
        return result if result >= 0 else default
    except Exception:
        return default

# =========================================================
# ===== Formula Injection Protection =====
# أي قيمة تبدأ بـ = أو + أو - أو @ → نضيف ' في البداية
# =========================================================
def safe_str(value):
    s = str(value)
    if s.startswith(("=", "+", "-", "@")):
        s = "'" + s
    return s

# =========================================================
# ===== Rate Limiting — max 10 رسائل/دقيقة =====
# =========================================================
def check_rate_limit(chat_id):
    now = now_local()
    if chat_id not in message_timestamps:
        message_timestamps[chat_id] = []
    message_timestamps[chat_id] = [
        t for t in message_timestamps[chat_id]
        if (now - t).total_seconds() < 60
    ]
    if len(message_timestamps[chat_id]) >= 10:
        return False
    message_timestamps[chat_id].append(now)
    return True

# =========================================================
# ===== Idempotency — منع التكرار 10 ثوانٍ =====
# =========================================================
def is_duplicate_entry(chat_id, item, amount):
    now = now_local()
    key = f"{item}_{amount}"
    if chat_id in last_registered:
        last = last_registered[chat_id]
        if last["key"] == key and (now - last["time"]).total_seconds() < DUPLICATE_SECONDS:
            return True
    last_registered[chat_id] = {"key": key, "time": now}
    return False

# =========================================================
# ===== كشف النفي — مستقل عن GPT =====
# =========================================================
def has_negation(message):
    patterns = [
        "لا تسجل", "ما تسجل", "لكن لا", "بس لا",
        "ما تضيف", "لكن ما تضيف", "بس ما تضيف",
        "لا تضيف", "لا تضيفها", "ما تضيفها",
        "لا تسجلها", "ما تسجلها", "لا الآن", "مو الآن",
        "لا تحفظ", "ما تحفظ", "بدون تسجيل", "من غير تسجيل"
    ]
    return any(p in message for p in patterns)

# =========================================================
# ===== كشف التصحيح — "اتسجل غلط" → حذف =====
# =========================================================
def is_mistake_correction(message):
    patterns = [
        "اتسجل بالغلط", "سجلته غلط", "سجّلته غلط",
        "م كان لازم", "ما كان لازم", "اتسجل غلط",
        "سجل بالغلط", "غلط في التسجيل", "سجّل بالغلط",
        "تسجيل غلط", "سجّلتها غلط", "سجلتها غلط"
    ]
    return any(p in message for p in patterns)

# =========================================================
# ===== تنظيف الجلسات المنتهية =====
# =========================================================
def cleanup_expired_sessions():
    now     = now_local()
    expired = [
        cid for cid, pa in pending_actions.items()
        if "created_at" in pa and (now - pa["created_at"]).total_seconds() > SESSION_TIMEOUT
    ]
    for cid in expired:
        del pending_actions[cid]

# =========================================================
# ===== Sheets Retry =====
# =========================================================
def sheets_call(func, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt < retries - 1:
                logging.warning(f"Sheets retry {attempt+1}/{retries}: {e}")
                time.sleep(delay)
            else:
                raise e

# =========================================================
# ===== تحويل العملات =====
# =========================================================
def convert_currency(amount, from_currency, to_currency="SAR"):
    if from_currency.upper() in ("SAR", "ر", "ريال", ""):
        return amount, 1.0
    try:
        url      = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_API_KEY}/pair/{from_currency.upper()}/{to_currency.upper()}"
        response = requests.get(url, timeout=5)
        data     = response.json()
        if data.get("result") == "success":
            rate      = data["conversion_rate"]
            converted = round(amount * rate, 2)
            return converted, rate
    except Exception as e:
        logging.warning(f"Currency conversion failed: {e}")
    return amount, 1.0

# =========================================================
# ===== قراءة البيانات =====
# =========================================================
def get_settings():
    return sheets_call(lambda: {str(r["key"]): r["value"] for r in settings_ws.get_all_records()})

def get_transactions():
    return sheets_call(lambda: transactions_ws.get_all_records())

def get_debts():
    return sheets_call(lambda: debts_ws.get_all_records())

# =========================================================
# ===== تحويل الوقت =====
# =========================================================
def to12h(time_str):
    try:
        t      = datetime.strptime(str(time_str), "%H:%M")
        h      = t.hour % 12 or 12
        period = "ص" if t.hour < 12 else "م"
        return f"{h}:{t.minute:02d} {period}"
    except Exception:
        return time_str

def resolve_date(date_str):
    now = now_local()
    if not date_str or date_str == "today":     return now.strftime("%Y-%m-%d")
    elif date_str == "yesterday":               return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    elif date_str == "day_before":              return (now - timedelta(days=2)).strftime("%Y-%m-%d")
    else:                                       return date_str

# =========================================================
# ===== إضافة معاملة =====
# =========================================================
def add_transaction(item, amount, type_, category, person="", notes="", custom_date=None, custom_time=None):
    with _write_lock:
        now      = now_local()
        date     = resolve_date(custom_date) if custom_date else now.strftime("%Y-%m-%d")
        time_str = custom_time if custom_time else now.strftime("%H:%M")
        month    = date[:7]
        rows     = get_transactions()
        new_id   = max([int(r.get("id", 0)) for r in rows], default=0) + 1
        transactions_ws.append_row([
            new_id, safe_str(date), safe_str(time_str),
            safe_str(item), safe_float(amount), safe_str(type_),
            safe_str(category), safe_str(person), safe_str(notes), safe_str(month)
        ])
        return new_id, date, time_str

# =========================================================
# ===== إضافة دين =====
# شيت debts: id|date|person|item|amount|paid|remaining|direction|status|notes
# =========================================================
def add_debt(person, item, amount, direction, installments=None, notes=""):
    with _write_lock:
        now        = now_local()
        rows       = get_debts()
        new_id     = max([int(r.get("id", 0)) for r in rows], default=0) + 1
        notes_full = notes
        if installments:
            monthly    = round(safe_float(amount) / int(installments), 1)
            notes_full = f"أقساط: {installments} شهر | {monthly} ر/شهر | {notes}".strip(" |")
        debts_ws.append_row([
            new_id, now.strftime("%Y-%m-%d"),
            safe_str(person), safe_str(item),
            safe_float(amount), 0, safe_float(amount),
            safe_str(direction), "active", safe_str(notes_full)
        ])
        return new_id

# =========================================================
# ===== تسديد دين جزئي أو كامل =====
# =========================================================
def pay_debt_amount(row_id, payment):
    with _write_lock:
        all_data = debts_ws.get_all_values()
        for i, row in enumerate(all_data):
            if i == 0: continue
            if str(row[0]) == str(row_id):
                current_paid      = safe_float(row[5] if len(row) > 5 else 0)
                current_remaining = safe_float(row[6] if len(row) > 6 else row[4])
                new_paid          = current_paid + payment
                new_remaining     = max(0, current_remaining - payment)
                new_status        = "paid" if new_remaining <= 0 else "active"
                debts_ws.update_cell(i + 1, 6, new_paid)
                debts_ws.update_cell(i + 1, 7, new_remaining)
                debts_ws.update_cell(i + 1, 9, new_status)
                return new_paid, new_remaining, new_status
        return None, None, None

# =========================================================
# ===== حذف معاملة =====
# =========================================================
def delete_transaction_by_id(row_id):
    with _write_lock:
        all_data = transactions_ws.get_all_values()
        for i, row in enumerate(all_data):
            if i == 0: continue
            if str(row[0]) == str(row_id):
                transactions_ws.delete_rows(i + 1)
                return True
        return False

# =========================================================
# ===== تعديل حقل في معاملة =====
# =========================================================
def update_transaction_field(row_id, field, new_value):
    with _write_lock:
        field_to_col = {
            "date": 2, "time": 3, "item": 4, "amount": 5,
            "type": 6, "category": 7, "person": 8, "notes": 9
        }
        col = field_to_col.get(field)
        if not col: return False
        all_data = transactions_ws.get_all_values()
        for i, row in enumerate(all_data):
            if i == 0: continue
            if str(row[0]) == str(row_id):
                transactions_ws.update_cell(i + 1, col, safe_str(new_value))
                if field == "date":
                    transactions_ws.update_cell(i + 1, 10, str(new_value)[:7])
                return True
        return False

# =========================================================
# ===== تعديل حقل في دين =====
# =========================================================
def update_debt_field(row_id, field, new_value):
    with _write_lock:
        field_to_col = {
            "person": 3, "item": 4, "amount": 5,
            "paid": 6, "remaining": 7,
            "direction": 8, "status": 9, "notes": 10
        }
        col = field_to_col.get(field)
        if not col: return False
        all_data = debts_ws.get_all_values()
        for i, row in enumerate(all_data):
            if i == 0: continue
            if str(row[0]) == str(row_id):
                debts_ws.update_cell(i + 1, col, safe_str(new_value))
                return True
        return False

# =========================================================
# ===== أسماء الحقول =====
# =========================================================
def get_field_labels(sheet):
    if sheet == "transactions":
        return {"item": "الاسم", "amount": "المبلغ", "date": "التاريخ",
                "time": "الوقت", "type": "النوع", "category": "الفئة"}
    return {"person": "الشخص", "item": "الوصف", "amount": "المبلغ",
            "paid": "المدفوع", "remaining": "المتبقي",
            "direction": "الاتجاه", "status": "الحالة", "notes": "الملاحظات"}

# =========================================================
# ===== حساب الأقساط الشهرية من شيت الديون تلقائياً =====
# =========================================================
def calc_monthly_installments():
    try:
        debts   = get_debts()
        monthly = 0
        for d in debts:
            if d.get("status") != "active": continue
            notes = str(d.get("notes", ""))
            if "أقساط" in notes and "ر/شهر" in notes:
                match = re.search(r"(\d+(?:\.\d+)?)\s*ر/شهر", notes)
                if match:
                    monthly += safe_float(match.group(1))
        return monthly
    except Exception:
        return 0

# =========================================================
# ===== حساب الرصيد — المنطق الصحيح =====
# 500 ثابتة للصرف | الادخار يتآكل فقط بعد استنفاد الـ500
# =========================================================
def calc_balance():
    try:
        settings = get_settings()
    except Exception as e:
        logging.error(f"calc_balance - settings error: {e}")
        settings = {}

    family          = safe_float(settings.get("family", 0))
    internet        = safe_float(settings.get("internet", 0))
    debt_monthly    = safe_float(settings.get("debt_monthly", 0))
    spending_cap    = safe_float(settings.get("spending_cap", 500))
    alert_threshold = safe_float(settings.get("alert_threshold", 100))

    # الأقساط تُحسب تلقائياً من شيت الديون
    installment = calc_monthly_installments()
    fixed       = family + internet + installment + debt_monthly

    try:
        transactions = get_transactions()
    except Exception as e:
        logging.error(f"calc_balance - transactions error: {e}")
        transactions = []

    now            = now_local()
    today          = now.strftime("%Y-%m-%d")
    current_month  = now.strftime("%Y-%m")
    expenses_month = 0
    expenses_today = 0
    income_month   = 0

    for r in transactions:
        try:
            if str(r.get("month", "")) == current_month:
                amount = safe_float(r.get("amount", 0))
                if r.get("type") == "expense":
                    expenses_month += amount
                    if str(r.get("date", "")) == today:
                        expenses_today += amount
                elif r.get("type") == "income":
                    income_month += amount
        except Exception as e:
            logging.warning(f"calc_balance - row skipped: {e}")
            continue

    total_available  = income_month - fixed
    spending_budget  = min(total_available, spending_cap)
    savings_pool     = max(0, total_available - spending_cap)
    budget_remaining = spending_budget - expenses_month

    if budget_remaining >= 0:
        spending_available = budget_remaining
        savings_now        = savings_pool
    else:
        spending_available = 0
        # الادخار يتآكل بمقدار الزيادة عن الـ500 فقط
        overspend  = abs(budget_remaining)
        savings_now = max(0, savings_pool - overspend)

    net            = total_available - expenses_month
    day_of_month   = now.day
    days_remaining = 30 - day_of_month
    avg_daily      = expenses_month / day_of_month if day_of_month > 0 else 0
    daily_target   = spending_cap / 30

    return {
        "net": net, "spending_available": spending_available,
        "savings_potential": savings_now, "spending_budget": spending_budget,
        "expenses_month": expenses_month, "expenses_today": expenses_today,
        "income_month": income_month, "fixed": fixed, "installment": installment,
        "avg_daily": avg_daily, "days_remaining": days_remaining,
        "spending_cap": spending_cap, "daily_target": daily_target,
        "alert_threshold": alert_threshold, "current_month": current_month,
    }

# =========================================================
# ===== البحث في المعاملات =====
# =========================================================
def search_transactions(search_criteria):
    now        = now_local()
    today      = now.strftime("%Y-%m-%d")
    yesterday  = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    results    = []

    for r in get_transactions():
        match       = True
        date_filter = str(search_criteria.get("date", "")).strip()
        if date_filter:
            if   date_filter == "today":      match = str(r.get("date")) == today
            elif date_filter == "yesterday":  match = str(r.get("date")) == yesterday
            elif date_filter == "day_before": match = str(r.get("date")) == day_before
            else:                             match = str(r.get("date")) == date_filter

        item_filter = str(search_criteria.get("item", "")).strip()
        if item_filter and match:
            item_clean = re.sub(r"^ال", "", item_filter)  # يتجاهل "ال" في البداية
            if item_clean.lower() not in str(r.get("item", "")).lower(): match = False

        type_filter = str(search_criteria.get("type", "")).strip()
        if type_filter and match:
            if str(r.get("type", "")) != type_filter: match = False

        person_filter = str(search_criteria.get("person", "")).strip()
        if person_filter and match:
            person_clean = re.sub(r"^ال", "", person_filter)
            if person_clean not in str(r.get("person", "")): match = False

        time_filter = str(search_criteria.get("time", "")).strip()
        if time_filter and match:
            try:
                t_f = datetime.strptime(time_filter, "%H:%M")
                t_r = datetime.strptime(str(r.get("time", "00:00")), "%H:%M")
                if abs((t_f - t_r).total_seconds()) / 60 > 30: match = False
            except Exception: pass

        if match: results.append(r)

    if search_criteria.get("last") and results:
        results = [results[-1]]
    return results

# =========================================================
# ===== البحث في الديون =====
# =========================================================
def search_debts(search_criteria):
    results = []
    for r in get_debts():
        match = True
        person_filter = str(search_criteria.get("person", "")).strip()
        if person_filter and person_filter not in str(r.get("person", "")): match = False

        item_filter = str(search_criteria.get("item", "")).strip()
        if item_filter and match:
            if item_filter.lower() not in str(r.get("item", "")).lower(): match = False

        direction_filter = str(search_criteria.get("direction", "")).strip()
        if direction_filter and match:
            if str(r.get("direction", "")) != direction_filter: match = False

        if match: results.append(r)

    if search_criteria.get("last") and results:
        results = [results[-1]]
    return results

# =========================================================
# ===== ملخص الفترة =====
# =========================================================
def build_summary(period="today"):
    now        = now_local()
    today      = now.strftime("%Y-%m-%d")
    yesterday  = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    transactions = get_transactions()

    if   period == "today":      label, rows = "اليوم",      [r for r in transactions if str(r.get("date")) == today]
    elif period == "yesterday":  label, rows = "أمس",        [r for r in transactions if str(r.get("date")) == yesterday]
    elif period == "day_before": label, rows = "قبل يومين",  [r for r in transactions if str(r.get("date")) == day_before]
    elif period == "week":       label, rows = "آخر 7 أيام", [r for r in transactions if str(r.get("date","")) >= week_start]
    else:                        label, rows = "اليوم",      [r for r in transactions if str(r.get("date")) == today]

    expenses  = [r for r in rows if r.get("type") == "expense"]
    income    = [r for r in rows if r.get("type") == "income"]
    total_exp = sum(safe_float(r.get("amount", 0)) for r in expenses)
    total_inc = sum(safe_float(r.get("amount", 0)) for r in income)

    if not rows: return f"📭 ما في عمليات مسجّلة {label}"

    msg = f"📋 ملخص {label}\n{'─'*20}\n"
    if expenses:
        msg += f"🛒 مصاريف ({len(expenses)} عملية):\n"
        for r in expenses:
            msg += f"  • {r.get('item')} — {fmt(r.get('amount'))} ر | {to12h(r.get('time',''))}\n"
        msg += f"  💰 المجموع: {fmt(total_exp)} ريال\n"
    if income:
        msg += f"\n💵 دخل ({len(income)} عملية):\n"
        for r in income:
            msg += f"  • {r.get('item')} — {fmt(r.get('amount'))} ر | {to12h(r.get('time',''))}\n"
        msg += f"  💰 المجموع: {fmt(total_inc)} ريال\n"
    if total_inc > 0:
        msg += f"\n{'─'*20}\n📊 صافي {label}: {fmt(total_inc - total_exp)} ريال"
    return msg

# =========================================================
# ===== مقارنة الفترات =====
# =========================================================
def build_comparison(period1, period2):
    now  = now_local()
    all_t = get_transactions()

    def get_period(period):
        today      = now.strftime("%Y-%m-%d")
        yesterday  = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        last_week_s = (now - timedelta(days=14)).strftime("%Y-%m-%d")
        last_week_e = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        month       = now.strftime("%Y-%m")
        last_month  = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

        if   period == "today":       label, rows = "اليوم",           [r for r in all_t if str(r.get("date")) == today]
        elif period == "yesterday":   label, rows = "أمس",             [r for r in all_t if str(r.get("date")) == yesterday]
        elif period == "this_week":   label, rows = "هذا الأسبوع",     [r for r in all_t if str(r.get("date","")) >= week_start]
        elif period == "last_week":   label, rows = "الأسبوع الماضي",  [r for r in all_t if last_week_s <= str(r.get("date","")) < last_week_e]
        elif period == "this_month":  label, rows = "هذا الشهر",      [r for r in all_t if str(r.get("month","")) == month]
        elif period == "last_month":  label, rows = "الشهر الماضي",    [r for r in all_t if str(r.get("month","")) == last_month]
        else:                         label, rows = period, []

        exp = sum(safe_float(r.get("amount",0)) for r in rows if r.get("type") == "expense")
        inc = sum(safe_float(r.get("amount",0)) for r in rows if r.get("type") == "income")
        return {"label": label, "expenses": exp, "income": inc, "count": len(rows)}

    d1   = get_period(period1)
    d2   = get_period(period2)
    diff = d1["expenses"] - d2["expenses"]
    pct  = (diff / d2["expenses"] * 100) if d2["expenses"] > 0 else 0

    msg  = f"📊 مقارنة\n{'─'*25}\n"
    msg += f"📅 {d1['label']}\n"
    msg += f"   🛒 مصاريف: {fmt(d1['expenses'])} ريال\n"
    if d1["income"] > 0: msg += f"   💵 دخل: {fmt(d1['income'])} ريال\n"
    msg += f"\n📅 {d2['label']}\n"
    msg += f"   🛒 مصاريف: {fmt(d2['expenses'])} ريال\n"
    if d2["income"] > 0: msg += f"   💵 دخل: {fmt(d2['income'])} ريال\n"
    msg += f"\n{'─'*25}\n"

    if   diff > 0:  msg += f"📈 صرفت أكثر بـ {fmt(diff)} ريال ({abs(pct):.0f}%)"
    elif diff < 0:  msg += f"📉 صرفت أقل بـ {fmt(abs(diff))} ريال ({abs(pct):.0f}%)"
    else:           msg += "➡️ نفس المبلغ تقريباً"
    return msg

# =========================================================
# ===== الفلترة المزدوجة — "كم صرفت على القهوة أمس" =====
# =========================================================
def build_query(item_filter, date_filter, person_filter=""):
    rows  = search_transactions({"item": item_filter, "date": date_filter, "person": person_filter})
    exps  = [r for r in rows if r.get("type") == "expense"]
    total = sum(safe_float(r.get("amount", 0)) for r in exps)

    if not rows: return f"🔍 ما وجدت عمليات" + (f" لـ'{item_filter}'" if item_filter else "")

    msg = f"🔍 نتائج البحث\n{'─'*20}\n"
    for r in rows:
        t = "🛒" if r.get("type") == "expense" else "💵"
        msg += f"{t} {r.get('item')} — {fmt(safe_float(r.get('amount')))} ر | {r.get('date')}\n"
    if len(exps) > 0:
        msg += f"{'─'*20}\n💰 المجموع: {fmt(total)} ريال"
    return msg

# =========================================================
# ===== البوت الذكي التحليلي =====
# =========================================================
def ask_gpt_analyze(question, chat_id):
    data         = calc_balance()
    transactions = get_transactions()
    debts        = get_debts()

    # ملخص الفئات
    categories = {}
    for r in transactions:
        if r.get("type") == "expense":
            cat             = str(r.get("category", "عام"))
            categories[cat] = categories.get(cat, 0) + safe_float(r.get("amount", 0))

    # آخر 30 عملية
    recent = [
        {"item": r.get("item"), "amount": r.get("amount"),
         "date": r.get("date"), "type": r.get("type"), "category": r.get("category")}
        for r in transactions[-30:]
    ]

    # ملخص الديون
    active_debts = [
        {"person": d.get("person"), "item": d.get("item"),
         "remaining": d.get("remaining", d.get("amount")),
         "direction": d.get("direction")}
        for d in debts if d.get("status") == "active"
    ]

    system_prompt = f"""أنت مستشار مالي ذكي وودود. لديك بيانات مالية كاملة للمستخدم وتجيب بشكل طبيعي وشخصي.

الوضع الحالي:
- الدخل هذا الشهر: {fmt(data['income_month'])} ريال
- المصاريف: {fmt(data['expenses_month'])} ريال
- باقي للصرف: {fmt(data['spending_available'])} ريال من 500
- ادخار متوقع: {fmt(data['savings_potential'])} ريال
- المعدل اليومي: {fmt(data['avg_daily'])} ريال
- باقي في الشهر: {data['days_remaining']} يوم

توزيع المصاريف: {json.dumps(categories, ensure_ascii=False)}
آخر العمليات: {json.dumps(recent, ensure_ascii=False)}
الديون النشطة: {json.dumps(active_debts, ensure_ascii=False)}

قواعد:
- رد بالعربية العامية ومباشر
- استخدم الأرقام الفعلية في ردودك
- لو سؤال عن سعر عملة → استخدم بيانات ExchangeRate API المتاحة
- الرد قصير (3-5 جمل) ومفيد
- لو المستخدم قلقان أو محتاج نصيحة → ابنِ ردك على بياناته الفعلية"""

    history  = conversation_history.get(chat_id, [])
    messages = [{"role": "system", "content": system_prompt}] + history[-4:]
    messages.append({"role": "user", "content": question})

    response = client.chat.completions.create(
        model="gpt-4o", messages=messages, temperature=0.7
    )
    return response.choices[0].message.content.strip()

# =========================================================
# ===== التحفيز =====
# =========================================================
def build_motivation(data):
    net   = data["net"]
    avg   = data["avg_daily"]
    days  = data["days_remaining"]
    inc   = data["income_month"]
    sav   = data["savings_potential"]
    spent = data["spending_available"]

    if days <= 0:   return ""
    if inc == 0:    return "\n\n💡 سجّل راتبك عشان أقدر أحسب وضعك"
    if net < 0:     return f"\n\n⚠️ تجاوزت دخلك بـ{fmt(abs(net))} ريال"
    if spent == 0:
        if sav > 0: return f"\n\n⚠️ خلصت ميزانية الصرف — تصرف من الادخار ({fmt(sav)} ر متبقي)"
        return         "\n\n🔴 تجاوزت الميزانية والادخار"
    if avg > 0 and days > 0 and avg > (spent / max(days, 1)):
        return f"\n\n⚠️ باقي {fmt(spent)} ريال لـ{days} يوم — معدلك مرتفع"
    if sav > 0:
        return f"\n\n🎯 ممتاز! ادخار متوقع {fmt(sav)} ريال"
    return f"\n\n💡 وضعك مستقر — باقي {fmt(spent)} ريال للصرف"

# =========================================================
# ===== التقرير الشهري =====
# =========================================================
def build_report(data):
    months_ar = {
        "January":"يناير","February":"فبراير","March":"مارس","April":"أبريل",
        "May":"مايو","June":"يونيو","July":"يوليو","August":"أغسطس",
        "September":"سبتمبر","October":"أكتوبر","November":"نوفمبر","December":"ديسمبر"
    }
    now      = now_local()
    month_ar = months_ar.get(now.strftime("%B"), now.strftime("%B"))
    avg      = data["avg_daily"]
    daily_t  = data["daily_target"]

    if data["days_remaining"] <= 5:
        avg_comment = "آخر الشهر"
    elif avg <= daily_t:
        avg_comment = "ممتاز 🟢"
    elif avg <= daily_t * 1.3:
        avg_comment = "مقبول 🟡"
    else:
        avg_comment = "مرتفع 🔴"

    r  = f"📊 تقرير {month_ar}\n{'─'*25}\n"
    r += f"💵 الدخل المسجّل: {fmt(data['income_month'])} ريال\n"
    r += f"🔒 الثوابت: {fmt(data['fixed'])} ريال\n"
    if data["installment"] > 0:
        r += f"   ↳ منها أقساط: {fmt(data['installment'])} ريال/شهر\n"
    r += f"🛒 مصاريف الشهر: {fmt(data['expenses_month'])} ريال\n"
    r += f"📅 مصاريف اليوم: {fmt(data['expenses_today'])} ريال\n"
    r += f"{'─'*25}\n"
    r += f"✅ الصافي: {fmt(data['net'])} ريال\n"
    r += f"{'─'*25}\n"
    r += f"💳 ميزانية الصرف: {fmt(data['spending_cap'])} ريال\n"
    r += f"📊 متاح للصرف: {fmt(data['spending_available'])} ريال\n"
    if data["savings_potential"] > 0:
        r += f"🔒 ادخار متوقع: {fmt(data['savings_potential'])} ريال\n"
    r += f"{'─'*25}\n"
    r += f"📈 المعدل اليومي: {fmt(avg)} ريال — {avg_comment}\n"
    r += f"⏳ باقي {data['days_remaining']} يوم في الشهر\n"

    debts      = get_debts()
    act_debts  = [d for d in debts if d.get("status") == "active"]
    if act_debts:
        r += f"\n{'─'*25}\n📌 الديون النشطة:\n"
        for d in act_debts:
            remaining = safe_float(d.get("remaining", d.get("amount", 0)))
            r += f"   • {d.get('person')} — متبقي {fmt(remaining)} ريال"
            if "أقساط" in str(d.get("notes", "")):
                match = re.search(r"(\d+(?:\.\d+)?)\s*ر/شهر", str(d.get("notes", "")))
                if match: r += f" ({fmt(safe_float(match.group(1)))} ر/شهر)"
            r += "\n"

    r += build_motivation(data)
    return r

# =========================================================
# ===== GPT Router =====
# =========================================================
def ask_gpt(user_message, data, chat_id):
    if len(user_message) > MAX_MSG_LENGTH:
        user_message = user_message[:MAX_MSG_LENGTH] + "..."

    now        = now_local()
    today      = now.strftime("%Y-%m-%d")
    yesterday  = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before = (now - timedelta(days=2)).strftime("%Y-%m-%d")

    system_prompt = f"""أنت نظام تحليل مالي ذكي. مهمتك: تحليل رسالة المستخدم وإرجاع JSON فقط بدون أي نص أو backticks.

التاريخ: {today} | الوقت: {now.strftime("%H:%M")}
أمس: {yesterday} | قبل يومين: {day_before}
متاح للصرف: {fmt(data.get('spending_available',0))} ريال من {fmt(data.get('spending_cap',500))} ريال
مصاريف الشهر: {fmt(data.get('expenses_month',0))} ريال

━━━ الأنواع المتاحة ━━━

1. تسجيل:
{{"action":"register","items":[{{"item":"اسم","amount":رقم,"type":"expense أو income","category":"فئة","person":"","notes":"","date":"today أو yesterday أو day_before أو فارغ","time":"HH:MM أو فارغ","currency":"SAR أو USD أو EUR أو غيرها"}}]}}

2. دين:
{{"action":"debt","person":"","item":"","amount":رقم,"direction":"علي أو لي","installments":null_أو_عدد,"notes":""}}

3. تسديد دين جزئي أو كامل:
{{"action":"pay_debt","search":{{"person":"","item":""}},"amount":رقم}}

4. حذف: {{"action":"delete","search":{{"item":"","date":"","time":"","type":"","person":"","last":false}}}}

5. حذف مجمع: {{"action":"delete_bulk","filter":{{"date":"","type":""}},"exclude":[]}}

6. تقرير: {{"action":"report"}}

7. ملخص: {{"action":"summary","period":"today أو yesterday أو day_before أو week"}}

8. مقارنة فترتين:
{{"action":"compare","period1":"today أو yesterday أو this_week أو last_week أو this_month أو last_month","period2":"..."}}

9. بحث مزدوج — سؤال عن فئة أو عنصر في فترة:
{{"action":"query","item":"قهوة أو فارغ","date":"today أو yesterday أو day_before أو فارغ","person":"أو فارغ"}}

10. سؤال عام أو تحليلي:
{{"action":"analyze","question":"السؤال كاملاً"}}

11. سؤال بسيط: {{"action":"answer","message":"ردك"}}

12. مختلطة: {{"action":"mixed","operations":[...]}}

13. تعديل: {{"action":"edit","sheet":"transactions أو debts","search":{{"item":"","date":"","person":"","last":false}}}}

━━━ قواعد حرجة ━━━

❌ لا تُسجِّل — أرجع analyze أو answer:
1. "لا تسجل/ما تسجل/لكن لا/بس لا/ما تضيف/لا الآن/مو الآن" → answer: "حاضر، لم أسجّل"
2. المبلغ صفر أو غير مذكور → answer: "بكم؟"
3. الاسم فارغ → answer: "إيش تقصد؟"
4. صيغة مستقبلية "لو/ممكن" → analyze
5. لا تسجّل من المحادثة السابقة إلا بطلب صريح

✅ مثنى بدون سعر → answer: "بكم؟"

━━━ التصحيح التلقائي ━━━
"اتسجل بالغلط/سجلته غلط/م كان لازم/سجل بالغلط" → delete last:true للعنصر المذكور

━━━ الحساب العربي ━━━
- "إلا" = ناقص | "وربع" = +0.25 | "ونص/ونصف" = +0.5 | "وثلث" = +0.33
- "خمسة موية/خمسمية" = 500 | "ألفين" = 2000
- احسب أولاً ثم ضع الناتج في amount

━━━ العملات ━━━
- لو ذكر عملة غير ريال → ضعها في currency (USD, EUR, GBP, AED...)
- "دولار" → USD | "يورو" → EUR | "درهم" → AED

━━━ التفسيرات ━━━
- "خبر/قول/استلمت/راتب" = income | "استلفت/تدينت" = debt علي | "فلان استلف مني" = debt لي
- "سدّدت/دفعت ... دين/قسط" = pay_debt
- "آخر عملية" = last:true
- "4 العصر" = 16:00 | "8 الصبح" = 08:00
- "احذف/امسح/شيل" = delete | "بدّل/غيّر/عدّل/صحّح" = edit
- "قارن/مقارنة" = compare | "كم صرفت على X في Y" = query
- "نصحني/حللي/أكثر شيء/وضعي/قلقان" = analyze

أرجع JSON فقط."""

    history  = conversation_history.get(chat_id, [])
    messages = [{"role": "system", "content": system_prompt}] + history
    messages.append({"role": "user", "content": user_message})

    response = client.chat.completions.create(
        model="gpt-4o", messages=messages, temperature=0.2
    )
    return response.choices[0].message.content.strip()

# =========================================================
# ===== تحديث تاريخ المحادثة =====
# =========================================================
def update_history(chat_id, user_message, assistant_reply):
    if chat_id not in conversation_history:
        conversation_history[chat_id] = []
    history = conversation_history[chat_id]
    history.append({"role": "user",      "content": user_message})
    history.append({"role": "assistant", "content": str(assistant_reply)})
    if len(history) > MAX_HISTORY:
        conversation_history[chat_id] = history[-MAX_HISTORY:]

# =========================================================
# ===== معالجة عملية واحدة =====
# =========================================================
async def process_operation(op, reply_lines, data, chat_id):
    action = op.get("action")

    if action == "register":
        items         = op.get("items", [])
        total_expense = 0
        saved_date    = now_local().strftime("%Y-%m-%d")
        saved_time    = now_local().strftime("%H:%M")
        registered    = 0

        if not items:
            reply_lines.append("⚠️ ما في عمليات للتسجيل")
            return

        for item_data in items:
            amount   = safe_float(item_data.get("amount", 0))
            iitem    = str(item_data.get("item", "")).strip()
            currency = str(item_data.get("currency", "SAR")).strip().upper()

            if amount <= 0:
                reply_lines.append(f"⚠️ تم تخطي '{iitem or '؟'}' — المبلغ غير صحيح")
                continue
            if not iitem:
                reply_lines.append("⚠️ تم تخطي عملية — الاسم فارغ")
                continue
            if is_duplicate_entry(chat_id, iitem, amount):
                reply_lines.append(f"⚠️ '{iitem}' سُجِّلت للتو — تجاهلت التكرار")
                continue

            # ── تحويل العملة ──
            currency_note = ""
            if currency not in ("SAR", "ر", ""):
                converted, rate = convert_currency(amount, currency)
                if rate != 1.0:
                    currency_note = f" ({fmt(amount)} {currency} = {fmt(converted)} ر)"
                    amount = converted

            itype       = item_data.get("type", "expense")
            custom_date = item_data.get("date", "") or None
            custom_time = item_data.get("time", "") or None
            is_big      = (amount >= data.get("alert_threshold", 100) and itype == "expense")
            notes       = str(item_data.get("notes", "")) + currency_note

            _, saved_date, saved_time = add_transaction(
                item=iitem, amount=amount, type_=itype,
                category=item_data.get("category", "عام"),
                person=item_data.get("person", ""),
                notes=notes.strip(),
                custom_date=custom_date, custom_time=custom_time
            )
            registered += 1

            line = f"💵 {iitem} — {fmt(amount)} ريال (دخل){currency_note}" if itype == "income" \
                   else (f"⚠️ {iitem} — {fmt(amount)} ريال ✅{currency_note}" if is_big
                         else f"✅ {iitem} — {fmt(amount)} ريال{currency_note}")
            reply_lines.append(line)

            if itype == "expense": total_expense += amount

        if registered > 0:
            reply_lines.append(f"   📅 {saved_date} | 🕐 {to12h(saved_time)}")
            if total_expense > 0:
                reply_lines.append(f"💰 المجموع: {fmt(total_expense)} ريال")

    elif action == "debt":
        amount = safe_float(op.get("amount", 0))
        if amount <= 0:
            reply_lines.append("⚠️ لم أسجّل الدين — المبلغ غير صحيح")
            return
        add_debt(
            person=op.get("person",""), item=op.get("item",""),
            amount=amount, direction=op.get("direction","علي"),
            installments=op.get("installments"), notes=op.get("notes","")
        )
        direction    = op.get("direction","علي")
        person       = op.get("person","")
        installments = op.get("installments")
        now          = now_local()
        line = f"📝 دين عليك لـ{person}" if direction == "علي" else f"📝 {person} عنده لك"
        line += f" — {fmt(amount)} ريال"
        if installments:
            line += f" | أقساط {installments} شهر ({fmt(round(amount/int(installments),1))} ر/شهر)"
        else:
            line += " | دين عادي"
        line += f"\n   📅 {now.strftime('%Y-%m-%d')} | 🕐 {to12h(now.strftime('%H:%M'))}"
        reply_lines.append(line)

    elif action == "answer":  reply_lines.append(op.get("message",""))
    elif action == "report":  reply_lines.append("__REPORT__")
    elif action == "summary": reply_lines.append(f"__SUMMARY__{op.get('period','today')}")
    elif action == "compare": reply_lines.append(f"__COMPARE__{op.get('period1','this_week')}__{op.get('period2','last_week')}")
    elif action == "query":   reply_lines.append(f"__QUERY__{op.get('item','')}||{op.get('date','')}||{op.get('person','')}")
    elif action == "analyze": reply_lines.append(f"__ANALYZE__{op.get('question','')}")

# =========================================================
# ===== تسديد الدين =====
# =========================================================
async def handle_pay_debt(update, search_criteria, payment_amount):
    debts = search_debts(search_criteria)
    if not debts:
        await update.message.reply_text("🔍 ما وجدت الدين")
        return

    debt      = debts[0]
    row_id    = debt.get("id")
    new_paid, new_remaining, new_status = pay_debt_amount(row_id, payment_amount)

    if new_paid is None:
        await update.message.reply_text("❌ فشل التحديث")
        return

    msg  = f"✅ تم تسجيل الدفعة\n{'─'*20}\n"
    msg += f"👤 {debt.get('person')} — {debt.get('item')}\n"
    msg += f"💰 المدفوع الآن: {fmt(payment_amount)} ريال\n"
    msg += f"📊 إجمالي المدفوع: {fmt(new_paid)} ريال\n"
    if new_remaining > 0:
        msg += f"⏳ المتبقي: {fmt(new_remaining)} ريال"
    else:
        msg += "🎉 تم سداد الدين بالكامل!"
    await update.message.reply_text(msg)

# =========================================================
# ===== الحذف =====
# =========================================================
async def handle_delete_action(update, search_criteria):
    chat_id = update.message.chat_id
    rows    = search_transactions(search_criteria)
    if not rows:
        await update.message.reply_text("🔍 ما وجدت عمليات تطابق طلبك")
        return
    initial_selected = {str(rows[0].get("id"))} if len(rows) == 1 else set()
    pending_actions[chat_id] = {"type": "delete_select", "rows": rows,
                                "selected": initial_selected, "created_at": now_local()}
    await update.message.reply_text(
        build_delete_message(rows, initial_selected),
        reply_markup=build_delete_keyboard(rows, initial_selected)
    )

async def handle_delete_bulk(update, filter_criteria, exclude_list):
    chat_id   = update.message.chat_id
    rows      = search_transactions(filter_criteria)
    to_delete = [r for r in rows if not any(ex.lower() in str(r.get("item","")).lower() for ex in exclude_list)]
    excluded  = [r for r in rows if     any(ex.lower() in str(r.get("item","")).lower() for ex in exclude_list)]
    if not to_delete:
        await update.message.reply_text("🔍 ما وجدت عمليات للحذف")
        return
    initial_selected = {str(r.get("id")) for r in to_delete}
    pending_actions[chat_id] = {"type": "delete_select", "rows": to_delete,
                                "selected": initial_selected, "created_at": now_local()}
    msg = build_delete_message(to_delete, initial_selected)
    if excluded:
        msg += f"{'─'*20}\n✅ مستثنى:\n" + "".join(f"   • {r.get('item')} — {fmt(r.get('amount'))} ر\n" for r in excluded)
    await update.message.reply_text(msg, reply_markup=build_delete_keyboard(to_delete, initial_selected))

# =========================================================
# ===== التعديل =====
# =========================================================
async def handle_edit_action(update, search_criteria, sheet):
    chat_id = update.message.chat_id
    rows    = search_debts(search_criteria) if sheet == "debts" else search_transactions(search_criteria)
    if not rows:
        await update.message.reply_text("🔍 ما وجدت عمليات تطابق طلبك")
        return
    if len(rows) == 1:
        pending_actions[chat_id] = {"type": "edit_field", "sheet": sheet,
                                    "rows": rows, "selected_row": rows[0], "created_at": now_local()}
        await update.message.reply_text(build_edit_record_display(rows[0], sheet),
                                        reply_markup=build_edit_field_keyboard(sheet))
    else:
        pending_actions[chat_id] = {"type": "edit_select", "sheet": sheet,
                                    "rows": rows, "selected_row": None, "created_at": now_local()}
        await update.message.reply_text(f"🔍 وجدت {len(rows)} نتائج — اختر:",
                                        reply_markup=build_edit_select_keyboard(rows, sheet))

async def handle_edit_value_input(update, context):
    chat_id   = update.message.chat_id
    new_value = update.message.text.strip()
    pa        = pending_actions[chat_id]
    field     = pa["field"]
    labels    = get_field_labels(pa["sheet"])

    if field == "amount":
        converted = safe_float(new_value)
        if converted <= 0:
            await update.message.reply_text("⚠️ أرسل رقماً صحيحاً أكبر من صفر")
            return
        new_value = converted

    pa["new_value"] = new_value
    pa["type"]      = "edit_confirm"
    selected        = pa["selected_row"]
    record_name     = selected.get("item") or selected.get("person") or "—"
    msg  = f"✅ تأكيد التعديل\n{'─'*20}\n📌 {record_name}\n\n"
    msg += f"{labels.get(field, field)}:\n   قبل: {selected.get(field,'—')}\n   بعد: {new_value}\n"
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ تأكيد", callback_data="edit_confirm"),
        InlineKeyboardButton("❌ إلغاء", callback_data="edit_cancel"),
    ]]))

# =========================================================
# ===== لوحات العرض =====
# =========================================================
def build_delete_keyboard(rows, selected_ids):
    keyboard = []
    for i, r in enumerate(rows):
        row_id = str(r.get("id"))
        check  = "✅" if row_id in selected_ids else "⬜"
        label  = f"{check} {i+1}. {r.get('item')} — {fmt(r.get('amount'))} ر | {r.get('date')} {to12h(r.get('time',''))}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"del_toggle_{row_id}")])
    keyboard.append([
        InlineKeyboardButton("تحديد الكل ✅", callback_data="del_all"),
        InlineKeyboardButton("إلغاء ❌",       callback_data="del_cancel"),
    ])
    keyboard.append([InlineKeyboardButton("🗑️ تأكيد الحذف", callback_data="del_confirm")])
    return InlineKeyboardMarkup(keyboard)

def build_delete_message(rows, selected_ids):
    msg = "🔍 اختر ما تريد حذفه:\n\n"
    for i, r in enumerate(rows):
        check  = "✅" if str(r.get("id")) in selected_ids else "⬜"
        msg   += f"{check} {i+1}. {r.get('item')} — {fmt(r.get('amount'))} ريال\n"
        msg   += f"   📅 {r.get('date')} | 🕐 {to12h(r.get('time',''))}\n\n"
    return msg

def build_edit_select_keyboard(rows, sheet):
    keyboard = []
    for r in rows:
        label = (f"{r.get('item')} — {fmt(r.get('amount'))} ر | {r.get('date')} {to12h(r.get('time',''))}"
                 if sheet == "transactions" else
                 f"{r.get('person')} — {r.get('item')} — {fmt(r.get('amount'))} ر")
        keyboard.append([InlineKeyboardButton(label, callback_data=f"edit_select_{r.get('id')}")])
    keyboard.append([InlineKeyboardButton("❌ إلغاء", callback_data="edit_cancel")])
    return InlineKeyboardMarkup(keyboard)

def build_edit_record_display(row, sheet):
    if sheet == "transactions":
        type_ar = "مصروف" if row.get("type") == "expense" else "دخل"
        msg  = f"✏️ تعديل عملية\n{'─'*20}\n"
        msg += f"📌 {row.get('item')} — {fmt(row.get('amount'))} ريال\n"
        msg += f"📅 {row.get('date')} | 🕐 {to12h(row.get('time',''))}\n"
        msg += f"🏷️ {type_ar} | {row.get('category','')}\n"
    else:
        direction_ar = "عليك" if row.get("direction") == "علي" else "لك"
        remaining    = safe_float(row.get("remaining", row.get("amount", 0)))
        msg  = f"✏️ تعديل دين\n{'─'*20}\n"
        msg += f"👤 {row.get('person')} — {fmt(row.get('amount'))} ريال\n"
        msg += f"⏳ المتبقي: {fmt(remaining)} ريال\n"
        msg += f"📝 {row.get('item')}\n"
        msg += f"↕️ {direction_ar} | 📊 {row.get('status','')}\n"
    msg += "\nاختر الحقل المراد تعديله:"
    return msg

def build_edit_field_keyboard(sheet):
    if sheet == "transactions":
        keyboard = [
            [InlineKeyboardButton("✏️ الاسم",   callback_data="edit_field_item"),
             InlineKeyboardButton("💰 المبلغ",  callback_data="edit_field_amount")],
            [InlineKeyboardButton("📅 التاريخ", callback_data="edit_field_date"),
             InlineKeyboardButton("🕐 الوقت",   callback_data="edit_field_time")],
            [InlineKeyboardButton("🏷️ النوع",   callback_data="edit_field_type"),
             InlineKeyboardButton("📂 الفئة",   callback_data="edit_field_category")],
            [InlineKeyboardButton("❌ إلغاء",    callback_data="edit_cancel")],
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("👤 الشخص",     callback_data="edit_field_person"),
             InlineKeyboardButton("📝 الوصف",     callback_data="edit_field_item")],
            [InlineKeyboardButton("💰 المبلغ",    callback_data="edit_field_amount"),
             InlineKeyboardButton("↕️ الاتجاه",   callback_data="edit_field_direction")],
            [InlineKeyboardButton("📊 الحالة",    callback_data="edit_field_status"),
             InlineKeyboardButton("📋 الملاحظات", callback_data="edit_field_notes")],
            [InlineKeyboardButton("❌ إلغاء",      callback_data="edit_cancel")],
        ]
    return InlineKeyboardMarkup(keyboard)

# =========================================================
# ===== الكول باك =====
# =========================================================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data    = query.data

    if data == "undo":
        buf = undo_buffer.get(chat_id)
        if buf and now_local() < buf["expires"]:
            for row in buf["rows"]:
                transactions_ws.append_row([
                    row.get("id"), row.get("date"), row.get("time"),
                    row.get("item"), row.get("amount"), row.get("type"),
                    row.get("category"), row.get("person"), row.get("notes"), row.get("month")
                ])
            del undo_buffer[chat_id]
            await query.edit_message_text("↩️ تم التراجع ✅")
        else:
            await query.edit_message_text("⏰ انتهت مدة التراجع")
        return

    if data == "del_cancel":
        pending_actions.pop(chat_id, None)
        await query.edit_message_text("❌ تم الإلغاء")
        return

    if data.startswith("del_toggle_"):
        row_id = data.replace("del_toggle_", "")
        if chat_id in pending_actions:
            selected = pending_actions[chat_id]["selected"]
            if row_id in selected: selected.discard(row_id)
            else:                  selected.add(row_id)
            rows = pending_actions[chat_id]["rows"]
            await query.edit_message_text(build_delete_message(rows, selected),
                                          reply_markup=build_delete_keyboard(rows, selected))
        return

    if data == "del_all":
        if chat_id in pending_actions:
            rows     = pending_actions[chat_id]["rows"]
            selected = {str(r.get("id")) for r in rows}
            pending_actions[chat_id]["selected"] = selected
            await query.edit_message_text(build_delete_message(rows, selected),
                                          reply_markup=build_delete_keyboard(rows, selected))
        return

    if data == "del_confirm":
        if chat_id not in pending_actions:
            await query.edit_message_text("❌ انتهت الجلسة")
            return
        selected     = pending_actions[chat_id].get("selected", set())
        rows         = pending_actions[chat_id].get("rows", [])
        if not selected:
            await query.answer("لم تحدد أي عملية", show_alert=True)
            return
        deleted_rows = [r for r in rows if str(r.get("id")) in selected and delete_transaction_by_id(r.get("id"))]
        undo_buffer[chat_id] = {"rows": deleted_rows, "expires": now_local() + timedelta(minutes=5)}
        pending_actions.pop(chat_id, None)
        msg = f"🗑️ تم حذف {len(deleted_rows)} عملية:\n\n"
        for r in deleted_rows:
            msg += f"• {r.get('item')} — {fmt(r.get('amount'))} ريال\n"
            msg += f"  📅 {r.get('date')} | 🕐 {to12h(r.get('time',''))}\n\n"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("↩️ تراجع (5 دقائق)", callback_data="undo")
        ]]))
        return

    if data.startswith("edit_select_"):
        row_id = data.replace("edit_select_", "")
        if chat_id in pending_actions:
            rows  = pending_actions[chat_id]["rows"]
            sheet = pending_actions[chat_id]["sheet"]
            sel   = next((r for r in rows if str(r.get("id")) == row_id), None)
            if sel:
                pending_actions[chat_id].update({"selected_row": sel, "type": "edit_field"})
                await query.edit_message_text(build_edit_record_display(sel, sheet),
                                              reply_markup=build_edit_field_keyboard(sheet))
        return

    if data.startswith("edit_field_"):
        field = data.replace("edit_field_", "")
        if chat_id in pending_actions:
            pending_actions[chat_id].update({"field": field, "type": "edit_await_value"})
            selected = pending_actions[chat_id]["selected_row"]
            sheet    = pending_actions[chat_id]["sheet"]
            labels   = get_field_labels(sheet)
            name     = selected.get("item") or selected.get("person") or "—"
            msg  = f"✏️ تعديل {labels.get(field, field)}\n{'─'*20}\n"
            msg += f"📌 {name}\n\nالقيمة الحالية: {selected.get(field,'—')}"
            await query.edit_message_text(msg)
            await query.message.reply_text("⌨️ أرسل القيمة الجديدة:", reply_markup=ForceReply(selective=True))
        return

    if data == "edit_confirm":
        if chat_id not in pending_actions:
            await query.edit_message_text("❌ انتهت الجلسة")
            return
        pa      = pending_actions[chat_id]
        sheet   = pa["sheet"]
        row_id  = pa["selected_row"]["id"]
        field   = pa["field"]
        new_val = pa["new_value"]
        labels  = get_field_labels(sheet)
        success = update_transaction_field(row_id, field, new_val) if sheet == "transactions" \
                  else update_debt_field(row_id, field, new_val)
        pending_actions.pop(chat_id, None)
        if success: await query.edit_message_text(f"✅ تم التعديل\n{'─'*20}\n{labels.get(field,field)}: {new_val}")
        else:       await query.edit_message_text("❌ فشل التعديل")
        return

    if data == "edit_cancel":
        pending_actions.pop(chat_id, None)
        await query.edit_message_text("❌ تم الإلغاء")
        return

# =========================================================
# ===== الأوامر =====
# =========================================================
async def fix_sheet(update, context):
    await update.message.reply_text("🔧 جاري فحص الشيت...")
    fixed = 0
    try:
        # إصلاح transactions
        for ws, col in [(transactions_ws, 5)]:
            all_data = ws.get_all_values()
            for i, row in enumerate(all_data):
                if i == 0: continue
                raw     = row[col-1] if len(row) >= col else ""
                cleaned = safe_float(raw)
                if raw != "" and str(raw) != str(cleaned):
                    ws.update_cell(i+1, col, cleaned)
                    fixed += 1

        # إصلاح debts — amount + paid + remaining
        all_data = debts_ws.get_all_values()
        for i, row in enumerate(all_data):
            if i == 0: continue
            # amount
            raw = row[4] if len(row) > 4 else ""
            cleaned = safe_float(raw)
            if raw != "" and str(raw) != str(cleaned):
                debts_ws.update_cell(i+1, 5, cleaned)
                fixed += 1
            # paid — لو فارغة اجعلها 0
            if len(row) <= 5 or row[5] == "":
                debts_ws.update_cell(i+1, 6, 0)
                fixed += 1
            # remaining — لو فارغة اجعلها = amount
            if len(row) <= 6 or row[6] == "":
                debts_ws.update_cell(i+1, 7, safe_float(row[4] if len(row) > 4 else 0))
                fixed += 1

        msg = f"✅ تم إصلاح {fixed} خلية" if fixed > 0 else "✅ الشيت نظيف"
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ خطأ: {e}")

async def send_backup(update, context):
    await update.message.reply_text("📦 جاري تجهيز النسخة الاحتياطية...")
    try:
        transactions = get_transactions()
        if not transactions:
            await update.message.reply_text("📭 ما في بيانات للنسخ")
            return
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=transactions[0].keys())
        writer.writeheader()
        writer.writerows(transactions)
        csv_bytes = output.getvalue().encode("utf-8-sig")
        bio       = io.BytesIO(csv_bytes)
        bio.name  = f"almo_backup_{now_local().strftime('%Y-%m-%d')}.csv"
        await update.message.reply_document(bio, caption=f"📊 نسخة احتياطية — {now_local().strftime('%Y-%m-%d')}")
    except Exception as e:
        await update.message.reply_text(f"❌ فشل الـ backup: {e}")

async def scheduled_backup(context):
    for chat_id in owner_chat_ids:
        try:
            transactions = get_transactions()
            if not transactions: continue
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=transactions[0].keys())
            writer.writeheader()
            writer.writerows(transactions)
            csv_bytes = output.getvalue().encode("utf-8-sig")
            bio       = io.BytesIO(csv_bytes)
            bio.name  = f"almo_backup_{now_local().strftime('%Y-%m-%d')}.csv"
            await context.bot.send_document(chat_id, bio, caption="📊 نسخة احتياطية أسبوعية تلقائية ✅")
        except Exception as e:
            logging.error(f"Scheduled backup failed for {chat_id}: {e}")

# =========================================================
# ===== الرسائل الرئيسية =====
# =========================================================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text
    chat_id      = update.message.chat_id

    # ── تسجيل المستخدم للـ backup ──
    if chat_id not in owner_chat_ids:
        owner_chat_ids.add(chat_id)
        if context.job_queue:
            context.job_queue.run_repeating(
                scheduled_backup, interval=BACKUP_INTERVAL,
                first=BACKUP_INTERVAL, name=f"backup_{chat_id}"
            )

    # ── تنظيف الجلسات المنتهية ──
    cleanup_expired_sessions()

    # ── Rate Limiting ──
    if not check_rate_limit(chat_id):
        await update.message.reply_text("⏳ أرسلت رسائل كثيرة، انتظر دقيقة")
        return

    # ── انتظار قيمة التعديل ──
    if chat_id in pending_actions and pending_actions[chat_id].get("type") == "edit_await_value":
        await handle_edit_value_input(update, context)
        return

    try:
        data         = calc_balance()
        gpt_response = ask_gpt(user_message, data, chat_id)
        gpt_response = gpt_response.replace("```json","").replace("```","").strip()
        parsed       = json.loads(gpt_response)
        action       = parsed.get("action","")

        # ── حماية من التسجيل رغم النفي — طبقة مستقلة ──
        if has_negation(user_message) and action in ["register","mixed"]:
            await update.message.reply_text("✅ حاضر، لم أسجّل شيئاً")
            update_history(chat_id, user_message, "لم يتم التسجيل")
            return

        # ── تصحيح تلقائي — "اتسجل غلط" ──
        if is_mistake_correction(user_message) and action not in ["delete","delete_bulk"]:
            parsed = {"action":"delete","search":{"last":True}}
            action = "delete"

        if action == "report":
            await update.message.reply_text(build_report(calc_balance()))
            update_history(chat_id, user_message, "تقرير")
            return

        if action == "summary":
            await update.message.reply_text(build_summary(parsed.get("period","today")))
            update_history(chat_id, user_message, "ملخص")
            return

        if action == "compare":
            await update.message.reply_text(build_comparison(parsed.get("period1","this_week"), parsed.get("period2","last_week")))
            update_history(chat_id, user_message, "مقارنة")
            return

        if action == "query":
            await update.message.reply_text(build_query(parsed.get("item",""), parsed.get("date",""), parsed.get("person","")))
            update_history(chat_id, user_message, "بحث")
            return

        if action == "analyze":
            await update.message.reply_text("🧠 جاري التحليل...")
            reply = ask_gpt_analyze(parsed.get("question", user_message), chat_id)
            await update.message.reply_text(reply)
            update_history(chat_id, user_message, reply)
            return

        if action == "pay_debt":
            await handle_pay_debt(update, parsed.get("search",{}), safe_float(parsed.get("amount",0)))
            update_history(chat_id, user_message, "تسديد دين")
            return

        if action == "delete":
            await handle_delete_action(update, parsed.get("search",{}))
            update_history(chat_id, user_message, gpt_response)
            return

        if action == "delete_bulk":
            await handle_delete_bulk(update, parsed.get("filter",{}), parsed.get("exclude",[]))
            update_history(chat_id, user_message, gpt_response)
            return

        if action == "edit":
            await handle_edit_action(update, parsed.get("search",{}), parsed.get("sheet","transactions"))
            update_history(chat_id, user_message, gpt_response)
            return

        operations  = parsed.get("operations",[]) if action == "mixed" else [parsed]
        delete_ops  = [op for op in operations if op.get("action") in ["delete","delete_bulk"]]
        other_ops   = [op for op in operations if op.get("action") not in ["delete","delete_bulk"]]

        reply_lines = []
        for op in other_ops:
            await process_operation(op, reply_lines, data, chat_id)

        final_reply = ""
        if reply_lines:
            data = calc_balance()

            # معالجة tags الخاصة
            for line in list(reply_lines):
                if line.startswith("__SUMMARY__"):
                    reply_lines.remove(line)
                    await update.message.reply_text(build_summary(line.replace("__SUMMARY__","")))
                elif line.startswith("__COMPARE__"):
                    reply_lines.remove(line)
                    parts = line.replace("__COMPARE__","").split("__")
                    await update.message.reply_text(build_comparison(parts[0], parts[1] if len(parts)>1 else "last_week"))
                elif line.startswith("__QUERY__"):
                    reply_lines.remove(line)
                    parts = line.replace("__QUERY__","").split("||")
                    await update.message.reply_text(build_query(parts[0] if len(parts)>0 else "", parts[1] if len(parts)>1 else "", parts[2] if len(parts)>2 else ""))
                elif line.startswith("__ANALYZE__"):
                    reply_lines.remove(line)
                    q = line.replace("__ANALYZE__","")
                    await update.message.reply_text("🧠 جاري التحليل...")
                    await update.message.reply_text(ask_gpt_analyze(q, chat_id))

            if "__REPORT__" in reply_lines:
                reply_lines.remove("__REPORT__")
                if reply_lines: await update.message.reply_text("\n".join(reply_lines))
                await update.message.reply_text(build_report(data))
                final_reply = "تقرير"
            elif reply_lines:
                reply       = "\n".join(reply_lines)
                has_expense = any(
                    op.get("action") == "register" and
                    any(i.get("type") == "expense" for i in op.get("items",[]))
                    for op in other_ops
                )
                if has_expense:
                    reply += f"\n\n💳 باقي للصرف: {fmt(data['spending_available'])} ريال"
                    reply += build_motivation(data)
                await update.message.reply_text(reply)
                final_reply = reply

        update_history(chat_id, user_message, final_reply or gpt_response)

        for op in delete_ops:
            if op.get("action") == "delete":
                await handle_delete_action(update, op.get("search",{}))
            elif op.get("action") == "delete_bulk":
                await handle_delete_bulk(update, op.get("filter",{}), op.get("exclude",[]))

    except json.JSONDecodeError:
        await update.message.reply_text(gpt_response)
        update_history(chat_id, user_message, gpt_response)
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text("⚠️ حدث خطأ مؤقت، أرسل رسالتك مرة ثانية")

# =========================================================
# ===== تشغيل البوت =====
# =========================================================
logging.basicConfig(level=logging.INFO)
app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("fix",    fix_sheet))
app.add_handler(CommandHandler("backup", send_backup))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
app.add_handler(CallbackQueryHandler(handle_callback))
print("✅ ألمو فاينانس v3.9 شغال!")
app.run_polling()
