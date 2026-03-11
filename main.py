import telebot
from telebot import types, apihelper
from flask import Flask
from threading import Thread, Lock
import psycopg2
from psycopg2 import pool as psycopg2_pool
from datetime import datetime, timedelta
import time
import os
import json
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# ENV & CONFIG
# ============================================================

TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set!")

DATABASE_URL = os.environ.get("DATABASE_URL") or ""

def _parse_admin_ids():
    env_val = os.environ.get("ADMIN_IDS", "")
    if env_val:
        try:
            return set(int(x.strip()) for x in env_val.split(",") if x.strip())
        except Exception:
            pass
    return {5880534778, 5541976681}

ADMIN_IDS = _parse_admin_ids()

def is_admin(user_id):
    return user_id in ADMIN_IDS

apihelper.ENABLE_MIDDLEWARE = True
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# ============================================================
# CONNECTION POOL
# ============================================================

_db_pool = None
_db_pool_lock = Lock()

def get_pool():
    global _db_pool
    if _db_pool is None:
        with _db_pool_lock:
            if _db_pool is None:
                if not DATABASE_URL:
                    raise RuntimeError("DATABASE_URL орнатылмаған!")
                _db_pool = psycopg2_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=DATABASE_URL,
                    connect_timeout=10
                )
    return _db_pool

def get_db():
    p = get_pool()
    conn = p.getconn()
    cursor = conn.cursor()
    return conn, cursor

def release_db(conn):
    try:
        get_pool().putconn(conn)
    except Exception as e:
        logger.warning(f"release_db қате: {e}")

# ============================================================
# DB INIT
# ============================================================

def init_db():
    tables = [
        """CREATE TABLE IF NOT EXISTS students (
            id BIGINT PRIMARY KEY, username TEXT, last_active TIMESTAMP,
            full_name TEXT, birth_date TEXT, phone TEXT, hemis TEXT, started INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS user_news (
            id SERIAL PRIMARY KEY, content TEXT, author_id BIGINT,
            author_username TEXT, date TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS materials (
            id SERIAL PRIMARY KEY, file_id TEXT, file_type TEXT,
            uploader_id BIGINT, uploader_username TEXT, date TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS gallery (
            id SERIAL PRIMARY KEY, file_id TEXT, file_type TEXT,
            uploader_id BIGINT, uploader_username TEXT, date TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS schedule (
            id SERIAL PRIMARY KEY, day TEXT, subject TEXT, time TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS suggestions (
            id SERIAL PRIMARY KEY, content TEXT, user_id BIGINT,
            date TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS attendance (
            id SERIAL PRIMARY KEY, date TEXT, para INTEGER, subject TEXT,
            student_id BIGINT, student_name TEXT, status TEXT,
            marked_at TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY, type TEXT, name TEXT, phone TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS contracts (
            id SERIAL PRIMARY KEY, student_id BIGINT UNIQUE,
            total_amount REAL, note TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS contract_payments (
            id SERIAL PRIMARY KEY, student_id BIGINT,
            amount REAL, date TEXT, note TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS test_variants (
            id SERIAL PRIMARY KEY, subject TEXT, file_id TEXT,
            file_type TEXT, file_name TEXT, uploader_id BIGINT,
            date TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS user_states (
            user_id BIGINT PRIMARY KEY, state TEXT, updated_at TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS attendance_sessions (
            admin_id BIGINT PRIMARY KEY, session_data TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS blocked_users (
            user_id BIGINT PRIMARY KEY, reason TEXT,
            blocked_at TIMESTAMP DEFAULT NOW()
        )""",
    ]
    migrations = [
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS full_name TEXT",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS birth_date TEXT",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS phone TEXT",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS hemis TEXT",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS started INTEGER DEFAULT 0",
    ]
    conn, cursor = get_db()
    try:
        for sql in tables:
            cursor.execute(sql)
        for sql in migrations:
            try:
                cursor.execute(sql)
            except Exception:
                conn.rollback()
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

init_db()

UZ_OFFSET = timedelta(hours=5)

def now_uz():
    return datetime.utcnow() + UZ_OFFSET

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive", 200

@app.route("/health")
def health():
    return {"status": "ok", "time": str(now_uz())}, 200

@app.route("/ping")
def ping():
    return "pong", 200

# ============================================================
# RATE LIMIT
# ============================================================

_rate_limit = {}
_rate_limit_lock = Lock()
RATE_LIMIT_MAX = 50
RATE_LIMIT_WINDOW = 30

def is_rate_limited(user_id):
    if is_admin(user_id):
        return False
    now = time.time()
    with _rate_limit_lock:
        history = [t for t in _rate_limit.get(user_id, []) if now - t < RATE_LIMIT_WINDOW]
        history.append(now)
        _rate_limit[user_id] = history
        return len(history) > RATE_LIMIT_MAX

def clean_rate_limit():
    now = time.time()
    with _rate_limit_lock:
        for uid in list(_rate_limit):
            if all(now - t > RATE_LIMIT_WINDOW * 2 for t in _rate_limit[uid]):
                del _rate_limit[uid]

# ============================================================
# SECURITY
# ============================================================

def is_blocked(user_id):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT user_id FROM blocked_users WHERE user_id=%s", (user_id,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        release_db(conn)

def is_authorized(user_id):
    if is_admin(user_id):
        return True
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id FROM students WHERE id=%s", (user_id,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        release_db(conn)

@bot.middleware_handler(update_types=['message'])
def security_middleware(bot_instance, message):
    user_id = message.from_user.id

    # 1) Блокталғандарды тоқтату
    if is_blocked(user_id):
        try:
            bot.send_message(user_id, "⛔ Сиз блокландыңыз. Admin-ге хабарласыңыз.")
        except Exception:
            pass
        raise Exception("blocked")

    # 2) /start — тіркелу үшін рұхсат тексерілмейді
    if message.text == "/start":
        return

    # 3) Тіркелмеген пайдаланушыны тоқтату
    # (next_step_handler-да authorized пайдаланушылар өтеді, тек тіркелмегендер тоқтатылады)
    if not is_admin(user_id) and not is_authorized(user_id):
        try:
            bot.send_message(user_id, "⛔ <b>Кируге рұхсат жоқ!</b>\nАдминге хабарласыңыз.")
        except Exception:
            pass
        raise Exception("unauthorized")

    # 4) Rate limit
    if is_rate_limited(user_id):
        try:
            bot.send_message(user_id, "⏳ Дым тез! Бираздан кейин қайталаңыз.")
        except Exception:
            pass
        raise Exception("rate_limited")

    # 5) last_active жаңарту
    try:
        conn, cursor = get_db()
        try:
            cursor.execute("UPDATE students SET last_active=%s WHERE id=%s", (now_uz(), user_id))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
    except Exception:
        pass

# ============================================================
# UTILS
# ============================================================

DAYS_RU = ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"]
DAYS_EN_TO_RU = {
    "Monday":"Понедельник","Tuesday":"Вторник","Wednesday":"Среда",
    "Thursday":"Четверг","Friday":"Пятница","Saturday":"Суббота","Sunday":"Воскресенье"
}
MONTHS_RU = {
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
    7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}
WEEKDAYS_RU = {0:"Понедельник",1:"Вторник",2:"Среда",3:"Четверг",4:"Пятница",5:"Суббота",6:"Воскресенье"}

def set_user_state(user_id, state):
    conn, cursor = get_db()
    try:
        cursor.execute(
            "INSERT INTO user_states (user_id,state,updated_at) VALUES(%s,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at",
            (user_id, state, now_uz())
        )
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

def get_user_state(user_id):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT state FROM user_states WHERE user_id=%s", (user_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    return row[0] if row else None

def clear_user_state(user_id):
    conn, cursor = get_db()
    try:
        cursor.execute("DELETE FROM user_states WHERE user_id=%s", (user_id,))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

def save_attendance_session(admin_id, session):
    data = json.dumps(session, ensure_ascii=False)
    conn, cursor = get_db()
    try:
        cursor.execute(
            "INSERT INTO attendance_sessions(admin_id,session_data,updated_at) VALUES(%s,%s,%s) "
            "ON CONFLICT(admin_id) DO UPDATE SET session_data=excluded.session_data,updated_at=excluded.updated_at",
            (admin_id, data, now_uz())
        )
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

def load_attendance_session(admin_id):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT session_data FROM attendance_sessions WHERE admin_id=%s", (admin_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    if not row:
        return None
    session = json.loads(row[0])
    if "results" in session:
        session["results"] = {int(k): v for k, v in session["results"].items()}
    return session

def delete_attendance_session(admin_id):
    conn, cursor = get_db()
    try:
        cursor.execute("DELETE FROM attendance_sessions WHERE admin_id=%s", (admin_id,))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

def cleanup_old_sessions():
    conn, cursor = get_db()
    try:
        cursor.execute("DELETE FROM attendance_sessions WHERE updated_at < %s", (now_uz()-timedelta(hours=2),))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)

def date_to_ru(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{WEEKDAYS_RU[dt.weekday()]}, {dt.day} {MONTHS_RU[dt.month]}"
    except Exception:
        return date_str

def get_online_status(last_active_val):
    # FIX #3: datetime instance қабылдайды, now_uz() пайдаланылады
    try:
        if isinstance(last_active_val, datetime):
            last = last_active_val
        else:
            last = datetime.strptime(str(last_active_val)[:19], "%Y-%m-%d %H:%M:%S")
        diff = max((now_uz() - last).total_seconds(), 0)
        if diff < 300:     return "🟢 Онлайн"
        elif diff < 3600:  return f"🟡 {int(diff//60)} мин бұрын"
        elif diff < 86400: return f"🔴 {int(diff//3600)} сағ бұрын"
        else:              return f"🔴 {int(diff//86400)} күн бұрын"
    except Exception:
        return "⚪ Белгисиз"

def _is_online(last_active_val, now_t):
    try:
        if isinstance(last_active_val, datetime):
            last = last_active_val
        else:
            last = datetime.strptime(str(last_active_val)[:19], "%Y-%m-%d %H:%M:%S")
        return (now_t - last).total_seconds() < 300
    except Exception:
        return False

def send_long_message(chat_id, text, reply_markup=None, chunk_size=3800):
    if len(text) <= chunk_size:
        bot.send_message(chat_id, text, reply_markup=reply_markup)
        return
    parts = []
    while len(text) > chunk_size:
        parts.append(text[:chunk_size])
        text = text[chunk_size:]
    if text:
        parts.append(text)
    for i, part in enumerate(parts):
        bot.send_message(chat_id, part, reply_markup=reply_markup if i == len(parts)-1 else None)

def send_to_students(text=None, file_id=None, file_type=None, exclude_id=None):
    conn, cursor = get_db()
    try:
        if exclude_id:
            cursor.execute("SELECT id FROM students WHERE started=1 AND id!=%s", (exclude_id,))
        else:
            cursor.execute("SELECT id FROM students WHERE started=1")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)

    def _do_send():
        for row in rows:
            sid = row[0]
            try:
                if file_id and file_type == "photo":
                    bot.send_photo(sid, file_id, caption=text)
                elif file_id and file_type == "document":
                    bot.send_document(sid, file_id, caption=text)
                elif file_id and file_type == "video":
                    bot.send_video(sid, file_id, caption=text)
                elif text:
                    bot.send_message(sid, text)
                time.sleep(0.05)
            except Exception as e:
                logger.warning(f"send_to_students қате (id={sid}): {e}")
                continue

    Thread(target=_do_send, daemon=True).start()

# FIX #4: started=0 болса да барлық студентке жіберу (туылған күн үшін)
def send_to_all_students(text=None, exclude_id=None):
    conn, cursor = get_db()
    try:
        if exclude_id:
            cursor.execute("SELECT id FROM students WHERE id!=%s", (exclude_id,))
        else:
            cursor.execute("SELECT id FROM students")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    def _do():
        for row in rows:
            sid = row[0]
            try:
                if text: bot.send_message(sid, text)
                time.sleep(0.05)
            except Exception as e:
                logger.warning(f"send_to_all_students қате (id={sid}): {e}")
    Thread(target=_do, daemon=True).start()

_processed_messages = set()
_processed_lock = Lock()

def is_already_processed(message_id):
    with _processed_lock:
        if message_id in _processed_messages:
            return True
        _processed_messages.add(message_id)
        if len(_processed_messages) > 500:
            _processed_messages.clear()
        return False

_last_saved = {}
_last_saved_lock = Lock()

def send_saved_once(chat_id, user_id):
    now = time.time()
    with _last_saved_lock:
        if now - _last_saved.get(user_id, 0) > 30:
            _last_saved[user_id] = now
            should_send = True
        else:
            should_send = False
    if should_send:
        bot.send_message(chat_id, "✅ <b>Сақланды!</b>")

def get_birthday_info(birth_date_str):
    try:
        if not birth_date_str:
            return None, None
        bd = datetime.strptime(str(birth_date_str)[:10], "%Y-%m-%d")
        today = now_uz().date()
        try:
            this_year_bd = bd.replace(year=today.year).date()
        except ValueError:
            this_year_bd = bd.replace(year=today.year, day=28).date()
        if this_year_bd < today:
            try:
                this_year_bd = bd.replace(year=today.year + 1).date()
            except ValueError:
                this_year_bd = bd.replace(year=today.year + 1, day=28).date()
        days_left = (this_year_bd - today).days
        return days_left, this_year_bd
    except Exception:
        return None, None

def generate_attendance_excel(students, results, date_str, para, subject):
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Барлау"
        hfont = Font(bold=True, color="FFFFFF")
        hfill = PatternFill("solid", fgColor="2E75B6")
        ctr = Alignment(horizontal="center", vertical="center")
        for col, h in enumerate(["№","ФИО","Күн","Пара","Пән","Барлау"], 1):
            c = ws.cell(row=1, column=col, value=h)
            c.font = hfont
            c.fill = hfill
            c.alignment = ctr
        ws.column_dimensions["A"].width = 5
        ws.column_dimensions["B"].width = 35
        ws.column_dimensions["C"].width = 12
        ws.column_dimensions["D"].width = 8
        ws.column_dimensions["E"].width = 20
        ws.column_dimensions["F"].width = 10
        green_fill = PatternFill("solid", fgColor="C6EFCE")
        red_fill = PatternFill("solid", fgColor="FFC7CE")
        for i, item in enumerate(students, 1):
            if isinstance(item, (list, tuple)) and len(item) == 2 and not isinstance(item[0], str):
                student_id, student_name = item[0], item[1]
                status = results.get(student_id, "absent") if results else "absent"
            else:
                student_name, status = item[0], item[1]
            status_text = "✅ Бар" if status == "present" else "❌ Жоқ"
            ws.append([i, student_name, date_str, para, subject, status_text])
            fill = green_fill if status == "present" else red_fill
            for col in range(1, 7):
                ws.cell(row=i+1, column=col).fill = fill
        path = f"/tmp/attendance_{date_str}_para{para}.xlsx"
        wb.save(path)
        return path
    except ImportError:
        return None
    except Exception as e:
        logger.error(f"Excel генерация қате: {e}")
        return None

# ============================================================
# МЕНЮ ФУНКЦИЯЛАРЫ
# ============================================================

def main_menu(user_id=None):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📰 Жаңалықлар", "📚 Сабақ материаллары")
    markup.row("📷 Фото/Видео", "📅 Сабақ кестеси")
    markup.row("💡 Ұсыныс / Шағым", "📋 Список")
    markup.row("📞 Байланыс", "💰 Контракт")
    markup.row("📖 Пәнлер", "📊 Сабақ/Ертеңге")
    markup.row("🤖 AI Көмекши")
    if user_id and is_admin(user_id):
        markup.row("👮 Админ панель")
    return markup

def back_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.add("⬅️ Артқа")
    return m

def admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("👥 Студентлер", "👤 Студент басқарыу")
    markup.row("📊 Excel басқарыу", "📊 Барлау басқарыу")
    markup.row("📅 Сабақ басқарыу", "❗ Сабақ болмайды")
    markup.row("📈 Статистика", "📩 Ус/Ша келген")
    markup.row("🗑 Өшириу", "📞 Байланыс басқарыу")
    markup.row("💰 Контракт басқарыу", "📖 Пән басқарыу")
    markup.row("🔒 Блок басқарыу")
    markup.row("⬅️ Артқа")
    return markup

def contacts_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("➕ Деканат қосыу", "➕ Муғаллим қосыу")
    m.row("❌ Байланыс өшириу")
    m.row("⬅️ Админге қайтыу")
    return m

def contract_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("💰 Контракт киргизиу", "➕ Төлем қосыу")
    m.row("📋 Барлық контрактлар")
    m.row("⬅️ Админге қайтыу")
    return m

def delete_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("🗑 Материал өшириу", "🗑 Фото/Видео өшириу")
    m.row("🗑 Жаңалық өшириу")
    m.row("⬅️ Админге қайтыу")
    return m

def student_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("➕ Студент қосыу/өзгертиу")
    m.row("❌ Студент өшириу")
    m.row("⬅️ Админге қайтыу")
    return m

def excel_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("📥 Excel жүклеу", "📤 Excel импорт")
    m.row("⬅️ Админге қайтыу")
    return m

def attendance_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("📊 Барлау", "📅 Барлау тарихы")
    m.row("⬅️ Админге қайтыу")
    return m

def schedule_admin_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("➕ Сабақ қосыу", "❌ Сабақ өшириу")
    m.row("⬅️ Админге қайтыу")
    return m

def news_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("✍️ Жазыңыз", "🗂 Архив Жаңалықлар")
    m.row("⬅️ Артқа")
    return m

def materials_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("📥 Мат жүклеңиз", "🗂 Архив материаллар")
    m.row("⬅️ Артқа")
    return m

GALLERY_UPLOAD_BTN = "📤 Жүклеңиз"

def gallery_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row(GALLERY_UPLOAD_BTN, "🎞 S6-DI естелиги")
    m.row("⬅️ Артқа")
    return m

def schedule_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("Понедельник","Вторник","Среда")
    m.row("Четверг","Пятница","Суббота")
    m.row("Воскресенье")
    m.row("⬅️ Артқа")
    return m

def panler_admin_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("➕ Пән қосыу")
    m.row("🗑 Пән өшириу")
    m.row("⬅️ Админге қайтыу")
    return m

def block_submenu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("🚫 Студентти блоклау")
    m.row("✅ Блоктан шығарыу")
    m.row("📋 Блокланғанлар дизими")
    m.row("⬅️ Админге қайтыу")
    return m

def sabak_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("✅ Бараман", "❌ Себеп бар")
    m.row("⬅️ Артқа")
    return m

def _sebep_file_menu():
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.row("⏭ Өткизип жибериу")
    m.row("⬅️ Артқа")
    return m

# ============================================================
# /start
# ============================================================

@bot.message_handler(commands=["start"])
def start(message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    if is_blocked(user_id):
        bot.send_message(user_id, "⛔ Сиз блокландыңыз.")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id FROM students WHERE id=%s", (user_id,))
        existing = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    if not existing and not is_admin(user_id):
        for aid in ADMIN_IDS:
            try:
                fn = message.from_user.first_name or ""
                ln = message.from_user.last_name or ""
                bot.send_message(aid, f"⚠️ <b>Рұхсатсыз кириу!</b>\n👤 {fn} {ln}\n🔗 @{username}\n🆔 <code>{user_id}</code>")
            except Exception:
                pass
        bot.send_message(user_id, "⛔ <b>Кириуге рұхсат жоқ!</b>\nБұл бот тек S6-DI адамлары үшын.\nАдминге хабарласыңыз.")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("UPDATE students SET username=%s,last_active=%s,started=1 WHERE id=%s", (username, now_uz(), user_id))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    clear_user_state(user_id)
    bot.send_message(user_id, "👋 <b>Хош келдиңиз!</b>\nS6-DI-23 группасы сизлерди коргенимнен қууанышлыман.\nБөлимди таңлаңыз:", reply_markup=main_menu(user_id))

# ============================================================
# БЛОК БАСҚАРЫУ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "🔒 Блок басқарыу")
def block_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "🔒 <b>Блок басқарыу</b>", reply_markup=block_submenu())

@bot.message_handler(func=lambda m: m.text == "🚫 Студентти блоклау")
def block_user_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    msg = bot.send_message(message.chat.id, "🚫 ID жазыңыз:\n(ямаса <code>ID;себеп</code>)", reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_block_user)

def handle_block_user(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🔒 Блок басқарыу", reply_markup=block_submenu())
        return
    try:
        parts = [p.strip() for p in message.text.split(";")]
        uid = int(parts[0])
        reason = parts[1] if len(parts) > 1 else "Себеп көрсетилмеген"
        if uid in ADMIN_IDS:
            bot.send_message(message.chat.id, "❌ Admin-ді блоклауға болмайды!", reply_markup=block_submenu())
            return
        conn, cursor = get_db()
        try:
            cursor.execute(
                "INSERT INTO blocked_users(user_id,reason) VALUES(%s,%s) "
                "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason",
                (uid, reason)
            )
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <code>{uid}</code> блокланды!\nСебеп: {reason}", reply_markup=block_submenu())
        try:
            bot.send_message(uid, "⛔ Сиз блокландыңыз. Admin-ге хабарласыңыз.")
        except Exception:
            pass
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ ID жазыңыз:", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_block_user)

@bot.message_handler(func=lambda m: m.text == "✅ Блоктан шығарыу")
def unblock_user_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT user_id,reason FROM blocked_users")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Блокланған жоқ.", reply_markup=block_submenu())
        return
    text = "✅ <b>Блоктан шығарыу — ID жазыңыз:</b>\n\n"
    for row in rows:
        text += f"🆔 <code>{row[0]}</code> — {row[1]}\n"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_unblock_user)

def handle_unblock_user(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🔒 Блок басқарыу", reply_markup=block_submenu())
        return
    try:
        uid = int(message.text.strip())
        conn, cursor = get_db()
        try:
            cursor.execute("DELETE FROM blocked_users WHERE user_id=%s", (uid,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <code>{uid}</code> блоктан шығарылды!", reply_markup=block_submenu())
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ ID жазыңыз:", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_unblock_user)

@bot.message_handler(func=lambda m: m.text == "📋 Блокланғанлар дизими")
def show_blocked_list(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT user_id,reason,blocked_at FROM blocked_users ORDER BY blocked_at DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Блокланған жоқ.", reply_markup=block_submenu())
        return
    text = f"🔒 <b>Блокланғанлар ({len(rows)}):</b>\n\n"
    for row in rows:
        text += f"🆔 <code>{row[0]}</code>\n📝 {row[1]}\n📅 {row[2]}\n{'─'*20}\n"
    bot.send_message(message.chat.id, text, reply_markup=block_submenu())

# ============================================================
# НАВИГАЦИЯ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "⬅️ Артқа")
def go_back(message):
    user_id = message.from_user.id
    mode = get_user_state(user_id)
    clear_user_state(user_id)
    if mode == "materials":
        bot.send_message(message.chat.id, "📚 Сабақ материаллары", reply_markup=materials_menu())
    elif mode == "gallery":
        bot.send_message(message.chat.id, "📷 Фото/Видео", reply_markup=gallery_menu())
    elif mode == "ai_chat":
        bot.send_message(message.chat.id, "🏠 Бас меню", reply_markup=main_menu(user_id))
    elif mode and mode.startswith("variant:"):
        bot.send_message(message.chat.id, "📖 Пән басқарыу", reply_markup=panler_admin_submenu())
    elif mode == "sebep_text":
        bot.send_message(message.chat.id, "📊 Сабақ/Ертеңге", reply_markup=sabak_menu())
    elif mode and mode.startswith("sebep_file:"):
        set_user_state(user_id, "sebep_text")
        bot.send_message(message.chat.id, "❌ <b>Себебиңизди қайта жазыңыз:</b>", reply_markup=back_menu())
    else:
        bot.send_message(message.chat.id, "🏠 Бас меню", reply_markup=main_menu(user_id))

@bot.message_handler(func=lambda m: m.text == "⬅️ Админге қайтыу")
def go_back_admin(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫", reply_markup=main_menu(message.from_user.id))
        return
    clear_user_state(message.from_user.id)
    bot.send_message(message.chat.id, "👮 <b>Админ панель</b>", reply_markup=admin_menu())

# ============================================================
# СПИСОК
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📋 Список")
def show_student_list(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT full_name,birth_date,phone,hemis FROM students ORDER BY full_name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Список бос.", reply_markup=main_menu(message.from_user.id))
        return
    HEMIS_URL = "https://student.nukusii.uz/dashboard/login"
    text = (
        f"📋 <b>Студентлер дизими ({len(rows)}):</b>\n"
        f"🎓 <a href='{HEMIS_URL}'>HEMIS Кабинетине кириу →</a>\n\n"
    )
    chunks = []
    for i, row in enumerate(rows, 1):
        hemis_raw = str(row[3]).strip() if row[3] else ""
        if hemis_raw.endswith(".0") and hemis_raw[:-2].lstrip("-").isdigit():
            hemis_raw = hemis_raw[:-2]
        hemis_d = f"<code>{hemis_raw}</code>" if hemis_raw else "—"
        phone_d = f"<code>{row[2]}</code>" if row[2] else "—"
        days_left, _ = get_birthday_info(row[1])
        if days_left == 0:
            bd_label = "🎂 <b>Бүгин тууылған күни!!!</b>"
        elif days_left == 1:
            bd_label = "🔔 <b>Ертең тууылған күни!</b>"
        elif days_left is not None and days_left <= 7:
            bd_label = f"⏳ {days_left} күннен кейин тууылған күни"
        else:
            bd_label = None
        entry = f"{i}. <b>{row[0] or '—'}</b>"
        if days_left == 0:
            entry = f"🎂 {i}. <b>{row[0] or '—'}</b>"
        entry += f"\n   📅 {row[1] or '—'}"
        if bd_label:
            entry += f"\n   {bd_label}"
        entry += f"\n   📞 {phone_d}\n   🎓 HEMIS: {hemis_d}\n{'─'*25}\n"
        if len(text)+len(entry) > 3800:
            chunks.append(text)
            text = ""
        text += entry
    if text:
        chunks.append(text)
    hemis_markup = types.InlineKeyboardMarkup()
    hemis_markup.add(types.InlineKeyboardButton(text="🎓 HEMIS Кабинетине кириу", url=HEMIS_URL))
    for i, chunk in enumerate(chunks):
        if i == len(chunks) - 1:
            bot.send_message(message.chat.id, chunk, reply_markup=hemis_markup, disable_web_page_preview=True)
        else:
            bot.send_message(message.chat.id, chunk)
    bot.send_message(message.chat.id, "🏠 Меню:", reply_markup=main_menu(message.from_user.id))

# ============================================================
# БАЙЛАНЫС
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📞 Байланыс")
def show_contacts(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT type,name,phone FROM contacts ORDER BY type,name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Байланыс мағлыуматы жоқ.", reply_markup=main_menu(message.from_user.id))
        return
    dekanat = [(r[1],r[2]) for r in rows if r[0]=="dekanat"]
    mugallim = [(r[1],r[2]) for r in rows if r[0]=="mugallim"]
    text = "📞 <b>Байланыс</b>\n\n"
    if dekanat:
        text += "🏛 <b>Деканат:</b>\n"
        for name, phone in dekanat:
            text += f"  👤 {name}\n  📞 <code>{phone}</code>\n\n"
    if mugallim:
        text += "👨‍🏫 <b>Муғаллимлер:</b>\n"
        for name, phone in mugallim:
            text += f"  👤 {name}\n  📞 <code>{phone}</code>\n\n"
    bot.send_message(message.chat.id, text, reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "📞 Байланыс басқарыу")
def contacts_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "📞 <b>Байланыс басқарыу</b>", reply_markup=contacts_submenu())

@bot.message_handler(func=lambda m: m.text == "➕ Деканат қосыу")
def add_dekanat_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    msg = bot.send_message(message.chat.id, "🏛 Формат: <code>Аты;Телефон</code>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, lambda m: handle_add_contact(m, "dekanat"))

@bot.message_handler(func=lambda m: m.text == "➕ Муғаллим қосыу")
def add_mugallim_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    msg = bot.send_message(message.chat.id, "👨‍🏫 Формат: <code>Аты;Телефон</code>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, lambda m: handle_add_contact(m, "mugallim"))

def handle_add_contact(message, contact_type):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📞 Байланыс басқарыу", reply_markup=contacts_submenu())
        return
    parts = [p.strip() for p in message.text.split(";")]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        msg = bot.send_message(message.chat.id, "❌ Формат: <code>Аты;Телефон</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, lambda m: handle_add_contact(m, contact_type))
        return
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO contacts(type,name,phone) VALUES(%s,%s,%s)", (contact_type, parts[0], parts[1]))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    icon = "🏛" if contact_type == "dekanat" else "👨‍🏫"
    bot.send_message(message.chat.id, f"✅ {icon} <b>{parts[0]}</b> қосылды!", reply_markup=contacts_submenu())

@bot.message_handler(func=lambda m: m.text == "❌ Байланыс өшириу")
def delete_contact_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,type,name,phone FROM contacts ORDER BY type,name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Байланыслар жоқ.", reply_markup=contacts_submenu())
        return
    text = "❌ <b>Байланыс өшириу — ID жазыңыз:</b>\n\n"
    for row in rows:
        icon = "🏛" if row[1]=="dekanat" else "👨‍🏫"
        text += f"ID:<code>{row[0]}</code> {icon} {row[2]} | 📞 {row[3]}\n"
    text += "\nID ямаса <code>all</code>:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_delete_contact)

def handle_delete_contact(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📞 Байланыс басқарыу", reply_markup=contacts_submenu())
        return
    conn, cursor = get_db()
    try:
        if message.text.strip().lower() == "all":
            cursor.execute("DELETE FROM contacts")
            d = cursor.rowcount
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {d} байланыс өширилди.", reply_markup=contacts_submenu())
        else:
            try:
                cid = int(message.text.strip())
                cursor.execute("SELECT name FROM contacts WHERE id=%s", (cid,))
                row = cursor.fetchone()
                if not row:
                    bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=contacts_submenu())
                    return
                cursor.execute("DELETE FROM contacts WHERE id=%s", (cid,))
                conn.commit()
                bot.send_message(message.chat.id, f"✅ <b>{row[0]}</b> өширилди.", reply_markup=contacts_submenu())
            except ValueError:
                msg = bot.send_message(message.chat.id, "❌ ID ямаса all:", reply_markup=back_menu())
                bot.register_next_step_handler(msg, handle_delete_contact)
    finally:
        cursor.close()
        release_db(conn)

# ============================================================
# КОНТРАКТ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "💰 Контракт")
def show_contract_user(message):
    user_id = message.from_user.id
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT total_amount,note FROM contracts WHERE student_id=%s", (user_id,))
        contract = cursor.fetchone()
        if not contract:
            bot.send_message(message.chat.id, "📭 Контрактыңыз орнатылмаған.\nАдминге хабарласыңыз.", reply_markup=main_menu(user_id))
            return
        total = contract[0]
        note = contract[1] or ""
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM contract_payments WHERE student_id=%s", (user_id,))
        paid = cursor.fetchone()[0]
        remaining = total - paid
        cursor.execute("SELECT amount,date,note FROM contract_payments WHERE student_id=%s ORDER BY date DESC", (user_id,))
        payments = cursor.fetchall()
        cursor.execute("""SELECT s.full_name, s.id, c.total_amount,
            COALESCE((SELECT SUM(p.amount) FROM contract_payments p WHERE p.student_id=s.id),0) as paid
            FROM students s JOIN contracts c ON c.student_id=s.id
            WHERE s.full_name IS NOT NULL ORDER BY s.full_name""")
        all_contracts = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    percent = int((paid/total)*100) if total > 0 else 0
    bar = "🟩"*(percent//10) + "⬜"*(10-percent//10)
    text = f"💰 <b>Мениң контрактым</b>\n{'─'*30}\n"
    if note:
        text += f"📝 {note}\n"
    text += f"\n💵 Улыума: <b>{total:,.0f} сум</b>\n✅ Төленди: <b>{paid:,.0f} сум</b>\n⏳ Қалды: <b>{remaining:,.0f} сум</b>\n{bar} <b>{percent}%</b>\n{'─'*30}\n"
    if payments:
        text += "\n📜 <b>Төлем тарихы:</b>\n"
        for p in payments:
            p_note = f" — {p[2]}" if p[2] else ""
            text += f"  ✅ {date_to_ru(p[1])} | <b>{p[0]:,.0f} сум</b>{p_note}\n"
    else:
        text += "\n📭 Төлем тарихы жоқ.\n"
    if all_contracts:
        text += f"\n{'─'*30}\n📋 <b>Группаның жағдайы ({len(all_contracts)} студент):</b>\n\n"
        for row in all_contracts:
            s_remain = row[2]-row[3]
            s_percent = int((row[3]/row[2])*100) if row[2] > 0 else 0
            s_bar = "🟩"*(s_percent//10) + "⬜"*(10-s_percent//10)
            me = " 👈 <i>сиз</i>" if row[1]==user_id else ""
            if s_remain <= 0:
                text += f"✅ <b>{row[0]}</b>{me}\n   {s_bar} <b>100%</b> — Толық төленди\n\n"
            else:
                text += f"⏳ <b>{row[0]}</b>{me}\n   {s_bar} <b>{s_percent}%</b>\n   Қалды: <b>{s_remain:,.0f} сум</b>\n\n"
    send_long_message(message.chat.id, text, reply_markup=main_menu(user_id))

@bot.message_handler(func=lambda m: m.text == "💰 Контракт басқарыу")
def contract_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "💰 <b>Контракт басқарыу</b>", reply_markup=contract_submenu())

@bot.message_handler(func=lambda m: m.text == "💰 Контракт киргизиу")
def contract_set_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,full_name FROM students WHERE full_name IS NOT NULL ORDER BY full_name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Студентлер жоқ.", reply_markup=contract_submenu())
        return
    text = "💰 <b>Контракт киргизиу:</b>\nФормат: <code>TelegramID;Сумма;Ескертиу</code>\n\n📋 <b>Студентлер:</b>\n"
    for row in rows:
        text += f"🆔 <code>{row[0]}</code> — {row[1]}\n"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_contract_set)

def handle_contract_set(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "💰 Контракт басқарыу", reply_markup=contract_submenu())
        return
    parts = [p.strip() for p in message.text.split(";")]
    if len(parts) < 2 or not parts[0].lstrip("-").isdigit():
        msg = bot.send_message(message.chat.id, "❌ Формат: <code>TelegramID;Сумма;Ескертиу</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_contract_set)
        return
    try:
        student_id = int(parts[0])
        amount = float(parts[1].replace(" ","").replace(",",""))
        note = parts[2] if len(parts) > 2 else ""
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT full_name FROM students WHERE id=%s", (student_id,))
            row = cursor.fetchone()
            if not row:
                msg = bot.send_message(message.chat.id, "⚠️ Студент табылмады.", reply_markup=back_menu())
                bot.register_next_step_handler(msg, handle_contract_set)
                return
            cursor.execute(
                "INSERT INTO contracts(student_id,total_amount,note) VALUES(%s,%s,%s) "
                "ON CONFLICT(student_id) DO UPDATE SET total_amount=excluded.total_amount,note=excluded.note",
                (student_id, amount, note)
            )
            conn.commit()
            student_name = row[0]
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <b>{student_name}</b>\n💵 Контракт: <b>{amount:,.0f} сум</b>", reply_markup=contract_submenu())
        try:
            bot.send_message(student_id, f"💰 <b>Контрактыңыз киргизилди!</b>\n💵 Улыума: <b>{amount:,.0f} сум</b>\n{'📝 '+note if note else ''}")
        except Exception:
            pass
    except Exception as e:
        msg = bot.send_message(message.chat.id, f"❌ Қате: {e}", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_contract_set)

@bot.message_handler(func=lambda m: m.text == "➕ Төлем қосыу")
def payment_add_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("""SELECT s.id, s.full_name, c.total_amount,
            COALESCE((SELECT SUM(p.amount) FROM contract_payments p WHERE p.student_id=s.id),0) as paid
            FROM students s JOIN contracts c ON c.student_id=s.id
            WHERE s.full_name IS NOT NULL ORDER BY s.full_name""")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Контракт киргизилген студент жоқ.", reply_markup=contract_submenu())
        return
    text = "➕ <b>Төлем қосыу:</b>\nФормат: <code>TelegramID;Сумма;Ескертиу</code>\n\n📋 <b>Контрактлар:</b>\n"
    for row in rows:
        remaining = row[2]-row[3]
        text += f"{'✅' if remaining<=0 else '⏳'} <code>{row[0]}</code> — {row[1]}\n   Қалды: <b>{remaining:,.0f} сум</b>\n"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_payment_add)

def handle_payment_add(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "💰 Контракт басқарыу", reply_markup=contract_submenu())
        return
    parts = [p.strip() for p in message.text.split(";")]
    if len(parts) < 2 or not parts[0].lstrip("-").isdigit():
        msg = bot.send_message(message.chat.id, "❌ Формат: <code>TelegramID;Сумма;Ескертиу</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_payment_add)
        return
    try:
        student_id = int(parts[0])
        amount = float(parts[1].replace(" ","").replace(",",""))
        note = parts[2] if len(parts) > 2 else ""
        date_str = now_uz().strftime("%Y-%m-%d")
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT full_name FROM students WHERE id=%s", (student_id,))
            s_row = cursor.fetchone()
            cursor.execute("SELECT total_amount FROM contracts WHERE student_id=%s", (student_id,))
            c_row = cursor.fetchone()
            if not s_row or not c_row:
                msg = bot.send_message(message.chat.id, "⚠️ Студент ямаса контракт табылмады.", reply_markup=back_menu())
                bot.register_next_step_handler(msg, handle_payment_add)
                return
            cursor.execute("INSERT INTO contract_payments(student_id,amount,date,note) VALUES(%s,%s,%s,%s)", (student_id, amount, date_str, note))
            cursor.execute("SELECT COALESCE(SUM(amount),0) FROM contract_payments WHERE student_id=%s", (student_id,))
            paid = cursor.fetchone()[0]
            total = c_row[0]
            remaining = total - paid
            conn.commit()
            s_name = s_row[0]
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ Төлем қосылды!\n👤 <b>{s_name}</b>\n💵 {amount:,.0f} сум\n⏳ Қалды: <b>{remaining:,.0f} сум</b>", reply_markup=contract_submenu())
        try:
            percent = int((paid/total)*100) if total > 0 else 0
            bar = "🟩"*(percent//10) + "⬜"*(10-percent//10)
            bot.send_message(student_id, f"💰 <b>Төлем қабылланды!</b>\n📅 {date_to_ru(date_str)}\n{'─'*25}\n✅ Төленди: <b>{amount:,.0f} сум</b>\n⏳ Қалды: <b>{remaining:,.0f} сум</b>\n\n{bar} {percent}%")
        except Exception:
            pass
    except Exception as e:
        msg = bot.send_message(message.chat.id, f"❌ Қате: {e}", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_payment_add)

@bot.message_handler(func=lambda m: m.text == "📋 Барлық контрактлар")
def show_all_contracts(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("""SELECT s.full_name, c.total_amount,
            COALESCE((SELECT SUM(p.amount) FROM contract_payments p WHERE p.student_id=s.id),0) as paid
            FROM students s JOIN contracts c ON c.student_id=s.id
            WHERE s.full_name IS NOT NULL ORDER BY s.full_name""")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Контрактлар жоқ.", reply_markup=contract_submenu())
        return
    total_sum = sum(r[1] for r in rows)
    paid_sum = sum(r[2] for r in rows)
    text = (f"📋 <b>Барлық контрактлар ({len(rows)}):</b>\n"
            f"💵 Улыума: <b>{total_sum:,.0f}</b>\n✅ Түскен: <b>{paid_sum:,.0f}</b>\n"
            f"⏳ Қалды: <b>{total_sum-paid_sum:,.0f} сум</b>\n{'─'*30}\n\n")
    for row in rows:
        remaining = row[1]-row[2]
        status = "✅ Толық" if remaining <= 0 else f"⏳ Қалды: {remaining:,.0f}"
        text += f"👤 <b>{row[0]}</b>\n   💵 {row[1]:,.0f} | ✅ {row[2]:,.0f} | {status}\n\n"
    send_long_message(message.chat.id, text, reply_markup=contract_submenu())

# ============================================================
# ЖАҢАЛЫҚЛАР
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📰 Жаңалықлар")
def show_news_menu(message):
    bot.send_message(message.chat.id, "📰 <b>Жаңалықлар бөлими</b>", reply_markup=news_menu())

@bot.message_handler(func=lambda m: m.text == "✍️ Жазыңыз")
def write_news(message):
    msg = bot.send_message(message.chat.id, "✍️ <b>Жаңалығыңызды жазыңыз:</b>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_user_news)

def handle_user_news(message):
    if not message.text:
        msg = bot.send_message(message.chat.id, "✍️ Тек текст жибериңиз:", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_user_news)
        return
    if message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📰 Жаңалықлар", reply_markup=news_menu())
        return
    if len(message.text) > 2000:
        msg = bot.send_message(message.chat.id, "❌ Текст тым ұзын (макс 2000 таңба).", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_user_news)
        return
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO user_news(content,author_id,author_username) VALUES(%s,%s,%s)", (message.text, user_id, username))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_to_students(text=f"📰 <b>Таза хабарлама!</b>\n\n👤 <b>@{username}</b>:\n\n{message.text}", exclude_id=user_id)
    bot.send_message(message.chat.id, "✅ Жиберилди!", reply_markup=news_menu())

@bot.message_handler(func=lambda m: m.text == "🗂 Архив Жаңалықлар")
def show_news_archive(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT content,author_username,date FROM user_news ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Архив бос.", reply_markup=news_menu())
        return
    text = "🗂 <b>Архив жаңалықлар:</b>\n\n"
    chunks = []
    for row in rows:
        entry = f"👤 <b>@{row[1]}</b>\n📌 {row[0]}\n🕐 {row[2]}\n{'─'*25}\n"
        if len(text)+len(entry) > 3800:
            chunks.append(text)
            text = ""
        text += entry
    if text:
        chunks.append(text)
    for i, chunk in enumerate(chunks):
        bot.send_message(message.chat.id, chunk, reply_markup=news_menu() if i == len(chunks)-1 else None)

# ============================================================
# МАТЕРИАЛДАР
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📚 Сабақ материаллары")
def show_materials_menu(message):
    bot.send_message(message.chat.id, "📚 <b>Сабақ материаллары</b>", reply_markup=materials_menu())

@bot.message_handler(func=lambda m: m.text == "📥 Мат жүклеңиз")
def upload_material_start(message):
    set_user_state(message.from_user.id, "materials")
    bot.send_message(message.chat.id, "📥 <b>Файл ямаса фото жибериңиз:</b>\nТайын болғанда <b>⬅️ Артқа</b> басыңыз.", reply_markup=back_menu())

@bot.message_handler(content_types=["document"], func=lambda m: get_user_state(m.from_user.id)=="materials")
def handle_upload_document(message):
    if is_already_processed(message.message_id):
        return
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    file_id = message.document.file_id
    file_name = message.document.file_name or "Файл"
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO materials(file_id,file_type,uploader_id,uploader_username) VALUES(%s,%s,%s,%s)", (file_id,"document",user_id,username))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_to_students(file_id=file_id, file_type="document", text=f"📚 <b>Таза материал!</b>\n👤 @{username}\n📎 {file_name}", exclude_id=user_id)
    send_saved_once(message.chat.id, user_id)

@bot.message_handler(content_types=["photo"], func=lambda m: get_user_state(m.from_user.id)=="materials")
def handle_upload_photo_mat(message):
    if is_already_processed(message.message_id):
        return
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    file_id = message.photo[-1].file_id
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO materials(file_id,file_type,uploader_id,uploader_username) VALUES(%s,%s,%s,%s)", (file_id,"photo",user_id,username))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_to_students(file_id=file_id, file_type="photo", text=f"📚 <b>Таза материал!</b>\n👤 @{username}", exclude_id=user_id)
    send_saved_once(message.chat.id, user_id)

@bot.message_handler(content_types=["video","audio","voice","sticker"], func=lambda m: get_user_state(m.from_user.id)=="materials")
def handle_upload_wrong_materials(message):
    bot.send_message(message.chat.id, "⚠️ Тек файл ямаса фото!")

@bot.message_handler(func=lambda m: m.text == "🗂 Архив материаллар")
def show_materials_archive(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT file_id,file_type,date,uploader_username FROM materials ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Архив бос.", reply_markup=materials_menu())
        return
    bot.send_message(message.chat.id, f"🗂 <b>Барлығы: {len(rows)}</b>\n\nЖүклениуде...")
    for row in rows:
        uname = f"@{row[3]}" if row[3] else "Белгисиз"
        cap = f"👤 {uname}\n🕐 {row[2]}"
        try:
            if row[1] == "document":
                bot.send_document(message.chat.id, row[0], caption=cap)
            elif row[1] == "photo":
                bot.send_photo(message.chat.id, row[0], caption=cap)
        except Exception:
            continue
    bot.send_message(message.chat.id, "✅ Тайын.", reply_markup=materials_menu())

# ============================================================
# ГАЛЕРЕЯ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📷 Фото/Видео")
def show_gallery_menu(message):
    bot.send_message(message.chat.id, "📷 <b>Фото/Видео бөлими</b>", reply_markup=gallery_menu())

@bot.message_handler(func=lambda m: m.text == GALLERY_UPLOAD_BTN)
def gallery_upload_start(message):
    set_user_state(message.from_user.id, "gallery")
    bot.send_message(message.chat.id, "📤 <b>Фото ямаса видео жибериңиз:</b>\nТайын болғанда <b>⬅️ Артқа</b> басыңыз.", reply_markup=back_menu())

@bot.message_handler(content_types=["photo"], func=lambda m: get_user_state(m.from_user.id)=="gallery")
def handle_gallery_photo(message):
    if is_already_processed(message.message_id):
        return
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    file_id = message.photo[-1].file_id
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO gallery(file_id,file_type,uploader_id,uploader_username) VALUES(%s,%s,%s,%s)", (file_id,"photo",user_id,username))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_to_students(file_id=file_id, file_type="photo", text=f"🎞 <b>S6-DI естелиги!</b>\n👤 @{username}", exclude_id=user_id)
    send_saved_once(message.chat.id, user_id)

@bot.message_handler(content_types=["video"], func=lambda m: get_user_state(m.from_user.id)=="gallery")
def handle_gallery_video(message):
    if is_already_processed(message.message_id):
        return
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    file_id = message.video.file_id
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO gallery(file_id,file_type,uploader_id,uploader_username) VALUES(%s,%s,%s,%s)", (file_id,"video",user_id,username))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_to_students(file_id=file_id, file_type="video", text=f"🎞 <b>S6-DI естелиги!</b>\n👤 @{username}", exclude_id=user_id)
    send_saved_once(message.chat.id, user_id)

@bot.message_handler(content_types=["document","audio","voice","sticker"], func=lambda m: get_user_state(m.from_user.id)=="gallery")
def handle_upload_wrong_gallery(message):
    bot.send_message(message.chat.id, "⚠️ Тек фото ямаса видео!")

@bot.message_handler(func=lambda m: m.text == "🎞 S6-DI естелиги")
def show_gallery_view(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT file_id,file_type,date,uploader_username FROM gallery ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Галерея бос.", reply_markup=gallery_menu())
        return
    bot.send_message(message.chat.id, f"🎞 <b>Барлығы: {len(rows)}</b>\n\nЖүклениуде...")
    for row in rows:
        uname = f"@{row[3]}" if row[3] else "Белгисиз"
        cap = f"👤 {uname}\n📅 {row[2]}"
        try:
            if row[1] == "photo":
                bot.send_photo(message.chat.id, row[0], caption=cap)
            elif row[1] == "video":
                bot.send_video(message.chat.id, row[0], caption=cap)
        except Exception:
            continue
    bot.send_message(message.chat.id, "✅ Тайын.", reply_markup=gallery_menu())

# ============================================================
# САБАҚ КЕСТЕСІ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📅 Сабақ кестеси")
def show_schedule_menu(message):
    bot.send_message(message.chat.id, "📅 <b>Сабақ кестеси</b>\nКүнди таңлаңыз:", reply_markup=schedule_menu())

@bot.message_handler(func=lambda m: m.text in DAYS_RU)
def show_day_schedule(message):
    day = message.text
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT subject,time FROM schedule WHERE day=%s ORDER BY time", (day,))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    today_ru = DAYS_EN_TO_RU.get(now_uz().strftime("%A"), "")
    today_mark = " 📌 <i>(бүгин)</i>" if day == today_ru else ""
    if not rows:
        bot.send_message(message.chat.id, f"📭 <b>{day}{today_mark}</b>\n\nСабақ жоқ.", reply_markup=schedule_menu())
        return
    text = f"📅 <b>{day}{today_mark}</b>\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}-пара 🕐 <b>{row[1]}</b> — {row[0]}\n"
    bot.send_message(message.chat.id, text, reply_markup=schedule_menu())

# ============================================================
# ҰСЫНЫС / ШАҒЫМ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "💡 Ұсыныс / Шағым")
def suggestion_start(message):
    msg = bot.send_message(message.chat.id, "💡 <b>Ұсыныс ямаса шағымыңызды жазыңыз:</b>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_suggestion)

def handle_suggestion(message):
    if not message.text:
        msg = bot.send_message(message.chat.id, "✍️ Текст жибериңиз:", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_suggestion)
        return
    if message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🏠 Бас меню", reply_markup=main_menu(message.from_user.id))
        return
    if len(message.text) > 1000:
        msg = bot.send_message(message.chat.id, "❌ Текст дым ұзын (макс 1000 таңба).", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_suggestion)
        return
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO suggestions(content,user_id) VALUES(%s,%s)", (message.text, message.from_user.id))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    bot.send_message(message.chat.id, "✅ Жиберилди! Рахмет!", reply_markup=main_menu(message.from_user.id))
    for aid in ADMIN_IDS:
        try:
            fn = message.from_user.first_name or ""
            ln = message.from_user.last_name or ""
            un = f"@{message.from_user.username}" if message.from_user.username else "username жоқ"
            bot.send_message(aid, f"💡 <b>Таза ұсыныс/шағым:</b>\n\n{message.text}\n\n👤 {fn} {ln}\n🔗 {un}\n🆔 <code>{message.from_user.id}</code>")
        except Exception:
            pass

# ============================================================
# АДМИН ПАНЕЛЬ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "👮 Админ панель")
def admin_panel(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫 Сиз админ емессиз!")
        return
    bot.send_message(message.chat.id, "👮 <b>Админ панель</b>", reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "👤 Студент басқарыу")
def student_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "👤 <b>Студент басқарыу</b>", reply_markup=student_submenu())

@bot.message_handler(func=lambda m: m.text == "➕ Студент қосыу/өзгертиу")
def student_add_or_edit_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,full_name,username FROM students ORDER BY full_name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    header = ("➕ <b>Студент қосыу / Өзгертиу:</b>\n\n🆕 <b>Таза қосу:</b>\n"
              "<code>таза;ФИО;Тууылған күни;Тел;HEMIS;TelegramID</code>\n"
              "📌 Мысал: <code>таза;Иванов Иван;2000-01-01;+998901234567;S12345678;123456789</code>\n\n"
              "✏️ <b>Өзгертиу:</b> студент ID-ін жазыңыз\n" + "─"*30 + "\n")
    if not rows:
        msg = bot.send_message(message.chat.id, header+"📭 Студентлер жоқ.", reply_markup=back_menu())
        bot.register_next_step_handler(msg, student_add_or_edit)
        return
    chunks = []
    current = header
    for i, row in enumerate(rows, 1):
        line = f"{i}. 👤 <b>{row[1] or '—'}</b>\n    🆔 <code>{row[0]}</code> | {'@'+row[2] if row[2] else 'username жоқ'}\n"
        if len(current)+len(line) > 3800:
            chunks.append(current)
            current = ""
        current += line
    current += "─"*30+"\n⬇️ <b>ID жазыңыз ямаса таза студент форматын жибериңиз:</b>"
    chunks.append(current)
    for chunk in chunks[:-1]:
        bot.send_message(message.chat.id, chunk)
    msg = bot.send_message(message.chat.id, chunks[-1], reply_markup=back_menu())
    bot.register_next_step_handler(msg, student_add_or_edit)

@bot.message_handler(func=lambda m: m.text == "❌ Студент өшириу")
def student_delete_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,full_name,username FROM students ORDER BY full_name")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Студентлер жоқ.", reply_markup=student_submenu())
        return
    text = "❌ <b>Студент өшириу — ID жазыңыз:</b>\n\n"
    for row in rows:
        text += f"ID:<code>{row[0]}</code> — {row[1] or '—'} (@{row[2] or '—'})\n"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, delete_student)

def student_add_or_edit(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "👤 Студент басқарыу", reply_markup=student_submenu())
        return
    if message.text.strip().lower().startswith("таза;"):
        parts = [p.strip() for p in message.text.split(";")]
        if len(parts) < 6 or not parts[1] or not parts[5]:
            msg = bot.send_message(message.chat.id, "❌ Формат:\n<code>таза;ФИО;Тууылған күни;Тел;HEMIS;TelegramID</code>", reply_markup=back_menu())
            bot.register_next_step_handler(msg, student_add_or_edit)
            return
        if not parts[5].lstrip("-").isdigit():
            msg = bot.send_message(message.chat.id, "❌ TelegramID тек сан болуы керек!", reply_markup=back_menu())
            bot.register_next_step_handler(msg, student_add_or_edit)
            return
        full_name = parts[1]
        birth_date = parts[2] if len(parts) > 2 else ""
        phone = parts[3] if len(parts) > 3 else ""
        hemis = parts[4] if len(parts) > 4 else ""
        tg_id = int(parts[5])
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT id,full_name FROM students WHERE id=%s", (tg_id,))
            existing = cursor.fetchone()
            if existing:
                bot.send_message(message.chat.id, f"⚠️ Бұл ID бұрыннан бар!\n👤 {existing[1] or '—'}\n\nӨзгертиу үшын ID жазыңыз: <code>{tg_id}</code>", reply_markup=back_menu())
                bot.register_next_step_handler(message, student_add_or_edit)
                return
            cursor.execute("INSERT INTO students(id,username,last_active,full_name,birth_date,phone,hemis) VALUES(%s,%s,%s,%s,%s,%s,%s)", (tg_id, None, now_uz(), full_name, birth_date, phone, hemis))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <b>{full_name}</b> қосылды!\n🆔 <code>{tg_id}</code>\n🎓 HEMIS: {hemis}\n\n📌 Студент ботқа /start берсин.", reply_markup=student_submenu())
        return
    try:
        sid = int(message.text.strip())
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT id,full_name,birth_date,phone,hemis FROM students WHERE id=%s", (sid,))
            row = cursor.fetchone()
        finally:
            cursor.close()
            release_db(conn)
        if not row:
            msg = bot.send_message(message.chat.id, "⚠️ ID табылмады:", reply_markup=back_menu())
            bot.register_next_step_handler(msg, student_add_or_edit)
            return
        sid, fname, bdate, phone, hemis = row[0], row[1], row[2], row[3], row[4]
        text = (f"✏️ <b>Студент:</b>\n👤 {fname or '—'} | 📅 {bdate or '—'} | 📞 {phone or '—'} | 🎓 {hemis or '—'}\n\n"
                "<code>ФИО;Күн;Тел;HEMIS</code>\nӨзгертпей <b>—</b> жазыңыз.")
        msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
        bot.register_next_step_handler(msg, lambda m: student_edit_save(m, sid, fname, bdate, phone, hemis))
    except ValueError:
        msg = bot.send_message(message.chat.id, "❌ ID ямаса <code>таза;ФИО;Күн;Тел;HEMIS;TelegramID</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, student_add_or_edit)

def student_edit_save(message, sid, old_fname, old_bdate, old_phone, old_hemis):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "👤 Студент басқарыу", reply_markup=student_submenu())
        return
    try:
        parts = [p.strip() for p in message.text.split(";")]
        if len(parts) != 4:
            raise ValueError
        new_fname = parts[0] if parts[0] != "—" else old_fname
        new_bdate = parts[1] if parts[1] != "—" else old_bdate
        new_phone = parts[2] if parts[2] != "—" else old_phone
        new_hemis = parts[3] if parts[3] != "—" else old_hemis
        conn, cursor = get_db()
        try:
            cursor.execute("UPDATE students SET full_name=%s,birth_date=%s,phone=%s,hemis=%s WHERE id=%s", (new_fname, new_bdate, new_phone, new_hemis, sid))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <b>{new_fname}</b> тазаланды!\n🎓 HEMIS: {new_hemis}", reply_markup=student_submenu())
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ <code>ФИО;Күн;Тел;HEMIS</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, lambda m: student_edit_save(m, sid, old_fname, old_bdate, old_phone, old_hemis))

def delete_student(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "👤 Студент басқарыу", reply_markup=student_submenu())
        return
    try:
        sid = int(message.text.strip())
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT full_name FROM students WHERE id=%s", (sid,))
            row = cursor.fetchone()
            if not row:
                bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=student_submenu())
                return
            student_name = row[0]
            cursor.execute("DELETE FROM students WHERE id=%s", (sid,))
            cursor.execute("DELETE FROM attendance WHERE student_id=%s", (sid,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <b>{student_name}</b> өширилди.", reply_markup=student_submenu())
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ ID жазыңыз:", reply_markup=back_menu())
        bot.register_next_step_handler(msg, delete_student)

# ============================================================
# EXCEL
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📊 Excel басқарыу")
def excel_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "📊 <b>Excel басқарыу</b>", reply_markup=excel_submenu())

@bot.message_handler(func=lambda m: m.text == "📥 Excel жүклеу")
def excel_download(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT id,full_name,birth_date,phone,hemis,username FROM students ORDER BY full_name")
            rows = cursor.fetchall()
        finally:
            cursor.close()
            release_db(conn)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Студентлер"
        hfont = Font(bold=True, color="FFFFFF")
        hfill = PatternFill("solid", fgColor="2E75B6")
        ctr = Alignment(horizontal="center", vertical="center")
        for col, h in enumerate(["№","ФИО","Тууылған күни","Телефон","HEMIS","Telegram","TelegramID"], 1):
            c = ws.cell(row=1, column=col, value=h)
            c.font = hfont
            c.fill = hfill
            c.alignment = ctr
        ws.column_dimensions["A"].width = 5
        ws.column_dimensions["B"].width = 35
        ws.column_dimensions["C"].width = 15
        ws.column_dimensions["D"].width = 18
        ws.column_dimensions["E"].width = 18
        ws.column_dimensions["F"].width = 20
        ws.column_dimensions["G"].width = 15
        for i, row in enumerate(rows, 1):
            ws.append([i, row[1] or "", row[2] or "", row[3] or "", row[4] or "", f"@{row[5]}" if row[5] else "", row[0]])
        path = "/tmp/students.xlsx"
        wb.save(path)
        with open(path, "rb") as f:
            bot.send_document(message.chat.id, f,
                caption=f"📥 <b>Студентлер дизими</b> — {len(rows)} студент\n✏️ B,C,D,E қатарларын толтырып 📤 Excel импорт арқалы жүклеңиз.\n⚠️ G қатарын (TelegramID) өзгертпеңиз!",
                visible_file_name="students.xlsx", reply_markup=excel_submenu())
        os.remove(path)
    except ImportError:
        bot.send_message(message.chat.id, "❌ pip install openpyxl", reply_markup=excel_submenu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Қате: {e}", reply_markup=excel_submenu())

@bot.message_handler(func=lambda m: m.text == "📤 Excel импорт")
def excel_import_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    msg = bot.send_message(message.chat.id, "📤 <b>Excel файлды жибериңиз (.xlsx):</b>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, handle_excel_import)

def handle_excel_import(message):
    if message.text = "⬅️ Артқа":
        bot.send_message(message.chat.id, "📊 Excel басқарыу", reply_markup=excel_submenu())
        return
    if not message.document:
        msg = bot.send_message(message.chat.id, "⚠️ .xlsx файл жибериңиз!", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_excel_import)
        return
    if not message.document.file_name.endswith(".xlsx"):
        msg = bot.send_message(message.chat.id, "⚠️ Тек .xlsx!", reply_markup=back_menu())
        bot.register_next_step_handler(msg, handle_excel_import)
        return
    try:
        import openpyxl
    except ImportError:
        bot.send_message(message.chat.id, "❌ pip install openpyxl", reply_markup=excel_submenu())
        return
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
        path = "/tmp/import_students.xlsx"
        with open(path, "wb") as f:
            f.write(downloaded)
        wb = openpyxl.load_workbook(path)
        ws = wb.active
        updated = 0
        added = 0
        errors = 0

        def clean_cell(val):
            if val is None:
                return ""
            s = str(val).strip()
            if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
                s = s[:-2]
            return s if s not in ("None", "") else ""

        conn, cursor = get_db()
        try:
            for row in ws.iter_rows(min_row=2, values_only=True):
                try:
                    if not row or len(row) < 2:
                        continue
                    fname = clean_cell(row[1])
                    bdate = clean_cell(row[2])
                    phone = clean_cell(row[3])
                    hemis = clean_cell(row[4])
                    uname = str(row[5]).strip().lstrip("@") if row[5] and str(row[5]).strip() not in ("","None") else None
                    tg_id = int(str(row[6]).strip().split(".")[0]) if row[6] and str(row[6]).strip().split(".")[0].lstrip("-").isdigit() else None
                    if not fname or fname == "None":
                        continue
                    if tg_id:
                        cursor.execute("SELECT id FROM students WHERE id=%s", (tg_id,))
                        if cursor.fetchone():
                            cursor.execute("UPDATE students SET full_name=%s,birth_date=%s,phone=%s,hemis=%s WHERE id=%s", (fname, bdate, phone, hemis, tg_id))
                            updated += 1
                        else:
                            cursor.execute("INSERT INTO students(id,username,last_active,full_name,birth_date,phone,hemis) VALUES(%s,%s,%s,%s,%s,%s,%s)", (tg_id, uname, now_uz(), fname, bdate, phone, hemis))
                            added += 1
                        continue
                    if uname:
                        cursor.execute("SELECT id FROM students WHERE username=%s", (uname,))
                        if cursor.fetchone():
                            cursor.execute("UPDATE students SET full_name=%s,birth_date=%s,phone=%s,hemis=%s WHERE username=%s", (fname, bdate, phone, hemis, uname))
                            updated += 1
                            continue
                    cursor.execute("SELECT id FROM students WHERE full_name=%s", (fname,))
                    if cursor.fetchone():
                        cursor.execute("UPDATE students SET birth_date=%s,phone=%s,hemis=%s WHERE full_name=%s", (bdate, phone, hemis, fname))
                        updated += 1
                    else:
                        errors += 1
                except Exception as row_err:
                    logger.warning(f"Excel import row error: {row_err}")
                    errors += 1
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        os.remove(path)
        bot.send_message(message.chat.id, f"✅ <b>Импорт жуумакланды!</b>\n\n🔄 Жаңаланды: <b>{updated}</b>\n➕ Қосылды: <b>{added}</b>\n❌ Қателер: <b>{errors}</b>", reply_markup=excel_submenu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Файлда қате: {e}", reply_markup=excel_submenu())

# ============================================================
# САБАҚ БАСҚАРЫУ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📅 Сабақ басқарыу")
def schedule_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "📅 <b>Сабақ басқарыу</b>", reply_markup=schedule_admin_submenu())

@bot.message_handler(func=lambda m: m.text == "➕ Сабақ қосыу")
def schedule_add_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    msg = bot.send_message(message.chat.id, "📝 Формат: <code>Понедельник;Математика;09:00</code>", reply_markup=back_menu())
    bot.register_next_step_handler(msg, add_lesson)

@bot.message_handler(func=lambda m: m.text == "❌ Сабақ өшириу")
def schedule_delete_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,day,subject,time FROM schedule ORDER BY day,time")
        lessons = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not lessons:
        bot.send_message(message.chat.id, "📭 Кесте бос.", reply_markup=schedule_admin_submenu())
        return
    text = "📋 <b>Барлық сабақлар:</b>\n\n"
    for row in lessons:
        text += f"ID:{row[0]} | {row[1]} | {row[2]} | {row[3]}\n"
    text += "\n<code>Күн;Уақыт</code> форматында жазыңыз:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, delete_lesson)

def add_lesson(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📅 Сабақ басқарыу", reply_markup=schedule_admin_submenu())
        return
    try:
        parts = [p.strip() for p in message.text.split(";")]
        if len(parts) != 3 or not all(parts):
            raise ValueError
        day, subject, time_ = parts
        if day not in DAYS_RU:
            raise ValueError
        conn, cursor = get_db()
        try:
            cursor.execute("INSERT INTO schedule(day,subject,time) VALUES(%s,%s,%s)", (day, subject, time_))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id, f"✅ <b>{day} | {subject} | {time_}</b> қосылды!", reply_markup=schedule_admin_submenu())
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ <code>Понедельник;Математика;09:00</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, add_lesson)

def delete_lesson(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📅 Сабақ басқарыу", reply_markup=schedule_admin_submenu())
        return
    try:
        parts = [p.strip() for p in message.text.split(";")]
        if len(parts) != 2 or not all(parts):
            raise ValueError
        day, time_ = parts
        conn, cursor = get_db()
        try:
            cursor.execute("DELETE FROM schedule WHERE day=%s AND time=%s", (day, time_))
            deleted = cursor.rowcount
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
        if deleted:
            bot.send_message(message.chat.id, f"✅ Өширилди: {day} — {time_}", reply_markup=schedule_admin_submenu())
        else:
            bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=schedule_admin_submenu())
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ <code>Понедельник;09:00</code>", reply_markup=back_menu())
        bot.register_next_step_handler(msg, delete_lesson)

# ============================================================
# БАРЛАУ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📊 Барлау басқарыу")
def attendance_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "📊 <b>Барлау басқарыу</b>", reply_markup=attendance_submenu())

@bot.message_handler(func=lambda m: m.text == "📊 Барлау")
def start_attendance(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    today = DAYS_EN_TO_RU.get(now_uz().strftime("%A"), "")
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT subject,time FROM schedule WHERE day=%s ORDER BY time", (today,))
        lessons = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not lessons:
        bot.send_message(message.chat.id, f"📭 Бүгин ({today}) сабақ жоқ.", reply_markup=attendance_submenu())
        return
    markup = types.InlineKeyboardMarkup()
    for i, (subject, time_) in enumerate(lessons, 1):
        markup.add(types.InlineKeyboardButton(text=f"{i}-пара: {subject} ({time_})", callback_data=f"att_para_{i}_{subject}"))
    bot.send_message(message.chat.id, f"📊 <b>Барлау — {today}</b>\n\nҚай параны белгилейсиз:", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "📅 Барлау тарихы")
def attendance_history(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT LEFT(date,7) as ym FROM attendance ORDER BY ym DESC")
        months = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    if not months:
        bot.send_message(message.chat.id, "📭 Барлау жазылмаған.", reply_markup=attendance_submenu())
        return
    markup = types.InlineKeyboardMarkup()
    for ym in months:
        year, month = ym.split("-")
        markup.add(types.InlineKeyboardButton(text=f"📅 {MONTHS_RU.get(int(month), month)} {year}", callback_data=f"hist_month_{ym}"))
    bot.send_message(message.chat.id, "📅 <b>Барлау тарихы</b>\n\nАйды таңлаңыз:", reply_markup=markup)

# ============================================================
# ✅ ТҮЗЕТІЛДІ: ӨШІРІУ — "⬅️ Артқа" дұрыс жұмыс істейді
# ============================================================

@bot.message_handler(func=lambda m: m.text == "🗑 Өшириу")
def delete_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "🗑 <b>Өшириу бөлими</b>", reply_markup=delete_submenu())

@bot.message_handler(func=lambda m: m.text == "🗑 Материал өшириу")
def delete_material_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,file_type,uploader_username,date FROM materials ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Материаллар жоқ.", reply_markup=delete_submenu())
        return
    text = "🗑 <b>Материалдарды өшириу:</b>\n\n"
    for row in rows:
        text += f"ID:<code>{row[0]}</code> | {row[1]} | {'@'+row[2] if row[2] else '—'} | {row[3]}\n"
    text += "\n\nID ямаса <code>all</code> жазыңыз:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, delete_material)

@bot.message_handler(func=lambda m: m.text == "🗑 Фото/Видео өшириу")
def delete_gallery_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,file_type,uploader_username,date FROM gallery ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Галерея бос.", reply_markup=delete_submenu())
        return
    text = "🗑 <b>Фото/Видео өшириу:</b>\n\n"
    for row in rows:
        text += f"ID:<code>{row[0]}</code> | {row[1]} | {'@'+row[2] if row[2] else '—'} | {row[3]}\n"
    text += "\n\nID ямаса <code>all</code> жазыңыз:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, delete_gallery_item)

@bot.message_handler(func=lambda m: m.text == "🗑 Жаңалық өшириу")
def delete_news_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,author_username,date,content FROM user_news ORDER BY date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Жаңалықлар жоқ.", reply_markup=delete_submenu())
        return
    text = "🗑 <b>Жаңалықларды өшириу:</b>\n\n"
    for row in rows:
        uname = f"@{row[1]}" if row[1] else "Белгисиз"
        preview = row[3][:40]+"..." if len(row[3]) > 40 else row[3]
        text += f"ID:<code>{row[0]}</code> | {uname}\n📌 {preview}\n{'─'*20}\n"
    text += "\n\nID ямаса <code>all</code> жазыңыз:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, delete_news_item)

# ✅ ТҮЗЕТІЛДІ: Өшіріу handler-лары — "⬅️ Артқа" delete_submenu қайтарады
def delete_material(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🗑 Өшириу бөлими", reply_markup=delete_submenu())
        return
    conn, cursor = get_db()
    try:
        if message.text.strip().lower() == "all":
            cursor.execute("DELETE FROM materials")
            d = cursor.rowcount
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {d} Материал өширилди.", reply_markup=delete_submenu())
        else:
            try:
                rid = int(message.text.strip())
                cursor.execute("SELECT id FROM materials WHERE id=%s", (rid,))
                if not cursor.fetchone():
                    bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=delete_submenu())
                    return
                cursor.execute("DELETE FROM materials WHERE id=%s", (rid,))
                conn.commit()
                bot.send_message(message.chat.id, f"✅ ID:{rid} өширилди.", reply_markup=delete_submenu())
            except ValueError:
                msg = bot.send_message(message.chat.id, "❌ ID ямаса <code>all</code> жазыңыз:", reply_markup=back_menu())
                bot.register_next_step_handler(msg, delete_material)
    finally:
        cursor.close()
        release_db(conn)

def delete_gallery_item(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🗑 Өшириу бөлими", reply_markup=delete_submenu())
        return
    conn, cursor = get_db()
    try:
        if message.text.strip().lower() == "all":
            cursor.execute("DELETE FROM gallery")
            d = cursor.rowcount
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {d} Фото/видео өширилди.", reply_markup=delete_submenu())
        else:
            try:
                rid = int(message.text.strip())
                cursor.execute("SELECT id FROM gallery WHERE id=%s", (rid,))
                if not cursor.fetchone():
                    bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=delete_submenu())
                    return
                cursor.execute("DELETE FROM gallery WHERE id=%s", (rid,))
                conn.commit()
                bot.send_message(message.chat.id, f"✅ ID:{rid} өширилди.", reply_markup=delete_submenu())
            except ValueError:
                msg = bot.send_message(message.chat.id, "❌ ID ямаса <code>all</code> жазыңыз:", reply_markup=back_menu())
                bot.register_next_step_handler(msg, delete_gallery_item)
    finally:
        cursor.close()
        release_db(conn)

def delete_news_item(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "🗑 Өшириу бөлими", reply_markup=delete_submenu())
        return
    conn, cursor = get_db()
    try:
        if message.text.strip().lower() == "all":
            cursor.execute("DELETE FROM user_news")
            d = cursor.rowcount
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {d} Жаңалық өширилди.", reply_markup=delete_submenu())
        else:
            try:
                rid = int(message.text.strip())
                cursor.execute("SELECT id FROM user_news WHERE id=%s", (rid,))
                if not cursor.fetchone():
                    bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=delete_submenu())
                    return
                cursor.execute("DELETE FROM user_news WHERE id=%s", (rid,))
                conn.commit()
                bot.send_message(message.chat.id, f"✅ ID:{rid} өширилди.", reply_markup=delete_submenu())
            except ValueError:
                msg = bot.send_message(message.chat.id, "❌ ID ямаса <code>all</code> жазыңыз:", reply_markup=back_menu())
                bot.register_next_step_handler(msg, delete_news_item)
    finally:
        cursor.close()
        release_db(conn)

# ============================================================
# СТУДЕНТЛЕР, СТАТИСТИКА, т.б.
# ============================================================

@bot.message_handler(func=lambda m: m.text in ["👥 Студентлер","❗ Сабақ болмайды","📈 Статистика","📩 Ус/Ша келген"])
def admin_panel_actions(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫 Сиз админ емессиз!")
        return
    if message.text == "👥 Студентлер":
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT id,username,last_active,full_name FROM students WHERE started=1 ORDER BY last_active DESC")
            started_rows = cursor.fetchall()
            cursor.execute("SELECT id,full_name FROM students WHERE started=0 OR started IS NULL ORDER BY full_name")
            not_started_rows = cursor.fetchall()
        finally:
            cursor.close()
            release_db(conn)
        now_t = now_uz()
        online_count = sum(1 for r in started_rows if _is_online(r[2], now_t))
        text = (f"👥 <b>Студентлер дизими</b>\n✅ Ботқа кирген: <b>{len(started_rows)}</b>\n"
                f"🟢 Онлайн: <b>{online_count}</b> | 🔴 Офлайн: <b>{len(started_rows)-online_count}</b>\n{'─'*30}\n\n")
        if started_rows:
            text += "📲 <b>Ботқа киргенлер:</b>\n\n"
            for i, row in enumerate(started_rows, 1):
                uname = f"@{row[1]}" if row[1] else "—"
                name = row[3] or uname
                status = get_online_status(row[2])
                text += f"{i}. {status}\n   👤 <b>{name}</b>\n   🔗 {uname}\n\n"
        else:
            text += "📭 Еле хеш ким ботқа кирмеген.\n\n"
        if not_started_rows:
            text += f"{'─'*30}\n⏳ <b>Ботқа кирмегенлер ({len(not_started_rows)}):</b>\n"
            for row in not_started_rows:
                text += f"  • {row[1] or '—'} (ID: <code>{row[0]}</code>)\n"
        send_long_message(message.chat.id, text, reply_markup=admin_menu())

    elif message.text == "📈 Статистика":
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT COUNT(*) FROM students"); s = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM schedule"); l = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM user_news"); n = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM materials"); m = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM gallery"); g = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM suggestions"); sg = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT date) FROM attendance"); ad = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM blocked_users"); bl = cursor.fetchone()[0]
        finally:
            cursor.close()
            release_db(conn)
        bot.send_message(message.chat.id,
            f"📈 <b>Статистика:</b>\n\n👥 Студентлер: <b>{s}</b>\n📅 Сабақлар: <b>{l}</b>\n"
            f"📰 Жаңалықлар: <b>{n}</b>\n📚 Материаллар: <b>{m}</b>\n🎞 Галерея: <b>{g}</b>\n"
            f"💡 Ұсыныслар: <b>{sg}</b>\n📊 Барлау күнлери: <b>{ad}</b>\n🔒 Блокланған: <b>{bl}</b>",
            reply_markup=admin_menu())

    elif message.text == "📩 Ус/Ша келген":
        conn, cursor = get_db()
        try:
            cursor.execute("SELECT s.content,s.user_id,s.date,st.username FROM suggestions s LEFT JOIN students st ON s.user_id=st.id ORDER BY s.date DESC")
            rows = cursor.fetchall()
        finally:
            cursor.close()
            release_db(conn)
        if not rows:
            bot.send_message(message.chat.id, "📭 Жоқ.", reply_markup=admin_menu())
            return
        text = f"📩 <b>Ұсыныс/Шағымлар ({len(rows)}):</b>\n\n"
        chunks = []
        for row in rows:
            entry = f"👤 {'@'+row[3] if row[3] else 'Белгисиз'} | <code>{row[1]}</code>\n🕐 {row[2]}\n💬 {row[0]}\n{'─'*25}\n"
            if len(text)+len(entry) > 3800:
                chunks.append(text)
                text = ""
            text += entry
        if text:
            chunks.append(text)
        for i, chunk in enumerate(chunks):
            bot.send_message(message.chat.id, chunk, reply_markup=admin_menu() if i == len(chunks)-1 else None)

    elif message.text == "❗ Сабақ болмайды":
        send_to_students(text="❗ <b>Назер аударыңыз!</b>\nБүгин сабақ болмайды!")
        bot.send_message(message.chat.id, "✅ Жиберилди!", reply_markup=admin_menu())

# ============================================================
# БАРЛАУ — CALLBACK HANDLERS
# ============================================================

def build_attendance_markup(session):
    idx = session["current_index"]
    if idx >= len(session["students"]):
        return None, None
    student_id, student_name = session["students"][idx][0], session["students"][idx][1]
    total = len(session["students"])
    done = len(session["results"])
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("✅ Бар", callback_data=f"att_mark_present_{student_id}"),
        types.InlineKeyboardButton("❌ Жоқ", callback_data=f"att_mark_absent_{student_id}")
    )
    markup.add(types.InlineKeyboardButton("🏁 Жуумаклау", callback_data="att_finish"))
    text = (f"📊 <b>Барлау — {session['para']}-пара: {session['subject']}</b>\n"
            f"📅 {session['date']}\n{'─'*30}\n"
            f"👤 <b>{student_name}</b>\n{'─'*30}\n<i>{done}/{total} белгиленди</i>")
    return text, markup

@bot.callback_query_handler(func=lambda c: c.data.startswith("att_para_"))
def att_select_para(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    parts = call.data.split("_", 3)
    para = int(parts[2])
    subject = parts[3]
    date_str = now_uz().strftime("%Y-%m-%d")
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,full_name FROM students WHERE full_name IS NOT NULL AND full_name!='' ORDER BY full_name")
        students = [[row[0], row[1]] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    if not students:
        bot.answer_callback_query(call.id, "Студентлер дизими бос!")
        bot.edit_message_text("📭 Студентлерде ФИО жоқ.", call.message.chat.id, call.message.message_id)
        return
    session = {"date": date_str, "para": para, "subject": subject, "students": students, "results": {}, "current_index": 0}
    save_attendance_session(call.from_user.id, session)
    text, markup = build_attendance_markup(session)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("att_mark_"))
def att_mark_student(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    admin_id = call.from_user.id
    session = load_attendance_session(admin_id)
    if not session:
        bot.answer_callback_query(call.id, "Сессия табылмады, қайта баслаңыз.")
        return
    parts = call.data.split("_")
    status = parts[2]
    try:
        student_id = int(parts[3])
    except Exception:
        bot.answer_callback_query(call.id, "Қате, қайта баслаңыз.")
        return
    session["results"][student_id] = status
    session["current_index"] += 1
    save_attendance_session(admin_id, session)
    student_name = next((s[1] for s in session["students"] if s[0]==student_id), "—")
    bot.answer_callback_query(call.id, f"{'✅' if status=='present' else '❌'} {student_name}")
    if session["current_index"] >= len(session["students"]):
        finish_attendance(call.message, admin_id)
    else:
        text, markup = build_attendance_markup(session)
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except Exception:
            pass

@bot.callback_query_handler(func=lambda c: c.data=="att_finish")
def att_finish_early(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id)
        return
    session = load_attendance_session(call.from_user.id)
    if not session:
        bot.answer_callback_query(call.id, "Сессия табылмады.")
        return
    bot.answer_callback_query(call.id, "Барлау жуумакланды!")
    finish_attendance(call.message, call.from_user.id)

def finish_attendance(message, admin_id):
    session = load_attendance_session(admin_id)
    if not session:
        return
    delete_attendance_session(admin_id)
    date_str = session["date"]
    para = session["para"]
    subject = session["subject"]
    students = session["students"]
    results = session["results"]
    present_list = []
    absent_list = []
    conn, cursor = get_db()
    try:
        for item in students:
            student_id, student_name = item[0], item[1]
            status = results.get(student_id, "absent")
            cursor.execute("INSERT INTO attendance(date,para,subject,student_id,student_name,status) VALUES(%s,%s,%s,%s,%s,%s)", (date_str, para, subject, student_id, student_name, status))
            if status == "present":
                present_list.append(student_name)
            else:
                absent_list.append((student_id, student_name))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    total = len(students)
    result_text = (f"📊 <b>Барлау нәтийжеси сақланды!</b>\n"
                   f"📅 {date_str} | {para}-пара: <b>{subject}</b>\n{'─'*30}\n"
                   f"✅ Бар: <b>{len(present_list)}/{total}</b>\n❌ Жоқ: <b>{len(absent_list)}/{total}</b>\n{'─'*30}\n")
    if absent_list:
        result_text += "❌ <b>Жоқлар:</b>\n"
        for _, name in absent_list:
            result_text += f"  • {name}\n"
    else:
        result_text += "🎉 Барлық студентлер бар!\n"
    try:
        bot.edit_message_text(result_text, message.chat.id, message.message_id, parse_mode="HTML", reply_markup=None)
    except Exception:
        bot.send_message(message.chat.id, result_text, parse_mode="HTML")

    path = generate_attendance_excel(students, results, date_str, para, subject)
    if path:
        try:
            with open(path, "rb") as f:
                bot.send_document(message.chat.id, f,
                    caption=f"📊 {date_str} | {para}-пара: {subject}",
                    visible_file_name=f"attendance_{date_str}_para{para}.xlsx")
            os.remove(path)
        except Exception as e:
            bot.send_message(message.chat.id, f"⚠️ Excel жиберилмеди: {e}")

    for student_id, student_name in absent_list:
        try:
            bot.send_message(student_id, f"⚠️ <b>Ескертиу!</b>\n\nСиз бүгин <b>{para}-парада</b> (<b>{subject}</b>) болмадыңыз!\n📅 {date_str}\n\nСебебиңизди группаға хабарлаңыз.")
        except Exception:
            pass
    bot.send_message(message.chat.id, "✅ Барлау сақланды!\n📅 Тарихты <b>Барлау тарихы</b> арқалы ашыңыз.", reply_markup=attendance_submenu())

@bot.callback_query_handler(func=lambda c: c.data.startswith("hist_month_"))
def hist_select_month(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    ym = call.data.replace("hist_month_", "")
    year, month = ym.split("-")
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT date FROM attendance WHERE LEFT(date,7)=%s ORDER BY date DESC", (ym,))
        days = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    if not days:
        bot.answer_callback_query(call.id, "Бұл айда барлау жоқ.")
        return
    markup = types.InlineKeyboardMarkup()
    for d in days:
        markup.add(types.InlineKeyboardButton(text=f"📆 {date_to_ru(d)}", callback_data=f"hist_day_{d}"))
    markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data="hist_back_months"))
    bot.edit_message_text(f"📅 <b>{MONTHS_RU.get(int(month), month)} {year}</b>\n\nКүнди таңлаңыз:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data=="hist_back_months")
def hist_back_to_months(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id)
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT LEFT(date,7) as ym FROM attendance ORDER BY ym DESC")
        months = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    markup = types.InlineKeyboardMarkup()
    for ym in months:
        year, month = ym.split("-")
        markup.add(types.InlineKeyboardButton(text=f"📅 {MONTHS_RU.get(int(month), month)} {year}", callback_data=f"hist_month_{ym}"))
    bot.edit_message_text("📅 <b>Барлау тарихы</b>\n\nАйды таңлаңыз:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("hist_day_"))
def hist_select_day(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    date_str = call.data.replace("hist_day_", "")
    ym = date_str[:7]
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT para,subject FROM attendance WHERE date=%s ORDER BY para", (date_str,))
        paras = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not paras:
        bot.answer_callback_query(call.id, "Бұл күнде барлау жоқ.")
        return
    markup = types.InlineKeyboardMarkup()
    for para, subject in paras:
        markup.add(types.InlineKeyboardButton(text=f"📖 {para}-пара: {subject}", callback_data=f"hist_para_{date_str}_{para}"))
    markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"hist_month_{ym}"))
    bot.edit_message_text(f"📆 <b>{date_to_ru(date_str)}</b>\n\nПараны таңлаңыз:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("hist_para_"))
def hist_show_para(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    parts = call.data.split("_")
    date_str = parts[2]
    para = int(parts[3])
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT student_name,status FROM attendance WHERE date=%s AND para=%s ORDER BY student_name", (date_str, para))
        records = cursor.fetchall()
        cursor.execute("SELECT DISTINCT subject FROM attendance WHERE date=%s AND para=%s", (date_str, para))
        subj_row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    subject = subj_row[0] if subj_row else "—"
    present_list = [r[0] for r in records if r[1]=="present"]
    absent_list = [r[0] for r in records if r[1]=="absent"]
    total = len(records)
    text = (f"📊 <b>Барлау нәтийжеси</b>\n📆 {date_to_ru(date_str)} | {para}-пара: <b>{subject}</b>\n{'─'*30}\n"
            f"✅ Бар: <b>{len(present_list)}/{total}</b>\n❌ Жоқ: <b>{len(absent_list)}/{total}</b>\n{'─'*30}\n")
    if present_list:
        text += "✅ <b>Болғанлар:</b>\n" + "".join(f"  • {n}\n" for n in present_list) + "\n"
    if absent_list:
        text += "❌ <b>Болмағанлар:</b>\n" + "".join(f"  • {n}\n" for n in absent_list)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📥 Excel жүклеу", callback_data=f"hist_excel_{date_str}_{para}"))
    markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"hist_day_{date_str}"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("hist_excel_"))
def hist_download_excel(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Тек admin-ге!")
        return
    parts = call.data.split("_")
    date_str = parts[2]
    para = int(parts[3])
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT student_name,status FROM attendance WHERE date=%s AND para=%s ORDER BY student_name", (date_str, para))
        records = cursor.fetchall()
        cursor.execute("SELECT DISTINCT subject FROM attendance WHERE date=%s AND para=%s", (date_str, para))
        subj_row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    subject = subj_row[0] if subj_row else "—"
    path = generate_attendance_excel(records, None, date_str, para, subject)
    if path:
        try:
            with open(path, "rb") as f:
                bot.send_document(call.message.chat.id, f,
                    caption=f"📊 Барлау: {date_str} | {para}-пара: {subject}",
                    visible_file_name=f"attendance_{date_str}_para{para}.xlsx")
            os.remove(path)
            bot.answer_callback_query(call.id, "✅ Excel жиберилди!")
        except Exception as e:
            bot.answer_callback_query(call.id, f"❌ Қате: {e}")
    else:
        bot.answer_callback_query(call.id, "❌ Excel жасалмады (openpyxl жоқ?)")

# ============================================================
# ТЕСТ / ВАРИАНТ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📖 Пәнлер")
def show_variants_menu(message):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT subject FROM test_variants ORDER BY subject")
        subjects = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    if not subjects:
        bot.send_message(message.chat.id, "📭 Еле вариант жүклемеген.", reply_markup=main_menu(message.from_user.id))
        return
    markup = types.InlineKeyboardMarkup()
    for subj in subjects:
        conn2, cursor2 = get_db()
        try:
            cursor2.execute("SELECT COUNT(*) FROM test_variants WHERE subject=%s", (subj,))
            count = cursor2.fetchone()[0]
        finally:
            cursor2.close()
            release_db(conn2)
        markup.add(types.InlineKeyboardButton(text=f"📖 {subj} ({count})", callback_data=f"var_subj_{subj}"))
    bot.send_message(message.chat.id, "📖 <b>Пәнлер</b>\n\nПәнди таңлаңыз:", reply_markup=markup)

@bot.callback_query_handler(func=lambda c: c.data.startswith("var_subj_"))
def show_variants_by_subject(call):
    subj = call.data.replace("var_subj_", "")
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,file_name,file_type,date FROM test_variants WHERE subject=%s ORDER BY date DESC", (subj,))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.answer_callback_query(call.id, "Бұл пәнде файл жоқ.")
        return
    markup = types.InlineKeyboardMarkup()
    for row in rows:
        icon = {"photo":"🖼","document":"📄","video":"🎬"}.get(row[2], "📎")
        name = row[1] or f"Файл #{row[0]}"
        markup.add(types.InlineKeyboardButton(text=f"{icon} {name}", callback_data=f"var_file_{row[0]}"))
    markup.add(types.InlineKeyboardButton("◀️ Артқа", callback_data="var_back"))
    bot.edit_message_text(f"📖 <b>{subj}</b>\n\nФайлды таңлаңыз:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("var_file_"))
def send_variant_file(call):
    vid = int(call.data.replace("var_file_", ""))
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT file_id,file_type,file_name,subject FROM test_variants WHERE id=%s", (vid,))
        row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    if not row:
        bot.answer_callback_query(call.id, "Файл табылмады.")
        return
    file_id, file_type, file_name, subject = row[0], row[1], row[2], row[3]
    cap = f"📖 <b>{subject}</b>\n📎 {file_name or ''}"
    try:
        if file_type == "photo":
            bot.send_photo(call.message.chat.id, file_id, caption=cap)
        elif file_type == "video":
            bot.send_video(call.message.chat.id, file_id, caption=cap)
        else:
            bot.send_document(call.message.chat.id, file_id, caption=cap)
        bot.answer_callback_query(call.id, "✅ Жиберилди!")
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Қате: {e}")

@bot.callback_query_handler(func=lambda c: c.data=="var_back")
def variants_back(call):
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT subject FROM test_variants ORDER BY subject")
        subjects = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    markup = types.InlineKeyboardMarkup()
    for subj in subjects:
        conn2, cursor2 = get_db()
        try:
            cursor2.execute("SELECT COUNT(*) FROM test_variants WHERE subject=%s", (subj,))
            count = cursor2.fetchone()[0]
        finally:
            cursor2.close()
            release_db(conn2)
        markup.add(types.InlineKeyboardButton(text=f"📖 {subj} ({count})", callback_data=f"var_subj_{subj}"))
    bot.edit_message_text("📖 <b>Пәнлер</b>\n\nПәнди таңлаңыз:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: m.text == "📖 Пән басқарыу")
def variant_management(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    bot.send_message(message.chat.id, "📖 <b>Пән басқарыу</b>", reply_markup=panler_admin_submenu())

@bot.message_handler(func=lambda m: m.text == "➕ Пән қосыу")
def variant_add_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT DISTINCT subject FROM test_variants ORDER BY subject")
        existing = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)
    text = "📖 <b>Пән қосыу</b>\n\n<b>Пән атын жазыңыз:</b>\n"
    if existing:
        text += "\n📋 <b>Бар пәнлер:</b>\n" + "".join(f"  • {s}\n" for s in existing)
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, variant_get_subject)

def variant_get_subject(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📖 Пән басқарыу", reply_markup=panler_admin_submenu())
        return
    subject = message.text.strip()
    set_user_state(message.from_user.id, f"variant:{subject}")
    bot.send_message(message.chat.id, f"📖 <b>Пән:</b> {subject}\n\n📎 Файл, фото ямаса видео жибериңиз.\nТайын болғанда <b>⬅️ Артқа</b> басыңыз.", reply_markup=back_menu())

@bot.message_handler(content_types=["document"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("variant:"))
def variant_upload_document(message):
    if is_already_processed(message.message_id):
        return
    state = get_user_state(message.from_user.id)
    subject = state.split(":", 1)[1]
    file_id = message.document.file_id
    file_name = message.document.file_name or f"{subject}_файл_{now_uz().strftime('%H%M%S')}"
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO test_variants(subject,file_id,file_type,file_name,uploader_id) VALUES(%s,%s,%s,%s,%s)", (subject, file_id, "document", file_name, message.from_user.id))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_saved_once(message.chat.id, message.from_user.id)

@bot.message_handler(content_types=["photo"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("variant:"))
def variant_upload_photo(message):
    if is_already_processed(message.message_id):
        return
    state = get_user_state(message.from_user.id)
    subject = state.split(":", 1)[1]
    file_id = message.photo[-1].file_id
    file_name = f"{subject}_фото_{now_uz().strftime('%H%M%S')}"
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO test_variants(subject,file_id,file_type,file_name,uploader_id) VALUES(%s,%s,%s,%s,%s)", (subject, file_id, "photo", file_name, message.from_user.id))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_saved_once(message.chat.id, message.from_user.id)

@bot.message_handler(content_types=["video"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("variant:"))
def variant_upload_video(message):
    if is_already_processed(message.message_id):
        return
    state = get_user_state(message.from_user.id)
    subject = state.split(":", 1)[1]
    file_id = message.video.file_id
    file_name = f"{subject}_видео_{now_uz().strftime('%H%M%S')}"
    conn, cursor = get_db()
    try:
        cursor.execute("INSERT INTO test_variants(subject,file_id,file_type,file_name,uploader_id) VALUES(%s,%s,%s,%s,%s)", (subject, file_id, "video", file_name, message.from_user.id))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)
    send_saved_once(message.chat.id, message.from_user.id)

@bot.message_handler(content_types=["audio","voice","sticker"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("variant:"))
def variant_upload_wrong(message):
    bot.send_message(message.chat.id, "⚠️ Тек файл, фото ямаса видео жибериңиз!")

@bot.message_handler(func=lambda m: m.text == "🗑 Пән өшириу")
def variant_delete_start(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🚫")
        return
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id,subject,file_name,file_type,date FROM test_variants ORDER BY subject,date DESC")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📭 Пән материаллары жоқ.", reply_markup=panler_admin_submenu())
        return
    text = "🗑 <b>Пән материалын өшириу:</b>\n\n"
    for row in rows:
        icon = "🖼" if row[3]=="photo" else ("🎬" if row[3]=="video" else "📄")
        text += f"ID:<code>{row[0]}</code> {icon} [{row[1]}] {row[2] or '—'}\n"
    text += "\nID ямаса <code>all</code> жазыңыз:"
    msg = bot.send_message(message.chat.id, text, reply_markup=back_menu())
    bot.register_next_step_handler(msg, variant_delete_handle)

def variant_delete_handle(message):
    if not message.text or message.text == "⬅️ Артқа":
        bot.send_message(message.chat.id, "📖 Пән басқарыу", reply_markup=panler_admin_submenu())
        return
    conn, cursor = get_db()
    try:
        if message.text.strip().lower() == "all":
            cursor.execute("DELETE FROM test_variants")
            d = cursor.rowcount
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {d} Пән материалы өширилди.", reply_markup=panler_admin_submenu())
        else:
            try:
                vid = int(message.text.strip())
                cursor.execute("SELECT subject,file_name FROM test_variants WHERE id=%s", (vid,))
                row = cursor.fetchone()
                if not row:
                    bot.send_message(message.chat.id, "⚠️ Табылмады.", reply_markup=panler_admin_submenu())
                    return
                cursor.execute("DELETE FROM test_variants WHERE id=%s", (vid,))
                conn.commit()
                bot.send_message(message.chat.id, f"✅ [{row[0]}] {row[1] or '—'} өширилди.", reply_markup=panler_admin_submenu())
            except ValueError:
                msg = bot.send_message(message.chat.id, "❌ ID ямаса all:", reply_markup=back_menu())
                bot.register_next_step_handler(msg, variant_delete_handle)
    finally:
        cursor.close()
        release_db(conn)

# ============================================================
# 📊 САБАҚ/ЕРТЕҢГЕ
# ============================================================

@bot.message_handler(func=lambda m: m.text == "📊 Сабақ/Ертеңге")
def sabak_ertenge_menu(message):
    tomorrow = now_uz() + timedelta(days=1)
    tomorrow_ru = WEEKDAYS_RU[tomorrow.weekday()]
    tomorrow_date = tomorrow.strftime("%d.%m.%Y")
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT subject,time FROM schedule WHERE day=%s ORDER BY time", (tomorrow_ru,))
        lessons = cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)
    if lessons:
        text = f"📊 <b>Ертеңги сабақлар ({tomorrow_ru}, {tomorrow_date}):</b>\n\n"
        for i, row in enumerate(lessons, 1):
            text += f"{i}-пара 🕐 {row[1]} — {row[0]}\n"
        text += "\n👇 Қатнасасыз ба?"
    else:
        text = f"📊 <b>Ертең ({tomorrow_ru}) сабақ жоқ.</b>\n\nДеген менен жууабыңызды белгилеңиз:"
    bot.send_message(message.chat.id, text, reply_markup=sabak_menu())

@bot.message_handler(func=lambda m: m.text == "✅ Бараман")
def sabak_baram(message):
    user_id = message.from_user.id
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT full_name FROM students WHERE id=%s", (user_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    student_name = row[0] if row and row[0] else (
        message.from_user.first_name or f"@{message.from_user.username or user_id}"
    )
    tomorrow = now_uz() + timedelta(days=1)
    tomorrow_ru = WEEKDAYS_RU[tomorrow.weekday()]
    tomorrow_date = tomorrow.strftime("%d.%m.%Y")
    bot.send_message(message.chat.id, f"✅ <b>Жолыңыз болсын!</b>\n📅 Ертең ({tomorrow_ru}) сизди күтемиз! 🎓", reply_markup=main_menu(user_id))
    broadcast_text = (f"📢 <b>Ертеңги сабаққа:</b>\n📅 {tomorrow_ru}, {tomorrow_date}\n{'─'*25}\n✅ <b>{student_name}</b> бараман деди! 🎓")
    send_to_students(text=broadcast_text, exclude_id=user_id)
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, broadcast_text)
        except Exception:
            pass

@bot.message_handler(func=lambda m: m.text == "❌ Себеп бар")
def sabak_sebep_start(message):
    set_user_state(message.from_user.id, "sebep_text")
    bot.send_message(message.chat.id,
        "❌ <b>Себебиңизди жазыңыз:</b>\n\n"
        "📌 Мысал: <i>Ауырып қалдым</i>\n"
        "📌 Мысал: <i>Жұмыстын арзасы бар</i>\n\n"
        "⬇️ Текст жазыңыз:",
        reply_markup=back_menu()
    )

@bot.message_handler(content_types=["text"], func=lambda m: get_user_state(m.from_user.id) == "sebep_text")
def handle_sebep_text(message):
    if message.text == "⬅️ Артқа":
        clear_user_state(message.from_user.id)
        bot.send_message(message.chat.id, "📊 Сабақ/Ертеңге", reply_markup=sabak_menu())
        return
    if len(message.text) > 500:
        bot.send_message(message.chat.id, "❌ Текст дым ұзын (макс 500 таңба). Қысқартып жазыңыз:", reply_markup=back_menu())
        return
    set_user_state(message.from_user.id, f"sebep_file:{message.text.strip()}")
    bot.send_message(message.chat.id,
        "📎 <b>Хұжжет/Справка бар ма?</b>\n\n"
        "Фото ямаса файл жибериңиз\n"
        "Жоқ болса — <b>⏭ Өткизип жибериу</b> басыңыз.",
        reply_markup=_sebep_file_menu()
    )

@bot.message_handler(content_types=["photo"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("sebep_file:"))
def handle_sebep_photo(message):
    state = get_user_state(message.from_user.id)
    sebep_text = state.split(":", 1)[1]
    file_id = message.photo[-1].file_id
    _send_sebep_broadcast(message, sebep_text, file_id=file_id, file_type="photo")

@bot.message_handler(content_types=["document"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("sebep_file:"))
def handle_sebep_document(message):
    state = get_user_state(message.from_user.id)
    sebep_text = state.split(":", 1)[1]
    file_id = message.document.file_id
    _send_sebep_broadcast(message, sebep_text, file_id=file_id, file_type="document")

@bot.message_handler(content_types=["text"], func=lambda m: (get_user_state(m.from_user.id) or "").startswith("sebep_file:"))
def handle_sebep_file_text(message):
    state = get_user_state(message.from_user.id)
    sebep_text = state.split(":", 1)[1]
    if message.text == "⬅️ Артқа":
        set_user_state(message.from_user.id, "sebep_text")
        bot.send_message(message.chat.id, "❌ <b>Себебиңизди қайта жазыңыз:</b>", reply_markup=back_menu())
        return
    if message.text == "⏭ Өткизип жибериу":
        _send_sebep_broadcast(message, sebep_text, file_id=None, file_type=None)
        return
    bot.send_message(message.chat.id, "⚠️ Фото ямаса файл жибериңиз, ямаса <b>⏭ Өткизип жибериу</b> басыңыз.")

def _send_sebep_broadcast(message, sebep_text, file_id=None, file_type=None):
    user_id = message.from_user.id
    clear_user_state(user_id)
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT full_name FROM students WHERE id=%s", (user_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)
    student_name = row[0] if row and row[0] else (
        message.from_user.first_name or f"@{message.from_user.username or user_id}"
    )
    tomorrow = now_uz() + timedelta(days=1)
    tomorrow_ru = WEEKDAYS_RU[tomorrow.weekday()]
    tomorrow_date = tomorrow.strftime("%d.%m.%Y")
    has_doc = file_id is not None
    doc_label = "📷 Фото" if file_type == "photo" else ("📄 Файл" if file_type == "document" else "")
    bot.send_message(message.chat.id, "✅ <b>Себеби жиберилди!</b>\nРахмет, хабарлағаныңыз үшын. 🙏", reply_markup=main_menu(user_id))
    broadcast_text = (
        f"📢 <b>Ертеңги сабаққа:</b>\n"
        f"📅 {tomorrow_ru}, {tomorrow_date}\n{'─'*25}\n"
        f"❌ <b>{student_name}</b> бара алмайды\n\n"
        f"📝 <b>Себеби:</b> {sebep_text}"
        + (f"\n{doc_label} қосып берилди ⬇️" if has_doc else "")
    )
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT id FROM students WHERE started=1 AND id!=%s", (user_id,))
        student_ids = [r[0] for r in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)

    def _broadcast():
        for sid in student_ids:
            try:
                bot.send_message(sid, broadcast_text)
                if file_id and file_type == "photo":
                    bot.send_photo(sid, file_id, caption=f"📷 {student_name} жиберген хұжжет")
                elif file_id and file_type == "document":
                    bot.send_document(sid, file_id, caption=f"📄 {student_name} жиберген хұжжет")
                time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Broadcast қате (sid={sid}): {e}")
                continue

    Thread(target=_broadcast, daemon=True).start()

    admin_text = (
        f"⚠️ <b>Студент ертең келмейди:</b>\n\n"
        f"👤 {student_name} (<code>{user_id}</code>)\n"
        f"📅 {tomorrow_ru}, {tomorrow_date}\n"
        f"📝 Себеби: {sebep_text}"
        + (f"\n{doc_label} жиберилди ⬇️" if has_doc else "\n📎 Хұжжет жоқ")
    )
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, admin_text)
            if file_id and file_type == "photo":
                bot.send_photo(aid, file_id, caption=f"📷 {student_name} — справка/арза")
            elif file_id and file_type == "document":
                bot.send_document(aid, file_id, caption=f"📄 {student_name} — справка/арза")
        except Exception:
            pass

# ============================================================
# 🤖 AI КӨМЕКШІ
# ============================================================

_ai_chat_history = {}
_ai_chat_history_lock = Lock()
_ai_last_active = {}

AI_SYSTEM_PROMPT = (
    "Сен S6-DI-23 группасының ақыллы көмекшисисең. "
    "Сорауларға қысқа, нуска және дослық түрде жууап бер. "
    "Пайдаланушы қай тилде жазса, сол тилде жууап бер (қарақалпақша, қазақша, орысша, английский — бәри болады). "
    "Егер сорау оқыуға, сабаққа, университетке байланыслы болса — итибарлы жууап бер."
)

def _ai_try_groq(messages: list) -> str:
    import requests
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise ValueError("GROQ_API_KEY жоқ")
    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        json={"model": "llama-3.3-70b-versatile", "messages": messages, "max_tokens": 1000, "temperature": 0.7},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()

def _ai_try_openai(messages: list) -> str:
    import requests
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise ValueError("OPENAI_API_KEY жоқ")
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        json={"model": "gpt-4o-mini", "messages": messages, "max_tokens": 1000, "temperature": 0.7},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()

def _ai_try_gemini(user_message: str, history: list) -> str:
    import requests
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY жоқ")
    contents = []
    for msg in history:
        role = "user" if msg["role"] == "user" else "model"
        contents.append({"role": role, "parts": [{"text": msg["content"]}]})
    contents.append({"role": "user", "parts": [{"text": user_message}]})
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}",
        headers={"Content-Type": "application/json"},
        json={
            "system_instruction": {"parts": [{"text": AI_SYSTEM_PROMPT}]},
            "contents": contents,
            "generationConfig": {"maxOutputTokens": 1000, "temperature": 0.7}
        },
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

def ai_ask_groq(user_id: int, user_message: str) -> str:
    with _ai_chat_history_lock:
        if user_id not in _ai_chat_history:
            _ai_chat_history[user_id] = []
        history = list(_ai_chat_history[user_id][-10:])
        _ai_last_active[user_id] = time.time()

    messages = [{"role": "system", "content": AI_SYSTEM_PROMPT}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    answer = None
    for fn, args in [
        (_ai_try_groq, (messages,)),
        (_ai_try_openai, (messages,)),
        (_ai_try_gemini, (user_message, history)),
    ]:
        try:
            answer = fn(*args)
            break
        except Exception as e:
            logger.error(f"❌ {fn.__name__} қате: {type(e).__name__}: {e}")

    if not answer:
        return (
            "❌ <b>AI уақытша жұмыс истемейди.</b>\n\n"
            "Барлық 3 сервис (Groq, OpenAI, Gemini) жууап бермеди.\n"
            "Кейинирек қайталаңыз ямаса admin-ге хабарласыңыз."
        )

    with _ai_chat_history_lock:
        if user_id not in _ai_chat_history:
            _ai_chat_history[user_id] = []
        _ai_chat_history[user_id].append({"role": "user", "content": user_message})
        _ai_chat_history[user_id].append({"role": "assistant", "content": answer})
        if len(_ai_chat_history[user_id]) > 20:
            _ai_chat_history[user_id] = _ai_chat_history[user_id][-20:]

    return answer

def ai_clear_history(user_id: int):
    with _ai_chat_history_lock:
        _ai_chat_history.pop(user_id, None)
        _ai_last_active.pop(user_id, None)

def cleanup_ai_history():
    now = time.time()
    with _ai_chat_history_lock:
        inactive = [uid for uid, t in _ai_last_active.items() if now - t > 7200]
        for uid in inactive:
            _ai_chat_history.pop(uid, None)
            _ai_last_active.pop(uid, None)
    if inactive:
        logger.info(f"AI history cleanup: {len(inactive)} пайдаланушы тазаланды")

def _md_to_html(text: str) -> str:
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text, flags=re.DOTALL)
    text = re.sub(r'__(.+?)__', r'<u>\1</u>', text, flags=re.DOTALL)
    text = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<i>\1</i>', text)
    text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
    return text

@bot.message_handler(func=lambda m: m.text == "🤖 AI Көмекши")
def ai_menu(message):
    user_id = message.from_user.id
    set_user_state(user_id, "ai_chat")
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("🗑 Тарихты тазалау")
    markup.row("⬅️ Артқа")
    bot.send_message(message.chat.id,
        "🤖 <b>AI Көмекши иске қосылды!</b>\n\n"
        "✏️ Кез-келген сорауыңызды жазыңыз.\n"
        "🌐 Қай тилде жазсаңыз, сол тилде жууап береди.\n\n"
        "🗑 Тарихты тазалау — таза сөйлесиу баслау үшын\n"
        "⚡ <i>Groq → OpenAI → Gemini (автоматлы резерв)</i>",
        reply_markup=markup
    )

@bot.message_handler(func=lambda m: m.text == "🗑 Тарихты тазалау", content_types=["text"])
def ai_clear(message):
    if get_user_state(message.from_user.id) != "ai_chat":
        return
    ai_clear_history(message.from_user.id)
    bot.send_message(message.chat.id, "✅ <b>AI тарихы тазаланды!</b>\nТаза сөйлесиу басланды.")

@bot.message_handler(content_types=["text"], func=lambda m: get_user_state(m.from_user.id) == "ai_chat")
def ai_chat_handler(message):
    user_id = message.from_user.id
    text = message.text.strip()
    if text in ("⬅️ Артқа", "🗑 Тарихты тазалау"):
        return
    if not text:
        bot.send_message(message.chat.id, "✏️ Сорауыңызды жазыңыз.")
        return
    bot.send_chat_action(message.chat.id, "typing")
    # FIX #7: барлық сорауда ⏳ хабары — пайдаланушы күтетінін біледі
    wait_msg = bot.send_message(message.chat.id, "⏳ <i>AI ойланып атыр...</i>")
    answer = ai_ask_groq(user_id, text)
    try:
        bot.delete_message(message.chat.id, wait_msg.message_id)
    except Exception:
        pass
    answer_html = _md_to_html(answer)
    try:
        bot.send_message(message.chat.id, f"🤖 {answer_html}", parse_mode="HTML")
    except Exception:
        bot.send_message(message.chat.id, f"🤖 {answer}")

# ============================================================
# AUTO SCHEDULER
# ============================================================

sent_daily = None
sent_birthdays = set()
sent_reminders = set()
_last_cleanup = 0
_last_rate_clean = 0
_last_ai_cleanup = 0

def auto_scheduler():
    global sent_daily, sent_reminders, _last_cleanup, _last_rate_clean, sent_birthdays, _last_ai_cleanup
    while True:
        try:
            now = now_uz()
            today = DAYS_EN_TO_RU.get(now.strftime("%A"), "")

            if now.hour == 8 and 0 <= now.minute < 1:
                day_key = now.strftime("%Y-%m-%d")
                if sent_daily != day_key:
                    conn, cursor = get_db()
                    try:
                        cursor.execute("SELECT subject,time FROM schedule WHERE day=%s ORDER BY time", (today,))
                        lessons = cursor.fetchall()
                    finally:
                        cursor.close()
                        release_db(conn)
                    if lessons:
                        text = f"🌅 <b>Қайырлы таң!</b>\n📅 <b>Бүгинги сабақ кесте ({today}):</b>\n\n"
                        for i, row in enumerate(lessons, 1):
                            text += f"{i}-пара 🕐 {row[1]} — {row[0]}\n"
                        send_to_students(text=text)
                    else:
                        send_to_students(text=f"🌅 <b>Қайырлы таң!</b>\n📭 Бүгин ({today}) сабақ жоқ.")
                    sent_daily = day_key

            if now.hour == 9 and 0 <= now.minute < 1:
                bday_key = now.strftime("%Y-%m-%d")
                if bday_key not in sent_birthdays:
                    conn, cursor = get_db()
                    try:
                        cursor.execute("SELECT id, full_name, birth_date FROM students WHERE birth_date IS NOT NULL AND birth_date != ''")
                        all_students = cursor.fetchall()
                    finally:
                        cursor.close()
                        release_db(conn)
                    today_md = now.strftime("%m-%d")
                    for sid, sname, sbirth in all_students:
                        try:
                            bd_md = str(sbirth)[5:10]
                            if bd_md == today_md:
                                congrats = (
                                    f"🎂 <b>Тууылған күниңиз құтлы болсын, {sname}!</b> 🎉\n\n"
                                    f"🎊 Бүгин сиздиң өзгеше күниңиз!\n"
                                    f"✨ S6-DI-23 группасы сизди шын жүректен құтлықлайды!\n"
                                    f"🌟 Денсаулық, бахыт және аумет тилеймиз! 🥳"
                                )
                                send_to_all_students(text=congrats)
                                try:
                                    bot.send_message(sid, f"🎂 <b>Тууылған күниңиз бенен!</b> 🎉\n\nБүгин сиздиң өзгеше күниңиз!\nS6-DI группасы сизди құтлықлайды! 🥳")
                                except Exception:
                                    pass
                        except Exception:
                            continue
                    sent_birthdays.add(bday_key)
                    if len(sent_birthdays) > 60:
                        old_keys = sorted(sent_birthdays)[:30]
                        for k in old_keys:
                            sent_birthdays.discard(k)

            conn, cursor = get_db()
            try:
                cursor.execute("SELECT subject,time FROM schedule WHERE day=%s", (today,))
                lessons = cursor.fetchall()
            finally:
                cursor.close()
                release_db(conn)
            for row in lessons:
                subj, time_str = row[0], row[1]
                try:
                    lesson_time = datetime.strptime(time_str.strip(), "%H:%M").replace(
                        year=now.year, month=now.month, day=now.day)
                    reminder_time = lesson_time - timedelta(minutes=3)
                    remind_key = f"{now.strftime('%Y-%m-%d')}-{time_str}"
                    if reminder_time <= now < lesson_time and remind_key not in sent_reminders:
                        send_to_students(text=f"⏰ <b>Назер аударыңыз!</b>\n3 минуттан кейин сабақ басланады!\n\n📖 <b>{subj}</b>\n🕐 {time_str}")
                        sent_reminders.add(remind_key)
                except Exception:
                    continue

            if now.hour == 0 and now.minute == 0:
                sent_reminders.clear()

            now_ts = time.time()
            if now_ts - _last_cleanup > 1800:
                cleanup_old_sessions()
                _last_cleanup = now_ts
            if now_ts - _last_rate_clean > 300:
                clean_rate_limit()
                _last_rate_clean = now_ts
            if now_ts - _last_ai_cleanup > 3600:
                cleanup_ai_history()
                _last_ai_cleanup = now_ts

        except Exception as e:
            logger.error(f"auto_scheduler error: {e}")
        time.sleep(20)

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    # 409 конфликтін болдырмау — іске қосылмай тұрып polling тоқтату
    try:
        bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook тазаланды")
    except Exception as e:
        logger.warning(f"Webhook тазалау: {e}")
    time.sleep(2)
    Thread(target=auto_scheduler, daemon=True).start()
    # 409 конфликтті болдырмау үшін алдымен webhook тазалаймыз
    try:
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(2)
    except Exception as e:
        logger.warning(f"delete_webhook: {e}")

    def _polling():
        while True:
            try:
                bot.infinity_polling(
                    skip_pending=True, timeout=60,
                    long_polling_timeout=60, none_stop=False,
                    allowed_updates=["message", "callback_query"]
                )
            except Exception as e:
                logger.error(f"Polling қате: {e}")
                time.sleep(5)

    Thread(target=_polling, daemon=True).start()
    logger.info("✅ Бот іске қосылды!")
    logger.info("✅ AI жүйесі қосылды: Groq → OpenAI → Gemini (резерв)")
    logger.info(f"✅ Admin IDлер: {ADMIN_IDS}")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
