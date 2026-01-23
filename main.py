import pandas as pd
import datetime
from datetime import date
import asyncio
import pytz
import logging
from dotenv import load_dotenv
import os
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes
import nest_asyncio
import sqlite3

MSK = pytz.timezone('Europe/Moscow')

def msk_today():
    return datetime.datetime.now(MSK).date()

def msk_date(offset_days=0):
    return (datetime.datetime.now(MSK) + datetime.timedelta(days=offset_days)).date()
load_dotenv('.env')
bot = Bot(token=os.getenv('BOT_TOKEN'))
SCHEDULE_FILE = os.getenv('SCHEDULE_FILE')
GROUPS = os.getenv('GROUPS', '').split(',')
logging.basicConfig(level=logging.INFO)

print(f"📄 Файл: {SCHEDULE_FILE}")
class SubsDB:
    def __init__(self, db_path='subscribers.db'):
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def get_active_ids(self):
        with self._connect() as conn:
            return [row[0] for row in conn.execute(
                'SELECT chat_id FROM subscribers WHERE subscribed=1'
            ).fetchall()]

    def add_subscriber(self, chat_id: int, username: str, first_name: str):
        with self._connect() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY, username TEXT, 
                    first_name TEXT, subscribed INTEGER DEFAULT 1
                )
            ''')
            conn.execute('INSERT OR REPLACE INTO subscribers VALUES (?, ?, ?, 1)',
                         (chat_id, username[:32], first_name[:64]))
            conn.commit()

db = SubsDB()
def list_subs():
    """Список подписчиков"""
    with db._connect() as conn:
        subs = conn.execute('SELECT chat_id, username, first_name FROM subscribers WHERE subscribed=1').fetchall()
    if not subs:
        print("❌ Подписчиков нет")
        return
    print("📋 Подписчики:")
    for chat_id, username, first_name in subs:
        print(f"  {chat_id} | {username} | {first_name}")



def load_schedule(target_date=None):
    if target_date is None:
        target_date = msk_today()
    target_str = target_date.strftime('%d.%m.%Y')

    try:
        df = pd.read_excel('schedule_test.xlsx')
        groups = ["40.05.03.К 2025_1", "40.05.03.К 2025_1/1",
                  "Поток 20 (40.05.03.К 2025_1, 40.05.03.К 2025_2, 40.05.03.К 2025_3, 40.05.03.К 2025_4)"]

        df['Группа'] = df['Группа'].astype(str).str.strip()
        df['Дата'] = df['Дата'].astype(str).str.strip()

        filtered = df[(df['Группа'].isin(groups)) & (df['Дата'] == target_str)]
        return filtered.to_dict('records')
    except Exception as e:
        print(f"❌ {e}")
        return []


def beautiful_format(schedule, date_label="Сегодня"):
    if not schedule:
        return f"<b>{date_label}: пар нет!</b>"

    msg = f"<b>📚 {date_label}</b>\n\n"
    for i, lesson in enumerate(schedule, 1):
        msg += f"<b>Пара {i}</b>\n🕐 <b>{lesson.get('Время', 'N/A')}</b>\n"
        msg += f"📖 {lesson.get('Дисциплина', 'N/A')}\n👨‍🏫 {lesson.get('Преподаватель', 'N/A')}\n📍 {lesson.get('Аудитория', 'N/A')}\n"
        if lesson.get('Вид занятия / форма контроля', 'N/A') != 'N/A':
            msg += f"📋 {lesson.get('Вид занятия / форма контроля')}\n"
        theme = lesson.get('Тема', 'N/A')
        if "Тема" in theme:
            msg += f"{theme}\n"
        else:
            msg += f"Тема {theme}\n"
        msg += "➖➖➖\n\n"
    return msg

async def send_broadcast(message: str, label: str = "Рассылка"):
    print(f"🌅 {label}...")
    for chat_id in db.get_active_ids():
        try:
            await bot.send_message(chat_id, message, parse_mode="HTML")
            print(f"✅ {label} → {chat_id}")
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"❌Ошибка:  {label} → {chat_id}: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username or "anon"
    first_name = update.effective_user.first_name or "anon"

    db.add_subscriber(chat_id, username, first_name)
    keyboard = [
        [KeyboardButton("Сегодня"), KeyboardButton("Завтра"), KeyboardButton("Неделя")]
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False
    )
    await update.message.reply_text(
        f"✅ Подписка!\n👤 {first_name} (@{username})\n📊 Подписчиков: {len(db.get_active_ids())}", reply_markup=reply_markup,
        parse_mode="HTML"
    )
from telegram.ext import MessageHandler, filters

async def generate_week_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        today = msk_today()
        days_to_this_monday = today.weekday()
        monday = today - datetime.timedelta(days=days_to_this_monday)

        for i in range(7):  # Пн → Вс
            current_date = monday + datetime.timedelta(days=i)
            date_label = current_date.strftime('%d.%m.%Y (%A)')
            schedule_data = load_schedule(current_date)
            formatted = beautiful_format(schedule_data, date_label)
            await update.message.reply_text(formatted, parse_mode="HTML")
        print('Расписание на неделю отправлено!')
    except Exception as e:
        print(f'Расписание на неделю не отправлено! Ошибка: {e}')
async def week_schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_ids = db.get_active_ids()
    if not chat_ids:
        await update.message.reply_text("Нет подписчиков")
        return

    await update.message.reply_text("Отправляю расписание на неделю...")
    await generate_week_schedule(update, context)

async def today_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        today = msk_today()
        date_label = today.strftime('%d.%m.%Y (%A)')
        schedule_data = load_schedule(today)
        formatted = beautiful_format(schedule_data, date_label)
        await update.message.reply_text(formatted, parse_mode="HTML")
        print('Расписание на сегодня отправлено!')
    except Exception as e:
        print(f'Расписание на сегодня не отправлено! Ошибка: {e}')

async def today_load_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_ids = db.get_active_ids()
    if not chat_ids:
        await update.message.reply_text("Нет подписчиков")
        return

    await update.message.reply_text("Отправляю расписание на сегодня...")
    await today_load(update, context)


async def tomorrow_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        today = (msk_today()+datetime.timedelta(days=1))
        date_label = today.strftime('%d.%m.%Y (%A)')
        schedule_data = load_schedule(today)
        formatted = beautiful_format(schedule_data, date_label)
        await update.message.reply_text(formatted, parse_mode="HTML")
        print('Расписание на завтра отправлено!')
    except Exception as e:
        print(f'Расписание на завтра не отправлено! Ошибка: {e}')

async def tomorrow_load_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_ids = db.get_active_ids()
    if not chat_ids:
        await update.message.reply_text("Нет подписчиков")
        return

    await update.message.reply_text("Отправляю расписание на завтра...")
    await tomorrow_load(update, context)
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "Сегодня":
        await today_load(update, context)  # ✅ await + скобки

    elif text == "Завтра":
        await tomorrow_load(update, context)  # ✅ await + скобки

    elif text == "Неделя":
        await generate_week_schedule(update, context)  # ✅ await + скобки

async def run_all():
    app = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))
    async def scheduler():
        while True:
            now = datetime.datetime.now(MSK)

            # 05:00 — сегодня
            if now.hour == 5 and now.minute <= 2:
                today = load_schedule()
                await send_broadcast(beautiful_format(today, "Сегодня"), "Сегодня")
                await asyncio.sleep(120)

            # 19:35 — завтра
            elif now.hour == 18 and now.minute == 10:
                tomorrow = load_schedule(msk_date(1))
                await send_broadcast(beautiful_format(tomorrow, "Завтра"), "Завтра")
                await asyncio.sleep(120)

            await asyncio.sleep(30)

    asyncio.create_task(scheduler())
    print("🚀 Бот + авто-рассылки!")
    await app.run_polling()





if __name__ == "__main__":
    nest_asyncio.apply()
    print("🧪 Запуск...")
    list_subs()
    asyncio.run(run_all())
