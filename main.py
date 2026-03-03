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
    def __init__(self, db_path='/data/subscribers.db'):
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def get_active_ids(self):
        with self._connect() as conn:
            return [row[0] for row in conn.execute(
                'SELECT chat_id FROM subscribers WHERE subscribed=1'
            ).fetchall()]

    def add_subscriber(self, chat_id: int, grp: str, subgrp: str):
        with self._connect() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY, 
                    grp TEXT,
                    subgrp TEXT,
                    potok TEXT DEFAULT `Поток 20 (40.05.03.К 2025_1, 40.05.03.К 2025_2, 40.05.03.К 2025_3, 40.05.03.К 2025_4)`,
                    subscribed INTEGER DEFAULT 1
                )
            ''')
            conn.execute('''INSERT OR REPLACE INTO subscribers 
                            (chat_id, grp, subgrp, potok, subscribed) 
                            VALUES (?, ?, ?, ?, 1)''',
                         (chat_id,  grp, subgrp,
                          "Поток 20 (40.05.03.К 2025_1, 40.05.03.К 2025_2, 40.05.03.К 2025_3, 40.05.03.К 2025_4)"))
            conn.commit()
    def get_user_info(self, chat_id: int):
        with self._connect() as conn:
            cursor = conn.execute(
                'SELECT grp, subgrp from subscribers WHERE chat_id=? AND subscribed=1', (chat_id,)
            )
            result = cursor.fetchone()
            return result if result else (None, None)


    def get_active_subs_with_groups(self):
        with self._connect() as conn:
            return conn.execute(
                'SELECT chat_id, grp, subgrp FROM subscribers WHERE subscribed=1'
            ).fetchall()

db = SubsDB()





def get_all_groups():
    df = pd.read_excel(SCHEDULE_FILE)
    list_of_groups = df['Группа'].dropna().unique().tolist()
    return sorted(list_of_groups)
def list_subs():
    """Список подписчиков"""
    with db._connect() as conn:
        subs = conn.execute('SELECT chat_id, grp, subgrp, potok FROM subscribers WHERE subscribed=1').fetchall()
    if not subs:
        print("❌ Подписчиков нет")
        return
    print("📋 Подписчики:")
    for chat_id, grp, subgrp, potok in subs:
        print(f"  {chat_id} | {grp} | {subgrp} | {potok}")



def load_schedule(target_date=None, chat_id=None):
    if target_date is None:
        target_date = msk_today()
    target_str = target_date.strftime('%d.%m.%Y')

    try:
        df = pd.read_excel(SCHEDULE_FILE)
        df['Группа'] = df['Группа'].astype(str).str.strip()
        df['Дата'] = df['Дата'].astype(str).str.strip()
        potok = 'Поток 20 (40.05.03.К 2025_1, 40.05.03.К 2025_2, 40.05.03.К 2025_3, 40.05.03.К 2025_4)'
        if chat_id:
            grp, subgrp = db.get_user_info(chat_id)
            if subgrp:
                filtered = df[((df['Группа'] == subgrp) | (df['Группа'] == grp) | (df['Группа'] == potok)) & (df['Дата'] == target_str)]
                if not filtered.empty:
                    return filtered.to_dict('records')
            filtered = df[(df['Группа'] == grp) & (df['Дата'] == target_str)]
            if not filtered.empty:
                return filtered.to_dict('records')


        potok = "Поток 20 (40.05.03.К 2025_1, 40.05.03.К 2025_2, 40.05.03.К 2025_3, 40.05.03.К 2025_4)"
        filtered = df[(df['Группа'] == potok) & (df['Дата'] == target_str)]
        if not filtered.empty:
            return filtered.to_dict('records')


        return df[df['Дата'] == target_str].to_dict('records')

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

async def send_broadcast(date_label="Сегодня"):
    subs = db.get_active_subs_with_groups()
    if not subs:
        print('DB is empty')
        return
    target_date = msk_today() if date_label == "Сегодня" else msk_date(1)
    for chat_id, grp, subgrp in subs:
        try:
            schedule = load_schedule(target_date, chat_id)
            msg = beautiful_format(schedule, date_label)
            await bot.send_message(chat_id, msg, parse_mode="HTML")
            print(f"✅ {subgrp} → {chat_id}")
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"❌Ошибка:  {subgrp} → {chat_id}: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username or "anon"
    first_name = update.effective_user.first_name or "anon"
    keyboard_start = [
        [KeyboardButton("40.05.03.К 2025_1/1"), KeyboardButton("40.05.03.К 2025_1/2")],
        [KeyboardButton("40.05.03.К 2025_2/1"), KeyboardButton("40.05.03.К 2025_2/2")],
        [KeyboardButton("40.05.03.К 2025_3/1"), KeyboardButton("40.05.03.К 2025_3/2")],
        [KeyboardButton("40.05.03.К 2025_4/1"), KeyboardButton("40.05.03.К 2025_4/2")]
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard=keyboard_start,
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await update.message.reply_text(
        f"👋 <b>{first_name}</b>\n\nВыбери группу:",
        reply_markup=reply_markup, parse_mode="HTML"
    )

from telegram.ext import MessageHandler, filters

async def generate_week_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        today = msk_today()
        days_to_this_monday = today.weekday()
        monday = today - datetime.timedelta(days=days_to_this_monday)

        for i in range(7):  # Пн → Вс
            current_date = monday + datetime.timedelta(days=i)
            date_label = current_date.strftime('%d.%m.%Y (%A)')
            schedule_data = load_schedule(current_date, chat_id)
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
        chat_id = update.effective_chat.id
        today = msk_today()
        date_label = today.strftime('%d.%m.%Y (%A)')
        schedule_data = load_schedule(today, chat_id)
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
        chat_id = update.effective_chat.id
        today = (msk_today()+datetime.timedelta(days=1))
        date_label = today.strftime('%d.%m.%Y (%A)')
        schedule_data = load_schedule(today, chat_id)
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
    chat_id = update.effective_chat.id
    if "/1" in text or "/2" in text:
        groups = {
            "40.05.03.К 2025_1/1": ("40.05.03.К 2025_1", "40.05.03.К 2025_1/1"),
            "40.05.03.К 2025_1/2": ("40.05.03.К 2025_1", "40.05.03.К 2025_1/2"),
            "40.05.03.К 2025_2/1": ("40.05.03.К 2025_2", "40.05.03.К 2025_2/1"),
            "40.05.03.К 2025_2/2": ("40.05.03.К 2025_2", "40.05.03.К 2025_2/2"),
            "40.05.03.К 2025_3/1": ("40.05.03.К 2025_3", "40.05.03.К 2025_3/1"),
            "40.05.03.К 2025_3/2": ("40.05.03.К 2025_3", "40.05.03.К 2025_3/2"),
            "40.05.03.К 2025_4/1": ("40.05.03.К 2025_4", "40.05.03.К 2025_4/1"),
            "40.05.03.К 2025_4/2": ("40.05.03.К 2025_4", "40.05.03.К 2025_4/2")
        }

        if text in groups:
            grp, subgrp = groups[text]
            db.add_subscriber(chat_id, grp, subgrp)
            keyboard = [[KeyboardButton("Сегодня"), KeyboardButton("Завтра"), KeyboardButton("Неделя")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

            await update.message.reply_text(
                f"✅ <b>{grp}</b> / <b>{subgrp}</b> выбрана!\n\n"
                f"\n📊 Подписчиков: {len(db.get_active_ids())}",
                reply_markup=reply_markup, parse_mode="HTML"
            )
            return
    if text == "Сегодня":
        await today_load(update, context)

    elif text == "Завтра":
        await tomorrow_load(update, context)

    elif text == "Неделя":
        await generate_week_schedule(update, context)

async def run_all():
    app = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))
    async def scheduler():
        while True:
            now = datetime.datetime.now(MSK)


            if now.hour == 4 and now.minute == 30:
                today = load_schedule()
                await send_broadcast("Сегодня")
                await asyncio.sleep(120)


            elif now.hour == 17 and now.minute == 20:
                tomorrow = load_schedule(msk_date(1))
                await send_broadcast("Завтра")
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
