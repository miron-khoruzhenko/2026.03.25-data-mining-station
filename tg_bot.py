import os
import sys
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

# Подключаем пути для доступа к базе
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.db.db_manager import DBManager

TG_TOKEN = "5835450415:AAFSBoAx4vB0w6BPeM4z8s4rbAyBnMP7Q2o"
ALLOWED_CHAT_ID = 387276184 # Целое число, без кавычек! Например: 123456789

bot = telebot.TeleBot(TG_TOKEN)
db = DBManager()

def get_keyboard():
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(KeyboardButton("📊 Статус базы"), KeyboardButton("🗑 Ошибки/Пустые"))
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    if message.chat.id != ALLOWED_CHAT_ID:
        return bot.reply_to(message, "⛔ Доступ запрещен.")
    
    bot.reply_to(message, "Привет! Я интерфейс мониторинга твоего парсера.\nВыбери действие ниже 👇", reply_markup=get_keyboard())

@bot.message_handler(func=lambda msg: msg.text == "📊 Статус базы")
def check_status(message):
    if message.chat.id != ALLOWED_CHAT_ID: return
    
    try:
        with db.get_connection() as conn:
            # Считаем статистику Скрейпера (Карточки)
            items = conn.execute("SELECT status, COUNT(*) as cnt FROM scraper_items GROUP BY status").fetchall()
            scraper_stats = {row['status']: row['cnt'] for row in items}
            
            pending = scraper_stats.get('pending', 0)
            done = scraper_stats.get('done', 0)
            errors = scraper_stats.get('error', 0)
            empty = scraper_stats.get('empty', 0)

            text = (
                f"🗄 <b>Статус Карточек (Scraper):</b>\n\n"
                f"⏳ В очереди: <b>{pending}</b>\n"
                f"✅ Готово: <b>{done}</b>\n"
                f"❌ Ошибок: <b>{errors}</b>\n"
                f"🕳 Пустых: <b>{empty}</b>\n\n"
                f"<i>Всего в базе: {sum(scraper_stats.values())}</i>"
            )
            bot.reply_to(message, text, parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"Ошибка чтения БД: {e}")

@bot.message_handler(func=lambda msg: msg.text == "🗑 Ошибки/Пустые")
def check_errors(message):
    if message.chat.id != ALLOWED_CHAT_ID: return
    bot.reply_to(message, "Эта функция пока просто показывает текст. Если захочешь — добавим кнопку 'Вернуть пустые в очередь' прямо из Телеграма!")

if __name__ == "__main__":
    print("[*] Telegram Бот запущен...")
    # Бесконечный легкий цикл (Long Polling)
    bot.infinity_polling()