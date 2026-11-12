import os
import asyncio
import threading
from flask import Flask, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------- Flask App ----------
app = Flask(__name__)

@app.route('/')
def health_check():
    return jsonify({"status": "Bot is running!"}), 200

@app.route('/ping')
def ping():
    return "pong", 200

# ---------- Telegram Bot ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Render पर env variable set करें
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set in environment")

# बॉट के कमांड हैंडलर
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("नमस्ते! मैं एक लाइव बॉट हूँ। 🚀")

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

# बॉट एप्लिकेशन बनाएँ
application = Application.builder().token(BOT_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("ping", ping_command))

# ---------- बॉट को अलग थ्रेड में चलाएँ ----------
def run_bot():
    print("🤖 Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# ---------- मुख्य प्रवेश बिंदु ----------
if __name__ == "__main__":
    # बॉट को एक अलग थ्रेड में शुरू करें
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Flask सर्वर (Render के लिए)
    port = int(os.environ.get("PORT", 5000))
    print(f"🌐 Web server running on port {port}")
    app.run(host="0.0.0.0", port=port)
