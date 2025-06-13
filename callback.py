from flask import Flask, request, jsonify
import sqlite3
import os
from dotenv import load_dotenv
import hashlib
import logging
from datetime import datetime
import asyncio
import discord
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('callback.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
app = Flask(__name__)

# Configuration (should be stored in .env)
PARTNER_ID = os.getenv("PARTNER_ID", "-1022521568")
API_KEY = os.getenv("API_KEY", "Fshukr0Ewx7n3vmdDUfSLqqaX7Uf5gUR")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Discord bot setup
discord_intents = discord.Intents.default()
discord_intents.message_content = True
callback_bot = discord.Client(intents=discord_intents)

def get_db_connection():
    conn = sqlite3.connect('napthe.db', timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def validate_signature(code, serial, received_sign):
    expected_sign = hashlib.md5(f"{API_KEY}{code}{serial}".encode()).hexdigest()
    return expected_sign == received_sign

def normalize_status_message(status, message=""):
    status_str = str(status)
    status_mapping = {
        '1': {'display': 'THÀNH CÔNG', 'description': 'Giao dịch đã được xử lý thành công', 'color': 'success'},
        '99': {'display': 'ĐANG XỬ LÝ', 'description': 'Giao dịch đang được xử lý', 'color': 'warning'},
        '3': {'display': 'THẤT BẠI', 'description': 'Thẻ không hợp lệ hoặc đã sử dụng', 'color': 'error'},
        '100': {'display': 'LỖI HỆ THỐNG', 'description': 'Lỗi từ phía nhà cung cấp', 'color': 'error'},
        '2': {'display': 'ĐANG KẾT NỐI', 'description': 'Đang kết nối với nhà mạng', 'color': 'info'},
        '4': {'display': 'HẾT HẠN', 'description': 'Thẻ đã hết hạn sử dụng', 'color': 'error'},
        '5': {'display': 'SAI MỆNH GIÁ', 'description': 'Mệnh giá thẻ không đúng', 'color': 'error'}
    }
    info = status_mapping.get(status_str, None)
    if info:
        return status_str, message or info['description'], info['display']
    logger.warning(f"Unknown status received: {status_str}")
    return status_str, message or f"Trạng thái không xác định: {status_str}", "KHÔNG XÁC ĐỊNH"

async def notify_discord_user(request_data, new_status, new_message, received_amount):
    try:
        if not callback_bot.is_ready():
            logger.warning("Discord bot not ready, skipping notification")
            return

        user = await callback_bot.fetch_user(int(request_data['discord_user_id']))
        status_str, final_message, display_status = normalize_status_message(new_status, new_message)

        color_mapping = {
            '1': 0x00ff00, '99': 0xffff00, '3': 0xff0000,
            '100': 0xff0000, '2': 0x0099ff, '4': 0xff6600, '5': 0xff3300
        }
        embed_color = color_mapping.get(status_str, 0x808080)

        status_icons = {
            '1': '✅', '99': '⏳', '3': '❌', '100': '⚠️', '2': '🔄', '4': '⏰', '5': '💰'
        }
        status_icon = status_icons.get(status_str, '❓')

        embed = discord.Embed(
            title=f"TRẠNG THÁI THẺ {request_data['telco'].upper()}",
            color=embed_color,
            timestamp=datetime.now()
        )
        embed.add_field(name="Request ID", value=f"`{request_data['request_id']}`", inline=False)
        embed.add_field(name="Mệnh giá", value=f"{request_data['amount']:,} VND", inline=True)
        embed.add_field(name="Trạng thái", value=f"{status_icon} {display_status}", inline=True)
        if final_message:
            embed.add_field(name="Chi tiết", value=final_message, inline=False)
        if status_str == '1' and received_amount > 0:
            embed.add_field(name="Thực nhận", value=f"{received_amount:,} VND", inline=True)
            rate = (received_amount / request_data['amount'] * 100) if request_data['amount'] > 0 else 0
            embed.add_field(name="Tỷ lệ", value=f"{rate:.1f}%", inline=True)

        await user.send(embed=embed)
        logger.info(f"Discord notification sent to {user.id} for request {request_data['request_id']}")
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")

def run_discord_bot():
    @callback_bot.event
    async def on_ready():
        logger.info(f'Callback Discord bot {callback_bot.user} is ready!')

    asyncio.set_event_loop(asyncio.new_event_loop())
    callback_bot.run(DISCORD_TOKEN, log_handler=None)

@app.route('/')
def home():
    return jsonify({'status': 'ok', 'message': '✅ Callback server is running.', 'timestamp': datetime.now().isoformat()})

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    return jsonify({'error': 'Internal server error'}), 500

def initialize_app():
    logger.info("Initializing callback server...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS napthe_requests (request_id TEXT PRIMARY KEY)")
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

if __name__ == '__main__':
    initialize_app()
    threading.Thread(target=run_discord_bot, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
