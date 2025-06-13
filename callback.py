from flask import Flask, request, jsonify
import sqlite3
import os
from dotenv import load_dotenv
import hashlib
import logging
from datetime import datetime
import asyncio
import discord
from discord.ext import tasks
import threading
import time

# Setup logging với format chi tiết hơn
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('callback.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

# Configuration - Đồng bộ với bot
PARTNER_ID = '-1022521568'
API_KEY = 'Fshukr0Ewx7n3vmdDUfSLqqaX7Uf5gUR'
DISCORD_TOKEN = os.getenv('TOKEN')

# Discord bot setup cho callback notifications
discord_intents = discord.Intents.default()
discord_intents.message_content = True
callback_bot = discord.Client(intents=discord_intents)

def get_db_connection():
    """Tạo kết nối database với timeout"""
    conn = sqlite3.connect('napthe.db', timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def validate_signature(code, serial, received_sign):
    """Validate signature từ Card2K"""
    expected_sign = hashlib.md5(f"{API_KEY}{code}{serial}".encode()).hexdigest()
    return expected_sign == received_sign

def normalize_status_message(status, message=""):
    """Chuẩn hóa status và message theo Card2K API"""
    status_str = str(status)
    
    # Mapping status theo Card2K API documentation
    status_mapping = {
        '1': {
            'display': 'THÀNH CÔNG',
            'description': 'Giao dịch đã được xử lý thành công',
            'color': 'success'
        },
        '99': {
            'display': 'ĐANG XỬ LÝ', 
            'description': 'Giao dịch đang được xử lý',
            'color': 'warning'
        },
        '3': {
            'display': 'THẤT BẠI',
            'description': 'Thẻ không hợp lệ hoặc đã sử dụng',
            'color': 'error'
        },
        '100': {
            'display': 'LỖI HỆ THỐNG',
            'description': 'Lỗi từ phía nhà cung cấp',
            'color': 'error'
        },
        '2': {
            'display': 'ĐANG KẾT NỐI',
            'description': 'Đang kết nối với nhà mạng',
            'color': 'info'
        },
        '4': {
            'display': 'HẾT HẠN',
            'description': 'Thẻ đã hết hạn sử dụng',
            'color': 'error'
        },
        '5': {
            'display': 'SAI MỆNH GIÁ',
            'description': 'Mệnh giá thẻ không đúng',
            'color': 'error'
        }
    }
    
    if status_str in status_mapping:
        status_info = status_mapping[status_str]
        final_message = message if message else status_info['description']
        return status_str, final_message, status_info['display']
    else:
        # Unknown status - giữ nguyên và log
        logger.warning(f"Unknown status received: {status_str}")
        return status_str, message or f"Trạng thái không xác định: {status_str}", "KHÔNG XÁC ĐỊNH"

async def notify_discord_user(request_data, new_status, new_message, received_amount):
    """Gửi thông báo cập nhật cho user qua Discord DM"""
    try:
        if not callback_bot.is_ready():
            logger.warning("Discord bot not ready, skipping notification")
            return

        user_id = int(request_data['discord_user_id'])
        user = await callback_bot.fetch_user(user_id)
        
        # Tạo embed theo format của bot chính
        status_str, final_message, display_status = normalize_status_message(new_status, new_message)
        
        # Màu sắc theo status
        color_mapping = {
            '1': 0x00ff00,    # Xanh lá - Thành công
            '99': 0xffff00,   # Vàng - Đang xử lý
            '3': 0xff0000,    # Đỏ - Thất bại
            '100': 0xff0000,  # Đỏ - Lỗi
            '2': 0x0099ff,    # Xanh dương - Đang kết nối
            '4': 0xff6600,    # Cam - Hết hạn
            '5': 0xff3300     # Đỏ cam - Sai mệnh giá
        }
        
        embed_color = color_mapping.get(status_str, 0x808080)
        
        # Icon theo status
        status_icons = {
            '1': '✅',
            '99': '⏳', 
            '3': '❌',
            '100': '⚠️',
            '2': '🔄',
            '4': '⏰',
            '5': '💰'
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
        
        # Hiển thị thực nhận nếu thành công
        if status_str == '1' and received_amount > 0:
            embed.add_field(name="Thực nhận", value=f"{received_amount:,} VND", inline=True)
            rate = (received_amount / request_data['amount'] * 100) if request_data['amount'] > 0 else 0
            embed.add_field(name="Tỷ lệ", value=f"{rate:.1f}%", inline=True)
        
        # Description theo status
        if status_str == '1':
            embed.description = f"✅ **GIAO DỊCH THÀNH CÔNG**\n{final_message}"
        elif status_str == '99':
            embed.description = f"⏳ **ĐANG XỬ LÝ**\n{final_message}"
        elif status_str in ['3', '100', '4', '5']:
            embed.description = f"❌ **GIAO DỊCH THẤT BẠI**\n{final_message}"
        else:
            embed.description = f"❓ **TRẠNG THÁI KHÔNG XÁC ĐỊNH**\n{final_message}"
        
        await user.send(embed=embed)
        logger.info(f"Successfully sent Discord notification to user {user_id} for request {request_data['request_id']}")
        
    except discord.NotFound:
        logger.error(f"Discord user {request_data['discord_user_id']} not found")
    except discord.Forbidden:
        logger.error(f"Cannot send DM to user {request_data['discord_user_id']} - DMs disabled")
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")

def run_discord_bot():
    """Chạy Discord bot trong thread riêng"""
    @callback_bot.event
    async def on_ready():
        logger.info(f'Callback Discord bot {callback_bot.user} is ready!')
    
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
        callback_bot.run(DISCORD_TOKEN, log_handler=None)
    except Exception as e:
        logger.error(f"Discord bot error: {e}")

# Routes
@app.route('/')
def home():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok', 
        'message': '✅ Callback server for Card2K is running.',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0'
    })

@app.route('/health')
def health():
    """Detailed health check"""
    try:
        # Test database connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM napthe_requests")
            total_requests = cursor.fetchone()[0]
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'discord_bot': 'connected' if callback_bot.is_ready() else 'disconnected',
            'total_requests': total_requests,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/status/<request_id>')
@app.route('/callback')
def get_status(request_id=None):
    """GET - Kiểm tra trạng thái giao dịch"""
    
    # Lấy request_id từ URL param hoặc query param
    if not request_id:
        request_id = request.args.get('request_id')
    
    if not request_id:
        return jsonify({'error': 'Missing request_id parameter'}), 400
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT request_id, discord_user_id, telco, amount, received_amount, 
                       status, message, created_at, updated_at
                FROM napthe_requests 
                WHERE request_id = ?
            """, (request_id,))
            
            row = cursor.fetchone()
        
        if not row:
            logger.warning(f"Request ID not found: {request_id}")
            return jsonify({'error': 'Request ID not found'}), 404
        
        # Chuẩn hóa response
        status_str, final_message, display_status = normalize_status_message(row['status'], row['message'])
        
        response_data = {
            'request_id': row['request_id'],
            'status': status_str,
            'message': final_message,
            'display_status': display_status,
            'received_amount': row['received_amount'] or 0,
            'telco': row['telco'],
            'amount': row['amount'],
            'created_at': row['created_at'],
            'updated_at': row['updated_at']
        }
        
        logger.info(f"Status check for {request_id}: {status_str} - {final_message}")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error checking status for {request_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/callback', methods=['POST'])
def callback():
    """POST - Nhận callback từ Card2K"""
    
    # Log raw request
    logger.info(f"Callback received - Method: {request.method}")
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Content-Type: {request.content_type}")
    
    # Parse data từ multiple sources
    data = {}
    
    if request.is_json:
        data = request.get_json() or {}
        logger.info(f"JSON data: {data}")
    elif request.form:
        data = request.form.to_dict()
        logger.info(f"Form data: {data}")
    elif request.args:
        data = request.args.to_dict()
        logger.info(f"Query data: {data}")
    else:
        # Try parsing raw data
        raw_data = request.get_data(as_text=True)
        logger.info(f"Raw data: {raw_data}")
        
        # Parse nếu là query string format
        if '&' in raw_data and '=' in raw_data:
            from urllib.parse import parse_qs
            parsed = parse_qs(raw_data)
            data = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in parsed.items()}
    
    if not data:
        logger.error("No data received in callback")
        return jsonify({'error': 'No data received'}), 400
    
    # Extract required fields
    request_id = data.get('request_id', '').strip()
    status = data.get('status', '99')
    received_amount = data.get('received_amount', '0')
    message = data.get('message', '').strip()
    partner_id = data.get('partner_id', '').strip()
    sign = data.get('sign', '').strip()
    code = data.get('code', '').strip()
    serial = data.get('serial', '').strip()
    
    # Convert received_amount to int
    try:
        received_amount = int(float(str(received_amount).replace(',', '')))
    except (ValueError, TypeError):
        received_amount = 0
    
    logger.info(f"Parsed callback data - request_id: {request_id}, status: {status}, "
                f"received_amount: {received_amount}, message: {message}")
    
    # Validation
    if not request_id:
        logger.error("Missing request_id in callback")
        return jsonify({'error': 'Missing request_id'}), 400
    
    # Chỉ validate signature nếu có đủ thông tin
    if all([partner_id, sign, code, serial]):
        if partner_id != PARTNER_ID:
            logger.error(f"Invalid partner_id: {partner_id} (expected: {PARTNER_ID})")
            return jsonify({'error': 'Invalid partner_id'}), 403
        
        if not validate_signature(code, serial, sign):
            logger.error(f"Invalid signature for request {request_id}")
            return jsonify({'error': 'Invalid signature'}), 403
    else:
        logger.warning(f"Incomplete signature data for request {request_id}, skipping validation")
    
    # Process status update
    try:
        current_time = datetime.now().isoformat()
        
        # Get existing request data
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First, get current request data
            cursor.execute("""
                SELECT request_id, discord_user_id, telco, amount, status as old_status, message as old_message
                FROM napthe_requests 
                WHERE request_id = ?
            """, (request_id,))
            
            existing_request = cursor.fetchone()
            
            if not existing_request:
                logger.error(f"Request ID not found in database: {request_id}")
                return jsonify({'error': 'Request ID not found in database'}), 404
            
            # Chuẩn hóa status và message
            final_status, final_message, display_status = normalize_status_message(status, message)
            
            # Update database
            cursor.execute("""
                UPDATE napthe_requests 
                SET status = ?, message = ?, received_amount = ?, updated_at = ?
                WHERE request_id = ?
            """, (final_status, final_message, received_amount, current_time, request_id))
            
            if cursor.rowcount == 0:
                logger.error(f"Failed to update request {request_id}")
                return jsonify({'error': 'Failed to update request'}), 500
            
            conn.commit()
            
            logger.info(f"Successfully updated request {request_id}: "
                       f"{existing_request['old_status']} -> {final_status}, "
                       f"received_amount: {received_amount}")
        
        # Send Discord notification nếu status thay đổi
        if str(existing_request['old_status']) != str(final_status):
            try:
                # Convert existing_request to dict để truyền vào async function
                request_dict = dict(existing_request)
                
                # Create event loop nếu chưa có
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # Run notification in background
                if loop.is_running():
                    # Nếu loop đang chạy, schedule task
                    asyncio.create_task(notify_discord_user(request_dict, final_status, final_message, received_amount))
                else:
                    # Nếu loop chưa chạy, run sync
                    loop.run_until_complete(notify_discord_user(request_dict, final_status, final_message, received_amount))
                    
            except Exception as e:
                logger.error(f"Failed to send Discord notification: {e}")
                # Không return error vì callback đã thành công
        
        # Return success response
        response_data = {
            'success': True,
            'request_id': request_id,
            'status': final_status,
            'display_status': display_status,
            'message': final_message,
            'received_amount': received_amount,
            'updated_at': current_time
        }
        
        logger.info(f"Callback processed successfully for request {request_id}")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error processing callback for request {request_id}: {e}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500

@app.route('/api/requests/<user_id>')
def get_user_requests(user_id):
    """Lấy danh sách requests của user"""
    try:
        limit = min(int(request.args.get('limit', 10)), 50)
        status_filter = request.args.get('status')
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT request_id, telco, amount, received_amount, status, message, created_at, updated_at
                FROM napthe_requests 
                WHERE discord_user_id = ?
            """
            params = [user_id]
            
            if status_filter:
                query += " AND status = ?"
                params.append(status_filter)
            
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            requests = cursor.fetchall()
        
        # Format response
        formatted_requests = []
        for req in requests:
            status_str, final_message, display_status = normalize_status_message(req['status'], req['message'])
            
            formatted_requests.append({
                'request_id': req['request_id'],
                'telco': req['telco'],
                'amount': req['amount'],
                'received_amount': req['received_amount'] or 0,
                'status': status_str,
                'display_status': display_status,
                'message': final_message,
                'created_at': req['created_at'],
                'updated_at': req['updated_at']
            })
        
        return jsonify({
            'success': True,
            'requests': formatted_requests,
            'total': len(formatted_requests)
        })
        
    except Exception as e:
        logger.error(f"Error getting requests for user {user_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

# Initialize
def initialize_app():
    """Initialize application"""
    logger.info("Initializing callback server...")
    
    # Test database connection
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM napthe_requests")
            logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    
    # Start Discord bot in background thread
    if DISCORD_TOKEN:
        discord_thread = threading.Thread(target=run_discord_bot, daemon=True)
        discord_thread.start()
        logger.info("Discord bot thread started")
        
        # Wait a bit for bot to initialize
        time.sleep(2)
    
    logger.info("Callback server initialized successfully")

if __name__ == '__main__':
    initialize_app()
    
    # Run Flask app
    app.run(
        host='0.0.0.0', 
        port=int(os.environ.get('PORT', 3000)),
        debug=False,  # Disable debug in production
        threaded=True
        )
