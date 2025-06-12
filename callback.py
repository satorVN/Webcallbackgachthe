
from flask import Flask, request, jsonify
import sqlite3
import os
from dotenv import load_dotenv
import hashlib
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

PARTNER_ID = os.getenv('GACHTHE_PARTNER_ID')
API_KEY = os.getenv('GACHTHE_KEY')

def get_db_connection():
    conn = sqlite3.connect('napthe.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def home():
    return jsonify({'status': 'ok', 'message': '✅ Callback server for Card2K is running.'})

@app.route('/callback', methods=['POST', 'GET'])
def callback():
    data = request.json or request.form.to_dict() or request.args.to_dict()
    request_id = data.get('request_id')
    status = data.get('status', 'pending')
    received_amount = int(data.get('received_amount', 0))
    message = data.get('message', '')
    partner_id = data.get('partner_id')
    sign = data.get('sign')
    code = data.get('code')
    serial = data.get('serial')

    # Log incoming request
    logger.info(f"Callback received: request_id={request_id}, status={status}, message={message}")

    if request.method == 'GET' and not request_id:
        return jsonify({'error': 'Missing request_id'}), 400

    if request.method == 'GET':
        conn = get_db_connection()
        cursor = conn.cursor()
        row = cursor.execute("SELECT * FROM napthe_requests WHERE request_id = ?", (request_id,)).fetchone()
        conn.close()

        if not row:
            return jsonify({'error': 'Request ID not found'}), 404

        return jsonify({
            'request_id': request_id,
            'status': row['status'],
            'message': row['message'],
            'received_amount': row['received_amount']
        })

    # POST xử lý callback từ Card2K
    if not all([request_id, status, partner_id, sign, code, serial]):
        logger.error(f"Missing required fields: {data}")
        return jsonify({'error': 'Missing required fields'}), 400

    if partner_id != PARTNER_ID:
        logger.error(f"Invalid partner_id: {partner_id}")
        return jsonify({'error': 'Invalid partner_id'}), 403

    expected_sign = hashlib.md5(f"{API_KEY}{code}{serial}".encode()).hexdigest()
    if sign != expected_sign:
        logger.error(f"Invalid signature: expected {expected_sign}, got {sign}")
        return jsonify({'error': 'Invalid signature'}), 403

    # ✅ SỬA: Giữ nguyên status từ Card2K, không chuẩn hóa sai
    # Card2K trả về: 1=thành công, 99=pending, 3=thất bại, 100=lỗi
    final_status = str(status)  # Giữ nguyên status từ Card2K
    
    # Log chi tiết
    logger.info(f"Processing callback: request_id={request_id}, original_status={status}, final_status={final_status}, message={message}, received_amount={received_amount}")

    # Cập nhật database
    conn = get_db_connection()
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()
    
    cursor.execute(
        "UPDATE napthe_requests SET status = ?, message = ?, received_amount = ?, updated_at = ? WHERE request_id = ?",
        (final_status, message, received_amount, current_time, request_id)
    )
    
    # Kiểm tra xem có update được không
    if cursor.rowcount == 0:
        logger.error(f"No rows updated for request_id: {request_id}")
        conn.close()
        return jsonify({'error': 'Request ID not found in database'}), 404
    
    conn.commit()
    conn.close()

    logger.info(f"Successfully updated request {request_id} with status {final_status}")
    
    return jsonify({
        'success': True, 
        'request_id': request_id, 
        'status': final_status, 
        'received_amount': received_amount,
        'message': message
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
