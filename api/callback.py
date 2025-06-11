from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime
import os
import hashlib

app = Flask(__name__)

# Load environment variables từ Vercel
GACHTHE_PARTNER_ID = os.getenv('GACHTHE_PARTNER_ID')
GACHTHE_KEY = os.getenv('GACHTHE_KEY')
if not GACHTHE_PARTNER_ID or not GACHTHE_KEY:
    raise ValueError("GACHTHE_PARTNER_ID and GACHTHE_KEY must be set in Vercel environment")

# Cấu hình SQLite (lưu ý: Vercel không hỗ trợ persistent file system, dùng in-memory hoặc database bên ngoài)
conn = sqlite3.connect(':memory:', check_same_thread=False)  # In-memory DB cho Vercel
cursor = conn.cursor()

# Tạo bảng trong memory
cursor.execute('''CREATE TABLE IF NOT EXISTS napthe_requests
                  (request_id TEXT PRIMARY KEY, 
                   discord_user_id TEXT, 
                   channel_id TEXT, 
                   telco TEXT, 
                   seri TEXT, 
                   mathe TEXT, 
                   amount INTEGER, 
                   received_amount INTEGER DEFAULT 0, 
                   status TEXT DEFAULT 'processing', 
                   message TEXT)''')
conn.commit()

# Hàm xác thực chữ ký
def verify_signature(data):
    sign_data = f"{GACHTHE_KEY}{data.get('request_id')}{data.get('amount', 0)}".encode()
    calculated_signature = hashlib.md5(sign_data).hexdigest()
    return calculated_signature == data.get('sign', '')

@app.route('/test', methods=['GET'])
def test():
    return jsonify({'status': 'success', 'message': 'Server is running'}), 200

@app.route('/callback', methods=['POST'])
def handle_callback():
    try:
        print("Received request at /callback")
        data = request.get_json()
        if not data or 'request_id' not in data:
            return jsonify({'status': 'error', 'message': 'Invalid data or missing request_id'}), 400

        request_id = data.get('request_id')
        status = data.get('status')
        amount = data.get('amount', 0)
        received_amount = data.get('received_amount', 0)
        message = data.get('message', 'No message')
        partner_id = data.get('partner_id')

        if partner_id != GACHTHE_PARTNER_ID:
            return jsonify({'status': 'error', 'message': 'Invalid partner_id'}), 403

        if 'sign' in data and not verify_signature(data):
            return jsonify({'status': 'error', 'message': 'Invalid signature'}), 403

        safe_status = str(status) if isinstance(status, int) else status
        if isinstance(status, int):
            if status == 1: safe_status = 'success'
            elif status == 99: safe_status = 'pending'
            elif status in [100, 3]: safe_status = 'error'
            else: safe_status = 'unknown'

        cursor.execute("INSERT OR REPLACE INTO napthe_requests (request_id, status, amount, received_amount, message) VALUES (?, ?, ?, ?, ?)",
                       (request_id, safe_status, amount, received_amount, message))
        conn.commit()

        print(f"Callback at {datetime.now().strftime('%H:%M:%S')} for {request_id}: {data}")
        return jsonify({'status': 'success', 'message': 'Processed'}), 200
    except Exception as e:
        print(f"Error in callback: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/callback-check', methods=['GET'])
def check_status():
    try:
        request_id = request.args.get('request_id')
        if not request_id:
            return jsonify({'status': 'error', 'message': 'No request_id'}), 400

        cursor.execute("SELECT status, message, received_amount FROM napthe_requests WHERE request_id = ?", (request_id,))
        result = cursor.fetchone()
        return jsonify(result[0:3] if result else {'error': 'Not found'}), 200 if result else 404
    except Exception as e:
        print(f"Error in check_status: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=False)
