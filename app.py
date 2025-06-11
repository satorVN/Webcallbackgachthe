from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime
import os
import hashlib

app = Flask(__name__)

# Load environment variables
GACHTHE_PARTNER_ID = os.getenv('GACHTHE_PARTNER_ID')
GACHTHE_KEY = os.getenv('GACHTHE_KEY')
if not GACHTHE_PARTNER_ID or not GACHTHE_KEY:
    raise ValueError("GACHTHE_PARTNER_ID and GACHTHE_KEY must be set in .env")

# SQLite setup
db_path = 'napthe.db'
conn = sqlite3.connect(db_path, check_same_thread=False)
cursor = conn.cursor()

# Ensure table exists
cursor.execute('''CREATE TABLE IF NOT EXISTS napthe_requests
                  (request_id TEXT PRIMARY KEY, discord_user_id TEXT, channel_id TEXT, telco TEXT, seri TEXT, mathe TEXT, amount INTEGER, received_amount INTEGER DEFAULT 0, status TEXT DEFAULT 'processing', message TEXT)''')
conn.commit()

def verify_signature(data, received_signature):
    # Giả định API gửi signature dựa trên request_id, amount, và GACHTHE_KEY
    # Điều này cần khớp với tài liệu API Card2K
    sign_data = f"{GACHTHE_KEY}{data.get('request_id')}{data.get('amount', 0)}".encode()
    calculated_signature = hashlib.md5(sign_data).hexdigest()
    return calculated_signature == received_signature

@app.route('/callback', methods=['POST'])
def handle_callback():
    data = request.get_json()
    if not data or 'request_id' not in data:
        return jsonify({'status': 'error', 'message': 'Invalid data'}), 400

    request_id = data.get('request_id')
    status = data.get('status')
    amount = data.get('amount', 0)
    received_amount = data.get('received_amount', 0)
    message = data.get('message', 'No message')
    partner_id = data.get('partner_id')  # Giả định API gửi partner_id
    signature = data.get('sign')  # Giả định API gửi signature

    # Xác thực partner_id
    if partner_id != GACHTHE_PARTNER_ID:
        return jsonify({'status': 'error', 'message': 'Invalid partner_id'}), 403

    # Xác thực signature (nếu có)
    if signature and not verify_signature(data, signature):
        return jsonify({'status': 'error', 'message': 'Invalid signature'}), 403

    # Map numeric status to string
    safe_status = str(status) if isinstance(status, int) else status
    if isinstance(status, int):
        if status == 1: safe_status = 'success'
        elif status == 99: safe_status = 'pending'
        elif status in [100, 3]: safe_status = 'error'
        else: safe_status = 'unknown'

    cursor.execute("UPDATE napthe_requests SET status = ?, amount = ?, received_amount = ?, message = ? WHERE request_id = ?",
                   (safe_status, amount, received_amount, message, request_id))
    conn.commit()

    print(f"Callback at {datetime.now().strftime('%H:%M:%S')} for {request_id}: {data}")
    return jsonify({'status': 'success', 'message': 'Processed'}), 200

@app.route('/callback-check', methods=['GET'])
def check_status():
    request_id = request.args.get('request_id')
    if not request_id:
        return jsonify({'status': 'error', 'message': 'No request_id'}), 400

    cursor.execute("SELECT status, message, received_amount FROM napthe_requests WHERE request_id = ?", (request_id,))
    result = cursor.fetchone()
    return jsonify(result[0:3] if result else {'error': 'Not found'}), 200 if result else 404

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3000))  # Glitch uses PORT environment variable
    app.run(host='0.0.0.0', port=port)
