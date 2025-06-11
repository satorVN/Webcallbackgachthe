from flask import Flask, request, jsonify
import sqlite3
import os
from dotenv import load_dotenv
import hashlib

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
        return jsonify({'error': 'Missing required fields'}), 400

    if partner_id != PARTNER_ID:
        return jsonify({'error': 'Invalid partner_id'}), 403

    expected_sign = hashlib.md5(f"{API_KEY}{code}{serial}".encode()).hexdigest()
    if sign != expected_sign:
        return jsonify({'error': 'Invalid signature'}), 403

    # Chuẩn hóa status
    if status.isdigit():
        status_int = int(status)
        if status_int == 1:
            safe_status = 'success'
        elif status_int == 99:
            safe_status = 'pending'
        elif status_int in [100, 3]:
            safe_status = 'error'
        else:
            safe_status = 'unknown'
    else:
        safe_status = status

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE napthe_requests SET status = ?, message = ?, received_amount = ? WHERE request_id = ?",
        (safe_status, message, received_amount, request_id)
    )
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'request_id': request_id, 'status': safe_status, 'received_amount': received_amount})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
