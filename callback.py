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

PARTNER_ID = ('-1022521568')
API_KEY = ('Fshukr0Ewx7n3vmdDUfSLqqaX7Uf5gUR')

def get_db_connection():
    conn = sqlite3.connect('napthe.db')
    conn.row_factory = sqlite3.Row
    return conn

def normalize_status(status):
    """
    Chuẩn hóa status từ Card2K về format chuẩn
    Card2K: 1=thành công, 99=pending, 3=thất bại, 100=lỗi
    """
    status = str(status).strip()
    
    if status == '1':
        return 'success'
    elif status == '99':
        return 'pending'
    elif status == '3':
        return 'failed'
    elif status == '100':
        return 'error'
    else:
        logger.warning(f"Unknown status received: {status}")
        return 'pending'  # Default to pending for unknown status

@app.route('/')
def home():
    return jsonify({'status': 'ok', 'message': '✅ Callback server for Card2K is running.'})

@app.route('/callback', methods=['POST', 'GET'])
def callback():
    try:
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

        # Handle GET request - query status
        if request.method == 'GET':
            if not request_id:
                return jsonify({'error': 'Missing request_id'}), 400

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

        # Handle POST request - callback from Card2K
        if not all([request_id, partner_id, sign, code, serial]):
            logger.error(f"Missing required fields: {data}")
            return jsonify({'error': 'Missing required fields'}), 400

        # Verify partner_id
        if str(partner_id) != str(PARTNER_ID):
            logger.error(f"Invalid partner_id: {partner_id}, expected: {PARTNER_ID}")
            return jsonify({'error': 'Invalid partner_id'}), 403

        # Verify signature
        expected_sign = hashlib.md5(f"{API_KEY}{code}{serial}".encode()).hexdigest()
        if sign != expected_sign:
            logger.error(f"Invalid signature: expected {expected_sign}, got {sign}")
            return jsonify({'error': 'Invalid signature'}), 403

        # Normalize status from Card2K
        normalized_status = normalize_status(status)
        
        # Log processing details
        logger.info(f"Processing callback: request_id={request_id}, original_status={status}, normalized_status={normalized_status}, message={message}, received_amount={received_amount}")

        # Update database
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now().isoformat()
        
        # Check if request exists
        existing = cursor.execute("SELECT * FROM napthe_requests WHERE request_id = ?", (request_id,)).fetchone()
        if not existing:
            logger.error(f"Request ID not found in database: {request_id}")
            conn.close()
            return jsonify({'error': 'Request ID not found in database'}), 404
        
        # Update the record
        cursor.execute(
            "UPDATE napthe_requests SET status = ?, message = ?, received_amount = ?, updated_at = ? WHERE request_id = ?",
            (normalized_status, message, received_amount, current_time, request_id)
        )
        
        if cursor.rowcount == 0:
            logger.error(f"Failed to update request_id: {request_id}")
            conn.close()
            return jsonify({'error': 'Failed to update database'}), 500
        
        conn.commit()
        conn.close()

        logger.info(f"Successfully updated request {request_id} with status {normalized_status}")
        
        return jsonify({
            'success': True, 
            'request_id': request_id, 
            'status': normalized_status, 
            'received_amount': received_amount,
            'message': message
        })

    except Exception as e:
        logger.error(f"Error processing callback: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
