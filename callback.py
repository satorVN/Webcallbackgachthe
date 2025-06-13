from flask import Flask, request, jsonify
import sqlite3
import os
import hashlib
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables (with fallback)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("python-dotenv not found, using environment variables directly")

app = Flask(__name__)

# Get credentials from environment variables
PARTNER_ID = ('-1022521568')
API_KEY = ('Fshukr0Ewx7n3vmdDUfSLqqaX7Uf5gUR')

def init_db():
    """Initialize database with required table"""
    try:
        conn = sqlite3.connect('napthe.db')
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS napthe_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'pending',
                message TEXT,
                received_amount INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")

def get_db_connection():
    """Get database connection"""
    try:
        conn = sqlite3.connect('napthe.db')
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def normalize_status(status):
    """
    Chuẩn hóa status từ Card2K về format chuẩn
    Card2K: 1=thành công, 99=pending, 3=thất bại, 100=lỗi
    """
    status = str(status).strip()
    
    status_map = {
        '1': 'success',
        '99': 'pending', 
        '3': 'failed',
        '100': 'error'
    }
    
    normalized = status_map.get(status, 'pending')
    if status not in status_map:
        logger.warning(f"Unknown status received: {status}, defaulting to pending")
    
    return normalized

@app.route('/')
def home():
    return jsonify({
        'status': 'ok', 
        'message': '✅ Callback server for Card2K is running.',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        # Test database connection
        conn = get_db_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/callback', methods=['POST', 'GET'])
def callback():
    try:
        # Get data from different sources
        data = {}
        if request.method == 'POST':
            data = request.json or request.form.to_dict()
        else:
            data = request.args.to_dict()
        
        request_id = data.get('request_id')
        status = data.get('status', 'pending')
        received_amount = int(data.get('received_amount', 0))
        message = data.get('message', '')
        partner_id = data.get('partner_id')
        sign = data.get('sign')
        code = data.get('code')
        serial = data.get('serial')

        # Log incoming request
        logger.info(f"Callback received: method={request.method}, request_id={request_id}, status={status}")

        # Handle GET request - query status
        if request.method == 'GET':
            if not request_id:
                return jsonify({'error': 'Missing request_id parameter'}), 400

            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                row = cursor.execute(
                    "SELECT * FROM napthe_requests WHERE request_id = ?", 
                    (request_id,)
                ).fetchone()
                conn.close()

                if not row:
                    return jsonify({'error': 'Request ID not found'}), 404

                return jsonify({
                    'request_id': request_id,
                    'status': row['status'],
                    'message': row['message'],
                    'received_amount': row['received_amount'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                })
            except Exception as e:
                logger.error(f"Database error in GET: {str(e)}")
                return jsonify({'error': 'Database error'}), 500

        # Handle POST request - callback from Card2K
        if not request_id:
            logger.error("Missing request_id in POST request")
            return jsonify({'error': 'Missing request_id'}), 400

        # For POST requests, verify signature if sign is provided
        if sign and code and serial and partner_id:
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
        
        logger.info(f"Processing callback: request_id={request_id}, status={status} -> {normalized_status}")

        # Update database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            current_time = datetime.now().isoformat()
            
            # Check if request exists
            existing = cursor.execute(
                "SELECT * FROM napthe_requests WHERE request_id = ?", 
                (request_id,)
            ).fetchone()
            
            if existing:
                # Update existing record
                cursor.execute(
                    """UPDATE napthe_requests 
                       SET status = ?, message = ?, received_amount = ?, updated_at = ? 
                       WHERE request_id = ?""",
                    (normalized_status, message, received_amount, current_time, request_id)
                )
                logger.info(f"Updated existing request: {request_id}")
            else:
                # Insert new record
                cursor.execute(
                    """INSERT INTO napthe_requests 
                       (request_id, status, message, received_amount, created_at, updated_at) 
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (request_id, normalized_status, message, received_amount, current_time, current_time)
                )
                logger.info(f"Created new request: {request_id}")
            
            conn.commit()
            conn.close()

            return jsonify({
                'success': True, 
                'request_id': request_id, 
                'status': normalized_status, 
                'received_amount': received_amount,
                'message': message,
                'timestamp': current_time
            })

        except Exception as e:
            logger.error(f"Database error in POST: {str(e)}")
            return jsonify({'error': 'Database error'}), 500

    except Exception as e:
        logger.error(f"Error processing callback: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# Initialize database when app starts
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=False)
