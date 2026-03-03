import os
from flask import Flask, jsonify, request, abort, send_file
from dotenv import load_dotenv
import hashlib
import hmac
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import threading
import requests
import logging
import csv
import io
import pandas as pd
from datetime import datetime
import hashlib
import json
import redis
import re
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Safeguard 5: Query limits
MAX_ROWS = 1000
QUERY_TIMEOUT_SECONDS = 10

# Safeguard 3: Table & Column Allowlist
ALLOWED_TABLES = {'sales_daily'}
ALLOWED_COLUMNS = {
    'sales_daily': {'date', 'region', 'category', 'revenue', 'orders', 'created_at'}
}
try:
    cache = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=0,
        decode_responses=True
    )
    cache.ping()
    print("Redis cache connected")
except:
    cache = None
    print("Redis not available - caching disabled")

last_results = {}
last_results_lock = threading.Lock()

def verify_slack_request(request):
    timestamp = request.headers.get('X-Slack-Request-Timestamp')
    signature = request.headers.get('X-Slack-Signature')
    if not timestamp or not signature:
        return False
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False
    sig_basestring = f"v0:{timestamp}:{request.get_data(as_text=True)}"
    my_signature = 'v0=' + hmac.new(
        os.environ['SLACK_SIGNING_SECRET'].encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(my_signature, signature)

def get_db_connection(read_only=True):
    """Connect to database - read-only by default"""
    try:
        if read_only:
            user = os.getenv('DB_USER', 'slack_bot_ro')
            password = os.getenv('DB_PASSWORD')
        else:
            user = os.getenv('DB_ADMIN_USER', 'postgres')
            password = os.getenv('DB_ADMIN_PASSWORD')
        
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=user,
            password=password,
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            cursor_factory=RealDictCursor
        )
        
        if read_only:
            with conn.cursor() as cur:
                cur.execute("SET default_transaction_read_only = on;")
        
        return conn
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

def validate_sql(sql):
    sql_upper = sql.upper().strip()

    forbidden_commands = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 
                          'CREATE', 'TRUNCATE', 'GRANT', 'REVOKE']
    
    for cmd in forbidden_commands:
        if re.search(rf'\b{cmd}\b', sql_upper):
            return False, f"{cmd} operations are not allowed. Only SELECT queries are permitted."
    
    if not sql_upper.startswith('SELECT'):
        return False, "Only SELECT queries are allowed"

    if sql.count(';') > 1:
        return False, "Multiple SQL statements are not allowed"

    table_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_\.]*)(?:\s|$)'
    tables = re.findall(table_pattern, sql, re.IGNORECASE)

    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_\.]*)(?:\s|$)'
    tables.extend(re.findall(join_pattern, sql, re.IGNORECASE))
    cleaned_tables = [t.split('.')[-1] for t in tables]
    
    for table in cleaned_tables:
        if table not in ALLOWED_TABLES:
            return False, f"Access to table '{table}' is not allowed. Allowed tables: {', '.join(ALLOWED_TABLES)}"

    if 'LIMIT' not in sql_upper:
        if sql.strip().endswith(';'):
            sql = sql.rstrip(';') + f" LIMIT {MAX_ROWS};"
        else:
            sql += f" LIMIT {MAX_ROWS}"
    else:
        # Extract existing LIMIT and ensure it doesn't exceed MAX_ROWS
        limit_pattern = r'LIMIT\s+(\d+)'
        match = re.search(limit_pattern, sql_upper)
        if match:
            limit_value = int(match.group(1))
            if limit_value > MAX_ROWS:
                sql = re.sub(limit_pattern, f"LIMIT {MAX_ROWS}", sql, flags=re.IGNORECASE)
    
    return True, sql

class SQLGenerator:
    def __init__(self):
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not found")
        
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0,
            groq_api_key=api_key,
            max_tokens=500
        )
        
        # Prompt with strict instructions and examples to ensure only safe SQL is generated
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert. Convert questions to SQL SELECT statements only.

IMPORTANT: Generate ONLY SELECT queries. Never generate INSERT, UPDATE, DELETE, DROP, or any other modification commands.

Table: sales_daily (this is the ONLY table you can query)
Columns:
- date (DATE): YYYY-MM-DD
- region (TEXT): North, South, East, West
- category (TEXT): Electronics, Grocery, Fashion
- revenue (NUMERIC): Dollars
- orders (INTEGER): Count

Rules:
1. Generate ONLY SELECT statements
2. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE
3. Always use LIMIT (default 1000 if not specified)
4. Only reference the sales_daily table
5. Output ONLY the SQL query, no explanations

Examples:
Question: show revenue by region for 2025-09-01
SQL: SELECT region, SUM(revenue) as total_revenue FROM sales_daily WHERE date = '2025-09-01' GROUP BY region LIMIT 1000;
"""),
            ("user", "Question: {question}\nSQL:")
        ])
        
        self.chain = self.prompt | self.llm
        print("Groq SQL ready with safeguards")
    
    def generate_sql(self, question):
        try:
            response = self.chain.invoke({"question": question})
            sql = response.content.strip().replace('```sql', '').replace('```', '').strip()
            
            # Basic cleanup
            if not sql.upper().startswith('SELECT'):
                import re
                match = re.search(r'SELECT.*', sql, re.IGNORECASE | re.DOTALL)
                sql = match.group(0) if match else "SELECT * FROM sales_daily ORDER BY date DESC LIMIT 5;"
            
            return sql
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return "SELECT * FROM sales_daily ORDER BY date DESC LIMIT 5;"

def apply_row_level_security(sql, user_id, user_info=None):
    """
    Apply row-level security based on user context
    This is where you'd add filters like:
    - Users can only see their own region
    - Users can only see data after certain date
    - Department-based restrictions
    """
    if user_info and user_info.get('email', '').endswith('@north.example.com'):
        if 'WHERE' not in sql.upper():
            if 'LIMIT' in sql.upper():
                sql = sql.replace('LIMIT', "WHERE region = 'North' LIMIT")
            else:
                sql += " WHERE region = 'North'"
        elif 'region' not in sql.upper():
            sql = sql.replace('WHERE', "WHERE region = 'North' AND")
    
    return sql

def execute_sql(sql, user_id, user_info=None):
    """
    Execute SQL with all safeguards:
    - Validation (Safeguard 2 & 3)
    - RLS (Safeguard 4)
    - Timeout (Safeguard 5)
    - Caching
    """
    is_valid, validation_result = validate_sql(sql)
    if not is_valid:
        return {"error": validation_result}

    secured_sql = apply_row_level_security(validation_result, user_id, user_info)

    cache_key = f"query:{user_id}:{hashlib.md5(secured_sql.encode()).hexdigest()}"
    if cache:
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for {cache_key}")
            return json.loads(cached)

    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = '{QUERY_TIMEOUT_SECONDS}s';")
        cur.execute(secured_sql)
        results = cur.fetchall()
        cur.close()
        conn.close()
        if cache and results:
            cache.setex(cache_key, 3600, json.dumps(results, default=str))
        
        return results
    except psycopg2.errors.QueryCanceled:
        return {"error": f"Query timed out after {QUERY_TIMEOUT_SECONDS} seconds"}
    except Exception as e:
        return {"error": str(e)}

@app.route('/export-csv/<user_id>', methods=['GET'])
def export_csv(user_id):
    """Export last query results as CSV"""
    with last_results_lock:
        if user_id not in last_results:
            return "No results found for export", 404
        
        results = last_results[user_id]
    
    if isinstance(results, dict) and "error" in results:
        return f"Error: {results['error']}", 400
    
    # Converting to csv
    output = io.StringIO()
    if results:
        writer = csv.DictWriter(output, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    
    mem = io.BytesIO()
    mem.write(output.getvalue().encode())
    mem.seek(0)
    output.close()
    
    return send_file(
        mem,
        mimetype='text/csv',
        download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
        as_attachment=True
    )

def format_slack_response(question, sql, results, user_id):
    with last_results_lock:
        last_results[user_id] = results

    if isinstance(results, dict) and "error" in results:
        return {
            "response_type": "in_channel",
            "blocks": [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Question:* {question}\n*Error:*\n```{results['error']}```"}
            }]
        }
    
    if not results:
        return {
            "response_type": "in_channel",
            "blocks": [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Question:* {question}\n*Result:* No data found"}
            }]
        }

    preview = ""
    for i, row in enumerate(results[:5]):
        preview += f"{i+1}. {dict(row)}\n"
    
    if len(results) > 5:
        preview += f"... and {len(results)-5} more rows"
    
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Question:* {question}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*SQL:* `{sql}`"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Results:*\n```{preview}```"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"📊 {len(results)} rows (max {MAX_ROWS})"}]}
    ]
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": "📥 Export CSV"},
            "url": f"{os.getenv('BASE_URL', 'https://your-ngrok-url.ngrok-free.app')}/export-csv/{user_id}",
            "action_id": "export_csv"
        }]
    })
    
    return {"response_type": "in_channel", "blocks": blocks}

# Initializing
sql_gen = SQLGenerator()

def process_question(text, response_url, user_id):
    logger.info(f"Processing: '{text}' for user {user_id}")
    cache_key = f"question:{user_id}:{hashlib.md5(text.encode()).hexdigest()}"
    if cache:
        cached_response = cache.get(cache_key)
        if cached_response:
            logger.info("Returning cached response")
            requests.post(response_url, json=json.loads(cached_response))
            return

    sql = sql_gen.generate_sql(text)

    results = execute_sql(sql, user_id)

    response = format_slack_response(text, sql, results, user_id)

    if cache:
        cache.setex(cache_key, 1800, json.dumps(response))
    
    requests.post(response_url, json=response)
    logger.info("Response sent to Slack")

@app.route('/slack/commands', methods=['POST'])
def slack_commands():
    if not verify_slack_request(request):
        abort(400)
    
    data = request.form
    text = data.get('text')
    response_url = data.get('response_url')
    user_id = data.get('user_id')
    
    if not text:
        return jsonify({"text": "Please ask a question"})
    
    thread = threading.Thread(target=process_question, args=(text, response_url, user_id))
    thread.start()
    
    return jsonify({"response_type": "ephemeral", "text": f"⏳ Processing: '{text}'..."})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "safeguards": {
            "read_only_user": True,
            "sql_validation": True,
            "table_allowlist": list(ALLOWED_TABLES),
            "row_limit": MAX_ROWS,
            "timeout_seconds": QUERY_TIMEOUT_SECONDS
        },
        "cache": "connected" if cache else "disabled"
    })

if __name__ == '__main__':
    print("Started Slack AI Bot with safeguards!")

    app.run(port=3000)

