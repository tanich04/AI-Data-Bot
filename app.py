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
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import hashlib
import json
import redis
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== CACHING SETUP ====================
try:
    cache = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=0,
        decode_responses=True
    )
    cache.ping()
    print("✅ Redis cache connected")
except:
    cache = None
    print("⚠️ Redis not available - caching disabled")

# Store last query results for CSV export 
last_results = {}
last_results_lock = threading.Lock()

# ==================== SLACK VERIFICATION ====================
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

# ==================== DATABASE CONNECTION ====================
def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            cursor_factory=RealDictCursor
        )
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

# ==================== SQL GENERATOR ====================
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
        
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert. Convert questions to SQL.

Table: sales_daily
Columns:
- date (DATE): YYYY-MM-DD
- region (TEXT): North, South, East, West
- category (TEXT): Electronics, Grocery, Fashion
- revenue (NUMERIC): Dollars
- orders (INTEGER): Count

Output ONLY the SQL query. No explanations.
Examples:
Question: show revenue by region for 2025-09-01
SQL: SELECT region, SUM(revenue) as total_revenue FROM sales_daily WHERE date = '2025-09-01' GROUP BY region;
"""),
            ("user", "Question: {question}\nSQL:")
        ])
        
        self.chain = self.prompt | self.llm
    
    def generate_sql(self, question):
        try:
            response = self.chain.invoke({"question": question})
            sql = response.content.strip().replace('```sql', '').replace('```', '').strip()
            
            if not sql.upper().startswith('SELECT'):
                import re
                match = re.search(r'SELECT.*', sql, re.IGNORECASE | re.DOTALL)
                sql = match.group(0) if match else "SELECT * FROM sales_daily ORDER BY date DESC LIMIT 5;"
            
            return sql
        except Exception as e:
            logger.error(f"Error: {e}")
            return "SELECT * FROM sales_daily ORDER BY date DESC LIMIT 5;"

# ==================== EXECUTE SQL WITH CACHING ====================
def execute_sql(sql, user_id):
    # Generate cache key
    cache_key = f"query:{user_id}:{hashlib.md5(sql.encode()).hexdigest()}"
    
    # Check cache first
    if cache:
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for {cache_key}")
            return json.loads(cached)
    
    # Execute query
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        # Store in cache (expiration of 1 hour)
        if cache and results:
            cache.setex(cache_key, 3600, json.dumps(results, default=str))
        
        return results
    except Exception as e:
        return {"error": str(e)}

# def generate_chart(results, question):
#     """Generate a chart for date range queries"""
#     if not results or len(results) < 2:
#         return None
    
#     # Check if this is a date range query
#     df = pd.DataFrame(results)
    
#     if 'date' in df.columns and ('revenue' in df.columns or 'orders' in df.columns):
#         plt.figure(figsize=(10, 6))
        
#         if 'revenue' in df.columns:
#             df['date'] = pd.to_datetime(df['date'])
#             df = df.sort_values('date')
#             plt.plot(df['date'], df['revenue'], marker='o', linewidth=2, label='Revenue')
#             plt.ylabel('Revenue ($)')
#             plt.title(f'Revenue Trend: {question[:50]}...')
        
#         elif 'orders' in df.columns:
#             df['date'] = pd.to_datetime(df['date'])
#             df = df.sort_values('date')
#             plt.plot(df['date'], df['orders'], marker='s', linewidth=2, color='green', label='Orders')
#             plt.ylabel('Number of Orders')
#             plt.title(f'Orders Trend: {question[:50]}...')
        
#         plt.xlabel('Date')
#         plt.grid(True, alpha=0.3)
#         plt.legend()
#         plt.xticks(rotation=45)
#         plt.tight_layout()
        
#         # Save chart to bytes
#         img_bytes = io.BytesIO()
#         plt.savefig(img_bytes, format='png')
#         plt.close()
#         img_bytes.seek(0)
        
#         return img_bytes
    
#     return None

# ==================== CSV EXPORT ====================
@app.route('/export-csv/<user_id>', methods=['GET'])
def export_csv(user_id):
    """Export last query results as CSV"""
    with last_results_lock:
        if user_id not in last_results:
            return "No results found for export", 404
        
        results = last_results[user_id]
    
    if isinstance(results, dict) and "error" in results:
        return f"Error: {results['error']}", 400
    
    # Convert to CSV
    output = io.StringIO()
    if results:
        writer = csv.DictWriter(output, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    
    # Create response
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

# ==================== FORMAT FOR SLACK ====================
def format_slack_response(question, sql, results, user_id):
    # Store results for CSV export
    with last_results_lock:
        last_results[user_id] = results
    
    # Error case
    if isinstance(results, dict) and "error" in results:
        return {
            "response_type": "in_channel",
            "blocks": [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Question:* {question}\n*SQL:* `{sql}`\n*Error:*\n```{results['error']}```"}
            }]
        }
    
    if not results:
        return {
            "response_type": "in_channel",
            "blocks": [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Question:* {question}\n*SQL:* `{sql}`\n*Result:* No data found"}
            }]
        }
    
    # Format preview
    preview = ""
    for i, row in enumerate(results[:5]):
        preview += f"{i+1}. {dict(row)}\n"
    
    if len(results) > 5:
        preview += f"... and {len(results)-5} more rows"
    
    # Build blocks
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Question:* {question}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*SQL:* `{sql}`"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Results:*\n```{preview}```"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"{len(results)} rows"}]}
    ]
    
    # Add CSV export button 
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

# Initialize
sql_gen = SQLGenerator()

def process_question(text, response_url, user_id):
    logger.info(f"Processing: '{text}' for user {user_id}")
    
    # Check cache first 
    cache_key = f"question:{user_id}:{hashlib.md5(text.encode()).hexdigest()}"
    if cache:
        cached_response = cache.get(cache_key)
        if cached_response:
            logger.info("Returning cached response")
            requests.post(response_url, json=json.loads(cached_response))
            return
    
    # Generate and execute SQL
    sql = sql_gen.generate_sql(text)
    results = execute_sql(sql, user_id)
    
    # Generate chart if it's a date range query
    # chart_img = None
    # if 'between' in sql.lower() and ('date' in sql.lower()):
    #     chart_img = generate_chart(results, text)
    #     if chart_img:
    #         # Upload chart to Slack
    #         # Note: This requires files:write scope
    #         pass
    
    response = format_slack_response(text, sql, results, user_id)
    
    # Cache the response 
    if cache:
        cache.setex(cache_key, 1800, json.dumps(response))  # 30 minutes
    
    requests.post(response_url, json=response)
    logger.info("✅ Response sent to Slack")

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
        "cache": "connected" if cache else "disabled",
        "model": "llama-3.3-70b-versatile"
    })

if __name__ == '__main__':
    print("🚀 Started Slack AI Bot")
    print("✅ Caching - " + ("Connected" if cache else "Disabled"))

    app.run(port=3000)
