# Slack AI Data Bot

A powerful Slack bot that converts natural language questions into SQL queries using LangChain. It queries a PostgreSQL database and returns formatted results with interactive visualizations.

## Features

### Core Functionality
- 1. **Natural Language to SQL**: Ask questions in plain English
- 2. **PostgreSQL Integration**: Direct query execution with safeguards
- 3. **Slack Slash Command**: `/ask-data` for easy access
- 4. **Formatted Responses**: Clean, readable output in Slack

### Enterprise Safeguards
- 1. **Read-Only Database User**: Prevents any data modification
- 2. **SQL Validation**: Blocks all non-SELECT commands
- 3. **Table Allowlist**: Restricted to `sales_daily` table only
- 4. **Row-Level Security**: User-specific data filtering
- 5. **Auto LIMIT + Timeout**: Prevents performance issues

### Advanced Features
-  **CSV Export**: One-click data export
-  **Redis Caching**: Faster repeated queries
-  **Background Processing**: No Slack timeouts

## Prerequisites

- Python 3.9+
- PostgreSQL database
- Slack workspace with admin access
- Groq API key (free)
- Redis (optional, for caching)
- ngrok (for local development)

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/slack-ai-data-bot.git
cd slack-ai-data-bot
```
### 2. Setup Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate it (Windows)
venv\Scripts\activate

# Activate it (Mac/Linux)
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your actual credentials
nano .env
```

### 5. Setup PostgreSQL Database
```bash
-- Create database
CREATE DATABASE xyz;

-- Create read-only user
CREATE USER slack_bot_ro WITH PASSWORD 'your-secure-password';

-- Grant permissions
\c xyz;
GRANT USAGE ON SCHEMA public TO slack_bot_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO slack_bot_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO slack_bot_ro;

-- Create and populate table
```

### 6. Setup Slack
### 7. Run the Bot
```bash
# Terminal 1: Start Flask app
python app.py

# Terminal 2: Start ngrok
ngrok http 3000

# Copy the ngrok URL and update .env BASE_URL
# Also update Slack app with new ngrok URL if it changed
```