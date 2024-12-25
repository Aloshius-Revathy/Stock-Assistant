import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Keys and Configuration
XAI_API_KEY = os.getenv("XAI_API_KEY")
UPSTOX_CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
UPSTOX_CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:5000/callback")

# API Endpoints
AUTH_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"
MARKET_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

# Grok AI Settings
GROK_SETTINGS = {
    "model": "grok-beta",
    "temperature": 0.7,
    "max_tokens": 1000,
    "top_p": 1,
    "frequency_penalty": 0,
    "presence_penalty": 0,
}