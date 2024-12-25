"""
Configuration module for the stock analysis application.
"""

from .settings import (
    XAI_API_KEY,
    UPSTOX_CLIENT_ID,
    UPSTOX_CLIENT_SECRET,
    REDIRECT_URI,
    AUTH_URL,
    TOKEN_URL,
    MARKET_QUOTE_URL,
    GROK_SETTINGS
)

__all__ = [
    'XAI_API_KEY',
    'UPSTOX_CLIENT_ID',
    'UPSTOX_CLIENT_SECRET',
    'REDIRECT_URI',
    'AUTH_URL',
    'TOKEN_URL',
    'MARKET_QUOTE_URL',
    'GROK_SETTINGS'
]

def validate_config():
    """Validate that all required configuration variables are set"""
    required_vars = [
        'XAI_API_KEY',
        'UPSTOX_CLIENT_ID',
        'UPSTOX_CLIENT_SECRET',
    ]
    
    missing_vars = [var for var in required_vars if not globals().get(var)]
    
    if missing_vars:
        raise ValueError(
            f"Missing required configuration variables: {', '.join(missing_vars)}"
        )

# Validate configuration when the package is imported
validate_config()