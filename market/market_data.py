import requests
from typing import Optional, Dict, Any
from config.settings import MARKET_QUOTE_URL
import logging

class MarketData:
    def __init__(self, access_token: str):
        """
        Initialize MarketData with access token.
        
        Args:
            access_token: Upstox API access token
        """
        self.access_token = access_token
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    def get_stock_price(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        """
        Fetch current stock price and related data.
        
        Args:
            instrument_key: Instrument identifier (e.g., 'NSE_EQ|INE001A01036')
            
        Returns:
            Optional[Dict]: Stock data or None if request fails
        """
        try:
            self.logger.info(f"Fetching data for {instrument_key}")
            response = requests.get(
                MARKET_QUOTE_URL,
                headers=self.headers,
                params={"instrument_key": instrument_key}
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'success':
                return data['data']
            else:
                self.logger.error(f"API returned error: {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            return None

    def format_market_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format raw market data into a clean structure.
        
        Args:
            data: Raw market data
            
        Returns:
            Dict: Formatted market data
        """
        return {
            'ltp': data.get('ltp', 0.0),
            'high': data.get('high', 0.0),
            'low': data.get('low', 0.0),
            'open': data.get('open', 0.0),
            'close': data.get('close', 0.0),
            'volume': data.get('volume', 0),
            'timestamp': data.get('timestamp', ''),
            'change': data.get('change', 0.0),
            'change_percentage': data.get('change_percentage', 0.0)
        }