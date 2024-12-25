import requests
from typing import Optional, List, Any
import logging
from datetime import datetime

class HistoricalDataFetcher:
    def __init__(self, access_token: str):
        """
        Initialize HistoricalDataFetcher with access token.
        
        Args:
            access_token: Upstox API access token
        """
        self.access_token = access_token
        self.base_url = "https://api.upstox.com/v2/historical-candle"
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def fetch_historical_data(
        self,
        instrument_key: str,
        interval: str,
        from_date: str,
        to_date: str
    ) -> Optional[List[List[Any]]]:
        """
        Fetch historical price data for a given instrument.
        
        Args:
            instrument_key: Instrument identifier
            interval: Time interval ('day', 'week', 'month')
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Optional[List[List[Any]]]: List of OHLCV data or None if request fails
        """
        try:
            url = f"{self.base_url}/{instrument_key}/{interval}/{from_date}/{to_date}"
            self.logger.info(f"Fetching historical data from {url}")
            
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'success':
                return data['data']['candles']
            else:
                self.logger.error(f"API returned error: {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            return None

    def validate_dates(self, from_date: str, to_date: str) -> bool:
        """
        Validate date format and range.
        
        Args:
            from_date: Start date string
            to_date: End date string
            
        Returns:
            bool: True if dates are valid
        """
        try:
            start = datetime.strptime(from_date, '%Y-%m-%d')
            end = datetime.strptime(to_date, '%Y-%m-%d')
            return start <= end
        except ValueError:
            return False