from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import asyncio

class StockProcessor:
    def __init__(self, access_token: str):
        """Initialize the stock processor with access token."""
        self.access_token = access_token
        self.base_url = "https://api-v2.upstox.com"
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        
        # Cache for storing frequently accessed data
        self._cache = {}
        self._cache_timeout = 300  # 5 minutes

    def setup_logging(self) -> None:
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def get_headers(self) -> Dict[str, str]:
        """Get API request headers."""
        return {
            'Accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {self.access_token}'
        }

    async def get_stock_data(self, symbol: str, from_date: datetime) -> Dict[str, Any]:
        """Fetch stock data from Upstox API."""
        try:
            # Check cache first
            cache_key = f"{symbol}_{from_date.strftime('%Y%m%d')}"
            if cache_key in self._cache:
                cache_data = self._cache[cache_key]
                if datetime.now() - cache_data['timestamp'] < timedelta(seconds=self._cache_timeout):
                    return cache_data['data']

            # Format symbol for NSE equity
            formatted_symbol = f"NSE_EQ|{symbol}"
            
            # Get market quotes
            quote_url = f"{self.base_url}/market-quote/quotes"
            params = {
                "symbol": formatted_symbol,
                "interval": "1day"
            }
            
            self.logger.info(f"Fetching data for {formatted_symbol}")
            response = requests.get(
                quote_url,
                headers=self.get_headers(),
                params=params
            )
            
            response.raise_for_status()
            quote_data = response.json()

            # Get historical data
            historical_url = f"{self.base_url}/historical-candle/intraday"
            historical_params = {
                "symbol": formatted_symbol,
                "interval": "1day",
                "from_date": from_date.strftime("%Y-%m-%d"),
                "to_date": datetime.now().strftime("%Y-%m-%d")
            }
            
            historical_response = requests.get(
                historical_url,
                headers=self.get_headers(),
                params=historical_params
            )
            
            historical_response.raise_for_status()
            historical_data = historical_response.json()

            # Combine current and historical data
            result = {
                'success': True,
                'data': {
                    'symbol': symbol,
                    'current': quote_data['data'][formatted_symbol],
                    'historical': historical_data['data'],
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            }

            # Cache the result
            self._cache[cache_key] = {
                'data': result,
                'timestamp': datetime.now()
            }

            return result

        except Exception as e:
            self.logger.error(f"Error fetching stock data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_multiple_stocks_data(self, symbols: List[str], from_date: datetime) -> Dict[str, Any]:
        """Fetch data for multiple stocks concurrently."""
        try:
            tasks = []
            for symbol in symbols:
                tasks.append(self.get_stock_data(symbol, from_date))
            
            results = await asyncio.gather(*tasks)
            return {
                'success': True,
                'data': {
                    symbol: result for symbol, result in zip(symbols, results)
                }
            }
        except Exception as e:
            self.logger.error(f"Error fetching multiple stocks data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def calculate_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate various technical indicators."""
        try:
            df = data.copy()
            
            # Moving averages
            df['MA20'] = df['close'].rolling(window=20).mean()
            df['MA50'] = df['close'].rolling(window=50).mean()
            df['MA200'] = df['close'].rolling(window=200).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # MACD
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
            
            # Bollinger Bands
            df['BB_middle'] = df['close'].rolling(window=20).mean()
            df['BB_upper'] = df['BB_middle'] + 2 * df['close'].rolling(window=20).std()
            df['BB_lower'] = df['BB_middle'] - 2 * df['close'].rolling(window=20).std()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {str(e)}")
            return data

    def find_support_resistance(self, data: pd.DataFrame, window: int = 20) -> Dict[str, List[float]]:
        """Find support and resistance levels."""
        try:
            df = data.copy()
            
            # Find local maxima and minima
            df['Local_Max'] = df['high'].rolling(window=window, center=True).max()
            df['Local_Min'] = df['low'].rolling(window=window, center=True).min()
            
            resistance_levels = df[df['high'] == df['Local_Max']]['high'].unique()
            support_levels = df[df['low'] == df['Local_Min']]['low'].unique()
            
            # Filter and sort levels
            resistance_levels = sorted(resistance_levels)[-5:]  # Top 5 resistance levels
            support_levels = sorted(support_levels)[:5]  # Bottom 5 support levels
            
            return {
                'support': support_levels.tolist(),
                'resistance': resistance_levels.tolist()
            }
            
        except Exception as e:
            self.logger.error(f"Error finding support/resistance levels: {str(e)}")
            return {'support': [], 'resistance': []}

    def analyze_volume_profile(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volume profile and trends."""
        try:
            df = data.copy()
            
            # Calculate volume metrics
            avg_volume = df['volume'].mean()
            volume_std = df['volume'].std()
            
            # Identify high volume days
            df['Volume_Signal'] = np.where(df['volume'] > avg_volume + volume_std, 1,
                                         np.where(df['volume'] < avg_volume - volume_std, -1, 0))
            
            # Calculate price-volume correlation
            price_volume_corr = df['close'].corr(df['volume'])
            
            return {
                'average_volume': avg_volume,
                'volume_std': volume_std,
                'high_volume_days': df[df['Volume_Signal'] == 1].shape[0],
                'low_volume_days': df[df['Volume_Signal'] == -1].shape[0],
                'price_volume_correlation': price_volume_corr
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing volume profile: {str(e)}")
            return {}

    def format_stock_data(self, data: Dict[str, Any]) -> str:
        """Format stock data for display."""
        if not data.get('success'):
            return f"❌ Error: {data.get('error')}"

        stock_data = data['data']
        current_data = stock_data['current']
        
        return f"""
        📊 {stock_data['symbol']} Stock Data (NSE):

        Current Price: ₹{current_data['ltp']:,.2f}
        Day High: ₹{current_data['high']:,.2f}
        Day Low: ₹{current_data['low']:,.2f}
        Open: ₹{current_data['open']:,.2f}
        Previous Close: ₹{current_data['close']:,.2f}
        Volume: {current_data['volume']:,}
        Change: {current_data.get('change_percentage', 0):.2f}%

        Last Updated: {stock_data['timestamp']}
        """

