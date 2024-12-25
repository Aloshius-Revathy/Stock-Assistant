import requests
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime, timedelta
import asyncio
from .instrument_mapper import InstrumentMapper

class HistoricalDataFetcher:
    def __init__(self, access_token: str, instrument_mapper: InstrumentMapper):
        """Initialize the historical data fetcher."""
        self.access_token = access_token
        self.instrument_mapper = instrument_mapper
        self.base_url = "https://api-v2.upstox.com"
        self.logger = logging.getLogger(__name__)
        
        # Cache settings
        self._cache: Dict[str, Dict] = {}
        self._cache_duration = timedelta(minutes=5)  # Cache data for 5 minutes

    def get_headers(self) -> Dict[str, str]:
        """Get API request headers."""
        return {
            'Accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {self.access_token}'
        }

    async def get_historical_data(
        self, 
        symbol: str, 
        from_date: datetime,
        to_date: Optional[datetime] = None,
        interval: str = "1day"
    ) -> Dict[str, Any]:
        """
        Fetch historical data for a given symbol.
        
        Args:
            symbol: Trading symbol
            from_date: Start date
            to_date: End date (defaults to current date)
            interval: Data interval ("1minute", "5minute", "15minute", "30minute", "1day")
        """
        try:
            # Validate symbol
            instrument = self.instrument_mapper.get_instrument_by_symbol(symbol)
            if not instrument:
                return {
                    'success': False,
                    'error': f"Invalid symbol: {symbol}"
                }

            # Set default to_date if not provided
            to_date = to_date or datetime.now()

            # Check cache
            cache_key = f"{symbol}_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}_{interval}"
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                return cached_data

            # Prepare request parameters
            params = {
                "symbol": f"NSE_EQ|{symbol}",
                "interval": interval,
                "from_date": from_date.strftime("%Y-%m-%d"),
                "to_date": to_date.strftime("%Y-%m-%d")
            }

            # Fetch data
            url = f"{self.base_url}/historical-candle/intraday"
            if interval == "1day":
                url = f"{self.base_url}/historical-candle/historical"

            response = requests.get(
                url,
                headers=self.get_headers(),
                params=params
            )
            
            response.raise_for_status()
            data = response.json()

            if not data.get('data'):
                return {
                    'success': False,
                    'error': "No data received from API"
                }

            # Process the data
            processed_data = self._process_historical_data(data['data'], instrument)
            
            # Cache the result
            self._add_to_cache(cache_key, processed_data)

            return processed_data

        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_multiple_historical_data(
        self,
        symbols: List[str],
        from_date: datetime,
        to_date: Optional[datetime] = None,
        interval: str = "1day"
    ) -> Dict[str, Any]:
        """Fetch historical data for multiple symbols concurrently."""
        try:
            tasks = []
            for symbol in symbols:
                tasks.append(self.get_historical_data(symbol, from_date, to_date, interval))
            
            results = await asyncio.gather(*tasks)
            
            return {
                'success': True,
                'data': {
                    symbol: result for symbol, result in zip(symbols, results)
                }
            }

        except Exception as e:
            self.logger.error(f"Error fetching multiple historical data: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _process_historical_data(self, raw_data: List[List], instrument: Dict) -> Dict[str, Any]:
        """Process raw historical data into structured format."""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Calculate additional metrics
            df['returns'] = df['close'].pct_change()
            df['volatility'] = df['returns'].rolling(window=20).std() * np.sqrt(252)
            df['sma_20'] = df['close'].rolling(window=20).mean()
            df['sma_50'] = df['close'].rolling(window=50).mean()
            df['sma_200'] = df['close'].rolling(window=200).mean()
            
            # Calculate VWAP
            df['vwap'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum() / df['volume'].cumsum()
            
            # Calculate Bollinger Bands
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            df['bb_upper'] = df['bb_middle'] + 2 * df['close'].rolling(window=20).std()
            df['bb_lower'] = df['bb_middle'] - 2 * df['close'].rolling(window=20).std()
            
            return {
                'success': True,
                'data': {
                    'symbol': instrument['tradingsymbol'],
                    'name': instrument.get('name'),
                    'exchange': instrument['exchange'],
                    'instrument_type': instrument['instrument_type'],
                    'historical_data': df.to_dict('records'),
                    'metadata': {
                        'start_date': df['timestamp'].min().strftime('%Y-%m-%d'),
                        'end_date': df['timestamp'].max().strftime('%Y-%m-%d'),
                        'total_days': len(df),
                        'last_price': df['close'].iloc[-1],
                        'change_percent': ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
                    }
                }
            }

        except Exception as e:
            self.logger.error(f"Error processing historical data: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _get_from_cache(self, key: str) -> Optional[Dict]:
        """Get data from cache if valid."""
        if key in self._cache:
            cache_entry = self._cache[key]
            if datetime.now() - cache_entry['timestamp'] < self._cache_duration:
                return cache_entry['data']
            else:
                del self._cache[key]
        return None

    def _add_to_cache(self, key: str, data: Dict) -> None:
        """Add data to cache."""
        self._cache[key] = {
            'data': data,
            'timestamp': datetime.now()
        }

    async def get_intraday_data(
        self,
        symbol: str,
        interval: str = "5minute"
    ) -> Dict[str, Any]:
        """Fetch intraday data for a symbol."""
        try:
            from_date = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
            return await self.get_historical_data(symbol, from_date, interval=interval)
        except Exception as e:
            self.logger.error(f"Error fetching intraday data for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_daily_data(
        self,
        symbol: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Fetch daily data for last n days."""
        try:
            from_date = datetime.now() - timedelta(days=days)
            return await self.get_historical_data(symbol, from_date, interval="1day")
        except Exception as e:
            self.logger.error(f"Error fetching daily data for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def calculate_returns(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate various return metrics."""
        try:
            returns = {
                'daily_returns': data['returns'].mean() * 100,
                'annualized_returns': data['returns'].mean() * 252 * 100,
                'volatility': data['returns'].std() * np.sqrt(252) * 100,
                'sharpe_ratio': (data['returns'].mean() * 252) / (data['returns'].std() * np.sqrt(252)),
                'max_drawdown': ((data['close'] / data['close'].expanding(min_periods=1).max()) - 1).min() * 100
            }
            return returns
        except Exception as e:
            self.logger.error(f"Error calculating returns: {e}")
            return {}