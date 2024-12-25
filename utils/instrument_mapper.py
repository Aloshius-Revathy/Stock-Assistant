from typing import Dict, List, Any, Optional
import logging
import json
import gzip
import io
import requests
from datetime import datetime, timedelta
import os
from fuzzywuzzy import fuzz  # For fuzzy string matching

class InstrumentMapper:
    def __init__(self, access_token: str = None, url: str = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"):
        """Initialize the instrument mapper."""
        self.access_token = access_token
        self.master_url = url
        self.base_url = "https://api-v2.upstox.com"
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        
        # Cache settings
        self.cache_file = "instrument_cache.json"
        self.cache_duration = timedelta(days=1)  # Refresh master data daily
        self._instruments_cache: List[Dict] = []
        self._last_cache_update: Optional[datetime] = None

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

    def set_access_token(self, access_token: str):
        """Set the access token after authentication."""
        self.access_token = access_token

    async def initialize(self) -> bool:
        """Initialize and load instrument data."""
        try:
            # Check if we have valid cached data
            if self._load_cache():
                self.logger.info("Loaded instruments from cache")
                return True

            # If no valid cache, fetch fresh data
            return await self.refresh_master_data()

        except Exception as e:
            self.logger.error(f"Error initializing instrument mapper: {e}")
            return False

    async def refresh_master_data(self) -> bool:
        """Fetch and refresh master data from Upstox."""
        try:
            self.logger.info("Fetching master data from Upstox...")
            
            # Fetch master contract from the gzipped URL
            response = requests.get(self.master_url)
            if response.status_code != 200:
                self.logger.error(f"Failed to download master data: {response.status_code}")
                return False

            # Decompress the gzipped content
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                data = json.loads(gz.read().decode('utf-8'))

            # Process and store instruments
            self._instruments_cache = self._process_master_data(data)
            self._last_cache_update = datetime.now()
            
            # Save to cache file
            self._save_cache()
            
            self.logger.info(f"Successfully loaded {len(self._instruments_cache)} instruments")
            return True

        except Exception as e:
            self.logger.error(f"Error refreshing master data: {e}")
            return False

    def _process_master_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Process raw master data into structured format."""
        processed_data = []
        
        for instrument in raw_data:
            try:
                processed_instrument = {
                    'instrument_key': f"{instrument.get('exchange')}-{instrument.get('symbol')}",
                    'exchange': instrument.get('exchange'),
                    'trading_symbol': instrument.get('symbol'),
                    'name': instrument.get('name'),
                    'instrument_type': instrument.get('type'),
                    'short_name': instrument.get('symbol'),
                    'isin': instrument.get('isin'),
                    'lot_size': instrument.get('lot_size'),
                    'tick_size': instrument.get('tick_size'),
                    'strike': instrument.get('strike'),
                    'expiry': instrument.get('expiry'),
                    'token': instrument.get('token')
                }
                processed_data.append(processed_instrument)
                
            except Exception as e:
                self.logger.error(f"Error processing instrument {instrument.get('symbol')}: {e}")
                continue
        
        return processed_data

    def _save_cache(self) -> None:
        """Save instruments data to cache file."""
        try:
            cache_data = {
                'last_update': self._last_cache_update.isoformat(),
                'instruments': self._instruments_cache
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f)
                
            self.logger.info(f"Saved {len(self._instruments_cache)} instruments to cache")
            
        except Exception as e:
            self.logger.error(f"Error saving cache: {e}")

    def _load_cache(self) -> bool:
        """Load instruments data from cache if valid."""
        try:
            if not os.path.exists(self.cache_file):
                return False
                
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            last_update = datetime.fromisoformat(cache_data['last_update'])
            if datetime.now() - last_update > self.cache_duration:
                self.logger.info("Cache expired")
                return False
            
            self._instruments_cache = cache_data['instruments']
            self._last_cache_update = last_update
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading cache: {e}")
            return False

    def smart_search(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Smart search for instruments using multiple parameters.
        
        Args:
            query: Search string from user
            limit: Maximum number of results to return
            
        Returns:
            List of matching instruments
        """
        query = query.upper().strip()
        matches = []
        
        # Split query into potential parts
        parts = query.split()
        
        for instrument in self._instruments_cache:
            score = 0
            
            # Check trading symbol (exact match gets highest score)
            if instrument.get('trading_symbol') == query:
                score += 100
            elif any(part == instrument.get('trading_symbol') for part in parts):
                score += 80
                
            # Check name similarity
            name_score = fuzz.partial_ratio(query, instrument.get('name', '').upper())
            score += name_score * 0.5
            
            # Check short name
            if instrument.get('short_name') == query:
                score += 90
            elif any(part == instrument.get('short_name') for part in parts):
                score += 70
                
            # Check ISIN (if query looks like ISIN)
            if query.startswith('IN') and len(query) == 12:
                if instrument.get('isin') == query:
                    score += 100
                    
            # Add instrument type bonus if specified
            instrument_types = {'EQ': 'EQUITY', 'FUT': 'FUTURES', 'OPT': 'OPTIONS', 'IDX': 'INDEX'}
            for short_type, full_type in instrument_types.items():
                if short_type in parts or full_type in parts:
                    if instrument.get('instrument_type') in [short_type, full_type]:
                        score += 50
                        
            # Check exchange if specified
            exchanges = {'NSE', 'BSE'}
            for exchange in exchanges:
                if exchange in parts and instrument.get('exchange') == exchange:
                    score += 50
                    
            if score > 50:  # Threshold for relevance
                matches.append({
                    'instrument': instrument,
                    'score': score,
                    'display_text': self._format_instrument_display(instrument)
                })
                
        # Sort by score and return top matches
        matches.sort(key=lambda x: x['score'], reverse=True)
        return matches[:limit]

    def _format_instrument_display(self, instrument: Dict[str, Any]) -> str:
        """Format instrument details for display."""
        parts = [
            f"Symbol: {instrument.get('trading_symbol')}",
            f"Name: {instrument.get('name')}",
            f"Type: {instrument.get('instrument_type')}",
            f"Exchange: {instrument.get('exchange')}",
        ]
        return " | ".join(parts)

    def get_instrument_by_token(self, token: str) -> Optional[Dict]:
        """Get instrument details by token."""
        for instrument in self._instruments_cache:
            if instrument.get('token') == token:
                return instrument
        return None

    def get_instruments_by_type(self, instrument_type: str) -> List[Dict]:
        """Get all instruments of a specific type."""
        return [
            instrument for instrument in self._instruments_cache
            if instrument.get('instrument_type') == instrument_type
        ]

    def get_instruments_by_exchange(self, exchange: str) -> List[Dict]:
        """Get all instruments from a specific exchange."""
        return [
            instrument for instrument in self._instruments_cache
            if instrument.get('exchange') == exchange
        ]

    def get_instrument_by_isin(self, isin: str) -> Optional[Dict]:
        """Get instrument details by ISIN."""
        for instrument in self._instruments_cache:
            if instrument.get('isin') == isin:
                return instrument
        return None