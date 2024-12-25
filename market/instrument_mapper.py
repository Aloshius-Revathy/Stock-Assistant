import json
import gzip
import requests
import pandas as pd
from io import BytesIO
from typing import Optional, Dict, Any
import logging

class InstrumentMapper:
    def __init__(self, url: str = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"):
        """
        Initialize InstrumentMapper with instrument data URL.
        
        Args:
            url: URL for instrument master data
        """
        self.url = url
        self.logger = logging.getLogger(__name__)
        self.df = self._load_instrument_data()

    def _load_instrument_data(self) -> pd.DataFrame:
        """
        Load and process instrument master data.
        
        Returns:
            pd.DataFrame: Processed instrument data
        """
        try:
            self.logger.info("Downloading instrument master data")
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()
            
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as file:
                data = json.load(file)
            
            df = pd.DataFrame(data)
            self.logger.info(f"Loaded {len(df)} instruments")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading instrument data: {e}")
            return pd.DataFrame()

    def search_by_trading_symbol(
        self,
        segment: str,
        trading_symbol: str
    ) -> Optional[str]:
        """
        Search for instrument key by trading symbol.
        
        Args:
            segment: Market segment (e.g., 'NSE_EQ')
            trading_symbol: Trading symbol (e.g., 'RELIANCE')
            
        Returns:
            Optional[str]: Instrument key if found
        """
        if self.df.empty:
            self.logger.error("No instrument data available")
            return None

        try:
            filtered_df = self.df[
                (self.df['segment'].str.upper() == segment.upper()) &
                (self.df['trading_symbol'].str.strip().str.upper() == trading_symbol.strip().upper())
            ]

            if not filtered_df.empty:
                instrument = filtered_df.iloc[0]
                self.logger.info(f"Found instrument: {instrument['trading_symbol']}")
                return f"{instrument['segment']}|{instrument['isin']}"
            else:
                self.logger.warning(f"No match found for {trading_symbol} in {segment}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error searching for instrument: {e}")
            return None

    def get_instrument_details(
        self,
        instrument_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for an instrument.
        
        Args:
            instrument_key: Instrument identifier
            
        Returns:
            Optional[Dict]: Instrument details if found
        """
        if self.df.empty:
            return None

        segment, isin = instrument_key.split('|')
        filtered_df = self.df[
            (self.df['segment'] == segment) &
            (self.df['isin'] == isin)
        ]

        if not filtered_df.empty:
            return filtered_df.iloc[0].to_dict()
        return None