from typing import Dict, Any, List
import re
from datetime import datetime, timedelta
import logging

class PromptProcessor:
    def __init__(self, instrument_mapper):
        """Initialize the prompt processor with instrument mapper."""
        self.instrument_mapper = instrument_mapper
        self.logger = logging.getLogger(__name__)
        
        # Define prompt patterns for different types of queries
        self.patterns = {
            'historical': r'(?i)(?:get|show|display)\s+(\d+)\s+(year|month|day|week)s?\s+(?:data|history)\s+(?:of\s+)?([A-Za-z\s]+)',
            'top_performers': r'(?i)(?:show|get|display)\s+top\s+(\d+)\s+(?:performing\s+)?stocks\s+(?:in|for|over)\s+(?:the\s+)?(?:last\s+)?(\d+)\s+(day|month|week)s?',
            'trend_analysis': r'(?i)(?:show|get|analyze|give)\s+(?:me\s+)?(?:a\s+)?trend\s+analysis\s+(?:of\s+)?([A-Za-z\s]+)?',
            'stock_details': r'(?i)(?:get|show|display)\s+(?:me\s+)?(?:the\s+)?(?:stock\s+)?(?:details|data|info)\s+(?:of\s+)?([A-Za-z\s]+)',
            'price_movement': r'(?i)(?:analyze|show)\s+(?:the\s+)?price\s+movement\s+(?:of\s+)?([A-Za-z\s]+)\s+(?:in|for|over)\s+(?:the\s+)?(?:last\s+)?(\d+)\s+(day|month|week)s?',
            'market_sentiment': r'(?i)(?:what|how)\s+is\s+(?:the\s+)?market\s+sentiment\s+(?:for\s+)?([A-Za-z\s]+)?',
            'volume_analysis': r'(?i)(?:analyze|show)\s+(?:the\s+)?volume\s+(?:analysis|data)\s+(?:of\s+)?([A-Za-z\s]+)',
            'sector_performance': r'(?i)(?:how|what)\s+is\s+(?:the\s+)?([A-Za-z\s]+)\s+sector\s+(?:performance|doing)',
            'comparison': r'(?i)(?:compare|show)\s+(?:the\s+)?(?:performance\s+)?(?:of\s+)?([A-Za-z\s]+)\s+(?:with|and|vs)\s+([A-Za-z\s]+)',
            'support_resistance': r'(?i)(?:show|get|find)\s+(?:the\s+)?support\s+and\s+resistance\s+(?:levels\s+)?(?:for\s+)?([A-Za-z\s]+)',
            'moving_averages': r'(?i)(?:show|get|calculate)\s+(?:the\s+)?(?:(\d+)\s+day\s+)?moving\s+average\s+(?:for\s+)?([A-Za-z\s]+)',
            'rsi_analysis': r'(?i)(?:show|get|calculate)\s+(?:the\s+)?rsi\s+(?:analysis\s+)?(?:for\s+)?([A-Za-z\s]+)',
            'dividend_history': r'(?i)(?:show|get)\s+(?:the\s+)?dividend\s+history\s+(?:of\s+)?([A-Za-z\s]+)',
            'news_sentiment': r'(?i)(?:show|get)\s+(?:the\s+)?news\s+sentiment\s+(?:for\s+)?([A-Za-z\s]+)',
        }

    def find_matching_instruments(self, query: str) -> List[Dict]:
        """Find matching instruments from master data."""
        try:
            query = query.strip().upper()
            matches = []
            
            for instrument in self.instrument_mapper.get_all_instruments():
                # Check both trading symbol and company name
                if (query in instrument['tradingsymbol'] or 
                    query in instrument.get('company_name', '').upper()):
                    matches.append({
                        'symbol': instrument['tradingsymbol'],
                        'name': instrument.get('company_name', instrument['tradingsymbol']),
                        'exchange': instrument['exchange'],
                        'instrument_type': instrument['instrument_type'],
                        'token': instrument.get('instrument_token')
                    })
            
            self.logger.info(f"Found {len(matches)} matches for query: {query}")
            return matches

        except Exception as e:
            self.logger.error(f"Error finding matching instruments: {e}")
            return []

    def process_prompt(self, prompt: str) -> Dict[str, Any]:
        """Process user prompt and identify the type of request."""
        try:
            result = {
                'action': None,
                'parameters': {},
                'matches': [],
                'query_type': None,
                'original_prompt': prompt
            }

            # Check each pattern
            for query_type, pattern in self.patterns.items():
                match = re.search(pattern, prompt)
                if match:
                    result['query_type'] = query_type
                    result['action'] = f'get_{query_type}'
                    
                    # Extract parameters based on query type
                    if query_type == 'moving_averages':
                        period = match.group(1) or '50'  # Default to 50-day MA
                        symbol = match.group(2)
                        result['parameters'] = {
                            'symbol': symbol.strip(),
                            'period': int(period),
                            'from_date': self._calculate_date_range(int(period), 'day')
                        }
                    
                    elif query_type == 'comparison':
                        symbol1, symbol2 = match.groups()
                        result['parameters'] = {
                            'symbol1': symbol1.strip(),
                            'symbol2': symbol2.strip(),
                            'duration': 30,  # Default comparison period
                            'unit': 'day'
                        }
                        # Find matches for both symbols
                        result['matches'] = {
                            'symbol1': self.find_matching_instruments(symbol1),
                            'symbol2': self.find_matching_instruments(symbol2)
                        }
                    
                    elif query_type == 'sector_performance':
                        sector = match.group(1)
                        result['parameters'] = {
                            'sector': sector.strip(),
                            'duration': 30,
                            'unit': 'day'
                        }
                    
                    elif query_type == 'support_resistance':
                        symbol = match.group(1)
                        result['parameters'] = {
                            'symbol': symbol.strip(),
                            'duration': 90,  # Default to 90 days for better S/R levels
                            'unit': 'day'
                        }
                    
                    elif query_type == 'rsi_analysis':
                        symbol = match.group(1)
                        result['parameters'] = {
                            'symbol': symbol.strip(),
                            'period': 14,  # Default RSI period
                            'duration': 30,
                            'unit': 'day'
                        }
                    
                    elif query_type == 'volume_analysis':
                        symbol = match.group(1)
                        result['parameters'] = {
                            'symbol': symbol.strip(),
                            'duration': 30,
                            'unit': 'day'
                        }
                    
                    else:
                        # Handle other query types
                        result['parameters'] = self._extract_basic_parameters(match, query_type)

                    # Find matching instruments if we have a symbol
                    if result['parameters'].get('symbol'):
                        matches = self.find_matching_instruments(result['parameters']['symbol'])
                        result['matches'] = matches
                        
                        # If we have exactly one match, update the symbol to the exact trading symbol
                        if len(matches) == 1:
                            result['parameters']['symbol'] = matches[0]['symbol']
                            result['parameters']['instrument_token'] = matches[0]['token']

                    break  # Stop after first match

            self.logger.info(f"Processed prompt: {result['query_type']}")
            return result

        except Exception as e:
            self.logger.error(f"Error processing prompt: {e}")
            return {
                'action': None,
                'error': str(e),
                'original_prompt': prompt
            }

    def _extract_basic_parameters(self, match: re.Match, query_type: str) -> Dict[str, Any]:
        """Extract basic parameters from regex match based on query type."""
        if query_type in ['historical', 'price_movement']:
            duration, unit, symbol = match.groups()
            return {
                'duration': int(duration),
                'unit': unit,
                'symbol': symbol.strip(),
                'from_date': self._calculate_date_range(int(duration), unit)
            }
        elif query_type == 'top_performers':
            count, duration, unit = match.groups()
            return {
                'count': int(count),
                'duration': int(duration),
                'unit': unit,
                'from_date': self._calculate_date_range(int(duration), unit)
            }
        else:
            # For simple queries that just need a symbol
            symbol = match.group(1)
            return {
                'symbol': symbol.strip() if symbol else None,
                'duration': 30,  # Default to 30 days
                'unit': 'day',
                'from_date': self._calculate_date_range(30, 'day')
            }

    def _calculate_date_range(self, duration: int, unit: str) -> datetime:
        """Calculate the from_date based on duration and unit."""
        today = datetime.now()
        unit_mapping = {
            'day': lambda x: timedelta(days=x),
            'week': lambda x: timedelta(weeks=x),
            'month': lambda x: timedelta(days=x*30),
            'year': lambda x: timedelta(days=x*365)
        }
        return today - unit_mapping[unit.lower()](duration)

    def get_example_prompts(self) -> List[str]:
        """Return example prompts that the processor can handle."""
        return [
            "Show me 5 year data of TCS",
            "Get top 10 performing stocks in last 30 days",
            "Show trend analysis of Reliance",
            "Compare performance of HDFC with ICICI",
            "Show 200 day moving average for Infosys",
            "Calculate RSI analysis for Wipro",
            "Show support and resistance levels for TATA MOTORS",
            "What is the market sentiment for IT sector",
            "Show volume analysis of SBI",
            "Get dividend history of ITC"
        ]