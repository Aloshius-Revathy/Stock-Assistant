"""
Grok AI analyzer for generating stock market insights.
"""

from openai import AsyncOpenAI
from typing import Dict, Any, Optional, List
import logging
from config.settings import XAI_API_KEY, GROK_SETTINGS

class GrokAnalyzer:
    def __init__(self):
        """Initialize Grok AI analyzer with API configuration."""
        self.client = AsyncOpenAI(
            api_key=XAI_API_KEY,
            base_url="https://api.x.ai/v1",
        )
        self.settings = GROK_SETTINGS
        self.logger = logging.getLogger(__name__)

    async def generate_insights(
        self,
        market_data: Dict[str, Any],
        historical_data: List[List[Any]]
    ) -> str:
        """
        Generate AI insights for stock data.
        
        Args:
            market_data: Current market data
            historical_data: Historical OHLCV data
            
        Returns:
            str: Generated insights
        """
        try:
            prompt = self._create_analysis_prompt(market_data, historical_data)
            response = await self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                **self.settings
            )
            return response.choices[0].message.content
        except Exception as e:
            self.logger.error(f"Error generating insights: {e}")
            return "Error generating insights. Please try again."

    def _create_analysis_prompt(
        self,
        market_data: Dict[str, Any],
        historical_data: List[List[Any]]
    ) -> str:
        """
        Create analysis prompt for Grok AI.
        
        Args:
            market_data: Current market data
            historical_data: Historical OHLCV data
            
        Returns:
            str: Formatted prompt
        """
        # Calculate basic metrics
        current_price = market_data.get('ltp', 'N/A')
        day_high = market_data.get('high', 'N/A')
        day_low = market_data.get('low', 'N/A')
        volume = market_data.get('volume', 'N/A')
        
        # Calculate price changes
        price_changes = self._calculate_price_changes(historical_data)
        
        return f"""
        As a stock market expert, analyze this data and provide detailed investment insights:
        
        Current Market Data:
        - Current Price: ₹{current_price}
        - Day High: ₹{day_high}
        - Day Low: ₹{day_low}
        - Volume: {volume}
        
        Price Changes:
        {price_changes}
        
        Historical Data Summary:
        {self._summarize_historical_data(historical_data)}
        
        Please provide:
        1. Technical Analysis
           - Key support and resistance levels
           - Trend analysis
           - Volume analysis
           
        2. Pattern Recognition
           - Identify any chart patterns
           - Key breakout or breakdown levels
           
        3. Risk Assessment
           - Volatility analysis
           - Risk factors
           - Position sizing recommendations
           
        4. Investment Recommendation
           - Short-term outlook (1-5 days)
           - Medium-term outlook (1-4 weeks)
           - Suggested entry/exit points
           - Stop-loss levels
           
        5. Additional Insights
           - Market sentiment
           - Trading volume analysis
           - Any notable observations
        """

    def _calculate_price_changes(self, historical_data: List[List[Any]]) -> str:
        """
        Calculate price changes from historical data.
        
        Args:
            historical_data: Historical OHLCV data
            
        Returns:
            str: Formatted price changes
        """
        try:
            if not historical_data or len(historical_data) < 2:
                return "Insufficient historical data"

            latest_close = float(historical_data[-1][4])
            prev_close = float(historical_data[-2][4])
            week_ago_close = float(historical_data[-5][4]) if len(historical_data) >= 5 else prev_close
            month_ago_close = float(historical_data[-20][4]) if len(historical_data) >= 20 else week_ago_close

            daily_change = ((latest_close - prev_close) / prev_close) * 100
            weekly_change = ((latest_close - week_ago_close) / week_ago_close) * 100
            monthly_change = ((latest_close - month_ago_close) / month_ago_close) * 100

            return f"""
            - Daily Change: {daily_change:.2f}%
            - Weekly Change: {weekly_change:.2f}%
            - Monthly Change: {monthly_change:.2f}%
            """
        except Exception as e:
            self.logger.error(f"Error calculating price changes: {e}")
            return "Error calculating price changes"

    def _summarize_historical_data(self, historical_data: List[List[Any]]) -> str:
        """
        Create summary of historical data.
        
        Args:
            historical_data: Historical OHLCV data
            
        Returns:
            str: Formatted summary
        """
        try:
            if not historical_data:
                return "No historical data available"

            # Extract prices
            closes = [float(candle[4]) for candle in historical_data]
            highs = [float(candle[2]) for candle in historical_data]
            lows = [float(candle[3]) for candle in historical_data]
            volumes = [float(candle[5]) for candle in historical_data]

            # Calculate metrics
            avg_price = sum(closes) / len(closes)
            max_price = max(highs)
            min_price = min(lows)
            avg_volume = sum(volumes) / len(volumes)

            return f"""
            Historical Data Analysis:
            - Average Price: ₹{avg_price:.2f}
            - Highest Price: ₹{max_price:.2f}
            - Lowest Price: ₹{min_price:.2f}
            - Average Volume: {int(avg_volume):,}
            - Price Range: ₹{max_price - min_price:.2f}
            - Days Analyzed: {len(historical_data)}
            """
        except Exception as e:
            self.logger.error(f"Error summarizing historical data: {e}")
            return "Error summarizing historical data"

    async def get_sentiment_analysis(self, symbol: str) -> str:
        """
        Generate market sentiment analysis.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            str: Sentiment analysis
        """
        try:
            prompt = f"""
            Analyze the market sentiment for {symbol} considering:
            1. Current market conditions
            2. Sector performance
            3. Technical indicators
            4. Trading patterns
            
            Provide a comprehensive sentiment analysis.
            """
            
            response = await self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                **self.settings
            )
            return response.choices[0].message.content
        except Exception as e:
            self.logger.error(f"Error generating sentiment analysis: {e}")
            return "Error generating sentiment analysis"