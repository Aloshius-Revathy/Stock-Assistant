from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json
import requests
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import asyncio
#from bs4 import BeautifulSoup

class GrokAnalyzer:
    def __init__(self, api_key: str):
        """Initialize the Grok analyzer with API key."""
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://api.grok-ai.com/v1"
        
        # Initialize sentiment analysis model
        self.tokenizer = AutoTokenizer.from_pretrained('ProsusAI/finbert')
        self.model = AutoModelForSequenceClassification.from_pretrained('ProsusAI/finbert')
        
        # News API settings
        self.news_api_url = "https://newsapi.org/v2/everything"
        self.news_api_key = api_key  # Using same key for now
        
        # Cache settings
        self._cache: Dict[str, Dict] = {}

    async def analyze_stock(self, symbol: str, historical_data: pd.DataFrame) -> Dict[str, Any]:
        """Perform comprehensive stock analysis using Grok AI."""
        try:
            # Gather all analysis components concurrently
            technical_analysis, fundamental_analysis, news_analysis = await asyncio.gather(
                self._analyze_technical_patterns(historical_data),
                self._analyze_fundamentals(symbol),
                self._analyze_news_sentiment(symbol)
            )

            # Combine analyses and generate insights
            analysis_result = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'technical_analysis': technical_analysis,
                'fundamental_analysis': fundamental_analysis,
                'news_analysis': news_analysis,
                'insights': self._generate_insights(
                    technical_analysis,
                    fundamental_analysis,
                    news_analysis
                )
            }

            return {
                'success': True,
                'data': analysis_result
            }

        except Exception as e:
            self.logger.error(f"Error in Grok analysis for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def _analyze_technical_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze technical patterns using Grok AI."""
        try:
            # Prepare data for analysis
            features = self._extract_technical_features(data)
            
            # Make API request to Grok AI
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'data': features,
                'analysis_type': 'technical',
                'timeframe': '1d'
            }
            
            response = requests.post(
                f"{self.base_url}/analyze/technical",
                headers=headers,
                json=payload
            )
            
            response.raise_for_status()
            analysis = response.json()

            return {
                'patterns': analysis.get('patterns', []),
                'signals': analysis.get('signals', {}),
                'predictions': analysis.get('predictions', {}),
                'confidence': analysis.get('confidence', {})
            }

        except Exception as e:
            self.logger.error(f"Error in technical analysis: {e}")
            return {}

    async def _analyze_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Analyze fundamental factors using Grok AI."""
        try:
            # Make API request for fundamental analysis
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'symbol': symbol,
                'analysis_type': 'fundamental'
            }
            
            response = requests.post(
                f"{self.base_url}/analyze/fundamental",
                headers=headers,
                json=payload
            )
            
            response.raise_for_status()
            analysis = response.json()

            return {
                'metrics': analysis.get('metrics', {}),
                'ratios': analysis.get('ratios', {}),
                'growth': analysis.get('growth', {}),
                'peer_comparison': analysis.get('peer_comparison', {}),
                'recommendations': analysis.get('recommendations', [])
            }

        except Exception as e:
            self.logger.error(f"Error in fundamental analysis: {e}")
            return {}

    async def _analyze_news_sentiment(self, symbol: str) -> Dict[str, Any]:
        """Analyze news sentiment using FinBERT."""
        try:
            # Fetch recent news
            news_articles = await self._fetch_news(symbol)
            
            # Analyze sentiment for each article
            sentiments = []
            for article in news_articles:
                sentiment = await self._analyze_text_sentiment(article['title'] + " " + article['description'])
                sentiments.append({
                    'title': article['title'],
                    'sentiment': sentiment['sentiment'],
                    'score': sentiment['score'],
                    'date': article['publishedAt']
                })

            # Aggregate sentiment metrics
            sentiment_counts = {
                'positive': len([s for s in sentiments if s['sentiment'] == 'positive']),
                'negative': len([s for s in sentiments if s['sentiment'] == 'negative']),
                'neutral': len([s for s in sentiments if s['sentiment'] == 'neutral'])
            }
            
            avg_sentiment_score = np.mean([s['score'] for s in sentiments]) if sentiments else 0

            return {
                'articles': sentiments,
                'sentiment_distribution': sentiment_counts,
                'average_sentiment_score': avg_sentiment_score,
                'overall_sentiment': self._get_overall_sentiment(sentiment_counts)
            }

        except Exception as e:
            self.logger.error(f"Error in news sentiment analysis: {e}")
            return {}

    async def _fetch_news(self, symbol: str) -> List[Dict[str, Any]]:
        """Fetch recent news articles for a symbol."""
        try:
            params = {
                'q': f"{symbol} stock",
                'apiKey': self.news_api_key,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 100
            }
            
            response = requests.get(self.news_api_url, params=params)
            response.raise_for_status()
            
            return response.json().get('articles', [])

        except Exception as e:
            self.logger.error(f"Error fetching news for {symbol}: {e}")
            return []

    async def _analyze_text_sentiment(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of text using FinBERT."""
        try:
            inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
            outputs = self.model(**inputs)
            
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=1)
            sentiment_score = probabilities[0].tolist()
            
            # Map to sentiment labels
            labels = ['negative', 'neutral', 'positive']
            sentiment_idx = torch.argmax(probabilities[0]).item()
            
            return {
                'sentiment': labels[sentiment_idx],
                'score': sentiment_score[sentiment_idx],
                'probabilities': {
                    label: score for label, score in zip(labels, sentiment_score)
                }
            }

        except Exception as e:
            self.logger.error(f"Error in sentiment analysis: {e}")
            return {
                'sentiment': 'neutral',
                'score': 0.0,
                'probabilities': {'negative': 0.0, 'neutral': 1.0, 'positive': 0.0}
            }

    def _extract_technical_features(self, data: pd.DataFrame) -> Dict[str, List[float]]:
        """Extract technical analysis features from historical data."""
        return {
            'close': data['close'].tolist(),
            'volume': data['volume'].tolist(),
            'high': data['high'].tolist(),
            'low': data['low'].tolist(),
            'open': data['open'].tolist()
        }

    def _get_overall_sentiment(self, sentiment_counts: Dict[str, int]) -> str:
        """Determine overall sentiment from distribution."""
        total = sum(sentiment_counts.values())
        if total == 0:
            return 'neutral'
        
        positive_ratio = sentiment_counts['positive'] / total
        negative_ratio = sentiment_counts['negative'] / total
        
        if positive_ratio > 0.6:
            return 'strongly positive'
        elif positive_ratio > 0.4:
            return 'positive'
        elif negative_ratio > 0.6:
            return 'strongly negative'
        elif negative_ratio > 0.4:
            return 'negative'
        else:
            return 'neutral'

    def _generate_insights(
        self,
        technical: Dict[str, Any],
        fundamental: Dict[str, Any],
        news: Dict[str, Any]
    ) -> List[str]:
        """Generate insights by combining all analyses."""
        insights = []
        
        # Technical insights
        if technical.get('patterns'):
            insights.extend([f"Technical Pattern: {p}" for p in technical['patterns']])
        
        # Fundamental insights
        if fundamental.get('recommendations'):
            insights.extend(fundamental['recommendations'])
        
        # News sentiment insights
        if news.get('overall_sentiment'):
            insights.append(f"News Sentiment: {news['overall_sentiment']}")
        
        return insights

    async def get_market_analysis(self) -> Dict[str, Any]:
        """Get overall market analysis."""
        try:
            # Implement market-wide analysis
            # This could include sector performance, market trends, etc.
            return {
                'success': True,
                'data': {
                    'market_sentiment': 'neutral',
                    'sector_performance': {},
                    'market_trends': []
                }
            }
        except Exception as e:
            self.logger.error(f"Error in market analysis: {e}")
            return {
                'success': False,
                'error': str(e)
            }