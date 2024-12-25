import chainlit as cl
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Import our modules
from auth.authenticator import Authenticator
from processors.prompt_processor import PromptProcessor
from processors.stock_processor import StockProcessor
from processors.analysis_processor import AnalysisProcessor  # Make sure this is imported
from utils.instrument_mapper import InstrumentMapper
from utils.historical_data import HistoricalDataFetcher
from utils.grok_analyzer import GrokAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
authenticator: Optional[Authenticator] = None
instrument_mapper: Optional[InstrumentMapper] = None
historical_data: Optional[HistoricalDataFetcher] = None
grok_analyzer: Optional[GrokAnalyzer] = None
prompt_processor: Optional[PromptProcessor] = None
stock_processor: Optional[StockProcessor] = None
analysis_processor: Optional[AnalysisProcessor] = None

@cl.on_chat_start
async def start():
    """Initialize components when chat starts."""
    try:
        global authenticator, instrument_mapper, historical_data, grok_analyzer
        global prompt_processor, stock_processor, analysis_processor

        # Send initial message
        await cl.Message(content="Initializing stock analysis system...").send()

        # Initialize authenticator
        authenticator = Authenticator()
        if not await authenticator.authenticate():
            await cl.Message(content="❌ Authentication failed").send()
            return

        # Get access token
        access_token = authenticator.get_access_token()
        if not access_token:
            await cl.Message(content="❌ No access token available").send()
            return

        # Initialize other components
        instrument_mapper = InstrumentMapper(access_token)
        await instrument_mapper.initialize()

        historical_data = HistoricalDataFetcher(access_token, instrument_mapper)
        grok_analyzer = GrokAnalyzer(os.getenv("GROK_API_KEY"))
        
        # Initialize processors
        prompt_processor = PromptProcessor(instrument_mapper)
        stock_processor = StockProcessor(access_token)
        analysis_processor = AnalysisProcessor(stock_processor)

        # Send ready message with example prompts
        examples = prompt_processor.get_example_prompts()
        example_text = "\n".join([f"- {example}" for example in examples])
        
        await cl.Message(content=f"""✅ System initialized! You can ask me about stocks using natural language.

Here are some example prompts you can try:

{example_text}
""").send()

    except Exception as e:
        logger.error(f"Startup error: {e}")
        await cl.Message(content=f"❌ Error during initialization: {str(e)}").send()

@cl.on_message
async def main(message: cl.Message):
    """Process user messages."""
    try:
        # Process the prompt
        prompt_result = prompt_processor.process_prompt(message.content)
        
        if not prompt_result.get('action'):
            await cl.Message(content="❌ I couldn't understand your request. Please try rephrasing it.").send()
            return

        # Get matching instruments
        matches = prompt_result.get('matches', [])
        if not matches and prompt_result['parameters'].get('symbol'):
            await cl.Message(
                content=f"❌ No matching instruments found for {prompt_result['parameters']['symbol']}"
            ).send()
            return

        # Show processing message
        await cl.Message(content="🔄 Processing your request...").send()

        # Process the request based on query type
        result = await analysis_processor.process_analysis_request(prompt_result)
        
        if not result['success']:
            await cl.Message(content=f"❌ Error: {result.get('error', 'Unknown error')}").send()
            return

        # Format and send the response
        if result.get('data'):
            # Create elements for visualization if needed
            if 'chart_data' in result['data']:
                chart = cl.Chart(data=result['data']['chart_data'])
                await chart.send()

            # Send formatted analysis
            await cl.Message(content=format_analysis_result(result['data'])).send()
        else:
            await cl.Message(content="❌ No data available for analysis.").send()

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await cl.Message(content=f"❌ An error occurred: {str(e)}").send()

def format_analysis_result(data: Dict[str, Any]) -> str:
    """Format analysis result for display."""
    # Implement formatting based on data structure
    # This is a basic example - enhance based on your needs
    formatted_text = "📊 Analysis Results:\n\n"
    
    if 'symbol' in data:
        formatted_text += f"Symbol: {data['symbol']}\n"
    
    if 'analysis' in data:
        formatted_text += f"\nAnalysis:\n{data['analysis']}\n"
    
    if 'recommendations' in data:
        formatted_text += "\nRecommendations:\n"
        for rec in data['recommendations']:
            formatted_text += f"- {rec}\n"
    
    return formatted_text

if __name__ == "__main__":
    cl.run()