"""
Error handling utility for the application.
"""

import logging
import traceback
from functools import wraps
import chainlit as cl
from typing import Callable, Any, Optional
from datetime import datetime
from typing import List, Dict, Any

class ErrorHandler:
    def __init__(self):
        """Initialize ErrorHandler with logging configuration."""
        self.setup_logging()
        self.error_log = []

    def setup_logging(self) -> None:
        """Configure logging settings."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('app.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def handle_error(
        self,
        error: Exception,
        context: str = "",
        user_message: Optional[str] = None
    ) -> None:
        """
        Handle and log errors, notify user.
        
        Args:
            error: Exception object
            context: Error context
            user_message: Custom message for user
        """
        timestamp = datetime.now().isoformat()
        error_details = {
            'timestamp': timestamp,
            'type': type(error).__name__,
            'message': str(error),
            'context': context,
            'traceback': traceback.format_exc()
        }
        
        # Log error
        self.logger.error(
            f"Error in {context}: {str(error)}\n{traceback.format_exc()}"
        )
        self.error_log.append(error_details)
        
        # Notify user
        message = user_message or f"❌ An error occurred while {context}"
        await cl.Message(content=message).send()

    def with_error_handling(self, context: str) -> Callable:
        """
        Decorator for handling errors in async functions.
        
        Args:
            context: Error context
            
        Returns:
            Callable: Decorated function
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    await self.handle_error(e, context)
                    return None
            return wrapper
        return decorator

    def with_sync_error_handling(self, context: str) -> Callable:
        """
        Decorator for handling errors in synchronous functions.
        
        Args:
            context: Error context
            
        Returns:
            Callable: Decorated function
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    self.logger.error(
                        f"Error in {context}: {str(e)}\n{traceback.format_exc()}"
                    )
                    return None
            return wrapper
        return decorator

    def get_error_log(self) -> List[Dict[str, Any]]:
        """Get the error log."""
        return self.error_log

    def clear_error_log(self) -> None:
        """Clear the error log."""
        self.error_log = []