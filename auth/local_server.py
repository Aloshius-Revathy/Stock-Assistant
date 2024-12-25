from flask import Flask, request
import logging
from typing import Optional
import threading

class LocalServer:
    def __init__(self, port: int = 5000):
        """Initialize the local server for OAuth callback."""
        self.app = Flask(__name__)
        self.auth_code: Optional[str] = None
        self.port = port
        self.logger = logging.getLogger(__name__)
        self.server_thread = None
        self.setup_logging()
        self.setup_routes()

    def setup_logging(self) -> None:
        """Configure logging for the server."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def setup_routes(self) -> None:
        """Set up Flask routes for the callback."""
        @self.app.route('/callback')
        def callback():
            try:
                # Get the authorization code from the callback
                self.auth_code = request.args.get('code')
                if self.auth_code:
                    self.logger.info(f"Received auth code: {self.auth_code}")
                    return """
                    <html>
                        <body style="background-color: #f0f0f0; font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                            <h2 style="color: #4CAF50;">Authentication Successful! ✅</h2>
                            <p>You can close this window and return to the application.</p>
                            <script>
                                setTimeout(function() {
                                    window.close();
                                }, 3000);
                            </script>
                        </body>
                    </html>
                    """
                else:
                    self.logger.error("No authorization code received")
                    return """
                    <html>
                        <body style="background-color: #f0f0f0; font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                            <h2 style="color: #f44336;">Authentication Failed! ❌</h2>
                            <p>Please try again in the main application.</p>
                        </body>
                    </html>
                    """
            except Exception as e:
                self.logger.error(f"Callback error: {e}")
                return "Error processing callback", 500

    def start(self) -> None:
        """Start the server in a new thread."""
        if not self.server_thread:
            self.server_thread = threading.Thread(target=self._run_server)
            self.server_thread.daemon = True
            self.server_thread.start()
            self.logger.info(f"Server started on port {self.port}")

    def _run_server(self) -> None:
        """Internal method to run the Flask server."""
        try:
            self.app.run(port=self.port, debug=False, threaded=True)
        except Exception as e:
            self.logger.error(f"Server error: {e}")

    def get_auth_code(self) -> Optional[str]:
        """Get the received authorization code."""
        return self.auth_code

    def shutdown(self) -> None:
        """Shutdown the Flask server."""
        try:
            func = request.environ.get('werkzeug.server.shutdown')
            if func:
                func()
                self.logger.info("Server shutdown successfully")
        except Exception as e:
            self.logger.error(f"Error shutting down server: {e}")