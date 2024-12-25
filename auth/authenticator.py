import requests
import webbrowser
import asyncio
from typing import Optional
import logging
from .local_server import LocalServer

class Authenticator:
    def __init__(self):
        """Initialize the authenticator with required components."""
        self.access_token: Optional[str] = None
        self.local_server = LocalServer()
        self.logger = logging.getLogger(__name__)
        
        # Upstox OAuth2 URLs
        self.auth_url = "https://api-v2.upstox.com/login/authorization/dialog"
        self.token_url = "https://api-v2.upstox.com/login/authorization/token"
        
        # API credentials - Updated with correct format
        self.client_id = "89c19dba-d9fc-4acb-af4f-10eb37a28d5e"
        self.client_secret = "hpcp3xiwjo"  # Updated client secret
        self.redirect_uri = "http://localhost:5000/callback"

    def generate_auth_url(self) -> str:
        """Generate the authorization URL for Upstox login."""
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri
        }
        
        # Create properly encoded URL
        auth_url = f"{self.auth_url}?"
        auth_url += "&".join([f"{key}={requests.utils.quote(str(value))}" for key, value in params.items()])
        
        self.logger.info(f"Generated auth URL: {auth_url}")
        return auth_url

    async def authenticate(self) -> bool:
        """Complete authentication flow with Upstox."""
        try:
            # Start the local server first
            self.local_server.start()
            
            # Wait a moment for the server to start
            await asyncio.sleep(1)

            # Generate and open auth URL
            auth_url = self.generate_auth_url()
            self.logger.info(f"Opening auth URL: {auth_url}")
            webbrowser.open(auth_url)

            # Wait for authorization code (with timeout)
            for _ in range(120):  # 2 minute timeout
                await asyncio.sleep(1)
                if self.local_server.get_auth_code():
                    auth_code = self.local_server.get_auth_code()
                    return await self.fetch_access_token(auth_code)
            
            self.logger.error("Authentication timeout")
            return False

        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            return False

    async def fetch_access_token(self, auth_code: str) -> bool:
        """Fetch access token using the authorization code."""
        try:
            payload = {
                "code": auth_code,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code"
            }

            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'Api-Version': '2.0'  # Added API version header
            }

            self.logger.info("Fetching access token...")
            self.logger.info(f"Using client_id: {self.client_id}")
            self.logger.info(f"Using auth code: {auth_code}")
            
            response = requests.post(
                self.token_url,
                data=payload,
                headers=headers
            )

            if response.status_code != 200:
                self.logger.error(f"Token request failed: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False

            data = response.json()
            self.access_token = data.get("access_token")
            
            if self.access_token:
                self.logger.info("Access token obtained successfully")
                return True
            else:
                self.logger.error("No access token in response")
                self.logger.error(f"Response data: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error fetching access token: {e}")
            self.logger.error(f"Response content: {getattr(response, 'text', 'No response content')}")
            return False

    def get_access_token(self) -> Optional[str]:
        """Get the current access token."""
        return self.access_token