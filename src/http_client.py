import base64
import json
import logging
import time
import asyncio
from typing import Optional
import aiohttp
from aiohttp import ClientSession, CookieJar, ClientTimeout
from galaxy.http import HttpClient
from yarl import URL
from galaxy.api.errors import AccessDenied, AuthenticationRequired, BackendError, NetworkError

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# HTTP request timeout and retry configuration
DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5  # exponential backoff multiplier

class CustomCookieJar(CookieJar):
    def __init__(self):
        super().__init__()
        self._cookies_updated_callback = None

    def set_cookies_updated_callback(self, callback):
        self._cookies_updated_callback = callback

    def update_cookies(self, cookies, url=URL()):
        super().update_cookies(cookies, url)
        if cookies and self._cookies_updated_callback:
            self._cookies_updated_callback(list(self))

class AuthenticatedHttpClient(HttpClient):
    def __init__(self):
        self._client_id = "JUNO_PC_CLIENT"
        self._client_secret = "4mRLtYMb6vq9qglomWEaT4ChxsXWcyqbQpuBNfMPOYOiDmYYQmjuaBsF2Zp0RyVeWkfqhE9TuGgAw7te"
        self._auth_lost_callback = None
        self._cookie_jar = CustomCookieJar()
        self._access_token = None
        self._refresh_token = None
        self._last_access_token_success = None
        self._save_lats_callback = None
        self._save_tokens_callback = None
        self._token_lock = asyncio.Lock()
        self._access_token_expires_at = None
        self._refreshing_token = False  # Flag to prevent multiple saves during refresh
        self._static_headers = {
            "User-Agent": "EAApp/PC/13.468.0.5981",
            "x-client-id": "EAX-JUNO-CLIENT"
        }

        timeout = ClientTimeout(total=DEFAULT_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=20,
            force_close=False,
            enable_cleanup_closed=True,
            ttl_dns_cache=300
        )

        self._session = ClientSession(
            cookie_jar=self._cookie_jar,
            timeout=timeout,
            connector=connector,
            headers=self._get_default_headers()
        )

        self._request_cache = {}
        self._cache_timestamps = {}
        self._cache_expiry = 300

    async def authenticate(self, cookies: Optional[dict] = None):
        """Compatibility method: optionally set cookies. Real auth happens via OAuth code exchange.
        """
        try:
            if cookies:
                self._cookie_jar.update_cookies(cookies)
        except Exception as e:
            logger.warning(f"Failed to apply cookies during authenticate(): {e}")
        return None

    def _get_default_headers(self):
        """Common headers for all requests"""
        headers = self._static_headers.copy()
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
            
        return headers

    async def _request(self, method: str, url: str, *args, **kwargs) -> dict:
        """
        Generic request handler with retry logic and caching for GET requests
        """
        if method.upper() == "GET":
            cache_key = f"{url}:{str(kwargs.get('params', ''))}"
            current_time = time.time()
            
            if cache_key in self._request_cache:
                if current_time - self._cache_timestamps[cache_key] < self._cache_expiry:
                    logger.debug(f"Using cached response for {url}")
                    return self._request_cache[cache_key]
                
        headers = kwargs.setdefault("headers", {})
        headers.update(self._get_default_headers())
        
        retry_count = 0
        last_exception = None
        
        while retry_count < MAX_RETRIES:
            try:
                async with self._session.request(method, url, *args, **kwargs) as response:
                    response.raise_for_status()
                    result = await response.json()
                    
                    # Cache the result for GET requests
                    if method.upper() == "GET":
                        self._request_cache[cache_key] = result
                        self._cache_timestamps[cache_key] = time.time()
                        
                    return result
                    
            except aiohttp.ClientResponseError as e:
                last_exception = e
                # Handle specific error codes
                if e.status == 401:  # Unauthorized
                    logger.warning(f"Received 401 from {url}, attempting to refresh token")
                    # Try to refresh the token if unauthorized
                    if self._refresh_token and retry_count < MAX_RETRIES - 1:
                        try:
                            await self._refresh_access_token(self._refresh_token)
                            # Update headers with new token
                            headers.update(self._get_default_headers())
                        except Exception as refresh_error:
                            logger.error(f"Failed to refresh token: {refresh_error}")
                            raise AuthenticationRequired("Authentication required after token refresh failed")
                    else:
                        raise AuthenticationRequired("Authentication required")
                elif e.status >= 500:  # Server error, can retry
                    retry_count += 1
                    wait_time = RETRY_BACKOFF ** retry_count
                    logger.warning(f"Server error {e.status} on attempt {retry_count}/{MAX_RETRIES}, retrying in {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    continue
                else:  # Other client errors, can't retry
                    logger.error(f"Client error: {e.status} - {e.message}")
                    raise
                    
            except aiohttp.ClientConnectionError as e:
                last_exception = e
                retry_count += 1
                wait_time = RETRY_BACKOFF ** retry_count
                logger.warning(f"Connection error on attempt {retry_count}/{MAX_RETRIES}, retrying in {wait_time:.1f}s: {str(e)}")
                await asyncio.sleep(wait_time)
                
            except aiohttp.ClientError as e:
                logger.error(f"Request failed with client error: {str(e)}")
                raise NetworkError(f"Network error: {str(e)}")
                
            except asyncio.TimeoutError:
                last_exception = TimeoutError("Request timed out")
                retry_count += 1
                wait_time = RETRY_BACKOFF ** retry_count
                logger.warning(f"Request timed out on attempt {retry_count}/{MAX_RETRIES}, retrying in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                logger.exception(f"Unexpected error: {str(e)}")
                raise BackendError(f"Unexpected error: {str(e)}")
        
        # If we got here, we've exhausted all retries
        if isinstance(last_exception, asyncio.TimeoutError):
            raise NetworkError("Request timed out after multiple retries")
        elif last_exception:
            raise BackendError(f"Request failed after {MAX_RETRIES} retries: {str(last_exception)}")
        else:
            raise BackendError(f"Request failed after {MAX_RETRIES} retries")

    async def get(self, url, *args, **kwargs):
        return await self._request("GET", url, *args, **kwargs)

    async def post(self, url, *args, **kwargs): 
        return await self._request("POST", url, *args, **kwargs)

    async def _exchange_auth_code_for_token(self, code: str):
        async with self._token_lock:
            token_url = "https://accounts.ea.com/connect/token"
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            token_params = {
                "token_format": "JWS",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "authorization_code",
                "redirect_uri": "qrc:///html/login_successful.html",
                "code": code
            }
            
            try:
                async with self._session.post(token_url, headers=headers, data=token_params) as response:
                    response.raise_for_status()
                    response_data = await response.json()
                
                if "access_token" not in response_data or "refresh_token" not in response_data:
                    logger.error(f"Invalid token response: {response_data}")
                    raise BackendError("Failed to exchange code for tokens: Invalid response")
                
                self._access_token = response_data["access_token"]
                self._refresh_token = response_data["refresh_token"]
                # token lifetime
                expires_in = response_data.get("expires_in")
                now = int(time.time())
                if isinstance(expires_in, (int, float)):
                    self._access_token_expires_at = now + int(expires_in) - 60  # 60s slack
                else:
                    # Fallback: parse JWT 'exp'
                    exp = _parse_jwt_exp(self._access_token)
                    if exp:
                        self._access_token_expires_at = exp - 60
                self._save_lats()
                
                # Save tokens via callback if available (only if not in refresh cycle)
                if self._save_tokens_callback and not self._refreshing_token:
                    self._save_tokens_callback(self._access_token, self._refresh_token)
                
                logger.info("Successfully exchanged code for tokens")
                return self._access_token, self._refresh_token
                
            except aiohttp.ClientError as e:
                logger.exception(f"Network error while exchanging code for tokens: {str(e)}")
                raise NetworkError("Failed to exchange code for tokens due to network error")
                
            except Exception as e:
                logger.exception(f"Unexpected error while exchanging code for tokens: {str(e)}")
                raise BackendError("Unexpected error while exchanging code for tokens")

    async def _refresh_access_token(self, refresh_token: str):
        async with self._token_lock:
            if not refresh_token:
                raise AuthenticationRequired("No refresh token available")
            
            # Set flag to prevent multiple saves during refresh
            self._refreshing_token = True
            
            try:
                url = "https://accounts.ea.com/connect/token"
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                params = {
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                }
                
                logger.info("Using stored credentials to refresh the access token...")
                async with self._session.post(url, headers=headers, data=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                
                if "access_token" in data and "refresh_token" in data:
                    self._access_token = data["access_token"]
                    self._refresh_token = data["refresh_token"]
                    # token lifetime
                    expires_in = data.get("expires_in")
                    now = int(time.time())
                    if isinstance(expires_in, (int, float)):
                        self._access_token_expires_at = now + int(expires_in) - 60
                    else:
                        try:
                            exp = _parse_jwt_exp(self._access_token)
                            if exp:
                                self._access_token_expires_at = exp - 60
                        except Exception:
                            self._access_token_expires_at = None
                    logger.info("Successfully refreshed the access token.")
                    self._save_lats()
                    
                    # Save tokens via callback if available - always save on successful refresh
                    if self._save_tokens_callback:
                        self._save_tokens_callback(self._access_token, self._refresh_token)
                    
                    return self._access_token, self._refresh_token
                else:
                    raise BackendError("Failed to refresh token: Invalid response")
                    
            except aiohttp.ClientError as e:
                logger.warning(f"Network error while refreshing token: {str(e)}")
                raise NetworkError("Failed to refresh token due to network error")
                
            except Exception as e:
                logger.exception(f"Failed to refresh token: {str(e)}")
                self._access_token = None
                self._refresh_token = None
                if self._auth_lost_callback:
                    self._auth_lost_callback()
                raise AccessDenied("Failed to refresh token")
            
            finally:
                # Always reset the flag
                self._refreshing_token = False

    def _save_lats(self):
        if self._save_lats_callback is not None:
            new_lats = int(time.time())
            # Only save if the value has changed significantly (> 1 second)
            if (self._last_access_token_success is None or 
                abs(new_lats - self._last_access_token_success) > 1):
                self._last_access_token_success = new_lats
                self._save_lats_callback(self._last_access_token_success)

    def set_save_lats_callback(self, callback):
        self._save_lats_callback = callback

    def set_save_tokens_callback(self, callback):
        """Set callback to save access and refresh tokens"""
        self._save_tokens_callback = callback

    def load_lats_from_cache(self, value: Optional[str]):
        self._last_access_token_success = int(value) if value else None

    def _log_session_details(self):
        try:
            utag_main_cookie = next(filter(lambda c: c.key == 'utag_main', self._cookie_jar))
            utag_main = {i.split(':')[0]: i.split(':')[1] for i in utag_main_cookie.value.split('$')}
            logger.info('now: %s st: %s ses_id: %s lats: %s',
                str(int(time.time())),
                utag_main['_st'][:10],
                utag_main['ses_id'][:10],
                str(self._last_access_token_success)
            )
        except Exception as e:
            logger.warning('Failed to get session duration: %s', repr(e))

    def set_auth_lost_callback(self, callback):
        self._auth_lost_callback = callback
        
    def set_cookies_updated_callback(self, callback):
        self._cookie_jar.set_cookies_updated_callback(callback)
        
    def clear_cache(self):
        """Clear the request cache"""
        self._request_cache = {}
        self._cache_timestamps = {}
        
    async def close(self):
        """Close the HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()

    def is_authenticated(self):
        """Return True if the user is authenticated, False otherwise"""
        return self._access_token is not None

    def is_access_token_valid(self) -> bool:
        if not self._access_token:
            return False
        if self._access_token_expires_at is None:
            # If we can't determine expiration, try to parse JWT
            exp = _parse_jwt_exp(self._access_token)
            if exp:
                self._access_token_expires_at = exp
                return time.time() < self._access_token_expires_at
            # If we can't parse, assume it's valid (fallback behavior)
            return True
        return time.time() < self._access_token_expires_at
    
def _parse_jwt_exp(jwt_token: str) -> Optional[int]:
    """Parse JWT token and extract expiration timestamp.
    
    Args:
        jwt_token: The JWT token string
        
    Returns:
        Expiration timestamp as int, or None if parsing fails
    """
    try:
        parts = jwt_token.split('.')
        if len(parts) < 2:
            return None
            
        # Correct base64 padding
        payload_b64 = parts[1]
        payload_b64 += '=' * (4 - len(payload_b64) % 4) if len(payload_b64) % 4 else ''
        
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes.decode('utf-8'))
        
        exp = payload.get('exp')
        if isinstance(exp, (int, float)):
            return int(exp)
    except Exception as e:
        logger.debug(f"Failed to parse JWT expiration: {e}")
    
    return None