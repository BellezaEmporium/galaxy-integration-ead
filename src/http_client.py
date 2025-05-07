import logging
import time
from typing import Optional, Tuple
import aiohttp
from aiohttp import ClientSession, CookieJar, ClientTimeout
from galaxy.http import HttpClient
from yarl import URL
import asyncio

from galaxy.api.errors import AccessDenied, AuthenticationRequired, BackendError, NetworkError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constantes pour la configuration des requêtes HTTP
DEFAULT_TIMEOUT = 30  # secondes
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # secondes entre les tentatives

class CookieJar(aiohttp.CookieJar):
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
        self._cookie_jar = CookieJar()
        self._access_token = None
        self._refresh_token = None
        self._last_access_token_success = None
        self._save_lats_callback = None
        
        # Configuration optimisée pour le client HTTP
        timeout = ClientTimeout(total=DEFAULT_TIMEOUT, connect=10.0, sock_connect=10.0, sock_read=10.0)
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
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            }
        )

    def set_auth_lost_callback(self, callback):
        self._auth_lost_callback = callback

    def set_cookies_updated_callback(self, callback):
        self._cookie_jar.set_cookies_updated_callback(callback)

    async def authenticate(self, cookies):
        self._cookie_jar.update_cookies(cookies)
        if self._last_access_token_success and self._last_access_token_success < int(time.time()) - 259199:
            await self._refresh_access_token()
        else:
            await self._get_access_token()

    def is_authenticated(self):
        return self._access_token is not None

    async def get(self, *args, **kwargs):
        if not self._access_token:
            raise AccessDenied("No access token")
        try:
            return await self._request_with_retry("GET", *args, **kwargs)
        except (AuthenticationRequired, AccessDenied):
            try:
                await self._refresh_access_token()
                return await self._request_with_retry("GET", *args, **kwargs)
            except Exception as e:
                logger.error(f"Error while processing GET request: {str(e)}")
                raise
        
    async def post(self, *args, **kwargs):
        if not self._access_token:
            raise AccessDenied("No access token")
        try:
            return await self._request_with_retry("POST", *args, **kwargs)
        except (AuthenticationRequired, AccessDenied):
            try:
                await self._refresh_access_token()
                return await self._request_with_retry("POST", *args, **kwargs)
            except Exception as e:
                logger.error(f"Error while processing POST request: {str(e)}")
                raise

    async def _request_with_retry(self, method, url, *args, **kwargs):
        headers = kwargs.setdefault("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"
        headers["AuthToken"] = self._access_token
        headers["X-AuthToken"] = self._access_token
        
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                if method == "GET":
                    async with self._session.get(url, *args, **kwargs) as response:
                        response.raise_for_status()
                        return await response.json()
                else:  # POST
                    async with self._session.post(url, *args, **kwargs) as response:
                        response.raise_for_status()
                        return await response.json()
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    logger.warning(f"Expired access token, unauthorized error on the {attempt+1}/{MAX_RETRIES}rd try")
                    raise AuthenticationRequired("Authentication required")
                elif e.status >= 500:
                    logger.warning(f"Server error {e.status} on the {attempt+1}/{MAX_RETRIES}rd try")
                    last_error = e
                    if attempt < MAX_RETRIES - 1:
                        await self._wait_before_retry(attempt)
                        continue
                    raise BackendError(f"Server error: {e.status}")
                else:
                    logger.error(f"HTTP error {e.status} while requesting {url}")
                    raise
            except aiohttp.ClientConnectionError as e:
                logger.warning(f"Connection error on the {attempt+1}/{MAX_RETRIES}rd try: {str(e)}")
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    await self._wait_before_retry(attempt)
                    continue
                raise NetworkError(f"Connection error: {str(e)}")
            except aiohttp.ClientError as e:
                logger.error(f"HTTP Client error while requesting {url}: {str(e)}")
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    await self._wait_before_retry(attempt)
                    continue
                raise NetworkError(f"HTTP client error: {str(e)}")
            except Exception as e:
                logger.exception(f"Unexpected error while requesting {url}: {str(e)}")
                raise BackendError(f"Unexpected error: {str(e)}")
        
        raise NetworkError(f"Failed after {MAX_RETRIES} attempts: {str(last_error)}")
    
    async def _wait_before_retry(self, attempt):
        delay = RETRY_DELAY * (2 ** attempt)
        logger.debug(f"Delaying with {delay:.2f} seconds before the next attempt")
        await asyncio.sleep(delay)

    async def _exchange_code_for_token(self, code: str) -> Tuple[str, str]:
        token_url = "https://accounts.ea.com/connect/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            data = {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "redirect_uri": "qrc:///html/login_successful.html",
                "token_format": "jwt"
            }
            
            logger.debug("Exchanging code for token...")
            last_error = None
            
            for attempt in range(MAX_RETRIES):
                try:
                    async with self._session.post(token_url, headers=headers, data=data) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Couldn't exchange code for token. Status: {response.status}, Answer: {error_text}")
                            if response.status == 401:
                                raise AuthenticationRequired("Invalid authorization code")
                            last_error = f"Status code: {response.status}"
                            if attempt < MAX_RETRIES - 1:
                                await self._wait_before_retry(attempt)
                                continue
                            raise AccessDenied(f"Failed to exchange code with status {response.status}")
                        
                        response_data = await response.json()
                        
                        if "access_token" not in response_data or "refresh_token" not in response_data:
                            logger.error(f"Invalid token response: {response_data}")
                            raise AccessDenied("Invalid token response format")
                        
                        self._access_token = response_data["access_token"]
                        self._refresh_token = response_data["refresh_token"]
                        
                        self._save_lats()
                        
                        logger.info("Successfully exchanged code for token")
                        return self._access_token, self._refresh_token
                except (aiohttp.ClientConnectionError, aiohttp.ClientOSError) as e:
                    logger.warning(f"Connection error on the {attempt+1}/{MAX_RETRIES}rd try: {str(e)}")
                    last_error = str(e)
                    if attempt < MAX_RETRIES - 1:
                        await self._wait_before_retry(attempt)
                    else:
                        raise NetworkError(f"Connection error during token exchange: {str(e)}")
            
            raise AccessDenied(f"Failed to exchange code after {MAX_RETRIES} attempts: {last_error}")
                
        except aiohttp.ClientError as e:
            logger.exception(f"Network error during token exchange: {str(e)}")
            raise NetworkError(f"Network error during token exchange: {str(e)}")
            
        except Exception as e:
            logger.exception(f"Unexpected error while exchanging code for tokens: {str(e)}")
            raise AccessDenied(f"Failed to exchange code: {str(e)}")

    async def _refresh_access_token(self, refresh_token: Optional[str] = None) -> Tuple[str, str]:
        if refresh_token is None:
            refresh_token = self._refresh_token
            
        if not refresh_token:
            raise AuthenticationRequired("No refresh token available")
        
        url = "https://accounts.ea.com/connect/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            data = {
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "token_format": "jwt"
            }
            
            logger.info("Refreshing access token...")
            
            last_error = None
            for attempt in range(MAX_RETRIES):
                try:
                    async with self._session.post(url, headers=headers, data=data) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Error while refreshing token. Status: {response.status}, Answer: {error_text}")
                            last_error = f"Status code: {response.status}"
                            
                            if attempt < MAX_RETRIES - 1:
                                await self._wait_before_retry(attempt)
                                continue
                            
                            if self._auth_lost_callback:
                                self._auth_lost_callback()
                            raise AuthenticationRequired("Failed to refresh token")
                        
                        response_data = await response.json()
                        
                        if "access_token" in response_data and "refresh_token" in response_data:
                            self._access_token = response_data["access_token"]
                            self._refresh_token = response_data["refresh_token"]
                            self._save_lats()
                            logger.info("Successfully refreshed access token")
                            return self._access_token, self._refresh_token
                        else:
                            logger.error(f"Invalid answer while refreshing token: {response_data}")
                            last_error = "Invalid response format"
                            
                            if attempt < MAX_RETRIES - 1:
                                await self._wait_before_retry(attempt)
                                continue
                                
                            if self._auth_lost_callback:
                                self._auth_lost_callback()
                            raise AuthenticationRequired("Invalid refresh token response")
                except (aiohttp.ClientConnectionError, aiohttp.ClientOSError) as e:
                    logger.warning(f"Connection error on the {attempt+1}/{MAX_RETRIES}rd try: {str(e)}")
                    last_error = str(e)
                    if attempt < MAX_RETRIES - 1:
                        await self._wait_before_retry(attempt)
                    else:
                        raise NetworkError(f"Connection error during token refresh: {str(e)}")
            
            # Si on arrive ici, toutes les tentatives ont échoué
            if self._auth_lost_callback:
                self._auth_lost_callback()
            raise AuthenticationRequired(f"Failed to refresh token after {MAX_RETRIES} attempts: {last_error}")
                    
        except aiohttp.ClientError as e:
            logger.exception(f"Network error during token refresh: {str(e)}")
            raise NetworkError(f"Network error during token refresh: {str(e)}")
            
        except Exception as e:
            if not isinstance(e, AuthenticationRequired):
                logger.exception(f"Unexpected error while refreshing token: {str(e)}")
                if self._auth_lost_callback:
                    self._auth_lost_callback()
            raise AuthenticationRequired(f"Failed to refresh token: {str(e)}")

    async def _get_access_token(self):
        url = "https://accounts.ea.com/connect/auth"
        params = {
            "client_id": self._client_id,
            "display": "junoWeb/login",
            "response_type": "code",
            "redirectUri": "nucleus:rest"
        }
        
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.get(url, params=params, allow_redirects=False) as response:
                    if "Location" not in response.headers:
                        logger.error("No location header in response")
                        last_error = "No Location header"
                        if attempt < MAX_RETRIES - 1:
                            await self._wait_before_retry(attempt)
                            continue
                        raise AccessDenied("No Location header in response")
                    
                    location = response.headers["Location"]
                    if "code" in location:
                        data = location
                        code = data.split("?")[1].split("=")[1]
                        return await self._exchange_code_for_token(code)
                    elif "code" not in location and "error=login_required" in location:
                        self._log_session_details()
                        raise AuthenticationRequired("Error parsing code. Must reauthenticate.")
                    else:
                        last_error = "Unexpected response format"
                        if attempt < MAX_RETRIES - 1:
                            await self._wait_before_retry(attempt)
                            continue
                        raise AccessDenied("Unexpected response when getting access token")
            except (aiohttp.ClientConnectionError, aiohttp.ClientOSError) as e:
                logger.warning(f"Connection error on the {attempt+1}/{MAX_RETRIES}rd try: {str(e)}")
                last_error = str(e)
                if attempt < MAX_RETRIES - 1:
                    await self._wait_before_retry(attempt)
                else:
                    raise NetworkError(f"Connection error during token acquisition: {str(e)}")
        
        raise AccessDenied(f"Failed to get access token after {MAX_RETRIES} attempts: {last_error}")

    def _save_lats(self):
        if self._save_lats_callback is not None:
            self._last_access_token_success = int(time.time())
            self._save_lats_callback(self._last_access_token_success)

    def set_save_lats_callback(self, callback):
        self._save_lats_callback = callback

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
    
    async def close(self):
        if self._session and not self._session.closed:
            try:
                # Closing the HTTP session securely with a timeout
                await asyncio.wait_for(self._session.close(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout while closing HTTP session, continuing anyway")
            except Exception as e:
                logger.warning(f"Error while closing HTTP session: {str(e)}")