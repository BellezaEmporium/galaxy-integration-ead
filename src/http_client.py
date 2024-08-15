import logging
import time
from typing import Optional
import aiohttp
from aiohttp import ClientSession, CookieJar
from galaxy.http import HttpClient
from yarl import URL

from galaxy.api.errors import AccessDenied, AuthenticationRequired

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        self._auth_lost_callback = None
        self._cookie_jar = CookieJar()
        self._access_token = None
        self._refresh_token = None
        self._last_access_token_success = None
        self._save_last_callback = None
        self._session = ClientSession(cookie_jar=self._cookie_jar)

    def set_auth_lost_callback(self, callback):
        self._auth_lost_callback = callback

    def set_cookies_updated_callback(self, callback):
        self._cookie_jar.set_cookies_updated_callback(callback)

    async def authenticate(self, cookies):
        self._cookie_jar.update_cookies(cookies)
        if self._last_access_token_success < int(time.time()) - 259199:
            await self._refresh_access_token()
        else:
            await self._get_access_token()

    def is_authenticated(self):
        return self._access_token is not None

    async def get(self, *args, **kwargs):
        if not self._access_token:
            raise AccessDenied("No access token")
        try:
            return await self._authorized_get(*args, **kwargs)
        except (AuthenticationRequired, AccessDenied):
            await self._refresh_token()
            return await self._authorized_get(*args, **kwargs)
        
    async def post(self, *args, **kwargs):
        if not self._access_token:
            raise AccessDenied("No access token")
        try:
            return await self._authorized_post(*args, **kwargs)
        except (AuthenticationRequired, AccessDenied):
            await self._refresh_token()
            return await self._authorized_post(*args, **kwargs)

    async def _authorized_get(self, url, *args, **kwargs):
        headers = kwargs.setdefault("headers", {})
        headers["Authorization"] = "Bearer {}".format(self._access_token)
        async with self._session.get(url, *args, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    
    async def _authorized_post(self, url, *args, **kwargs):
        headers = kwargs.setdefault("headers", {})
        headers["Authorization"] = "Bearer {}".format(self._access_token)
        async with self._session.post(url, *args, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    async def _exchange_code_for_token(self, code):
        token_url = "https://accounts.ea.com/connect/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        token_params = f"client_id=JUNO_PC_CLIENT&client_secret=4mRLtYMb6vq9qglomWEaT4ChxsXWcyqbQpuBNfMPOYOiDmYYQmjuaBsF2Zp0RyVeWkfqhE9TuGgAw7te&grant_type=authorization_code&code={code}"
        try:
            async with self._session.post(token_url, headers=headers, params=token_params) as token_response:
                token_response_json = await token_response.json()
                if "access_token" in token_response_json:
                    self._access_token = token_response_json["access_token"]
                    self._refresh_token = token_response_json["refresh_token"]
                    return self._access_token, self._refresh_token
                elif token_response_json.get('error') == "invalid_request":
                    self._log_session_details()
                    raise AuthenticationRequired("Error parsing access token. Must reauthenticate.")
                else:
                    raise AccessDenied("Unexpected response when exchanging code for token")
        except Exception as e:
            logger.exception(f"Error in _exchange_code_for_token: {str(e)}")
            raise

    async def _refresh_access_token(self):
        url = "https://accounts.ea.com/connect/token"
        params = f"client_id=JUNO_PC_CLIENT&client_secret=4mRLtYMb6vq9qglomWEaT4ChxsXWcyqbQpuBNfMPOYOiDmYYQmjuaBsF2Zp0RyVeWkfqhE9TuGgAw7te&grant_type=refresh_token&refresh_token={self._refresh_token}"
        async with self._session.post(url, params=params, allow_redirects=False) as response:
            response_json = await response.json()
            if "access_token" in response_json:
                self._access_token = response_json["access_token"]
                self._refresh_token = response_json["refresh_token"]
                return self._access_token, self._refresh_token
            elif response_json.get('error') == "invalid_request":
                self._log_session_details()
                raise AuthenticationRequired("Error refreshing access token. Must reauthenticate.")

    async def _get_access_token(self):
        url = "https://accounts.ea.com/connect/auth"
        params = {
            "client_id": "JUNO_PC_CLIENT",
            "display": "junoWeb/login",
            "response_type": "code",
            "redirectUri": "nucleus:rest"
        }
        try:
            async with self._session.get(url, params=params, allow_redirects=False) as response:
                if "Location" not in response.headers:
                    logger.error("No Location header in response")
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
                    raise AccessDenied("Unexpected response when getting access token")
        except Exception as e:
            logger.exception(f"Error in _get_access_token: {str(e)}")
            raise
            
    # more logging for auth lost investigation

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