import base64
import json
import logging
import time
import asyncio
import re

import aiohttp
from aiohttp import ClientSession, CookieJar, ClientTimeout
from galaxy.http import HttpClient
from yarl import URL
from galaxy.api.errors import AccessDenied, AuthenticationRequired, BackendError, NetworkError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_TIMEOUT   = 30
MAX_RETRIES       = 3
RETRY_BACKOFF     = 1.5

_USER_AGENTS = [
    "EAApp/PC/13.680.0.6193",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Origin/10.6.0.00000 EAApp/13.680.0.6193 Chrome/109.0.5414.120 Safari/537.36",
]
_ua_index = 0

_GQL_OP_RE = re.compile(r'(?:query|mutation)\s+(\w+)')


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
        self._client_id     = "JUNO_PC_CLIENT"
        self._client_secret = "4mRLtYMb6vq9qglomWEaT4ChxsXWcyqbQpuBNfMPOYOiDmYYQmjuaBsF2Zp0RyVeWkfqhE9TuGgAw7te"
        self._auth_lost_callback  = None
        self._save_lats_callback  = None
        self._save_tokens_callback = None

        self._cookie_jar = CustomCookieJar()
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._last_access_token_success: int | None = None
        self._access_token_expires_at: int | None = None
        self._token_lock = asyncio.Lock()
        self._refreshing_token = False

        self._static_headers = {
            "x-client-id": "EAX-JUNO-CLIENT",
            "referer":     "https://pc.ea.com/",
        }

        self._session = ClientSession(
            cookie_jar=self._cookie_jar,
            timeout=ClientTimeout(total=DEFAULT_TIMEOUT),
            connector=aiohttp.TCPConnector(
                limit=10, force_close=True,
                enable_cleanup_closed=True, ttl_dns_cache=300,
            ),
            headers=self._get_default_headers(),
        )

        self._request_cache:     dict = {}
        self._cache_timestamps:  dict = {}
        self._cache_expiry:      int  = 300

    def set_auth_lost_callback(self, cb):       self._auth_lost_callback   = cb
    def set_save_lats_callback(self, cb):       self._save_lats_callback   = cb
    def set_save_tokens_callback(self, cb):     self._save_tokens_callback = cb
    def set_cookies_updated_callback(self, cb): self._cookie_jar.set_cookies_updated_callback(cb)

    def load_lats_from_cache(self, value: str | None):
        self._last_access_token_success = int(value) if value else None

    def is_authenticated(self) -> bool:
        return self._access_token is not None

    def is_access_token_valid(self) -> bool:
        if not self._access_token:
            return False
        if self._access_token_expires_at is None:
            exp = self._parse_jwt_exp(self._access_token)
            if exp:
                self._access_token_expires_at = exp
            else:
                return True
        return time.time() < self._access_token_expires_at

    def _parse_jwt_exp(self, token: str) -> int | None:
        try:
            parts = token.split('.')
            if len(parts) < 2:
                return None
            padded = parts[1] + '=' * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
            exp = payload.get('exp')
            return int(exp) if isinstance(exp, (int, float)) else None
        except Exception:
            return None

    def _save_lats(self):
        if not self._save_lats_callback:
            return
        new_lats = int(time.time())
        if self._last_access_token_success is None or abs(new_lats - self._last_access_token_success) > 1:
            self._last_access_token_success = new_lats
            self._save_lats_callback(new_lats)

    def _get_default_headers(self) -> dict:
        global _ua_index
        headers = self._static_headers.copy()
        headers["User-Agent"] = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
        _ua_index += 1
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        return headers

    async def _exchange_auth_code_for_token(self, code: str, code_verifier: str):
        async with self._token_lock:
            params = {
                "token_format":  "JWS",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
                "code_verifier": code_verifier,
                "grant_type":   "authorization_code",
                "redirect_uri": "qrc:///html/login_successful.html",
                "code":         code,
            }
            try:
                async with self._session.post(
                    "https://accounts.ea.com/connect/token",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=params,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except aiohttp.ClientError as e:
                raise NetworkError(f"Token exchange network error: {e}")
            except Exception as e:
                raise BackendError(f"Token exchange error: {e}")

            if "access_token" not in data or "refresh_token" not in data:
                raise BackendError("Token exchange: invalid response")

            self._store_token_data(data)
            if self._save_tokens_callback and not self._refreshing_token:
                self._save_tokens_callback(self._access_token, self._refresh_token)
            logger.info("Token exchange successful")
            return self._access_token, self._refresh_token

    async def _refresh_access_token(self, refresh_token: str):
        async with self._token_lock:
            if not refresh_token:
                raise AuthenticationRequired("No refresh token")
            self._refreshing_token = True
            try:
                async with self._session.post(
                    "https://accounts.ea.com/connect/token",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={
                        "client_id":     self._client_id,
                        "client_secret": self._client_secret,
                        "grant_type":    "refresh_token",
                        "refresh_token": refresh_token,
                    },
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except aiohttp.ClientError as e:
                raise NetworkError(f"Token refresh network error: {e}")
            except Exception as e:
                self._access_token = self._refresh_token = None
                if self._auth_lost_callback:
                    self._auth_lost_callback()
                raise AccessDenied(f"Token refresh failed: {e}")
            finally:
                self._refreshing_token = False

            if "access_token" not in data:
                raise BackendError("Token refresh: invalid response")
            self._store_token_data(data)
            if self._save_tokens_callback:
                self._save_tokens_callback(self._access_token, self._refresh_token)
            logger.info("Token refresh successful")
            return self._access_token, self._refresh_token

    def _store_token_data(self, data: dict):
        self._access_token  = data["access_token"]
        self._refresh_token = data.get("refresh_token", self._refresh_token)
        expires_in = data.get("expires_in")
        now = int(time.time())
        if isinstance(expires_in, (int, float)):
            self._access_token_expires_at = now + int(expires_in) - 60
        else:
            exp = self._parse_jwt_exp(self._access_token if self._access_token else "")
            self._access_token_expires_at = (exp - 60) if exp else None
        self._save_lats()

    async def _request(self, method: str, url: str, *args, **kwargs) -> dict:
        label = f"{method} {url}"
        jbody = kwargs.get("json")
        if isinstance(jbody, dict) and "query" in jbody:
            m = _GQL_OP_RE.search(jbody["query"])
            if m:
                label = f"{method} {url} [{m.group(1)}]"

        if method.upper() == "GET":
            cache_key = f"{url}:{kwargs.get('params', '')}"
            if cache_key in self._request_cache:
                if time.time() - self._cache_timestamps[cache_key] < self._cache_expiry:
                    logger.debug("Cache hit: %s", url)
                    return self._request_cache[cache_key]

        headers = kwargs.setdefault("headers", {})
        headers.update(self._get_default_headers())

        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self._session.request(method, url, *args, **kwargs) as resp:
                    resp.raise_for_status()
                    result = await resp.json()
                    if method.upper() == "GET":
                        self._request_cache[cache_key]    = result
                        self._cache_timestamps[cache_key] = time.time()
                    return result

            except aiohttp.ClientResponseError as e:
                last_exc = e
                if e.status == 401:
                    if self._refresh_token and attempt < MAX_RETRIES:
                        try:
                            await self._refresh_access_token(self._refresh_token)
                            headers.update(self._get_default_headers())
                            continue
                        except Exception as re_err:
                            raise AuthenticationRequired(str(re_err))
                    raise AuthenticationRequired("401 Unauthorized")
                if e.status >= 500:
                    wait = RETRY_BACKOFF ** attempt
                    logger.warning("%s — server error %d, retry %d/%d in %.1fs",
                                   label, e.status, attempt, MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                    continue
                raise

            except (aiohttp.ClientConnectionError, OSError, asyncio.TimeoutError) as e:
                last_exc = e
                wait = RETRY_BACKOFF ** attempt
                logger.warning("%s — %s, retry %d/%d in %.1fs",
                               label, type(e).__name__, attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)

            except aiohttp.ClientError as e:
                raise NetworkError(str(e))
            except Exception as e:
                raise BackendError(str(e))

        if isinstance(last_exc, asyncio.TimeoutError):
            raise NetworkError("Request timed out after retries")
        raise BackendError(f"Request failed after {MAX_RETRIES} retries: {last_exc}")

    async def get(self, url, *args, **kwargs):
        return await self._request("GET", url, *args, **kwargs)

    async def post(self, url, *args, **kwargs):
        return await self._request("POST", url, *args, **kwargs)

    async def authenticate(self, cookies: dict | None = None):
        if cookies:
            try:
                self._cookie_jar.update_cookies(cookies)
            except Exception as e:
                logger.warning("Failed to apply cookies: %s", e)

    def clear_cache(self):
        self._request_cache.clear()
        self._cache_timestamps.clear()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _log_session_details(self):
        try:
            utag = next(c for c in self._cookie_jar if c.key == 'utag_main')
            parts = {i.split(':')[0]: i.split(':')[1] for i in utag.value.split('$')}
            logger.info('now=%s st=%s ses_id=%s lats=%s',
                        int(time.time()), parts['_st'][:10],
                        parts['ses_id'][:10], self._last_access_token_success)
        except Exception as e:
            logger.warning("Failed to get session duration: %s", e)