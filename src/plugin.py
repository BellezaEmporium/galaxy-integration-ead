import asyncio
import json
import logging
from operator import is_
import pathlib
import platform
import re
import subprocess
import sys
import time
import webbrowser
from functools import partial
from typing import Any, Callable, Dict, List, NewType, Optional, AsyncGenerator, NamedTuple, Set, Iterable, Tuple
from urllib.parse import urlparse, parse_qs

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import (
    AuthenticationRequired, BackendError, UnknownBackendResponse, UnknownError
)
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, UserInfo, Game, GameTime, LicenseInfo, LocalGame,
    NextStep, Subscription, SubscriptionGame
)

from backend import MasterTitleId, OfferId, EABackendClient, Timestamp, AchievementSet, Json
from http_client import AuthenticatedHttpClient
from lgames_manifests import (
    RegistryManager,
    local_game_status,
    parse_total_size,
    update_local_games,
    parse_registry_expression,
    resolve_registry_expression,
)
from uri_scheme_handler import is_uri_handler_installed
from version import __version__
from pcsign_hash import preload_pc_sign_cache, generate_pc_sign_fast, extract_user_info_from_jwt

logger = logging.getLogger(__name__)

def is_windows():
    return platform.system().lower() == "windows"

LOCAL_GAMES_CACHE_VALID_PERIOD = 5 * 60  # 5 minutes

def regex_pattern(regex):
    return ".*" + re.escape(regex) + ".*"

JS = {regex_pattern(r"juno/login?execution"): [
r'''
    document.getElementById("rememberMe").checked = true;
'''
]}

MultiplayerId = NewType("MultiplayerId", str)
GameId = NewType("GameId", str)  # eg. Origin.OFR:12345 or Origin.OFR:12345@epic
GameSlug = NewType("GameSlug", str)  # eg. "battlefield-1"


class AchievementsImportContext(NamedTuple):
    owned_games: Dict[GameSlug, AchievementSet]
    achievements: Dict[AchievementSet, List[Achievement]]


class GameLibrarySettingsContext(NamedTuple):
    favorite: Set[OfferId]
    hidden: Set[OfferId]


class AuthenticationManager:
    """Handles all authentication-related functionality."""
    
    def __init__(self, http_client: AuthenticatedHttpClient, backend_client: EABackendClient):
        self.http_client = http_client
        self.backend_client = backend_client
        self._user_id = None
        self._persona_id = None
    
    @property
    def user_id(self) -> Optional[str]:
        return self._user_id
    
    @property
    def persona_id(self) -> Optional[str]:
        return self._persona_id
    
    def is_authenticated(self) -> bool:
        return self.http_client.is_authenticated()
    
    def check_authenticated(self):
        """Raises AuthenticationRequired if not authenticated."""
        if not self.is_authenticated():
            raise AuthenticationRequired("User not authenticated")
    
    async def begin_auth_flow(self) -> NextStep:
        """Start new authentication flow."""
        try:
            pc_sign = generate_pc_sign_fast()
        except Exception as e:
            logger.error(f"Failed to generate PC sign: {e}")
            pc_sign = ""
        
        params = {
            "window_title": "Login to EA Desktop",
            "window_width": 495 if is_windows() else 480,
            "window_height": 850 if is_windows() else 825,
            "start_uri": f"https://accounts.ea.com/connect/auth"
                         f"?response_type=code&client_id=JUNO_PC_CLIENT&display=junoClient/login"
                         f"&redirect_uri=qrc:///html/login_successful.html"
                         f"&locale=en_US&pc_sign={pc_sign}",
            "end_uri_regex": "qrc:/html/login_successful.html.*"
        }
        return NextStep("web_session", params, js=JS)
    
    async def authenticate_with_code(self, code: str) -> Tuple[str, str, str]:
        """Authenticate using authorization code and return user info."""
        if code:
            await self.http_client._exchange_auth_code_for_token(code)
        else:
            raise AuthenticationRequired("No authorization code provided")
        
        return await self.get_identity()
    
    async def get_identity(self) -> Tuple[str, str, str]:
        """Get user identity from JWT token or backend."""
        try:
            # Try to extract from JWT token first
            if hasattr(self.http_client, '_access_token') and self.http_client._access_token:
                try:
                    logger.debug("Attempting to get identity from JWT extraction")
                    self._user_id, self._persona_id, user_name = extract_user_info_from_jwt(self.http_client._access_token)
                    logger.info(f"Identity successfully obtained from JWT extraction: {user_name}")
                    return self._user_id, self._persona_id, user_name
                except Exception as e:
                    logger.warning(f"Failed to extract from JWT: {e}")
            
            # Fallback to backend
            self._user_id, self._persona_id, user_name = await self.backend_client.get_identity()
            logger.info(f"Identity successfully obtained from backend: {user_name}")
            return self._user_id, self._persona_id, user_name
            
        except Exception as e:
            logger.error(f"Failed to get identity: {e}")
            raise AuthenticationRequired("Failed to get identity")


class CacheManager:
    """Manages persistent caching for the plugin."""
    
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self._game_time_cache = {}
        self._offer_id_cache = {}
    
    @property
    def game_time_cache(self) -> Dict[OfferId, GameTime]:
        return self._game_time_cache
    
    @game_time_cache.setter 
    def game_time_cache(self, value: Dict[OfferId, GameTime]):
        self._game_time_cache = value
        self.plugin.push_cache()
    
    @property
    def offer_id_cache(self) -> Dict[OfferId, Json]:
        return self._offer_id_cache
    
    @offer_id_cache.setter
    def offer_id_cache(self, value: Dict[OfferId, Json]):
        self._offer_id_cache = value
        self.plugin.push_cache()
    
    def get_offer_from_cache(self, offer_id: OfferId) -> Optional[Json]:
        return self._offer_id_cache.get(offer_id)
    
    def cache_offers(self, offers: Dict[OfferId, Json]):
        """Cache multiple offers."""
        self._offer_id_cache.update(offers)
        self.plugin.push_cache()


class LocalGameManager:
    """Manages local game detection and status updates."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self._local_games = []
        self._local_games_last_update = 0
        self._local_games_update_in_progress = False
    
    def update_local_games(self):
        """Update local games list."""
        return update_local_games(self)
    
    def get_local_game_status(self):
        """Get local game status changes."""
        return local_game_status(self)
    
    @property
    def _offer_id_cache(self):
        """Provide interface expected by lgames_manifests functions."""
        return self.cache_manager.offer_id_cache
    
    def should_update_cache(self) -> bool:
        """Check if local games cache should be updated."""
        return (
            not self._local_games_update_in_progress and
            time.time() - self._local_games_last_update >= LOCAL_GAMES_CACHE_VALID_PERIOD
        )


class EAPlugin(Plugin):
    """Main EA Desktop plugin class with simplified, modular architecture."""
    
    def __init__(self, reader, writer, token):
        super().__init__(Platform.Origin, __version__, reader, writer, token)
        
        # Initialize HTTP client and backend
        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(lambda: self.lost_authentication())
        self._http_client.set_cookies_updated_callback(self._update_stored_cookies)
        self._http_client.set_save_lats_callback(self._save_lats)
        self._http_client.set_save_tokens_callback(self._store_tokens)
        
        self._backend_client = EABackendClient(self._http_client)
        
        # Initialize managers
        self._auth_manager = AuthenticationManager(self._http_client, self._backend_client)
        self._cache_manager = CacheManager(self)
        self._local_game_manager = LocalGameManager(self._cache_manager)
        
        self._persistent_cache_updated = False
        self._last_offers_prefetch = 0

    async def _prefetch_offers_background(self):
        try:
            # Throttle prefetch to avoid hammering right after startup
            now = int(time.time())
            if now - self._last_offers_prefetch < 60:
                return
            # Only if authenticated and token is valid
            if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
                return
            # let the session settle a bit after auth/login
            await asyncio.sleep(2)
            entitlements = await self._backend_client.get_entitlements()
            offer_ids: List[OfferId] = []
            for e in entitlements:
                origin_offer_id = e.get("originOfferId")
                if isinstance(origin_offer_id, str) and origin_offer_id:
                    offer_ids.append(OfferId(origin_offer_id))
            if offer_ids:
                await self._get_offers(offer_ids)
            self._last_offers_prefetch = now
        except Exception as e:
            logger.debug(f"Background offers prefetch failed: {e}")

    @property
    def _game_time_cache(self) -> Dict[OfferId, GameTime]:
        return self._cache_manager.game_time_cache

    @_game_time_cache.setter
    def _game_time_cache(self, value: Dict[OfferId, GameTime]):
        self._cache_manager.game_time_cache = value

    @property
    def _offer_id_cache(self) -> Dict[OfferId, Json]:
        return self._cache_manager.offer_id_cache

    @_offer_id_cache.setter
    def _offer_id_cache(self, value: Dict[OfferId, Json]):
        self._cache_manager.offer_id_cache = value

    async def shutdown(self):
        await self._http_client.close()

    def tick(self):
        self.handle_local_game_update_notifications()    
    
    def _check_authenticated(self):
        self._auth_manager.check_authenticated()
    
    async def authenticate(self, stored_credentials):
        if stored_credentials:
            try:
                cookies = stored_credentials.get("cookies")
                if cookies:
                    self._http_client._cookie_jar.update_cookies(cookies)
                
                access_token = stored_credentials.get("access_token")
                refresh_token = stored_credentials.get("refresh_token")

                # Load tokens into http client
                if access_token:
                    self._http_client._access_token = access_token
                if refresh_token:
                    self._http_client._refresh_token = refresh_token

                # If we have a valid access token, try to proceed without refresh
                if access_token and self._http_client.is_access_token_valid():
                    try:
                        user_id, persona_id, user_name = await self._auth_manager.get_identity()
                        try:
                            asyncio.create_task(self._prefetch_offers_background())
                        except Exception:
                            pass
                        return Authentication(user_id, user_name)
                    except Exception as e:
                        logger.info(f"Stored access token may be invalid, will attempt refresh if possible: {e}")

                # If we have a refresh token, attempt to refresh
                if refresh_token:
                    try:
                        await self._force_refresh_access_token()
                        user_id, persona_id, user_name = await self._auth_manager.get_identity()
                        try:
                            asyncio.create_task(self._prefetch_offers_background())
                        except Exception:
                            pass
                        return Authentication(user_id, user_name)
                    except Exception as e:
                        logger.info(f"Stored refresh token failed, starting fresh auth: {e}")
                        
            except Exception as e:
                logger.error(f"Error processing stored credentials: {e}")
        
        # Start new authentication flow
        logger.info("Starting new authentication flow")
        return await self._auth_manager.begin_auth_flow()

    async def _force_refresh_access_token(self):
        try:
            if not self._http_client._refresh_token:
                raise AuthenticationRequired("No refresh token available")
            await self._http_client._refresh_access_token(self._http_client._refresh_token)
        except AuthenticationRequired:
            self.lost_authentication()
            raise
        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            self.lost_authentication()
            raise AuthenticationRequired("Failed to refresh access token")
    # Tokens are persisted via http client callback to avoid duplicates

    def _store_tokens(self, access_token, refresh_token):
        current_credentials = self.persistent_cache.get("credentials", {})
        if isinstance(current_credentials, str):
            current_credentials = {}
        
        credentials = current_credentials.copy() if current_credentials else {}
        # Skip write if nothing changed
        if (
            credentials.get("access_token") == access_token and
            credentials.get("refresh_token") == refresh_token
        ):
            return
        credentials["access_token"] = access_token
        credentials["refresh_token"] = refresh_token
        self.store_credentials(credentials)

    async def pass_login_credentials(self, step, credentials, cookies):
        logger.debug(f"Web process succeeded, passing credentials to plugin.")
        parsed_uri = urlparse(credentials["end_uri"])

        if parsed_uri.query:
            params = parse_qs(parsed_uri.query)
            code = params.get("code", [None])[0]
            if not code:
                raise AuthenticationRequired("No authorization code found in callback URL")
        else:
            raise AuthenticationRequired("Failed to extract query parameters from callback URL")

        # Persist cookies received from the web session
        try:
            if cookies and isinstance(cookies, list):
                cookie_dict = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name is not None and value is not None:
                        cookie_dict[name] = value
                if cookie_dict:
                    self._store_cookies(cookie_dict)
        except Exception as e:
            logger.warning(f"Failed to persist web cookies: {e}")

        user_id, persona_id, user_name = await self._auth_manager.authenticate_with_code(code)
    # Tokens already persisted via http client callback
        try:
            asyncio.create_task(self._prefetch_offers_background())
        except Exception:
            pass
        return Authentication(user_id, user_name)

    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        # Keep the full offer id prefix (e.g., DR:123, OFB-EAST:xxxx) to match backend/cache keys
        return OfferId(game_id.split('@')[0])

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> Dict[OfferId, Json]:
        """Retrieves offer data from a list of offer IDs.
        First checks the local cache, then makes requests for missing offers."""
        offers = {}
        missing_offers = []
        
        # First check offers in the cache
        for offer_id in offer_ids:
            cached_offer = self._offer_id_cache.get(offer_id)
            if cached_offer and isinstance(cached_offer, dict):
                offers[offer_id] = cached_offer
            else:
                missing_offers.append(offer_id)
        
        if missing_offers:
            try:
                gathered_offers = await self._backend_client.get_offers(missing_offers)
            except Exception as e:
                logger.error(f"Failed to fetch offers batch: {e}")
                gathered_offers = {}
            if isinstance(gathered_offers, dict):
                for key, offer in gathered_offers.items():
                    if isinstance(offer, dict):
                        origin_offer_id = offer.get('offerId') or offer.get('originOfferId') or key
                        if origin_offer_id:
                            offers[OfferId(origin_offer_id)] = offer
                            self._offer_id_cache[OfferId(origin_offer_id)] = offer
                
        return offers

    async def get_owned_games(self) -> List[Game]:
        self._check_authenticated()

        entitlements = await self._backend_client.get_entitlements()
        offer_ids: List[OfferId] = []
        for e in entitlements:
            origin_offer_id = e.get("originOfferId")
            if isinstance(origin_offer_id, str) and origin_offer_id:
                offer_ids.append(OfferId(origin_offer_id))
        offers = await self._get_offers(offer_ids)

        games = []
        for origin_offer_id, offer in offers.items():
            if not isinstance(offer, dict):
                continue
            display_name = offer.get('displayName') or offer.get('game_product', {}).get('name')
            if not display_name:
                continue
            raw_offer_id = offer.get('offerId') or str(origin_offer_id)
            if not raw_offer_id:
                continue
            game_id = GameId(raw_offer_id)
            games.append(
                Game(
                    game_id,
                    display_name,
                    None,
                    LicenseInfo(LicenseType.SinglePurchase, None)
                )
            )
        return games

    async def prepare_achievements_context(self, game_ids: List[GameId]) -> AchievementsImportContext:
        self._check_authenticated()
        achievement_sets: Dict[GameSlug, AchievementSet] = {}
        achievements: Dict[AchievementSet, List[Achievement]] = {}

        if self._auth_manager.persona_id is None:
            logger.error("Persona ID is None, user might not be properly authenticated")
            raise AuthenticationRequired("User not properly authenticated")

        # Build mapping from game slug to achievement set using achievementSetOverride
        slug_to_ach_set: Dict[GameSlug, AchievementSet] = {}
        unique_ach_sets: Set[AchievementSet] = set()
        for game_id in game_ids:
            try:
                offer_id = self._offer_id_from_game_id(game_id)
                offer_data = self._offer_id_cache.get(offer_id)
                if not offer_data:
                    continue
                # Resolve slug
                game_slug_val = offer_data.get("gameSlug") or offer_data.get("gameNameFacetKey")
                if not game_slug_val:
                    continue
                game_slug = GameSlug(game_slug_val)
                # Resolve achievement set
                ach_set_val = offer_data.get("achievementSetOverride")
                if not ach_set_val:
                    logger.debug(f"No achievementSetOverride for offer {offer_id}")
                    continue
                ach_set = AchievementSet(str(ach_set_val))
                slug_to_ach_set[game_slug] = ach_set
                unique_ach_sets.add(ach_set)
            except Exception as e:
                logger.error(f"Error processing game {game_id}: {e}")

        if not slug_to_ach_set:
            return AchievementsImportContext(
                owned_games=achievement_sets,
                achievements=achievements
            )

        # Fetch achievements per achievement set to preserve mapping
        for ach_set in unique_ach_sets:
            set_id, ach_list = await self._backend_client.get_achievements([ach_set], self._auth_manager.persona_id)
            if set_id:
                achievement_set_obj = AchievementSet(set_id)
                achievements[achievement_set_obj] = ach_list or []

        # Build mapping owned_games from slug_to_ach_set
        achievement_sets.update(slug_to_ach_set)

        return AchievementsImportContext(
            owned_games=achievement_sets,
            achievements=achievements
        )

    async def get_unlocked_achievements(self, game_id: GameId, context: AchievementsImportContext) -> List[Achievement]:
        try:
            offer_id = self._offer_id_from_game_id(game_id)
            offer = self._offer_id_cache.get(offer_id)
            if not offer:
                return []
            game_slug_val = offer.get("gameSlug") or offer.get("gameNameFacetKey")
            if not game_slug_val:
                return []
            game_slug = GameSlug(game_slug_val)
            achievement_set = context.owned_games.get(game_slug)
            if not achievement_set:
                return []
            return context.achievements.get(achievement_set, [])
        except Exception:
            return []

    async def get_subscriptions(self) -> List[Subscription]:
        self._check_authenticated()
        return await self._backend_client.get_user_subscriptions()

    async def prepare_subscription_games_context(self, subscription_names: List[str]) -> Any:
        self._check_authenticated()
        return {
            'EA Play': 'standard',
            'EA Play Pro': 'premium'
        }

    async def get_subscription_games(self, subscription_name: str, context: Dict[str, str]) -> AsyncGenerator[List[SubscriptionGame], None]:
        try:
            tier = context[subscription_name]
        except KeyError:
            raise UnknownError(f'Unknown subscription name {subscription_name}!')
        yield await self._backend_client.get_subscription_games_for_tier(tier)

    async def _get_game_times_for_master_title(self, game_id: GameId, game_slug: GameSlug, lastplayed_time: Optional[Timestamp]) -> GameTime:
        """
        :param game_id - to get from cache
        :param game_slug - to fetch from backend
        :param lastplayed_time - to decide on cache freshness
        """
        def get_cached_game_times(_game_id: GameId, _lastplayed_time: Optional[Timestamp]) -> Optional[GameTime]:
            """"returns None if a new entry should be retrieved"""
            if _lastplayed_time is None:
                return None

            offer_id = self._offer_id_from_game_id(_game_id)
            _cached_game_time: Optional[GameTime] = self._game_time_cache.get(offer_id)
            if _cached_game_time is None or _cached_game_time.last_played_time is None:
                # played time unknown yet
                return None
            if _lastplayed_time > _cached_game_time.last_played_time:
                # newer played time available
                return None
            return _cached_game_time

        cached_game_time: Optional[GameTime] = get_cached_game_times(game_id, lastplayed_time)
        if cached_game_time is not None:
            return cached_game_time

        total_play_time, last_played_time = await self._backend_client.get_game_time(game_slug)
        game_time: GameTime = GameTime(game_id, total_play_time, last_played_time)
        self._game_time_cache[self._offer_id_from_game_id(game_id)] = game_time
        self._persistent_cache_updated = True
        return game_time

    async def prepare_game_times_context(self, game_ids: List[GameId]) -> Any:
        self._check_authenticated()
        offer_ids = [self._offer_id_from_game_id(game_id) for game_id in game_ids]

        try:
            await self._get_offers(offer_ids)  # update local cache, ignore return value
        except Exception as e:
            logger.exception("Failed to fetch offers in batch: %s", repr(e))

        game_slugs = [
            GameSlug(self._offer_id_cache[offer_id]["gameSlug"])
            for offer_id in offer_ids
            if offer_id in self._offer_id_cache and "gameSlug" in self._offer_id_cache[offer_id]
        ]

        try:
            last_played_games = await self._backend_client.get_lastplayed_games(game_slugs)
            if last_played_games is None:
                last_played_games = {}
        except Exception as e:
            logger.exception("Failed to get last played games: %s", repr(e))
            last_played_games = {}

        return last_played_games

    async def get_game_time(self, game_id: GameId, last_played_games: Any) -> GameTime:
        offer_id = self._offer_id_from_game_id(game_id)
        try:
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                # Try to fetch offer on-demand to heal cache
                fetched = await self._backend_client.get_offers([offer_id])
                if isinstance(fetched, dict):
                    self._offer_id_cache.update({OfferId(k): v for k, v in fetched.items()})
                offer = self._offer_id_cache.get(offer_id)
                if offer is None:
                    logger.error("Offer %s not found after fetch", offer_id)
                    raise UnknownBackendResponse()

            game_slug_val = offer.get("gameSlug") or offer.get("game_product", {}).get("gameSlug")
            if not game_slug_val:
                logger.error("Missing gameSlug for offer %s", offer_id)
                raise UnknownBackendResponse()
            game_slug = GameSlug(game_slug_val)

            return await self._get_game_times_for_master_title(
                game_id,
                game_slug,
                last_played_games.get(game_slug)
            )

        except KeyError as e:
            logger.exception("Failed to import game times %s", repr(e))
            raise UnknownBackendResponse()

    def game_times_import_complete(self):
        if self._persistent_cache_updated:
            self.push_cache()
            self._persistent_cache_updated = False

    async def get_friends(self):
        self._check_authenticated()

        return [
            UserInfo(user_id=str(user_id), user_name=str(user_name), avatar_url=str(avatar_url))
            for user_id, (user_name, avatar_url) in (await self._backend_client.get_friends()).items()
        ]

    @staticmethod
    def _open_uri(uri):
        logger.info(f"Opening {uri}")
        webbrowser.open(uri)
    
    async def launch_game(self, game_id: GameId):
        offer_id = self._offer_id_from_game_id(game_id)
        offer = self._offer_id_cache.get(offer_id)
        if offer is None:
            logger.exception("Internal cache out of sync")
            raise UnknownError()

        master_title_id: MasterTitleId = offer["contentId"]
        if is_uri_handler_installed("origin2"):
            uri = f"origin2://game/launch?offerIds={master_title_id}"
        else:
            uri = "https://www.ea.com/ea-app"

        self._open_uri(uri)

    async def install_game(self, game_id: GameId):
        async def get_subscription_game_store_uri(offer_id):
            try:
                offers = await self._backend_client.get_offers([offer_id])
                if offers and offer_id in offers:
                    offer = offers[offer_id]
                    return f"https://www.ea.com/games/{offer['gdpPath']}"
                return "https://www.ea.com/ea-play/games"
            except (KeyError, UnknownError, BackendError, UnknownBackendResponse):
                return "https://www.ea.com/ea-play/games"

        offer_id = self._offer_id_from_game_id(game_id)
        if game_id.endswith('subscription') and offer_id not in self._offer_id_cache:
            uri = await get_subscription_game_store_uri(offer_id)
        elif is_uri_handler_installed("origin2"):
            offer_id = self._offer_id_from_game_id(game_id)
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                logger.exception("Internal cache out of sync")
                raise UnknownError()

            master_title_id: MasterTitleId = offer["contentId"]
            uri = f"origin2://game/launch?offerIds={master_title_id}&autoDownload=1"
        else:
            uri = "https://www.ea.com/ea-app"

        self._open_uri(uri)

    if is_windows():
        async def uninstall_game(self, game_id: GameId):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(subprocess.run, ["control", "appwiz.cpl"]))    
    
    async def shutdown_platform_client(self) -> None:
        self._open_uri("origin2://quit")

    def _store_cookies(self, cookies):
        current_credentials = self.persistent_cache.get("credentials", {})
        if isinstance(current_credentials, str):
            try:
                current_credentials = json.loads(current_credentials)
            except:
                current_credentials = {}
        
        # Skip write if unchanged
        if current_credentials.get("cookies") == cookies:
            return
        current_credentials["cookies"] = cookies
        self.store_credentials(current_credentials)

    def _update_stored_cookies(self, morsels):
        cookies = {}
        for morsel in morsels:
            cookies[morsel.key] = morsel.value
        self._store_cookies(cookies)

    async def get_local_games(self) -> List[LocalGame]:
        # If offers cache is empty, schedule a background prefetch and continue using current cache
        if not self._offer_id_cache:
            try:
                asyncio.create_task(self._prefetch_offers_background())
            except Exception:
                pass

        if self._local_game_manager._local_games_update_in_progress:
            logger.debug("Local games are being updated, returning cached values")
            return self._local_game_manager._local_games if isinstance(self._local_game_manager._local_games, list) else []
        
        loop = asyncio.get_running_loop()
        try:
            self._local_game_manager._local_games_update_in_progress = True
            local_games = await loop.run_in_executor(None, partial(self._local_game_manager.update_local_games))
            self._local_game_manager._local_games_last_update = int(time.time())
            self._local_game_manager._local_games = local_games
            return local_games
        finally:
            self._local_game_manager._local_games_update_in_progress = False

    def handle_local_game_update_notifications(self):
        # Skip notifications until authenticated with a valid token to avoid early 400s
        if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
            return
        # If offers cache isn't ready yet, schedule background prefetch and skip this tick
        if not self._offer_id_cache:
            try:
                asyncio.create_task(self._prefetch_offers_background())
            except Exception:
                pass
            return
        async def notify_local_games_changed():
            notify_list = []
            try:
                self._local_game_manager._local_games_update_in_progress = True
                notify_list = await loop.run_in_executor(None, partial(self._local_game_manager.get_local_game_status))
                self._local_game_manager._local_games_last_update = int(time.time())
            finally:
                self._local_game_manager._local_games_update_in_progress = False

            for local_games_notify in notify_list:
                self.update_local_game_status(local_games_notify)

        # don't overlap update operations
        if self._local_game_manager._local_games_update_in_progress:
            logger.debug("Local games are being updated, skipping cache update")
            return

        if not self._local_game_manager.should_update_cache():
            logger.debug("Local games cache is fresh enough")
            return

        loop = asyncio.get_running_loop()
        asyncio.create_task(notify_local_games_changed())

    async def prepare_local_size_context(self, game_ids: List[GameId]) -> Dict[str, pathlib.PurePath]:
        if not is_windows():
            return {}
        game_id_manifest_map: Dict[str, pathlib.PurePath] = {}
        for game_id in game_ids:
            game = self._offer_id_cache.get(self._offer_id_from_game_id(game_id))
            if not game:
                continue

            # Get install path from either field
            path = game.get("installCheckOverride") or game.get("executePathOverride")
            if not path or not path.startswith('[') or ']' not in path:
                continue

            try:
                parsed_expr = parse_registry_expression(path)
                manifest_path: Optional[pathlib.Path] = None

                if parsed_expr:
                    hive, key_path, value_name, tail = parsed_expr
                    if tail:
                        resolved = resolve_registry_expression(path)
                        if resolved:
                            resolved_path = pathlib.Path(resolved)
                            base_dir = resolved_path if resolved_path.is_dir() else resolved_path.parent
                            manifest_path = base_dir / "Support" / "mnfst.txt"
                    else:
                        install_location = RegistryManager.get_registry_value(hive, key_path, value_name)
                        if install_location:
                            manifest_path = pathlib.Path(install_location) / "Support" / "mnfst.txt"
                else:
                    head = path.split(']', 1)[0] + ']'
                    install_location = resolve_registry_expression(head)
                    if install_location:
                        base_dir = pathlib.Path(install_location)
                        manifest_path = base_dir / "Support" / "mnfst.txt"

                if manifest_path is not None:
                    game_id_manifest_map[str(game_id)] = manifest_path
            except Exception as e:
                logger.error(f"Error processing registry path for {game_id}: {e}")

        return game_id_manifest_map

    async def get_local_size(self, game_id: GameId, context: Dict[str, pathlib.PurePath]) -> Optional[int]:
        try:
            manifest_path = context[game_id]
            return parse_total_size(manifest_path)
        except FileNotFoundError:
            return None
        except KeyError:
            return None

    def handshake_complete(self):
        def game_time_decoder(cache: dict) -> Dict[OfferId, GameTime]:
            outdated_keys = [key.split('@')[0] for key in cache if "@" in key]
            for i in outdated_keys:
                cache.pop(i, None)
            return {
                game_id: GameTime(entry["game_id"], entry["time_played"], entry.get("last_played_time"))
                for game_id, entry in cache.items()
                if entry and game_id
            }
        def safe_decode(_cache, _key: str, _decoder: Callable):
            if not _cache:
                return {}
            if _decoder is None:
                _decoder = lambda x: x
            try:
                if isinstance(_cache, str):
                    return _decoder(json.loads(_cache))
                return _decoder(_cache)
            except Exception:
                logger.exception("Failed to decode persistent '%s' cache", _key)
                return {}
        cache_decoders = {
            "offers": None,
            "game_time": game_time_decoder,
        }
        for key, decoder in cache_decoders.items():
            decoded = safe_decode(self.persistent_cache.get(key), key, decoder)
            self.persistent_cache[key] = json.dumps(decoded)
        self._http_client.load_lats_from_cache(self.persistent_cache.get('lats'))
        self._http_client.set_save_lats_callback(self._save_lats)

    def _save_lats(self, lats: int):
        self.persistent_cache['lats'] = str(lats)
        self.push_cache()

def main():
    preload_pc_sign_cache()
    create_and_run_plugin(EAPlugin, sys.argv)

if __name__ == "__main__":
    main()