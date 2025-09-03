import asyncio
import json
import logging
import pathlib
import platform
import re
import struct
import subprocess
import sys
import time
import webbrowser
from functools import partial
from typing import Any, Dict, List, NewType, Optional, AsyncGenerator, NamedTuple, Set, Iterable, Tuple, Callable
from urllib.parse import urlparse, parse_qs
import grpc

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import AuthenticationRequired, BackendError, UnknownBackendResponse, UnknownError
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, UserInfo, UserPresence, PresenceState, Game, GameTime, LicenseInfo, LocalGame,
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

# Constants
LOCAL_GAMES_CACHE_VALID_PERIOD = 5 * 60  # 5 minutes
IS_WINDOWS = platform.system().lower() == "windows"

# JavaScript injection for login page
LOGIN_JS = {
    ".*" + re.escape(r"juno/login?execution") + ".*": [
        'document.getElementById("rememberMe").checked = true;'
    ]
}

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
    """Handles authentication functionality."""
    
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
        if not self.is_authenticated():
            raise AuthenticationRequired("User not authenticated")
    
    async def begin_auth_flow(self) -> NextStep:
        try:
            pc_sign = generate_pc_sign_fast()
        except Exception as e:
            logger.error(f"Failed to generate PC sign: {e}")
            pc_sign = ""
        
        params = {
            "window_title": "Login to EA Desktop",
            "window_width": 495 if IS_WINDOWS else 480,
            "window_height": 850 if IS_WINDOWS else 825,
            "start_uri": f"https://accounts.ea.com/connect/auth"
                         f"?response_type=code&client_id=JUNO_PC_CLIENT&display=junoClient/login"
                         f"&redirect_uri=qrc:///html/login_successful.html"
                         f"&locale=en_US&pc_sign={pc_sign}",
            "end_uri_regex": "qrc:/html/login_successful.html.*"
        }
        return NextStep("web_session", params, js=LOGIN_JS)
    
    async def authenticate_with_code(self, code: str) -> Tuple[str, str, str]:
        if not code:
            raise AuthenticationRequired("No authorization code provided")
        
        await self.http_client._exchange_auth_code_for_token(code)
        return await self.get_identity()
    
    async def get_identity(self) -> Tuple[str, str, str]:
        try:
            # Try JWT extraction first
            if hasattr(self.http_client, '_access_token') and self.http_client._access_token:
                try:
                    self._user_id, self._persona_id, user_name = extract_user_info_from_jwt(
                        self.http_client._access_token
                    )
                    logger.info(f"Identity obtained from JWT: {user_name}")
                    return self._user_id, self._persona_id, user_name
                except Exception as e:
                    logger.warning(f"JWT extraction failed: {e}")
            
            # Fallback to backend
            self._user_id, self._persona_id, user_name = await self.backend_client.get_identity()
            logger.info(f"Identity obtained from backend: {user_name}")
            return self._user_id, self._persona_id, user_name
            
        except Exception as e:
            logger.error(f"Failed to get identity: {e}")
            raise AuthenticationRequired("Failed to get identity")


class CacheManager:
    """Manages persistent caching."""
    
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self._game_time_cache = {}
        self._offer_id_cache = {}

    @property
    def game_time_cache(self) -> Dict[OfferId, GameTime]:
        # simple in-memory cache; persistence is managed explicitly by the plugin
        return self._game_time_cache

    @game_time_cache.setter
    def game_time_cache(self, value: Dict[OfferId, GameTime]):
        self._game_time_cache = value

    @property
    def offer_id_cache(self) -> Dict[OfferId, Json]:
        return self._offer_id_cache

    @offer_id_cache.setter
    def offer_id_cache(self, value: Dict[OfferId, Json]):
        self._offer_id_cache = value

    def cache_offers(self, offers: Dict[OfferId, Json]):
        # update in-memory cache only; plugin will decide when to persist
        self._offer_id_cache.update(offers)


class LocalGameManager:
    """Manages local game detection."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self._local_games = []
        self._local_games_last_update = 0
        self._local_games_update_in_progress = False
    
    def update_local_games(self):
        return update_local_games(self)
    
    def get_local_game_status(self):
        return local_game_status(self)
    
    @property
    def _offer_id_cache(self):
        return self.cache_manager.offer_id_cache
    
    def should_update_cache(self) -> bool:
        return (
            not self._local_games_update_in_progress and
            time.time() - self._local_games_last_update >= LOCAL_GAMES_CACHE_VALID_PERIOD
        )


class EAPlugin(Plugin):
    """Main EA Desktop plugin class."""
    
    def __init__(self, reader, writer, token):
        super().__init__(Platform.Origin, __version__, reader, writer, token)
        
        # Initialize HTTP client and backend
        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(self.lost_authentication)
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
            now = int(time.time())
            # Throttle prefetch
            if now - self._last_offers_prefetch < 60:
                return
            # Only if authenticated and token is valid
            if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
                return
            
            await asyncio.sleep(2)  # Let session settle
            entitlements = await self._backend_client.get_entitlements()
            offer_ids = [
                OfferId(e["originOfferId"]) 
                for e in entitlements 
                if e.get("originOfferId")
            ]
            
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

    def _check_authenticated(self):
        self._auth_manager.check_authenticated()
    
    async def shutdown(self):
        await self._http_client.close()

    def tick(self):
        self.handle_local_game_update_notifications()
    
    async def authenticate(self, stored_credentials=None):
        if stored_credentials:
            try:
                # Load cookies
                cookies = stored_credentials.get("cookies")
                if cookies:
                    self._http_client._cookie_jar.update_cookies(cookies)
                
                # Load tokens
                access_token = stored_credentials.get("access_token")
                refresh_token = stored_credentials.get("refresh_token")
                if access_token:
                    self._http_client._access_token = access_token
                if refresh_token:
                    self._http_client._refresh_token = refresh_token

                # Try with valid access token
                if access_token and self._http_client.is_access_token_valid():
                    try:
                        user_id, persona_id, user_name = await self._auth_manager.get_identity()
                        asyncio.create_task(self._prefetch_offers_background())
                        return Authentication(user_id, user_name)
                    except Exception as e:
                        logger.info(f"Stored access token invalid, trying refresh: {e}")

                # Try refresh if available
                if refresh_token:
                    try:
                        await self._force_refresh_access_token()
                        user_id, persona_id, user_name = await self._auth_manager.get_identity()
                        asyncio.create_task(self._prefetch_offers_background())
                        return Authentication(user_id, user_name)
                    except Exception as e:
                        logger.info(f"Refresh token failed, starting fresh auth: {e}")
                        
            except Exception as e:
                logger.error(f"Error processing stored credentials: {e}")
        
        logger.info("Starting new authentication flow")
        return await self._auth_manager.begin_auth_flow()

    async def _force_refresh_access_token(self):
        if not self._http_client._refresh_token:
            raise AuthenticationRequired("No refresh token available")
        try:
            await self._http_client._refresh_access_token(self._http_client._refresh_token)
        except AuthenticationRequired:
            # Re-raise authentication errors directly
            raise AuthenticationRequired("Failed to refresh access token")  
        except Exception as e:
            logger.error(f"Something went wrong while trying to refresh token: {e}")
            # Only call lost_authentication if we have the callback set
            if hasattr(self, 'lost_authentication'):
                self.lost_authentication()
            raise AuthenticationRequired("Failed to refresh access token")
        
    def _store_tokens(self, access_token, refresh_token):
        # Centralize credential storage: only persist when tokens are available.
        # Keep cookies previously captured in persistent cache, but don't rewrite repeatedly.
        try:
            current_credentials = self.persistent_cache.get("credentials", {})
            if isinstance(current_credentials, str):
                try:
                    current_credentials = json.loads(current_credentials)
                except (json.JSONDecodeError, TypeError):
                    current_credentials = {}

            current_access = current_credentials.get("access_token")
            current_refresh = current_credentials.get("refresh_token")

            # If tokens unchanged and cookies present, skip write
            if (current_access == access_token and current_refresh == refresh_token):
                logger.debug("Tokens unchanged, skipping store_credentials call")
                return

            credentials = current_credentials.copy()
            credentials.update({
                "access_token": access_token,
                "refresh_token": refresh_token
            })

            # Merge any pending cookies kept in memory under _pending_cookies
            pending = getattr(self, '_pending_cookies', None)
            if pending:
                credentials['cookies'] = pending

            logger.debug("Storing updated tokens and cookies")
            # Single write to persistent storage
            self.store_credentials(credentials)
            # Clear pending cookies after successful store
            if hasattr(self, '_pending_cookies'):
                delattr(self, '_pending_cookies')
        except Exception as e:
            logger.exception(f"Failed to store tokens: {e}")

    async def pass_login_credentials(self, step, credentials, cookies):
        logger.debug("Web process succeeded, passing credentials to plugin.")
        parsed_uri = urlparse(credentials["end_uri"])

        if not parsed_uri.query:
            raise AuthenticationRequired("Failed to extract query parameters from callback URL")
        
        params = parse_qs(parsed_uri.query)
        code = params.get("code", [None])[0]
        if not code:
            raise AuthenticationRequired("No authorization code found in callback URL")

        # Persist cookies from web session
        if cookies and isinstance(cookies, list):
            try:
                cookie_dict = {c["name"]: c["value"] for c in cookies if c.get("name") and c.get("value")}
                if cookie_dict:
                    self._store_cookies(cookie_dict)
            except Exception as e:
                logger.warning(f"Failed to persist web cookies: {e}")

        user_id, persona_id, user_name = await self._auth_manager.authenticate_with_code(code)
        asyncio.create_task(self._prefetch_offers_background())
        return Authentication(user_id, user_name)

    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        # Keep the full offer id prefix (e.g., DR:123, OFB-EAST:xxxx) to match backend/cache keys
        return OfferId(game_id.split('@')[0])

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> Dict[OfferId, Json]:
        """Retrieve offer data, checking cache first. Fetch missing offers in batches of max 100."""
        offers: Dict[OfferId, Json] = {}
        missing_offers: List[OfferId] = []

        # Check cache first
        for offer_id in offer_ids:
            cached_offer = self._offer_id_cache.get(offer_id)
            if cached_offer and isinstance(cached_offer, dict):
                offers[offer_id] = cached_offer
            else:
                missing_offers.append(offer_id)

        # Fetch missing offers in batches
        if missing_offers:
            MAX_BATCH = 100
            # Deduplicate while preserving order
            seen = set()
            unique_missing = []
            for oid in missing_offers:
                if oid not in seen:
                    seen.add(oid)
                    unique_missing.append(oid)

            for start in range(0, len(unique_missing), MAX_BATCH):
                chunk = unique_missing[start:start + MAX_BATCH]
                try:
                    gathered_offers = await self._backend_client.get_offers(chunk)
                    if isinstance(gathered_offers, dict):
                        for key, offer in gathered_offers.items():
                            if not isinstance(offer, dict):
                                continue
                            origin_offer_id = offer.get('offerId') or offer.get('originOfferId') or key
                            if origin_offer_id:
                                oid = OfferId(origin_offer_id)
                                offers[oid] = offer
                                self._offer_id_cache[oid] = offer
                except Exception as e:
                    logger.error(f"Failed to fetch offers batch starting at {start}: {e}")

        return offers

    async def get_owned_games(self) -> List[Game]:
        self._check_authenticated()

        entitlements = await self._backend_client.get_entitlements()
        offer_ids = [
            OfferId(e["originOfferId"]) 
            for e in entitlements 
            if e.get("originOfferId")
        ]
        offers = await self._get_offers(offer_ids)

        games = []
        for origin_offer_id, offer in offers.items():
            if not isinstance(offer, dict):
                continue
            display_name = offer.get('displayName') or offer.get('game_product', {}).get('name')
            raw_offer_id = offer.get('offerId') or str(origin_offer_id)
            if display_name and raw_offer_id:
                games.append(Game(
                    GameId(raw_offer_id),
                    display_name,
                    None,
                    LicenseInfo(LicenseType.SinglePurchase, None)
                ))
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
                    logger.debug(f"{offer_id} does not have any achievements.")
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
    
    async def update_user_presence(self, user_id: str, user_presence: UserPresence) -> None:
        """Updates user presence via gRPC without .proto files"""
        try:
            access_token = getattr(self._http_client, "_access_token", None)
            if not access_token:
                logger.warning("No access token available for gRPC presence update.")
                return

            # EA Complete workflow: CreateSession -> ConnectSession -> SubscribeToPresence
            success = await self._complete_presence_workflow(access_token, user_id, user_presence)
            
                # Fallback methods if the full workflow fails
            if not success:
                logger.info("Complete workflow failed, trying individual methods")
                
                # Method 1: Create a presence session
                if not success:
                    success = await self._create_presence_session(access_token, user_id, user_presence)
                
                # Method 2: Connect to an existing presence session
                if not success:
                    success = await self._connect_to_presence_session(access_token, user_id, user_presence)
                
                # Method 3: Native gRPC with fallback
                if not success:
                    success = await self._try_grpc_reflection_call(access_token, user_id, user_presence)
            
            if success:
                logger.info(f"Successfully updated presence for user {user_id}")
            else:
                logger.warning(f"All gRPC methods failed for user {user_id}")
                
        except Exception as e:
            logger.error(f"Failed to update presence via gRPC: {e}")

    async def _complete_presence_workflow(self, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """Full EA workflow: CreateSession -> ConnectSession -> SubscribeToPresence"""
        session_token = None
        
        try:
            # Step 1: Create a presence session and retrieve the token
            session_token = await self._create_presence_session_with_token(access_token, user_id, user_presence)
            if not session_token:
                logger.warning("Failed to create presence session")
                return False
            
            # Step 2: Connect to the session with configuration (optional)
            connected = await self._connect_to_presence_session(access_token, user_id, user_presence)
            if not connected:
                logger.warning("Failed to connect to presence session, continuing anyway")
            
            # Step 3: Subscribe to friends' presence to enable updates
            subscribed = await self._subscribe_to_friends_presence(access_token, session_token)
            if subscribed:
                logger.info("Complete presence workflow successful")
                return True
            else:
                logger.warning("Failed to subscribe to friends presence")
                return False
                
        except Exception as e:
            logger.error(f"Complete presence workflow failed: {e}")
            return False

    async def _create_presence_session_with_token(self, access_token: str, user_id: str, user_presence: UserPresence) -> Optional[bytes]:
        """Creates a presence session and returns the session token"""
        try:
            url = "https://api.k.social.ea.com/eadp.social.presence.v1.PresenceService/CreatePresenceSession"
            headers = {
                "user-agent": "ProtoHttp 2.0/DS 18.0.0 (Windows)",
                "te": "trailers",
                "content-type": "application/grpc+proto",
                "authorization": f"Bearer {access_token}",
                "grpc-accept-encoding": "gzip",
                "grpc-timeout": "30s",
                "x-call-sequence": "1"
            }
            
            # Message simple pour CreatePresenceSession
            presence_status = 1 if user_presence.presence_state == PresenceState.Online else 0
            message = self._encode_minimal_presence_message(user_id, presence_status)
            
            # gRPC prefix (5 bytes: compression flag + message length)
            grpc_prefix = struct.pack('>BI', 0, len(message))  # 0 = no compression
            grpc_data = grpc_prefix + message
            
            response = await self._http_client.post(url, headers=headers, data=grpc_data)
            
            status_code = response.get('status') if isinstance(response, dict) else getattr(response, 'status_code', None)
            
            if status_code == 200:
                content = response.get('content', b'') if isinstance(response, dict) else getattr(response, 'content', b'')
                if content and len(content) > 5:
                    # Extract the session token (skip the first 5 bytes of the gRPC prefix)
                    session_data = content[5:] if len(content) > 5 else content
                    logger.info(f"Session token received, length: {len(session_data)} bytes")
                    return session_data
                else:
                    logger.warning("Empty session response")
                    return None
            else:
                logger.warning(f"CreatePresenceSession failed: {status_code}")
                return None
                
        except Exception as e:
            logger.error(f"CreatePresenceSession with token failed: {e}")
            return None

    async def _subscribe_to_friends_presence(self, access_token: str, session_token: bytes) -> bool:
        """Subscribe to friends' presence to enable presence updates"""
        try:
            url = "https://api.k.social.ea.com/eadp.social.presence.v1.PresenceService/SubscribeToFriendsPresence"
            headers = {
                "user-agent": "ProtoHttp 2.0/DS 18.0.0 (Windows)",
                "te": "trailers",
                "content-type": "application/grpc+proto",
                "authorization": f"Bearer {access_token}",
                "grpc-accept-encoding": "gzip",
                "grpc-timeout": "30s",
                "x-call-sequence": "1"
            }
            
            # Use the session token as the message (binary data from CreatePresenceSession)
            message = session_token
            
            # gRPC prefix (5 bytes: compression flag + message length)
            grpc_prefix = struct.pack('>BI', 0, len(message))  # 0 = no compression
            grpc_data = grpc_prefix + message
            
            response = await self._http_client.post(url, headers=headers, data=grpc_data)
            
            status_code = response.get('status') if isinstance(response, dict) else getattr(response, 'status_code', None)
            
            if status_code == 200:
                logger.info("Successfully subscribed to friends presence")
                
                    # Parse the response (should be an empty protobuf message)
                content = response.get('content', b'') if isinstance(response, dict) else getattr(response, 'content', b'')
                if content:
                    self._log_grpc_response_analysis(content, "SubscribeToFriendsPresence")
                else:
                    logger.info("Empty response as expected for SubscribeToFriendsPresence")
                
                return True
            else:
                content = response.get('content', b'') if isinstance(response, dict) else getattr(response, 'content', b'')
                logger.warning(f"SubscribeToFriendsPresence failed: {status_code} - {str(content)[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"SubscribeToFriendsPresence failed: {e}")
            return False

    async def _create_presence_session(self, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """Creates a new presence session"""
        return await self._try_grpc_http_call(
            access_token, user_id, user_presence, 
            "CreatePresenceSession"
        )

    async def _connect_to_presence_session(self, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """Connects to an existing presence session with EA configuration"""
        return await self._try_grpc_http_call(
            access_token, user_id, user_presence, 
            "ConnectToPresenceSession"
        )

    async def _try_grpc_http_call(self, access_token: str, user_id: str, user_presence: UserPresence, method: str = "CreatePresenceSession") -> bool:
        """Attempt gRPC call over HTTP/2 with appropriate headers"""
        try:
            url = f"https://api.k.social.ea.com/eadp.social.presence.v1.PresenceService/{method}"
            headers = {
                "user-agent": "ProtoHttp 2.0/DS 18.0.0 (Windows)",
                "te": "trailers",
                "content-type": "application/grpc+proto",
                "authorization": f"Bearer {access_token}",
                "grpc-accept-encoding": "gzip",
                "grpc-timeout": "30s",
                "x-call-sequence": "1"
            }
            
            # Protobuf message with presence configuration based on the method
            if method == "ConnectToPresenceSession":
                message = self._encode_connect_presence_message(user_id, user_presence)
            else:
                # CreatePresenceSession - message simple
                presence_status = 1 if user_presence.presence_state == PresenceState.Online else 0
                message = self._encode_minimal_presence_message(user_id, presence_status)
            
            # gRPC prefix (5 bytes: compression flag + message length)
            grpc_prefix = struct.pack('>BI', 0, len(message))  # 0 = no compression
            grpc_data = grpc_prefix + message
            
            response = await self._http_client.post(url, headers=headers, data=grpc_data)
            
            # Le client HTTP peut retourner un dict au lieu d'un objet Response 
            status_code = response.get('status') if isinstance(response, dict) else getattr(response, 'status_code', None)
            logger.info(f"gRPC HTTP call ({method}) response: {status_code}")
            
            if status_code == 200:
                logger.info(f"Presence update successful via HTTP gRPC call ({method})")
                
                # Parse the response for debugging
                content = response.get('content', b'') if isinstance(response, dict) else getattr(response, 'content', b'')
                if content:
                    self._log_grpc_response_analysis(content, method)
                
                return True
            else:
                content = response.get('content', b'') if isinstance(response, dict) else getattr(response, 'content', b'')
                logger.warning(f"gRPC HTTP call ({method}) failed: {status_code} - {str(content)[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"HTTP gRPC call ({method}) failed: {e}")
            return False

    async def _try_grpc_reflection_call(self, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """Attempt to use gRPC reflection to discover the API"""
        try:
            # Essai d'import optionnel des modules gRPC reflection
            try:
                from grpc_reflection.v1alpha import reflection_pb2
                from grpc_reflection.v1alpha import reflection_pb2_grpc
                logger.info("gRPC reflection modules imported successfully")
            except ImportError:
                logger.info("grpcio-reflection not available, trying basic gRPC call")
                return await self._try_basic_grpc_call(access_token, user_id, user_presence)
                
            # Si les modules sont disponibles, essayer la reflection
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.aio.secure_channel('api.k.social.ea.com:443', credentials)
            
            try:
                return await self._try_basic_grpc_call_with_channel(channel, access_token, user_id, user_presence)
            finally:
                try:
                    await channel.close()
                except:
                    pass
                
        except Exception as e:
            logger.error(f"gRPC reflection setup failed: {e}")
            return False
    
    async def _try_basic_grpc_call(self, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """Basic gRPC call without reflection"""
        try:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.aio.secure_channel('api.k.social.ea.com:443', credentials)
            
            try:
                return await self._try_basic_grpc_call_with_channel(channel, access_token, user_id, user_presence)
            finally:
                try:
                    await channel.close()
                except:
                    pass
        except Exception as e:
            logger.error(f"Basic gRPC call failed: {e}")
            return False

    async def _try_basic_grpc_call_with_channel(self, channel, access_token: str, user_id: str, user_presence: UserPresence) -> bool:
        """gRPC call using a provided channel"""
        try:
            # Create a generic gRPC unary_unary call for raw bytes
            call = channel.unary_unary(
                '/eadp.social.presence.v1.PresenceService/CreatePresenceSession'
            )
            
            # Message with authorization metadata
            metadata = [
                ('authorization', f'Bearer {access_token}'),
                ('user-agent', 'ProtoHttp 2.0/DS 18.0.0 (Windows)')
            ]
            
            presence_status = 1 if user_presence.presence_state == PresenceState.Online else 0
            message = self._encode_minimal_presence_message(user_id, presence_status)
            
            response = await call(message, metadata=metadata)
            
            # Parse the response for debugging
            if response:
                self._log_grpc_response_analysis(response, "basic_call")
            
            logger.info(f"Basic gRPC call successful, response length: {len(response) if response else 0}")
            return True
        except Exception as e:
            logger.error(f"Basic gRPC call with channel failed: {e}")
            return False

    def _encode_minimal_presence_message(self, user_id: str, status: int) -> bytes:
        """Encode a minimal protobuf message for presence"""
        try:
            # Very basic manual protobuf encoding
            # Field 1 (user_id as string): tag 1, wire type 2 (length-delimited)
            user_id_bytes = user_id.encode('utf-8')
            user_id_field = b'\x0a' + self._encode_varint(len(user_id_bytes)) + user_id_bytes
            
            # Field 2 (status as varint): tag 2, wire type 0 (varint)
            status_field = b'\x10' + self._encode_varint(status)
            
            return user_id_field + status_field
        except Exception as e:
            logger.error(f"Failed to encode presence message: {e}")
            return b''

    def _encode_connect_presence_message(self, user_id: str, user_presence: UserPresence) -> bytes:
        """Encode a protobuf message for ConnectToPresenceSession with EA configuration"""
        try:
            # Field 1: user_id (string)
            user_id_bytes = user_id.encode('utf-8')
            user_id_field = b'\x0a' + self._encode_varint(len(user_id_bytes)) + user_id_bytes
            
            # Field 2: Presence configuration (embedded message)
            config_fields = b''
            
            # Sub-field 1: ea_app.presenceAvailability (sint32)
            # Convert Galaxy presence state to EA: Online=1, Away=0, Offline=-1
            if user_presence.presence_state == PresenceState.Online:
                availability = 1
            elif user_presence.presence_state == PresenceState.Away:
                availability = 0
            else:  # Offline ou Unknown
                availability = -1
            
            # Encoder sint32 (zigzag encoding puis varint)
            availability_zigzag = (availability << 1) ^ (availability >> 31)
            avail_field = b'\x0a' + self._encode_string_field("ea_app.presenceAvailability") + b'\x12' + b'\x18' + self._encode_varint(availability_zigzag)
            
            # Sub-field 2: ea_app.presenceIsInvisible (sint32)
            # Invisible if the game is present but we want to be discreet
            invisible = 0  # Default visible
            if hasattr(user_presence, 'game_id') and user_presence.game_id and user_presence.presence_state != PresenceState.Online:
                invisible = 1
            
            invisible_zigzag = (invisible << 1) ^ (invisible >> 31)
            invisible_field = b'\x0a' + self._encode_string_field("ea_app.presenceIsInvisible") + b'\x12' + b'\x18' + self._encode_varint(invisible_zigzag)
            
            config_fields = avail_field + invisible_field
            
            # Field 2: Configuration (embedded message)
            config_field = b'\x12' + self._encode_varint(len(config_fields)) + config_fields
            
            return user_id_field + config_field
            
        except Exception as e:
            logger.error(f"Failed to encode connect presence message: {e}")
            # Fallback vers message simple
            return self._encode_minimal_presence_message(user_id, 1 if user_presence.presence_state == PresenceState.Online else 0)

    def _encode_string_field(self, value: str) -> bytes:
        """Encode a protobuf string field (without tag)"""
        value_bytes = value.encode('utf-8')
        return self._encode_varint(len(value_bytes)) + value_bytes

    def _encode_varint(self, value: int) -> bytes:
        """Encode an integer in protobuf varint format"""
        result = b''
        while value >= 0x80:
            result += bytes([(value & 0x7f) | 0x80])
            value >>= 7
        result += bytes([value & 0x7f])
        return result

    def _decode_protobuf_response(self, data: bytes) -> Dict[str, Any]:
        """Decode a protobuf response for analysis (useful for debugging)"""
        try:
            if isinstance(data, str):
                # If it's a hex string, convert to bytes
                data = bytes.fromhex(data)

            # Ensure we have bytes
            if not isinstance(data, bytes):
                data = bytes(data)
                
            fields = {}
            offset = 0
            
            while offset < len(data):
                if offset >= len(data):
                    break
                    
                # Lire le tag (field number + wire type)
                tag = int(data[offset])
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1
                
                if wire_type == 0:  # varint
                    value = 0
                    shift = 0
                    while offset < len(data):
                        byte = int(data[offset])
                        offset += 1
                        value |= (byte & 0x7f) << shift
                        if not (byte & 0x80):
                            break
                        shift += 7
                    fields[f'field_{field_number}'] = {'type': 'varint', 'value': value}
                    
                elif wire_type == 2:  # length-delimited (string/bytes)
                    length = 0
                    shift = 0
                    while offset < len(data):
                        byte = int(data[offset])
                        offset += 1
                        length |= (byte & 0x7f) << shift
                        if not (byte & 0x80):
                            break
                        shift += 7
                    
                    if offset + length <= len(data):
                        value_bytes = bytes(data[offset:offset + length])
                        offset += length
                        try:
                            # Try to decode as UTF-8
                            decoded = value_bytes.decode('utf-8')
                            fields[f'field_{field_number}'] = {'type': 'string', 'value': decoded}
                        except UnicodeDecodeError:
                            fields[f'field_{field_number}'] = {'type': 'bytes', 'value': value_bytes.hex()}
                    else:
                        break
                else:
                    # Unsupported types
                    fields[f'field_{field_number}'] = {'type': f'wire_type_{wire_type}', 'value': 'unsupported'}
                    break
            
            return {'success': True, 'fields': fields}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _log_grpc_response_analysis(self, response_data: bytes, method: str):
        """Log the analysis of a gRPC response for debugging"""
        try:
            analysis = self._decode_protobuf_response(response_data)
            if analysis.get('success'):
                logger.info(f"gRPC {method} response analysis: {analysis['fields']}")
            else:
                logger.warning(f"Failed to analyze gRPC {method} response: {analysis.get('error')}")
        except Exception as e:
            logger.error(f"Error analyzing gRPC {method} response: {e}")

    async def _call_presence_service(self, channel, access_token: str, user_id: str, user_presence: UserPresence):
        """Direct call to the presence service via gRPC"""
        try:
            # Create a generic gRPC call
            call = channel.unary_unary(
                '/eadp.social.presence.v1.PresenceService/CreatePresenceSession',
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x
            )
            
            # Message with authorization metadata
            metadata = [
                ('authorization', f'Bearer {access_token}'),
                ('user-agent', 'ProtoHttp 2.0/DS 18.0.0 (Windows)')
            ]
            
            # Encoded message
            presence_status = 1 if user_presence.presence_state == PresenceState.Online else 0
            message = self._encode_minimal_presence_message(user_id, presence_status)
            
            response = await call(message, metadata=metadata)
            logger.info(f"Direct gRPC call successful, response length: {len(response)}")
            
        except Exception as e:
            logger.error(f"Direct gRPC call failed: {e}")

        return super().update_user_presence(user_id, user_presence)

    def _open_uri(self, uri):
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

    if IS_WINDOWS:
        async def uninstall_game(self, game_id: GameId):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(subprocess.run, ["control", "appwiz.cpl"]))    
    
    async def shutdown_platform_client(self) -> None:
        self._open_uri("origin2://quit")

    def _store_cookies(self, cookies):
        # Avoid writing to persistent storage on every cookie update.
        # Keep cookies in-memory until tokens are available and then persisted by _store_tokens.
        try:
            # Keep pending cookies so _store_tokens can persist them together with tokens
            self._pending_cookies = cookies
        except Exception as e:
            logger.exception(f"Failed to cache cookies in memory: {e}")

    def _update_stored_cookies(self, morsels):
        try:
            cookies = {morsel.key: morsel.value for morsel in morsels}
            self._store_cookies(cookies)
        except Exception as e:
            logger.exception(f"Failed to update stored cookies: {e}")

    async def get_local_games(self) -> List[LocalGame]:
        # If offers cache is empty, try to prefetch offers now (best-effort) so local detection
        # can use offer metadata (gameSlug, install overrides). Fall back to background prefetch.
        if not self._offer_id_cache:
            if self._auth_manager.is_authenticated() and self._http_client.is_access_token_valid():
                try:
                    entitlements = await self._backend_client.get_entitlements()
                    offer_ids = [
                        OfferId(e["originOfferId"]) 
                        for e in entitlements 
                        if e.get("originOfferId")
                    ]
                    if offer_ids:
                        # populate offer cache synchronously (best-effort)
                        await self._get_offers(offer_ids)
                except Exception as e:
                    logger.debug(f"Synchronous offers prefetch failed: {e}")
            else:
                # schedule background prefetch if we cannot fetch now
                asyncio.create_task(self._prefetch_offers_background())

        if self._local_game_manager._local_games_update_in_progress:
            logger.debug("Local games update in progress, returning cached values")
            return (self._local_game_manager._local_games 
                   if isinstance(self._local_game_manager._local_games, list) else [])
        
        loop = asyncio.get_running_loop()
        try:
            self._local_game_manager._local_games_update_in_progress = True
            local_games = await loop.run_in_executor(None, 
                partial(self._local_game_manager.update_local_games))
            self._local_game_manager._local_games_last_update = int(time.time())
            self._local_game_manager._local_games = local_games
            return local_games
        finally:
            self._local_game_manager._local_games_update_in_progress = False

    def handle_local_game_update_notifications(self):
        # Skip notifications until authenticated with valid token
        if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
            return
        # If offers cache isn't ready, schedule background prefetch
        if not self._offer_id_cache:
            asyncio.create_task(self._prefetch_offers_background())
            return
        # Don't overlap update operations
        if self._local_game_manager._local_games_update_in_progress:
            logger.debug("Local games update in progress, skipping")
            return
        if not self._local_game_manager.should_update_cache():
            logger.debug("Local games cache is fresh")
            return

        async def notify_local_games_changed():
            try:
                self._local_game_manager._local_games_update_in_progress = True
                loop = asyncio.get_running_loop()
                notify_list = await loop.run_in_executor(None, 
                    partial(self._local_game_manager.get_local_game_status))
                self._local_game_manager._local_games_last_update = int(time.time())
                
                for local_games_notify in notify_list:
                    self.update_local_game_status(local_games_notify)
            finally:
                self._local_game_manager._local_games_update_in_progress = False

        asyncio.create_task(notify_local_games_changed())

    async def prepare_local_size_context(self, game_ids: List[GameId]) -> Dict[str, pathlib.PurePath]:
        if not IS_WINDOWS:
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
            # Remove outdated keys
            outdated_keys = [key for key in cache if "@" in key]
            for key in outdated_keys:
                cache.pop(key, None)
            return {
                game_id: GameTime(entry["game_id"], entry["time_played"], entry.get("last_played_time"))
                for game_id, entry in cache.items()
                if entry and game_id
            }

        def safe_decode(cache, key: str, decoder: Callable):
            if not cache:
                return {}
            try:
                if isinstance(cache, str):
                    return decoder(json.loads(cache)) if decoder else json.loads(cache)
                return decoder(cache) if decoder else cache
            except Exception:
                logger.exception(f"Failed to decode persistent '{key}' cache")
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