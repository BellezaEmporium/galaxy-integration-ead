import asyncio
import json
import pathlib
import platform
import re
import subprocess
import sys
import time
import webbrowser
import logging

from functools import partial
from typing import Any, Dict, List, NewType, Optional, AsyncGenerator, NamedTuple, Set, Iterable, Tuple, Callable, cast
from urllib.parse import urlparse, parse_qs
from rtm_client import RtmClient

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# Constants
LOCAL_GAMES_CACHE_VALID_PERIOD = 5 * 60  # 5 minutes
IS_WINDOWS = platform.system().lower() == "windows"
OFFERS_FETCH_BATCH_SIZE = 16

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
        
        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(self.lost_authentication)
        self._http_client.set_cookies_updated_callback(self._update_stored_cookies)
        self._http_client.set_save_lats_callback(self._save_lats)
        self._http_client.set_save_tokens_callback(self._store_tokens)

        self._backend_client = EABackendClient(self._http_client)
        self._presence_manager = RtmClient(
            access_token_provider=lambda: getattr(self._http_client, "_access_token", None),
            on_presence_update=cast(Callable[[str, Dict[Any, Any]], None], self.update_user_presence),
        )

        self._auth_manager = AuthenticationManager(self._http_client, self._backend_client)
        self._cache_manager = CacheManager(self)
        self._local_game_manager = LocalGameManager(self._cache_manager)

        self._persistent_cache_updated = False
        self._last_offers_prefetch = 0
        self._prefetch_task = None

    def _schedule_offers_prefetch(self):
        """Schedule background offers prefetch if not already scheduled.
        
        Prevents duplicate asyncio.create_task() calls for the same operation.
        If a prefetch task is already pending, this is a no-op.
        """
        if self._prefetch_task is None or self._prefetch_task.done():
            self._prefetch_task = asyncio.create_task(self._prefetch_offers_background())
            logger.debug("Scheduled offers background prefetch")
        else:
            logger.debug("Offers prefetch already scheduled, skipping duplicate")

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
        if self._presence_manager:
            await self._presence_manager.stop()
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
                        self._schedule_offers_prefetch()
                        self._schedule_background_services()
                        return Authentication(user_id, user_name)
                    except Exception as e:
                        logger.info(f"Stored access token invalid, trying refresh: {e}")

                # Try refresh if available
                if refresh_token:
                    try:
                        await self._force_refresh_access_token()
                        user_id, persona_id, user_name = await self._auth_manager.get_identity()
                        self._schedule_offers_prefetch()
                        self._schedule_background_services()
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
        self._schedule_offers_prefetch()
        self._schedule_background_services()
        return Authentication(user_id, user_name)

    async def _start_background_services(self):
        if self._presence_manager and self._http_client.supports_presence():
            try:
                self._presence_manager.start()
            except Exception:
                logger.exception("Failed to start presence manager")

    def _schedule_background_services(self):
        """Schedule background services to start after a short delay."""
        async def _delayed_start():
            try:
                await asyncio.sleep(2)  # Let Galaxy finish the handshake
                await self._start_background_services()
            except Exception:
                logger.exception("Background services startup failed")
        asyncio.create_task(_delayed_start())

    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        # Keep the full offer id prefix (e.g., DR:123, OFB-EAST:xxxx) to match backend/cache keys
        return OfferId(game_id.split('@')[0])

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> Dict[OfferId, Json]:
        """Retrieve offer data, checking cache first. Fetch missing offers in smaller batches."""
        offers: Dict[OfferId, Json] = {}
        missing_offers: List[OfferId] = []

        # Check cache first
        for offer_id in offer_ids:
            cached_offer = self._offer_id_cache.get(offer_id)
            if cached_offer and isinstance(cached_offer, dict):
                offers[offer_id] = cached_offer
            else:
                missing_offers.append(offer_id)

        # Fetch missing offers progressively. Juno tends to time out on large or bursty offer lookups.
        if missing_offers:
            # Deduplicate while preserving order
            seen = set()
            unique_missing = []
            for oid in missing_offers:
                if oid not in seen:
                    seen.add(oid)
                    unique_missing.append(oid)

            async def fetch_batch(chunk):
                if not chunk:
                    return {}

                try:
                    result = await self._backend_client.get_offers(chunk)
                    logger.info(f"fetch_batch: got {len(result)} offers for chunk of {len(chunk)}")
                    return result
                except Exception as e:
                    if len(chunk) == 1:
                        logger.error(f"Failed to fetch offers batch (single item {chunk[0]}): {e}")
                        return {}

                    midpoint = len(chunk) // 2
                    logger.warning(
                        "Offers batch of %d failed, retrying in smaller chunks",
                        len(chunk)
                    )

                    first_half = await fetch_batch(chunk[:midpoint])
                    second_half = await fetch_batch(chunk[midpoint:])

                    merged: Dict[OfferId, Json] = {}
                    if isinstance(first_half, dict):
                        for key, value in first_half.items():
                            merged[OfferId(key)] = value
                    if isinstance(second_half, dict):
                        for key, value in second_half.items():
                            merged[OfferId(key)] = value
                    return merged

            results = []
            for start in range(0, len(unique_missing), OFFERS_FETCH_BATCH_SIZE):
                chunk = unique_missing[start:start + OFFERS_FETCH_BATCH_SIZE]
                results.append(await fetch_batch(chunk))
            
            # Process results
            for gathered_offers in results:
                if isinstance(gathered_offers, dict):
                    for key, offer in gathered_offers.items():
                        if not isinstance(offer, dict):
                            continue
                        origin_offer_id = offer.get('offerId') or offer.get('originOfferId') or key
                        if origin_offer_id:
                            oid = OfferId(origin_offer_id)
                            offers[oid] = offer
                            self._offer_id_cache[oid] = offer

        return offers

    async def get_owned_games(self) -> List[Game]:
        self._check_authenticated()

        entitlements = await self._backend_client.get_entitlements()
        logger.info(f"Fetched {len(entitlements)} entitlements for user {self._auth_manager.user_id}")

        # Deduplicate by gameSlug — keep the best edition per game
        # Priority: downloadable > not downloadable, then earliest entitlement date (original purchase)
        best_by_slug: Dict[str, dict] = {}
        no_slug = []
        for e in entitlements:
            product = e.get("product") or {}
            slug = product.get("gameSlug")
            if not slug:
                no_slug.append(e)
                continue
            existing = best_by_slug.get(slug)
            if existing is None:
                best_by_slug[slug] = e
            else:
                # Prefer downloadable
                new_dl = product.get("downloadable", False)
                old_dl = (existing.get("product") or {}).get("downloadable", False)
                if new_dl and not old_dl:
                    best_by_slug[slug] = e
                elif new_dl == old_dl:
                    # Among same downloadable status, prefer earliest entitlement (original purchase)
                    new_date = (product.get("gameProductUser") or {}).get("initialEntitlementDate", "")
                    old_date = ((existing.get("product") or {}).get("gameProductUser") or {}).get("initialEntitlementDate", "")
                    if new_date and old_date and new_date < old_date:
                        best_by_slug[slug] = e

        deduped = list(best_by_slug.values()) + no_slug
        logger.info(f"Deduplicated {len(entitlements)} entitlements to {len(deduped)} unique games")

        offer_ids = [
            OfferId(e["id"]) 
            for e in deduped 
            if e.get("id")
        ]
        offers = await self._get_offers(offer_ids)
        logger.info(f"_get_offers returned {len(offers)} offers for {len(offer_ids)} offer IDs")

        games = []
        if offers:
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
        else:
            # Fallback: build games directly from entitlement data
            logger.warning("get_offers returned empty — falling back to entitlement data")
            for e in deduped:
                offer_id = e.get("id")
                product = e.get("product") or {}
                display_name = product.get("name")
                if offer_id and display_name:
                    games.append(Game(
                        GameId(offer_id),
                        display_name,
                        None,
                        LicenseInfo(LicenseType.SinglePurchase, None)
                    ))
                    # Populate cache so achievements etc. still work
                    oid = OfferId(offer_id)
                    self._offer_id_cache[oid] = {
                        "offerId": offer_id,
                        "displayName": display_name,
                        "gameSlug": product.get("gameSlug"),
                        "game_product": product,
                    }
        logger.info(f"get_owned_games returning {len(games)} games")
        return games

    async def prepare_achievements_context(self, game_ids: List[GameId]) -> AchievementsImportContext:
        self._check_authenticated()
        achievement_sets: Dict[GameSlug, AchievementSet] = {}
        achievements: Dict[AchievementSet, List[Achievement]] = {}

        if self._auth_manager.persona_id is None:
            logger.error("Persona ID is None, user might not be properly authenticated")
            raise AuthenticationRequired("User not properly authenticated")

        # Ensure offers are in cache
        offer_ids = [self._offer_id_from_game_id(game_id) for game_id in game_ids]
        try:
            await self._get_offers(offer_ids)
        except Exception as e:
            logger.exception("Failed to fetch offers in batch: %s", repr(e))

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

        # Fetch achievements per achievement set in parallel to reduce latency
        persona_id = self._auth_manager.persona_id
        if not persona_id:
            logger.warning("persona_id not available, skipping achievements")
            return AchievementsImportContext(
                owned_games=achievement_sets,
                achievements=achievements
            )

        async def fetch_achievement_set(ach_set: AchievementSet):
            set_id, ach_list = await self._backend_client.get_achievements([ach_set], persona_id)
            return (set_id, ach_list)
        
        # Gather all achievement set requests in parallel
        try:
            results = await asyncio.gather(
                *[fetch_achievement_set(ach_set) for ach_set in unique_ach_sets],
                return_exceptions=True
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch achievement set: {result}")
                    continue
                if isinstance(result, tuple) and len(result) == 2:
                    set_id, ach_list = result
                    if set_id:
                        achievement_set_obj = AchievementSet(set_id)
                        achievements[achievement_set_obj] = ach_list or []
        except Exception as e:
            logger.error(f"Achievement fetch failed: {e}")

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
                        OfferId(e["id"]) 
                        for e in entitlements 
                        if e.get("id")
                    ]
                    if offer_ids:
                        # populate offer cache synchronously (best-effort)
                        await self._get_offers(offer_ids)
                except Exception as e:
                    logger.debug(f"Synchronous offers prefetch failed: {e}")
            else:
                # schedule background prefetch if we cannot fetch now
                self._schedule_offers_prefetch()

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
            self._schedule_offers_prefetch()
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