import asyncio
import itertools
import json
import pathlib
import platform
import re
import subprocess
import sys
import time
import webbrowser
import logging
from collections.abc import Callable, Iterable, AsyncIterator
from functools import partial
from typing import Any, AsyncGenerator, NamedTuple, NewType, cast


from urllib.parse import urlparse, parse_qs, urlencode
from rtm_client import RtmClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
LOCAL_GAMES_CACHE_VALID_PERIOD = 15
IS_WINDOWS = platform.system().lower() == "windows"
OFFERS_FETCH_BATCH_SIZE = 16

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import AuthenticationRequired, BackendError, UnknownBackendResponse, UnknownError
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, UserInfo, UserPresence, PresenceState, Game, GameTime, LicenseInfo, LocalGame,
    NextStep, Subscription, SubscriptionGame
)

from backend import MasterTitleId, OfferId, EABackendClient, Timestamp, AchievementSet, Json, GameSlug
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

LOGIN_JS = {
    ".*" + re.escape(r"juno/login?execution") + ".*": [
        'document.getElementById("rememberMe").checked = true;'
    ]
}

MultiplayerId = NewType("MultiplayerId", str)
GameId = NewType("GameId", str)

class _AsyncListIterator:
    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self) -> AsyncIterator[SubscriptionGame]:
        return self

    async def __anext__(self) -> SubscriptionGame:
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration

class AchievementsImportContext(NamedTuple):
    owned_games: dict[GameSlug, AchievementSet]
    achievements: dict[AchievementSet, list[Achievement]]


class GameLibrarySettingsContext(NamedTuple):
    favorite: set[OfferId]
    hidden: set[OfferId]


class AuthenticationManager:
    def __init__(self, http_client: AuthenticatedHttpClient, backend_client: EABackendClient):
        self.http_client = http_client
        self.backend_client = backend_client
        self._user_id: str | None = None
        self._persona_id: str | None = None

    @property
    def user_id(self) -> str | None:
        return self._user_id

    @property
    def persona_id(self) -> str | None:
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
            logger.error("Failed to generate PC sign fast, using dummy: %s", e)
            import hashlib, time as _time
            # A SHA-256 hex digest is a plausible-looking value that won't cause 
            # an outright parse error on EA's side. 
            # Whether EA validates pc_sign cryptographically on their end 
            # (and thus would reject any value that wasn't machine-signed) 
            # is unknown from the source alone — but it's strictly better than sending an empty string, 
            # which is a guaranteed rejection.
            dummy = hashlib.sha256(str(_time.time()).encode()).hexdigest()
            pc_sign = dummy

        query = urlencode({
            "response_type": "code",
            "client_id": "JUNO_PC_CLIENT",
            "display": "junoClient/login",
            "redirect_uri": "qrc:///html/login_successful.html",
            "locale": "en_US",
            "pc_sign": pc_sign,
        })
        params = {
            "window_title": "Login to EA Desktop",
            "window_width": 495 if IS_WINDOWS else 480,
            "window_height": 850 if IS_WINDOWS else 825,
            "start_uri": f"https://accounts.ea.com/connect/auth?{query}",
            "end_uri_regex": "qrc:/html/login_successful.html.*"
        }
        return NextStep("web_session", params, js=LOGIN_JS)

    async def authenticate_with_code(self, code: str) -> tuple[str, str, str]:
        if not code:
            raise AuthenticationRequired("No authorization code provided")
        await self.http_client._exchange_auth_code_for_token(code)
        return await self.get_identity()

    async def get_identity(self) -> tuple[str, str, str]:
        token = getattr(self.http_client, "_access_token", None)
        if token:
            try:
                self._user_id, self._persona_id, user_name = extract_user_info_from_jwt(token)
                logger.info("Identity obtained from JWT: %s", user_name)
                return self._user_id, self._persona_id, user_name
            except Exception as e:
                logger.warning("JWT extraction failed: %s", e)

        self._user_id, self._persona_id, user_name = await self.backend_client.get_identity()
        logger.info("Identity obtained from backend: %s", user_name)
        return self._user_id, self._persona_id, user_name


class CacheManager:
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self._game_time_cache: dict[OfferId, GameTime] = {}
        self._offer_id_cache: dict[OfferId, Json] = {}

    @property
    def game_time_cache(self) -> dict[OfferId, GameTime]:
        return self._game_time_cache

    @game_time_cache.setter
    def game_time_cache(self, value: dict[OfferId, GameTime]):
        self._game_time_cache = value

    @property
    def offer_id_cache(self) -> dict[OfferId, Json]:
        return self._offer_id_cache

    @offer_id_cache.setter
    def offer_id_cache(self, value: dict[OfferId, Json]):
        self._offer_id_cache = value

    def cache_offers(self, offers: dict[OfferId, Json]):
        self._offer_id_cache.update(offers)


class LocalGameManager:
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self._local_games: list[LocalGame] = []
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
            not self._local_games_update_in_progress
            and time.time() - self._local_games_last_update >= LOCAL_GAMES_CACHE_VALID_PERIOD
        )


class EAPlugin(Plugin):
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
            on_presence_update=cast(Callable[[str, dict[Any, Any]], None], self.update_user_presence),
        )

        self._auth_manager = AuthenticationManager(self._http_client, self._backend_client)
        self._cache_manager = CacheManager(self)
        self._local_game_manager = LocalGameManager(self._cache_manager)

        self._persistent_cache_updated = False
        self._last_offers_prefetch = 0
        self._prefetch_task: asyncio.Task | None = None

    def _schedule_offers_prefetch(self):
        if self._prefetch_task is None or self._prefetch_task.done():
            self._prefetch_task = asyncio.create_task(self._prefetch_offers_background())
            logger.debug("Scheduled offers background prefetch")
        else:
            logger.debug("Offers prefetch already scheduled, skipping duplicate")

    async def _prefetch_offers_background(self):
        now = int(time.time())
        if now - self._last_offers_prefetch < 60:
            return
        if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
            return

        await asyncio.sleep(2)
        try:
            entitlements = await self._backend_client.get_entitlements()
            offer_ids = [OfferId(e["originOfferId"]) for e in entitlements if e.get("originOfferId")]
            if offer_ids:
                await self._get_offers(offer_ids)
            self._last_offers_prefetch = now
        except Exception as e:
            logger.debug("Background offers prefetch failed: %s", e)

    @property
    def _game_time_cache(self) -> dict[OfferId, GameTime]:
        return self._cache_manager.game_time_cache

    @_game_time_cache.setter
    def _game_time_cache(self, value: dict[OfferId, GameTime]):
        self._cache_manager.game_time_cache = value

    @property
    def _offer_id_cache(self) -> dict[OfferId, Json]:
        return self._cache_manager.offer_id_cache

    @_offer_id_cache.setter
    def _offer_id_cache(self, value: dict[OfferId, Json]):
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
        if not stored_credentials:
            logger.info("Starting new authentication flow")
            return await self._auth_manager.begin_auth_flow()

        cookies = stored_credentials.get("cookies")
        if cookies:
            self._http_client._cookie_jar.update_cookies(cookies)

        access_token = stored_credentials.get("access_token")
        refresh_token = stored_credentials.get("refresh_token")
        if access_token:
            self._http_client._access_token = access_token
        if refresh_token:
            self._http_client._refresh_token = refresh_token

        if access_token and self._http_client.is_access_token_valid():
            try:
                user_id, persona_id, user_name = await self._auth_manager.get_identity()
                self._schedule_offers_prefetch()
                self._schedule_background_services()
                return Authentication(user_id, user_name)
            except Exception as e:
                logger.info("Stored access token invalid, trying refresh: %s", e)

        if refresh_token:
            try:
                await self._force_refresh_access_token()
                user_id, persona_id, user_name = await self._auth_manager.get_identity()
                self._schedule_offers_prefetch()
                self._schedule_background_services()
                return Authentication(user_id, user_name)
            except Exception as e:
                logger.info("Refresh token failed, starting fresh auth: %s", e)

        logger.info("Starting new authentication flow")
        return await self._auth_manager.begin_auth_flow()

    async def _force_refresh_access_token(self):
        if not self._http_client._refresh_token:
            raise AuthenticationRequired("No refresh token available")
        try:
            await self._http_client._refresh_access_token(self._http_client._refresh_token)
        except AuthenticationRequired:
            raise
        except Exception as e:
            logger.error("Token refresh failed: %s", e)
            self.lost_authentication()
            raise AuthenticationRequired("Failed to refresh access token")

    def _store_tokens(self, access_token, refresh_token):
        try:
            current = self.persistent_cache.get("credentials", {})
            if isinstance(current, str):
                try:
                    current = json.loads(current)
                except (json.JSONDecodeError, TypeError):
                    current = {}

            if current.get("access_token") == access_token and current.get("refresh_token") == refresh_token:
                logger.debug("Tokens unchanged, skipping store_credentials call")
                return

            credentials = current | {"access_token": access_token, "refresh_token": refresh_token}
            if pending := getattr(self, "_pending_cookies", None):
                credentials["cookies"] = pending

            logger.debug("Storing updated tokens and cookies")
            self.store_credentials(credentials)
            self.__dict__.pop("_pending_cookies", None)
        except Exception:
            logger.exception("Failed to store tokens")

    async def pass_login_credentials(self, step, credentials, cookies):
        logger.debug("Web process succeeded, passing credentials to plugin.")
        parsed_uri = urlparse(credentials["end_uri"])

        if not parsed_uri.query:
            raise AuthenticationRequired("Failed to extract query parameters from callback URL")

        params = parse_qs(parsed_uri.query)
        code = params.get("code", [None])[0]
        if not code:
            raise AuthenticationRequired("No authorization code found in callback URL")

        if cookies and isinstance(cookies, list):
            try:
                cookie_dict = {c["name"]: c["value"] for c in cookies if c.get("name") and c.get("value")}
                if cookie_dict:
                    self._store_cookies(cookie_dict)
            except Exception as e:
                logger.warning("Failed to persist web cookies: %s", e)

        user_id, persona_id, user_name = await self._auth_manager.authenticate_with_code(code)
        self._schedule_offers_prefetch()
        self._schedule_background_services()
        return Authentication(user_id, user_name)

    async def _start_background_services(self):
        if self._presence_manager:
            try:
                self._presence_manager.start()
            except Exception:
                logger.exception("Failed to start presence manager")

    def _schedule_background_services(self):
        async def _delayed_start():
            await asyncio.sleep(2)
            try:
                await self._start_background_services()
            except Exception:
                logger.exception("Background services startup failed")
        asyncio.create_task(_delayed_start())

    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        return OfferId(game_id.split("@")[0])

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> dict[OfferId, Json]:
        offers: dict[OfferId, Json] = {}
        missing: list[OfferId] = []

        for oid in offer_ids:
            if (cached := self._offer_id_cache.get(oid)) and isinstance(cached, dict):
                offers[oid] = cached
            else:
                missing.append(oid)

        if not missing:
            return offers

        unique_missing: list[OfferId] = list(dict.fromkeys(missing))

        async def fetch_with_retry(chunk: list[OfferId]) -> dict[OfferId, Json]:
            if not chunk:
                return {}
            try:
                result = await self._backend_client.get_offers([str(oid) for oid in chunk])
                logger.info("fetch_batch: got %d offers for chunk of %d", len(result), len(chunk))
                return {OfferId(k): v for k, v in result.items()}
            except Exception as e:
                if len(chunk) == 1:
                    logger.error("Failed to fetch offer %s: %s", chunk[0], e)
                    return {}
                midpoint = len(chunk) // 2
                logger.warning("Offers batch of %d failed, retrying in smaller chunks", len(chunk))
                first = await fetch_with_retry(chunk[:midpoint])
                second = await fetch_with_retry(chunk[midpoint:])
                return first | second

        for start in range(0, len(unique_missing), OFFERS_FETCH_BATCH_SIZE):
            chunk = unique_missing[start:start + OFFERS_FETCH_BATCH_SIZE]
            batch = await fetch_with_retry(chunk)
            for key, offer in batch.items():
                if not isinstance(offer, dict):
                    continue
                oid = OfferId(offer.get("offerId") or offer.get("originOfferId") or key)
                offers[oid] = offer
                self._offer_id_cache[oid] = offer

        return offers

    @staticmethod
    def _entitlement_key(e: dict) -> tuple[bool, str, str]:
        product = e.get("product") or {}
        downloadable = not product.get("downloadable", False)
        date = (product.get("gameProductUser") or {}).get("initialEntitlementDate", "9999")
        slug = product.get("gameSlug", "")
        return (downloadable, date, slug)

    async def get_owned_games(self) -> list[Game]:
        self._check_authenticated()

        entitlements = await self._backend_client.get_entitlements()
        logger.info("Fetched %d entitlements for user %s", len(entitlements), self._auth_manager.user_id)

        entitlements.sort(key=lambda e: (e.get("product") or {}).get("gameSlug", ""))

        deduped: list[dict] = []
        no_slug: list[dict] = []
        for slug, group in itertools.groupby(entitlements, key=lambda e: (e.get("product") or {}).get("gameSlug")):
            if slug is None:
                no_slug.extend(group)
            else:
                deduped.append(min(group, key=self._entitlement_key))

        logger.info("Deduplicated %d entitlements to %d unique games", len(entitlements), len(deduped) + len(no_slug))

        offer_ids = [OfferId(e["id"]) for e in itertools.chain(deduped, no_slug) if e.get("id")]
        offers = await self._get_offers(offer_ids)
        logger.info("_get_offers returned %d offers for %d offer IDs", len(offers), len(offer_ids))

        games: list[Game] = []
        if offers:
            for origin_offer_id, offer in offers.items():
                if not isinstance(offer, dict):
                    continue
                display_name = offer.get("displayName") or (offer.get("game_product") or {}).get("name")
                raw_offer_id = offer.get("offerId") or str(origin_offer_id)
                if display_name and raw_offer_id:
                    games.append(Game(GameId(raw_offer_id), display_name, None, LicenseInfo(LicenseType.SinglePurchase, None)))
        else:
            logger.warning("get_offers returned empty — falling back to entitlement data")
            for e in itertools.chain(deduped, no_slug):
                if not (offer_id := e.get("id")):
                    continue
                if not (display_name := (e.get("product") or {}).get("name")):
                    continue
                games.append(Game(GameId(offer_id), display_name, None, LicenseInfo(LicenseType.SinglePurchase, None)))
                product = e.get("product") or {}
                self._offer_id_cache[OfferId(offer_id)] = {
                    "offerId": offer_id,
                    "displayName": display_name,
                    "gameSlug": product.get("gameSlug"),
                    "game_product": product,
                }

        logger.info("get_owned_games returning %d games", len(games))
        return games

    async def prepare_achievements_context(self, game_ids: list[GameId]) -> AchievementsImportContext:
        self._check_authenticated()

        if self._auth_manager.persona_id is None:
            logger.error("Persona ID is None, user might not be properly authenticated")
            raise AuthenticationRequired("User not properly authenticated")

        offer_ids = [self._offer_id_from_game_id(gid) for gid in game_ids]
        try:
            await self._get_offers(offer_ids)
        except Exception as e:
            logger.exception("Failed to fetch offers in batch: %s", repr(e))

        slug_to_ach_set: dict[GameSlug, AchievementSet] = {}
        unique_ach_sets: set[AchievementSet] = set()

        for game_id in game_ids:
            try:
                offer_id = self._offer_id_from_game_id(game_id)
                if not (offer_data := self._offer_id_cache.get(offer_id)):
                    continue
                if not (slug_val := offer_data.get("gameSlug") or offer_data.get("gameNameFacetKey")):
                    continue
                if not (ach_set_val := offer_data.get("achievementSetOverride")):
                    logger.debug("%s does not have any achievements.", offer_id)
                    continue
                game_slug = GameSlug(slug_val)
                ach_set = AchievementSet(str(ach_set_val))
                slug_to_ach_set[game_slug] = ach_set
                unique_ach_sets.add(ach_set)
            except Exception as e:
                logger.error("Error processing game %s: %s", game_id, e)

        if not slug_to_ach_set or not (persona_id := self._auth_manager.persona_id):
            return AchievementsImportContext(owned_games=slug_to_ach_set, achievements={})

        async def fetch_achievement_set(ach_set: AchievementSet):
            set_id, ach_list = await self._backend_client.get_achievements([ach_set], persona_id)
            return (set_id, ach_list)

        try:
            results = await asyncio.gather(
                *(fetch_achievement_set(a) for a in unique_ach_sets),
                return_exceptions=True
            )
        except Exception as e:
            logger.error("Achievement fetch failed: %s", e)
            results = []

        achievements: dict[AchievementSet, list[Achievement]] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error("Failed to fetch achievement set: %s", result)
                continue
            if isinstance(result, tuple) and len(result) == 2 and (set_id := result[0]):
                achievements[AchievementSet(set_id)] = result[1] or []

        return AchievementsImportContext(owned_games=slug_to_ach_set, achievements=achievements)

    async def get_unlocked_achievements(self, game_id: GameId, context: AchievementsImportContext) -> list[Achievement]:
        try:
            offer_id = self._offer_id_from_game_id(game_id)
            if not (offer := self._offer_id_cache.get(offer_id)):
                return []
            if not (slug_val := offer.get("gameSlug") or offer.get("gameNameFacetKey")):
                return []
            game_slug = GameSlug(slug_val)
            if not (ach_set := context.owned_games.get(game_slug)):
                return []
            return context.achievements.get(ach_set, [])
        except Exception:
            return []

    async def get_subscriptions(self) -> list[Subscription]:
        self._check_authenticated()
        return await self._backend_client.get_user_subscriptions()

    async def prepare_subscription_games_context(self, subscription_names: list[str]) -> dict[str, str]:
        self._check_authenticated()
        return {"EA Play": "standard", "EA Play Pro": "premium"}

    async def get_subscription_games(
        self, subscription_name: str, context: dict[str, str]
    ):
        try:
            tier = context[subscription_name]
        except KeyError:
            raise UnknownError(f"Unknown subscription name {subscription_name}!")

        games = await self._backend_client.get_subscription_games_for_tier(tier)
        return _AsyncListIterator(games)

    async def _get_game_times_for_master_title(
        self, game_id: GameId, game_slug: GameSlug, lastplayed_time: Timestamp | None
    ) -> GameTime:
        def get_cached(_game_id: GameId, _lastplayed_time: Timestamp | None) -> GameTime | None:
            if _lastplayed_time is None:
                return None
            offer_id = self._offer_id_from_game_id(_game_id)
            cached = self._game_time_cache.get(offer_id)
            if cached is None or cached.last_played_time is None:
                return None
            return cached if _lastplayed_time <= cached.last_played_time else None

        if cached := get_cached(game_id, lastplayed_time):
            return cached

        total_play_time, last_played_time = await self._backend_client.get_game_time(game_slug)
        game_time = GameTime(game_id, total_play_time, last_played_time)
        self._game_time_cache[self._offer_id_from_game_id(game_id)] = game_time
        self._persistent_cache_updated = True
        return game_time

    async def prepare_game_times_context(self, game_ids: list[GameId]) -> dict[GameSlug, Timestamp]:
        offer_ids = [self._offer_id_from_game_id(gid) for gid in game_ids]
        try:
            await self._get_offers(offer_ids)
        except Exception as e:
            logger.exception("Failed to fetch offers in batch: %s", repr(e))

        game_slugs = [
            GameSlug(self._offer_id_cache[oid]["gameSlug"])
            for oid in offer_ids
            if oid in self._offer_id_cache and "gameSlug" in self._offer_id_cache[oid]
        ]

        try:
            last_played = await self._backend_client.get_lastplayed_games(game_slugs)
        except Exception as e:
            logger.exception("Failed to get last played games: %s", repr(e))
            last_played = {}

        normalized_last_played: dict[GameSlug, Timestamp] = {}
        for slug, timestamp in (last_played or {}).items():
            normalized_last_played[GameSlug(str(slug))] = timestamp

        return normalized_last_played

    async def get_game_time(self, game_id: GameId, last_played_games: dict[GameSlug, Timestamp]) -> GameTime:
        offer_id = self._offer_id_from_game_id(game_id)
        try:
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                fetched = await self._backend_client.get_offers([offer_id])
                if isinstance(fetched, dict):
                    self._offer_id_cache.update({OfferId(k): v for k, v in fetched.items()})
                offer = self._offer_id_cache.get(offer_id)
                if offer is None:
                    logger.error("Offer %s not found after fetch", offer_id)
                    raise UnknownBackendResponse()

            if not (slug_val := offer.get("gameSlug") or offer.get("game_product", {}).get("gameSlug")):
                logger.error("Missing gameSlug for offer %s", offer_id)
                raise UnknownBackendResponse()

            return await self._get_game_times_for_master_title(
                game_id, GameSlug(slug_val), last_played_games.get(GameSlug(slug_val))
            )
        except KeyError as e:
            logger.exception("Failed to import game times %s", repr(e))
            raise UnknownBackendResponse()

    def game_times_import_complete(self):
        if self._persistent_cache_updated:
            self.push_cache()
            self._persistent_cache_updated = False

    async def get_friends(self) -> list[UserInfo]:
        self._check_authenticated()
        friends = await self._backend_client.get_friends()
        return [
            UserInfo(user_id=str(uid), user_name=str(uname), avatar_url=str(url))
            for uid, (uname, url) in friends.items()
        ]

    def _open_uri(self, uri: str):
        logger.info("Opening %s", uri)
        webbrowser.open(uri)

    async def launch_game(self, game_id: GameId):
        offer_id = self._offer_id_from_game_id(game_id)
        offer = self._offer_id_cache.get(offer_id)
        if offer is None:
            logger.exception("Internal cache out of sync")
            raise UnknownError()

        master_title_id: MasterTitleId = offer["contentId"]
        uri = (
            f"origin2://game/launch?offerIds={master_title_id}"
            if is_uri_handler_installed("origin2")
            else "https://www.ea.com/ea-app"
        )
        self._open_uri(uri)

    async def install_game(self, game_id: GameId):
        async def get_subscription_game_store_uri(offer_id: OfferId) -> str:
            try:
                offers = await self._backend_client.get_offers([offer_id])
                if offers and offer_id in offers:
                    return f"https://www.ea.com/games/{offers[offer_id]['gdpPath']}"
            except (KeyError, UnknownError, BackendError, UnknownBackendResponse):
                pass
            return "https://www.ea.com/ea-play/games"

        offer_id = self._offer_id_from_game_id(game_id)
        if game_id.endswith("subscription") and offer_id not in self._offer_id_cache:
            uri = await get_subscription_game_store_uri(offer_id)
        elif is_uri_handler_installed("origin2"):
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                logger.exception("Internal cache out of sync")
                raise UnknownError()
            uri = f"origin2://game/launch?offerIds={offer['contentId']}&autoDownload=1"
        else:
            uri = "https://www.ea.com/ea-app"

        self._open_uri(uri)

    if IS_WINDOWS:
        async def uninstall_game(self, game_id: GameId):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(subprocess.run, ["control", "appwiz.cpl"]))

    async def shutdown_platform_client(self) -> None:
        self._open_uri("origin2://quit")

    def _store_cookies(self, cookies: dict[str, str]):
        self._pending_cookies = cookies

    def _update_stored_cookies(self, morsels):
        try:
            self._store_cookies({m.key: m.value for m in morsels})
        except Exception:
            logger.exception("Failed to update stored cookies")

    async def get_local_games(self) -> list[LocalGame]:
        if not self._offer_id_cache:
            if self._auth_manager.is_authenticated() and self._http_client.is_access_token_valid():
                try:
                    entitlements = await self._backend_client.get_entitlements()
                    offer_ids = [OfferId(e["id"]) for e in entitlements if e.get("id")]
                    if offer_ids:
                        await self._get_offers(offer_ids)
                except Exception as e:
                    logger.debug("Synchronous offers prefetch failed: %s", e)
            else:
                self._schedule_offers_prefetch()

        if self._local_game_manager._local_games_update_in_progress:
            logger.debug("Local games update in progress, returning cached values")
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
        if not self._auth_manager.is_authenticated() or not self._http_client.is_access_token_valid():
            return
        if not self._offer_id_cache:
            self._schedule_offers_prefetch()
            return
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
                notify_list = await loop.run_in_executor(None, partial(self._local_game_manager.get_local_game_status))
                self._local_game_manager._local_games_last_update = int(time.time())
                for local_games_notify in notify_list:
                    self.update_local_game_status(local_games_notify)
            finally:
                self._local_game_manager._local_games_update_in_progress = False

        asyncio.create_task(notify_local_games_changed())

    async def prepare_local_size_context(self, game_ids: list[GameId]) -> dict[str, pathlib.PurePath]:
        if not IS_WINDOWS:
            return {}

        game_id_manifest_map: dict[str, pathlib.PurePath] = {}
        for game_id in game_ids:
            game = self._offer_id_cache.get(self._offer_id_from_game_id(game_id))
            if not game:
                continue

            path = game.get("installCheckOverride") or game.get("executePathOverride")
            if not path or not path.startswith("[") or "]" not in path:
                continue

            try:
                parsed_expr = parse_registry_expression(path)
                manifest_path: pathlib.Path | None = None

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
                    head = path.split("]", 1)[0] + "]"
                    install_location = resolve_registry_expression(head)
                    if install_location:
                        manifest_path = pathlib.Path(install_location) / "Support" / "mnfst.txt"

                if manifest_path is not None:
                    game_id_manifest_map[str(game_id)] = manifest_path
            except Exception as e:
                logger.error("Error processing registry path for %s: %s", game_id, e)

        return game_id_manifest_map

    async def get_local_size(self, game_id: GameId, context: dict[str, pathlib.PurePath]) -> int | None:
        try:
            return parse_total_size(str(context[game_id]))
        except (FileNotFoundError, KeyError):
            return None

    def handshake_complete(self):
        def game_time_decoder(cache: dict) -> dict[OfferId, GameTime]:
            for key in list(cache):
                if "@" in key:
                    del cache[key]
            return {
                game_id: GameTime(entry["game_id"], entry["time_played"], entry.get("last_played_time"))
                for game_id, entry in cache.items()
                if entry and game_id
            }

        def safe_decode(cache, key: str, decoder: Callable | None):
            if not cache:
                return {}
            try:
                data = json.loads(cache) if isinstance(cache, str) else cache
                return decoder(data) if decoder else data
            except Exception:
                logger.exception("Failed to decode persistent '%s' cache", key)
                return {}

        offers_decoded = safe_decode(self.persistent_cache.get("offers"), "offers", None)
        game_time_decoded = safe_decode(self.persistent_cache.get("game_time"), "game_time", game_time_decoder)

        self.persistent_cache["offers"] = json.dumps(offers_decoded)
        self.persistent_cache["game_time"] = json.dumps(game_time_decoded)

        if offers_decoded and isinstance(offers_decoded, dict):
            for offer_id, offer_data in offers_decoded.items():
                if isinstance(offer_data, dict):
                    self._offer_id_cache[OfferId(offer_id)] = offer_data
            logger.info("handshake_complete: loaded %d offers from persistent cache", len(offers_decoded))

        if game_time_decoded and isinstance(game_time_decoded, dict):
            self._game_time_cache.update(game_time_decoded)
            logger.info("handshake_complete: loaded %d game_time entries from persistent cache", len(game_time_decoded))

        self._http_client.load_lats_from_cache(self.persistent_cache.get("lats"))
        self._http_client.set_save_lats_callback(self._save_lats)

    def _save_lats(self, lats: int):
        self.persistent_cache["lats"] = str(lats)
        self.push_cache()


def main():
    preload_pc_sign_cache()
    create_and_run_plugin(EAPlugin, sys.argv)


if __name__ == "__main__":
    main()