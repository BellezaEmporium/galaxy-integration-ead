import asyncio
import pathlib
import json
import logging
import platform
import subprocess
import sys
import time
import webbrowser
from functools import partial
from typing import Any, Callable, Dict, List, NewType, Optional, AsyncGenerator, NamedTuple, Set, Iterable

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import (
    AccessDenied, AuthenticationRequired, BackendError, InvalidCredentials, UnknownBackendResponse, UnknownError
)
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, FriendInfo, Game, GameTime, LicenseInfo, LocalGame,
    NextStep, GameLibrarySettings, Subscription, SubscriptionGame
)

from backend import AuthenticatedHttpClient, MasterTitleId, OfferId, OriginBackendClient, Timestamp, AchievementSet, Json
from local_games import get_local_content_path, LocalGames, parse_map_crc_for_total_size
from uri_scheme_handler import is_uri_handler_installed
from version import __version__
import re


logger = logging.getLogger(__name__)


def is_windows():
    return platform.system().lower() == "windows"


LOCAL_GAMES_CACHE_VALID_PERIOD = 5
AUTH_PARAMS = {
    "window_title": "Login to EA Desktop",
    "window_width": 495 if is_windows() else 480,
    "window_height": 746 if is_windows() else 708,
    "start_uri": "https://accounts.ea.com/connect/auth"
                 "?response_type=code&client_id=JUNO_PC_CLIENT&display=junoWeb/login"
                 "&nonce=nonce&locale=en_US"
                 "&redirect_uri=qrc:///html/login_successful.html",
    "end_uri_regex": r"^qrc:/html/login\_successful\.html.*"
}
def regex_pattern(regex):
    return ".*" + re.escape(regex) + ".*"

JS = {regex_pattern(r"juno/login?execution"): [
r'''
    document.getElementById("rememberMe").checked = true;
'''
]}

MultiplayerId = NewType("MultiplayerId", str)
GameId = NewType("GameId", str)  # eg. Origin.OFR:12345 or Origin.OFR:12345@epic
# but since EA Desktop has changed their launch format, we need to use the contentId to launch the games (eg: "1026023" for Battlefield 1)


class AchievementsImportContext(NamedTuple):
    owned_games: Dict[GameId, AchievementSet]
    achievements: Dict[AchievementSet, List[Achievement]]


class GameLibrarySettingsContext(NamedTuple):
    favorite: Set[OfferId]
    hidden: Set[OfferId]


class OriginPlugin(Plugin):
    def __init__(self, reader, writer, token):
        super().__init__(Platform.Origin, __version__, reader, writer, token)
        self._user_id = None
        self._persona_id = None

        def auth_lost():
            self.lost_authentication()

        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(auth_lost)
        self._http_client.set_cookies_updated_callback(self._update_stored_cookies)
        self._backend_client = OriginBackendClient(self._http_client)
        self._persistent_cache_updated = False

        self._local_games = LocalGames()
        self._local_games_last_update = 0
        self._local_games_update_in_progress = False

    @property
    def _game_time_cache(self) -> Dict[OfferId, GameTime]:
        return self.persistent_cache.setdefault("game_time", {})

    @property
    def _offer_id_cache(self) -> Dict[OfferId, Json]:
        return self.persistent_cache.setdefault("offers", {})

    async def shutdown(self):
        await self._http_client.close()

    def tick(self):
        self.handle_local_game_update_notifications()

    def _check_authenticated(self):
        if not self._http_client.is_authenticated():
            logger.exception("Plugin not authenticated")
            raise AuthenticationRequired()

    async def _do_authenticate(self, cookies):
        try:
            await self._http_client.authenticate(cookies)

            self._user_id, self._persona_id, user_name = await self._backend_client.get_identity()
            return Authentication(self._user_id, user_name)

        except (AccessDenied, InvalidCredentials, AuthenticationRequired) as e:
            logger.exception("Failed to authenticate %s", repr(e))
            raise InvalidCredentials()

    async def authenticate(self, stored_credentials=None):
        stored_cookies = stored_credentials.get("cookies") if stored_credentials else None

        if not stored_cookies:
            return NextStep("web_session", AUTH_PARAMS, js=JS)

        return await self._do_authenticate(stored_cookies)

    async def pass_login_credentials(self, step, credentials, cookies):
        new_cookies = {cookie["name"]: cookie["value"] for cookie in cookies}
        auth_info = await self._do_authenticate(new_cookies)
        self._store_cookies(new_cookies)
        return auth_info

    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        return OfferId(game_id.split('@')[0])

    async def get_owned_games(self) -> List[Game]:
        self._check_authenticated()

        owned_offers = await self._get_owned_offers()
        games = []
        for game_id, offer in owned_offers.items():
            game = Game(
                game_id,
                offer["i18n"]["displayName"],
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            )
            games.append(game)

        return games

    @staticmethod
    def _get_achievement_set_override(offer: Json) -> Optional[AchievementSet]:
        potential_achievement_set = None
        for achievement_set in offer["platforms"]:
            potential_achievement_set = achievement_set["achievementSetOverride"]
            if achievement_set["platform"] == "PCWIN":
                return potential_achievement_set
        return potential_achievement_set

    async def prepare_achievements_context(self, game_ids: List[GameId]) -> AchievementsImportContext:
        self._check_authenticated()
        owned_offers: Dict[GameId, Json] = await self._get_owned_offers()
        achievement_sets: Dict[OfferId, AchievementSet] = dict()
        for game_id, offer in owned_offers.items():
            achievement_sets[game_id] = self._get_achievement_set_override(offer)
        return AchievementsImportContext(
            owned_games=achievement_sets,
            achievements=await self._backend_client.get_achievements(self._persona_id)
        )

    async def get_unlocked_achievements(self, game_id: GameId, context: AchievementsImportContext) -> List[Achievement]:
        try:
            achievements_set = context.owned_games[game_id]
        except KeyError:
            logger.exception("Game '{}' not found amongst owned".format(game_id))
            raise UnknownBackendResponse()

        if not achievements_set:
            return []

        try:
            # for some games(e.g.: ApexLegends) achievement set is not present in "all". have to fetch it explicitly
            achievements = context.achievements.get(achievements_set)
            if achievements is not None:
                return achievements

            return (await self._backend_client.get_achievements(
                self._persona_id, achievements_set
            ))[achievements_set]

        except KeyError:
            logger.exception("Failed to parse achievements for game {}".format(game_id))
            raise UnknownBackendResponse()

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> Dict[OfferId, Json]:
        """
            Get offers from cache if exists.
            Fetch from backend if not and update cache.
        """
        offers = {}
        missing_offers = []
        for offer_id in offer_ids:
            offer = self._offer_id_cache.get(offer_id, None)
            if offer is not None:
                offers[offer_id] = offer
            else:
                missing_offers.append(offer_id)

        # request for missing offers
        if missing_offers:
            requests = [self._backend_client.get_offer(offer_id) for offer_id in missing_offers]
            new_offers = await asyncio.gather(*requests, return_exceptions=True)

            for offer in new_offers:
                if isinstance(offer, Exception):
                    logger.error(repr(offer))
                    continue
                offer_id = offer["offerId"]
                offers[offer_id] = offer
                self._offer_id_cache[offer_id] = offer

            self.push_cache()

        return offers
    
    async def _get_owned_offers(self) -> Dict[GameId, Json]:
        def get_game_id(entitlement: Json) -> GameId:
            offer_id = entitlement["offerId"]
            external_type = entitlement.get("externalType")
            return GameId(f"{offer_id}@{external_type.lower()}" if external_type else offer_id)

        entitlements = await self._backend_client.get_entitlements(self._user_id)
        basegame_entitlements = [x for x in entitlements if x["offerType"] == "basegame"]
        basegame_offers = await self._get_offers([x["offerId"] for x in basegame_entitlements])

        return {
            get_game_id(ent): basegame_offers[ent["offerId"]]
            for ent in basegame_entitlements
            if ent["offerId"] in basegame_offers
        }

    async def get_subscriptions(self) -> List[Subscription]:
        self._check_authenticated()
        return await self._backend_client.get_subscriptions(user_id=self._user_id)

    async def prepare_subscription_games_context(self, subscription_names: List[str]) -> Any:
        self._check_authenticated()
        return {
            'EA Play': 'standard',
            'EA Play Pro': 'premium'
        }

    async def get_subscription_games(self, subscription_name: str, context: Dict[str, str]
    ) -> AsyncGenerator[List[SubscriptionGame], None]:
        try:
            tier = context[subscription_name]
        except KeyError:
            raise UnknownError(f'Unknown subscription name {subscription_name}!')
        yield await self._backend_client.get_games_in_subscription(tier)

    async def get_local_games(self) -> List[LocalGame]:
        if self._local_games_update_in_progress:
            logger.debug("LocalGames.update in progress, returning cached values")
            return self._local_games.local_games

        loop = asyncio.get_running_loop()
        try:
            self._local_games_update_in_progress = True
            local_games, _ = await loop.run_in_executor(None, partial(LocalGames.update, self._local_games))
            self._local_games_last_update = time.time()
        finally:
            self._local_games_update_in_progress = False
        return local_games

    def handle_local_game_update_notifications(self):
        async def notify_local_games_changed():
            notify_list = []
            try:
                self._local_games_update_in_progress = True
                _, notify_list = await loop.run_in_executor(None, partial(LocalGames.update, self._local_games))
                self._local_games_last_update = time.time()
            finally:
                self._local_games_update_in_progress = False

            for local_game_notify in notify_list:
                self.update_local_game_status(local_game_notify)

        # don't overlap update operations
        if self._local_games_update_in_progress:
            logger.debug("LocalGames.update in progress, skipping cache update")
            return

        if time.time() - self._local_games_last_update < LOCAL_GAMES_CACHE_VALID_PERIOD:
            logger.debug("Local games cache is fresh enough")
            return

        loop = asyncio.get_running_loop()
        asyncio.create_task(notify_local_games_changed())

    async def prepare_local_size_context(self, game_ids: List[GameId]) -> Dict[str, pathlib.PurePath]:
        game_id_crc_map: Dict[GameId, str] = {}
        for filepath, manifest in zip(self._local_games._manifests_stats.keys(), self._local_games._manifests):
            game_id_crc_map[manifest.game_id] = pathlib.PurePath(filepath).parent / 'map.crc'
        return game_id_crc_map

    async def get_local_size(self, game_id: GameId, context: Dict[str, pathlib.PurePath]) -> Optional[int]:
        try:
            return parse_map_crc_for_total_size(context[game_id])
        except FileNotFoundError:
            return None
        except KeyError:
            raise UnknownError("Manifest not found")

    @staticmethod
    def _get_multiplayer_id(offer) -> Optional[MultiplayerId]:
        for game_platform in offer["platforms"]:
            multiplayer_id = game_platform["multiPlayerId"]
            if multiplayer_id is not None:
                return multiplayer_id
        return None

    async def _get_game_times_for_master_title(
        self,
        game_id: GameId,
        master_title_id: MasterTitleId,
        multiplayer_id: Optional[MultiplayerId],
        lastplayed_time: Optional[Timestamp]
    ) -> GameTime:
        """
        :param game_id - to get from cache
        :param master_title_id - to fetch from backend
        :param multiplayer_id - to fetch from backend
        :param lastplayed_time - to decide on cache freshness
        """
        def get_cached_game_times(_game_id: GameId, _lastplayed_time: Optional[Timestamp]) -> Optional[GameTime]:
            """"returns None if a new entry should be retrieved"""
            if _lastplayed_time is None:
                # double-check if 'lastplayed_time' is unknown (maybe it was just to long ago)
                return None

            _cached_game_time: GameTime = self._game_time_cache.get(_game_id)
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

        response = await self._backend_client.get_game_time(self._user_id, master_title_id, multiplayer_id)
        game_time: GameTime = GameTime(game_id, response[0], response[1])
        self._game_time_cache[game_id] = game_time
        self._persistent_cache_updated = True
        return game_time

    async def prepare_game_times_context(self, game_ids: List[GameId]) -> Any:
        self._check_authenticated()
        offer_ids = [self._offer_id_from_game_id(game_id) for game_id in game_ids]

        _, last_played_games = await asyncio.gather(
            self._get_offers(offer_ids),  # update local cache ignoring return value
            self._backend_client.get_lastplayed_games(self._user_id)
        )

        return last_played_games

    async def get_game_time(self, game_id: GameId, last_played_games: Any) -> GameTime:
        offer_id = self._offer_id_from_game_id(game_id)
        try:
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                logger.exception("Internal cache out of sync")
                raise UnknownError()

            master_title_id: MasterTitleId = offer["masterTitleId"]
            multiplayer_id: Optional[MultiplayerId] = self._get_multiplayer_id(offer)

            return await self._get_game_times_for_master_title(
                game_id,
                master_title_id,
                multiplayer_id,
                last_played_games.get(master_title_id)
            )

        except KeyError as e:
            logger.exception("Failed to import game times %s", repr(e))
            raise UnknownBackendResponse()

    def game_times_import_complete(self):
        if self._persistent_cache_updated:
            self.push_cache()
            self._persistent_cache_updated = False

    async def prepare_game_library_settings_context(self, game_ids: List[GameId]) -> GameLibrarySettingsContext:
        self._check_authenticated()
        favorite_games, hidden_games = await asyncio.gather(
            self._backend_client.get_favorite_games(self._user_id),
            self._backend_client.get_hidden_games(self._user_id)
        )
        return GameLibrarySettingsContext(
            favorite=favorite_games,
            hidden=hidden_games
        )

    async def get_game_library_settings(self, game_id: GameId, context: GameLibrarySettingsContext) -> GameLibrarySettings:
        normalized_id = game_id.strip("@subscription")
        return GameLibrarySettings(
            game_id,
            tags=['favorite'] if normalized_id in context.favorite else [],
            hidden=normalized_id in context.hidden
        )

    async def get_friends(self):
        self._check_authenticated()

        return [
            FriendInfo(user_id=str(user_id), user_name=str(user_name))
            for user_id, user_name in (await self._backend_client.get_friends(self._user_id)).items()
        ]

    @staticmethod
    def _open_uri(uri):
        logger.info("Opening {}".format(uri))
        webbrowser.open(uri)
    
    async def launch_game(self, game_id: GameId):
        offer_id = self._offer_id_from_game_id(game_id)
        offer = self._offer_id_cache.get(offer_id)
        if offer is None:
            logger.exception("Internal cache out of sync")
            raise UnknownError()

        master_title_id: MasterTitleId = offer["contentId"]
        if is_uri_handler_installed("origin2"):
            uri = "origin2://game/launch?offerIds={}&autoDownload=1".format(master_title_id)
        else:
            uri = "https://www.ea.com/ea-app"

        self._open_uri(uri)

    async def install_game(self, game_id: GameId):

        def is_subscription_game(game_id: GameId) -> bool:
            return game_id.endswith('subscription')

        def is_offer_missing_from_user_library(offer_id: OfferId):
            return offer_id not in self._offer_id_cache
        
        async def get_subscription_game_store_uri(offer_id):
            try:
                offer = await self._backend_client.get_offer(offer_id)
                return "https://www.ea.com/games/{}".format(offer["gdpPath"])
            except (KeyError, UnknownError, BackendError, UnknownBackendResponse):
                return "https://www.ea.com/ea-play/games"

        offer_id = self._offer_id_from_game_id(game_id)
        if is_subscription_game(game_id) and is_offer_missing_from_user_library(offer_id):
            uri = await get_subscription_game_store_uri(offer_id)
        elif is_uri_handler_installed("origin2"):
            offer_id = self._offer_id_from_game_id(game_id)
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                logger.exception("Internal cache out of sync")
                raise UnknownError()

            master_title_id: MasterTitleId = offer["contentId"]
            uri = "origin2://game/launch?offerIds={}".format(master_title_id)
        else:
            uri = "https://www.ea.com/ea-app"

        self._open_uri(uri)

    if is_windows():
        async def uninstall_game(self, game_id: GameId):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(subprocess.run, ["control", "appwiz.cpl"]))

    async def shutdown_platform_client(self) -> None:
        self._open_uri("origin://quit")

    def _store_cookies(self, cookies):
        credentials = {
            "cookies": cookies
        }
        self.store_credentials(credentials)

    def _update_stored_cookies(self, morsels):
        cookies = {}
        for morsel in morsels:
            cookies[morsel.key] = morsel.value
        self._store_cookies(cookies)

    def handshake_complete(self):
        def game_time_decoder(cache: Dict) -> Dict[OfferId, GameTime]:

            # after offerId -> gameId migration
            outdated_keys = [key.split('@')[0] for key in cache if "@" in key]
            for i in outdated_keys:
                cache.pop(i, None)

            return {
                game_id: GameTime(entry["game_id"], entry["time_played"], entry.get("last_played_time"))
                for game_id, entry in cache.items()
                if entry and game_id
            }

        def safe_decode(_cache: Dict, _key: str, _decoder: Callable):
            if not _cache:
                return {}
            if _decoder is None:
                _decoder = lambda x: x

            try:
                return _decoder(json.loads(_cache))
            except Exception:
                logger.exception("Failed to decode persistent '%s' cache", _key)
                return {}

        # parse caches
        cache_decoders = {
            "offers": None,
            "game_time": game_time_decoder,
        }
        for key, decoder in cache_decoders.items():
            self.persistent_cache[key] = safe_decode(self.persistent_cache.get(key), key, decoder)

        self._http_client.load_lats_from_cache(self.persistent_cache.get('lats'))
        self._http_client.set_save_lats_callback(self._save_lats)

    def _save_lats(self, lats: int):
        self.persistent_cache['lats'] = str(lats)
        self.push_cache()

def main():
    create_and_run_plugin(OriginPlugin, sys.argv)


if __name__ == "__main__":
    main()
