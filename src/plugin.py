import asyncio
import os
import pathlib
import json
import logging
import platform
import subprocess
import sys
import tempfile
import time
import webbrowser
from functools import partial
from typing import Any, Callable, Dict, List, NewType, Optional, AsyncGenerator, NamedTuple, Set, Iterable
import winreg

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import (
    AccessDenied, AuthenticationRequired, BackendError, InvalidCredentials, UnknownBackendResponse, UnknownError
)
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, FriendInfo, Game, GameTime, LicenseInfo, LocalGame,
    NextStep, Subscription, SubscriptionGame
)

from backend import AuthenticatedHttpClient, MasterTitleId, OfferId, EABackendClient, Timestamp, AchievementSet, Json
from local_games import LocalGames, get_install_location, parse_total_size
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
                 "?response_type=code&client_id=EADOTCOM-WEB-SERVER&display=junoWeb/login"
                 "&locale=en_US&release_type=prod"
                 "&redirect_uri=https://www.ea.com/ea-play",
    "end_uri_regex": r"^https://www\.ea\.com/ea-play.*"
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
GameSlug = NewType("GameSlug", str)  # eg. "battlefield-1"
# but since EA Desktop has changed their launch format, we need to use the contentId to launch the games (eg: "1026023" for Battlefield 1)

class AchievementsImportContext(NamedTuple):
    owned_games: Dict[GameSlug, AchievementSet]
    achievements: Dict[AchievementSet, List[Achievement]]


class GameLibrarySettingsContext(NamedTuple):
    favorite: Set[OfferId]
    hidden: Set[OfferId]


class EAPlugin(Plugin):
    def __init__(self, reader, writer, token):
        super().__init__(Platform.Origin, __version__, reader, writer, token)
        self._user_id = None
        self._persona_id = None

        def auth_lost():
            self.lost_authentication()

        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(auth_lost)
        self._http_client.set_cookies_updated_callback(self._update_stored_cookies)
        self._backend_client = EABackendClient(self._http_client)
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
            if game_id is not None:
                game = Game(
                    game_id,
                    offer["displayName"],
                    None,
                    LicenseInfo(LicenseType.SinglePurchase, None)
                )
                games.append(game)

        return games

    async def prepare_achievements_context(self, game_ids: List[GameId]) -> AchievementsImportContext:
        self._check_authenticated()
        achievement_sets: Dict[OfferId, AchievementSet] = dict()
        achievements = []
        for game_id in game_ids:
            try:
                offer = self._offer_id_from_game_id(game_id)
                achievement_set = await self._backend_client.get_achievement_set(offer, self._persona_id)
                if achievement_set is not None:
                    achievement_sets[offer] = achievement_set
                    achievements = await self._backend_client.get_achievements(offer, self._persona_id)
                else:
                    logger.debug(f"No achievements found for game {offer}")
            except TypeError as e:
                print(f"Error retrieving achievements for game {offer}: {e}")
        return AchievementsImportContext(
            owned_games=achievement_sets,
            achievements=achievements
        )

    async def get_unlocked_achievements(self, game_id: GameId, context: AchievementsImportContext) -> List[Achievement]:
        offer = self._offer_id_from_game_id(game_id)
        if offer not in context.owned_games:
            logger.warning("Game '{}' doesn't have achievements.".format(game_id))
            return []
        else:
            achievements_set = context.owned_games[offer]
            achievements = context.achievements.get(achievements_set)
            if achievements is not None:
                return achievements

            return (await self._backend_client.get_achievements(
                offer, self._persona_id
            ))[achievements_set]

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
                if offer[0] and offer[1]:
                    # Prevent having multiple entries without any sense.
                    if "executePathOverride" in offer[0] and offer[0]["executePathOverride"] != "":
                        offer_id = offer[1]["originOfferId"]
                        offer[0]["gameSlug"] = offer[1]["gameSlug"]
                        offers[offer_id] = offer[0]
                        self._offer_id_cache[offer_id] = offer[0]
                else:
                    logger.warning(f"Data for {offer} not found.")
                    continue

            self.push_cache()

        return offers
    
    async def _get_owned_offers(self) -> Dict[GameId, Json]:
        def get_game_id(entitlement: Json) -> GameId:
            offer_id = entitlement["originOfferId"]
            external_type = entitlement["product"]["gameProductUser"]["ownershipMethods"][0]
            if external_type == "STEAM":
                return GameId(f"{offer_id}@steam")
            elif external_type == "EPIC":
                return GameId(f"{offer_id}@epic")
            else: 
                return GameId(offer_id)

        entitlement_data = await self._backend_client.get_entitlements()
        basegame_entitlements = [x for x in entitlement_data if x["product"] is not None and x["product"]["baseItem"]["gameType"] == "BASE_GAME"]
        basegame_offers = await self._get_offers([x["originOfferId"] for x in basegame_entitlements])

        # dump the offers into a json file, for local games
        with open(os.path.join(tempfile.gettempdir(), "offer_cache.json"), "w") as file:
            json.dump(basegame_offers, file, indent=4)

        return {
            get_game_id(ent): basegame_offers[ent["originOfferId"]]
            for ent in basegame_entitlements
            if ent["originOfferId"] in basegame_offers
        }

    async def get_subscriptions(self) -> List[Subscription]:
        self._check_authenticated()
        return await self._backend_client.get_subscriptions()

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

    async def _get_game_times_for_master_title(self, game_id: GameId, game_slug: GameSlug, lastplayed_time: Optional[Timestamp]) -> GameTime:
        """
        :param game_id - to get from cache
        :param game_slug - to fetch from backend
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

        total_play_time, last_played_time = await self._backend_client.get_game_time(game_slug)
        game_time: GameTime = GameTime(game_id, total_play_time, last_played_time)
        self._game_time_cache[game_id] = game_time
        self._persistent_cache_updated = True
        return game_time

    async def prepare_game_times_context(self, game_ids: List[GameId]) -> Any:
        self._check_authenticated()
        offer_ids = [self._offer_id_from_game_id(game_id) for game_id in game_ids]
        game_slugs = [GameSlug(self._offer_id_cache[offer_id]["gameSlug"]) for offer_id in offer_ids if offer_id in self._offer_id_cache and "gameSlug" in self._offer_id_cache[offer_id]]

        _, last_played_games = await asyncio.gather(
            self._get_offers(offer_ids),  # update local cache ignoring return value
            self._backend_client.get_lastplayed_games(game_slugs)
        )

        return last_played_games

    async def get_game_time(self, game_id: GameId, last_played_games: Any) -> GameTime:
        offer_id = self._offer_id_from_game_id(game_id)
        try:
            offer = self._offer_id_cache.get(offer_id)
            if offer is None:
                logger.exception("Internal cache out of sync")
                raise UnknownError()
            game_slug = GameSlug(offer["gameSlug"])

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
            FriendInfo(user_id=str(user_id), user_name=str(user_name))
            for user_id, user_name in (await self._backend_client.get_friends()).items()
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
        for game_id in game_ids: 
            game = self._offer_id_cache.get(self._offer_id_from_game_id(game_id))
            regkey_full = None
            if "installCheckOverride" in game and game["installCheckOverride"] != "" and game["installCheckOverride"].endswith(".exe"):
                regkey_full = game["installCheckOverride"]
            elif "executePathOverride" in game and game["executePathOverride"] != "" and game["executePathOverride"].endswith(".exe"):
                regkey_full = game["executePathOverride"]

            if regkey_full:
                regkey_path, part = regkey_full.split(']')
                game_name = regkey_full.split(']')[1].split('\\')[-1]
                regkey_path = regkey_path.replace('[', '')
                regkey_parts = regkey_path.split("\\")
                if len(regkey_parts) < 2:
                    logger.error(f"Invalid registry key format: {regkey_full}")
                    continue

                hive = getattr(winreg, regkey_parts[0]) # Convert hive to integer
                regkey_path = "\\".join(regkey_parts[1:-1]) # Join the rest of the path excluding the last part
                part = regkey_parts[-1] # The last part of the path is the part you want to get the value of

                install_location = get_install_location(hive, regkey_path, part)

                if install_location:
                    logger.info(f"Game file name: {game_name}")
                    logger.debug(f"Install location found: {install_location}")
                    if os.path.exists(install_location):
                        game_id_crc_map[game_id] = pathlib.Path(install_location)
                    else:
                        game_id_crc_map[game_id] = None
            else:
                game_id_crc_map[game_id] = None
        return game_id_crc_map

    async def get_local_size(self, game_id: GameId, context: Dict[str, pathlib.PurePath]) -> Optional[int]:
        try:
            return parse_total_size(context[game_id])
        except FileNotFoundError:
            return None
        except KeyError:
            raise UnknownError("Manifest not found")

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
    create_and_run_plugin(EAPlugin, sys.argv)


if __name__ == "__main__":
    main()
