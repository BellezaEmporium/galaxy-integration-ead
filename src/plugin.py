import asyncio
import os
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
import winreg

from galaxy.api.consts import LicenseType, Platform
from galaxy.api.errors import (
    AccessDenied, AuthenticationRequired, BackendError, InvalidCredentials, UnknownBackendResponse, UnknownError
)
from galaxy.api.plugin import create_and_run_plugin, Plugin
from galaxy.api.types import (
    Achievement, Authentication, FriendInfo, Game, GameTime, LicenseInfo, LocalGame, LocalGameState,
    NextStep, Subscription, SubscriptionGame
)

from backend import MasterTitleId, OfferId, EABackendClient, Timestamp, AchievementSet, Json
from http_client import AuthenticatedHttpClient
from lgames_manifests import get_install_location, get_state_changes, parse_total_size, process_iter
from uri_scheme_handler import is_uri_handler_installed
from version import __version__
from pcsign_hash import preload_pc_sign_cache, generate_pc_sign_fast
from urllib.parse import urlparse, parse_qs
import re
import base64
import json


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
        self._access_token = None
        self._refresh_token = None

        def auth_lost():
            self.lost_authentication()

        self._http_client = AuthenticatedHttpClient()
        self._http_client.set_auth_lost_callback(auth_lost)
        self._http_client.set_cookies_updated_callback(self._update_stored_cookies)
        self._backend_client = EABackendClient(self._http_client)
        self._persistent_cache_updated = False

        self._local_games = {}
        self._local_games_last_update = 0
        self._local_games_update_in_progress = False

    @property
    def _game_time_cache(self) -> Dict[OfferId, GameTime]:
        cache = self.persistent_cache.get("game_time")
        if isinstance(cache, str):
            try:
                cache = json.loads(cache)
            except Exception:
                cache = {}
        if cache is None:
            cache = {}
        result = {}
        for k, v in cache.items():
            if isinstance(v, GameTime):
                result[k] = v
            elif isinstance(v, dict):
                result[k] = GameTime(v["game_id"], v["time_played"], v.get("last_played_time"))
        return result

    @_game_time_cache.setter
    def _game_time_cache(self, value: Dict[OfferId, GameTime]):
        serializable = {k: v.__dict__ for k, v in value.items()}
        self.persistent_cache["game_time"] = json.dumps(serializable)

    @property
    def _offer_id_cache(self) -> Dict[OfferId, Json]:
        cache = self.persistent_cache.get("offers")
        if isinstance(cache, str):
            try:
                cache = json.loads(cache)
            except Exception:
                cache = {}
        if cache is None:
            cache = {}
        return cache

    @_offer_id_cache.setter
    def _offer_id_cache(self, value: Dict[OfferId, Json]):
        self.persistent_cache["offers"] = json.dumps(value)
    
    def _find_executable_in_dir(self, directory):
        """Cherche un exécutable dans le dossier donné (retourne le premier .exe trouvé)."""
        if not directory or not os.path.isdir(directory):
            return None
        for entry in os.listdir(directory):
            if entry.lower().endswith('.exe'):
                return os.path.join(directory, entry)
        return None

    def _update_local_games(self):
        local_games = []
        running_exes = set(os.path.basename(exe).lower() for _, exe in process_iter() if exe)

        for offer_id, game_data in self._offer_id_cache.items():
            if "displayName" in game_data:
                logger.info(f"Checking local game status for {offer_id}, game name is {game_data.get('displayName')}")
                state = LocalGameState.None_
                install_path = None

                path = game_data.get("installCheckOverride") or game_data.get("executePathOverride")
                if path:
                    if path.endswith("installerdata.xml"):
                        base_path = self._get_install_path_from_xml(path)
                        if base_path:
                            exe = self._find_executable_in_dir(base_path)
                            install_path = exe or base_path
                    elif path.startswith('[') and ']' in path:
                        try:
                            reg = path.split(']', 1)[0][1:]
                            comps = reg.split('\\')
                            hive = getattr(winreg, comps[0])
                            key_path = "\\".join(comps[1:-1])
                            value_name = comps[-1]
                            install_path = get_install_location(hive, key_path, value_name)
                        except Exception as e:
                            logger.error(f"Error accessing registry key {path}: {e}")
                    else:
                        install_path = path

                if install_path and os.path.exists(install_path):
                    state = LocalGameState.Installed
                    exe_name = os.path.basename(install_path).lower()
                    if exe_name in running_exes:
                        state |= LocalGameState.Running
                    logger.info(f"{offer_id} is installed at {install_path}")

                local_games.append(LocalGame(offer_id, state))

            else:
                continue

        return local_games
        
    def _get_install_path_from_xml(self, xml_path):
        """Extract the installation path from the XML file or registry key."""
        try:
            # If the path is a XML file, parse it to get the installation path
            if xml_path.startswith('[') and ']' in xml_path:
                reg_path, xml_relative_path = xml_path.split(']', 1)
                reg_key = reg_path[1:]  # Remove the [ at the beginning
                
                # Divide the registry key into its components
                reg_components = reg_key.split('\\')
                if len(reg_components) < 3:
                    logger.error(f"Invalid registry key format: {xml_path}")
                    return None
                
                try:
                    hive_name = reg_components[0]
                    hive = getattr(winreg, hive_name)
                    value_name = reg_components[-1]
                    key_path = "\\".join(reg_components[1:-1])
                    
                    # Get the base installation location from the registry
                    base_install_location = get_install_location(hive, key_path, value_name)
                    
                    if base_install_location:
                        full_xml_path = os.path.join(base_install_location, xml_relative_path)
                        if os.path.exists(full_xml_path):
                            return base_install_location  # Give the base installation location
                except AttributeError:
                    logger.error(f"Unknown registry hive: {hive_name}")
                    return None
            elif os.path.exists(xml_path):
                return os.path.dirname(xml_path)
                
        except Exception as e:
            logger.info(f"Error while parsing installerdata.xml file: {e}")

        return None

    def _local_game_status(self):
        '''
        returns list of changed games (added, removed, or changed)
        updated local_games property
        '''
        new_local_games = self._update_local_games()
        notify_list = get_state_changes(self._local_games, new_local_games)
        self._local_games = new_local_games

        return self._local_games
        

    async def shutdown(self):
        await self._http_client.close()

    def tick(self):
        self.handle_local_game_update_notifications()    
    
    def _check_authenticated(self):
        if not self._http_client.is_authenticated():
            logger.exception("Plugin not authenticated")
            raise AuthenticationRequired()

    async def authenticate(self, stored_credentials=None):
        if stored_credentials:
            self._refresh_token = stored_credentials.get('refresh_token')
            if self._refresh_token:
                try:
                    # Force refresh the token every time for fresh session
                    logger.info("Authenticating with stored credentials")
                    await self._force_refresh_access_token()
                    identity_result = await self.get_identity()
                    if identity_result is None:
                        logger.error("get_identity returned None")
                        raise AuthenticationRequired("Failed to get identity")
                    user_id, persona_id, user_name = identity_result
                    logger.info(f"Successfully authenticated {user_name} with stored credentials")
                    self._user_id = user_id
                    self._persona_id = persona_id
                    return Authentication(self._user_id, user_name)
                except Exception as e:
                    logger.error(f"Failed to refresh: {str(e)}")
            self._refresh_token = None
        
        # Start new authentication flow
        logger.info("Starting new authentication flow")
        return await self._begin_auth_flow()

    async def _begin_auth_flow(self):
        try:
            pc_sign = generate_pc_sign_fast()
        except Exception as e:
            logger.error(f"Failed to generate PC_Sign: {e}")
            try:
                logger.info("Retrying PC_Sign generation...")
                from pcsign_hash import PCSign
                pc_sign = PCSign().generate_pc_sign()
            except Exception as e2:
                logger.error(f"Fallback PC_Sign generation also failed: {e2}")
                raise AuthenticationRequired(f"Unable to generate PC_Sign: {e}")
        
        params = {
            "window_title": "Login to EA Desktop",
            "window_width": 495 if is_windows() else 480,
            "window_height": 850 if is_windows() else 825,
            "start_uri": "https://accounts.ea.com/connect/auth"
                        "?response_type=code&client_id=JUNO_PC_CLIENT&display=junoClient/login"
                        "&redirect_uri=qrc:///html/login_successful.html"
                        "&locale=en_US&pc_sign={}".format(pc_sign),
            "end_uri_regex": "qrc:/html/login_successful.html.*"
        }
        return NextStep("web_session", params, js=JS)
    
    async def get_identity(self):
        try:
            if not self._access_token:
                logger.error("Access token not set.")
                raise AccessDenied("No access token obtained")

            # JWT tokens have 3 parts separated by dots: header.payload.signature
            token_parts = self._access_token.split('.')
            if len(token_parts) >= 2:
                try: 
                    # Decode the payload (second part)
                    payload = token_parts[1]
                    # Add padding if needed for base64 decoding
                    payload += '=' * (4 - len(payload) % 4)
                    decoded_payload = base64.b64decode(payload)
                    token_data = json.loads(decoded_payload)
                    
                    if "nexus" in token_data:
                        token = token_data["nexus"]
                        psif = token.get('psif')  # Utilise .get() pour éviter KeyError
                        if not psif or not isinstance(psif, list):
                            logger.error("Invalid token format: 'psif' is not a list")
                            raise AuthenticationRequired("Invalid token format")
                        
                        # Extract user information from token
                        self._user_id = token['pid']
                        self._persona_id = psif[0]['id']  # persona id
                        user_name = psif[0]['dis']  # user name
                        
                        logger.info(f"Identity successfully obtained from JWT token: user_id={self._user_id}, persona_id={self._persona_id}, user_name={user_name}")
                        return self._user_id, self._persona_id, user_name
                    else:
                        logger.warning("'nexus' key not found in token, falling back to backend method")
                        raise ValueError("'nexus' key missing from token")
                        
                except (ValueError, json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to decode JWT token: {e}, falling back to backend method.")
                    
                # Fall back to getting identity from backend if JWT decode fails
                try:
                    user_id, persona_id, user_name = await self._backend_client.get_identity()
                    self._user_id = user_id
                    self._persona_id = persona_id
                    # Pas besoin de reassigner user_name = user_name
                    return self._user_id, self._persona_id, user_name
                except Exception as e:
                    logger.error(f"Both methods (JWT & backend) failed: {e}")
                    raise AuthenticationRequired("Failed to get identity from both methods")
                    
        except Exception as e:
            logger.error(f"Something happened while trying to fetch token: {e}")
            raise AuthenticationRequired("Couldn't fetch token")
        
    async def _force_refresh_access_token(self):
        try:
            if not self._refresh_token:
                raise AuthenticationRequired("No refresh token available")
            self._access_token, self._refresh_token = await self._http_client._refresh_access_token(self._refresh_token)
            self.store_credentials({
                'refresh_token': self._refresh_token
            })
        except AuthenticationRequired:
            logger.warning("Authentication required, starting new authentication flow")
            self.store_credentials({})
            self._refresh_token = None
            self._access_token = None
            raise AuthenticationRequired("Token refresh failed, re-authentication required")
        except Exception as e:
            logging.error(f"Failed to refresh token: {e}")
            raise AuthenticationRequired()

    def _store_tokens(self, access_token, refresh_token):
        self.store_credentials({
            "access_token": access_token,
            "refresh_token": refresh_token
        })

    async def pass_login_credentials(self, step, credentials, cookies):
        logger.debug(f"Web process succeeded, passing credentials to plugin.")
        parsed_uri = urlparse(credentials["end_uri"])

        if parsed_uri.query:
            query_params = parse_qs(parsed_uri.query)

            logger.debug(f"parsed_uri.query: {query_params}")
            if 'code' in query_params:
                code = query_params.get('code', [None])[0]
            else:
                logger.error(f"No code found in query: {query_params}")
            if code:
                logger.info(f"Code obtained: {code}")
                self._access_token = code
                return await self._do_authenticate(code)
        else:
            raise AuthenticationRequired("No code found in redirect URI")

    async def _do_authenticate(self, code: str):
        try:
            self._access_token, self._refresh_token = await self._http_client._exchange_auth_code_for_token(code)

            # decipher JWT token, it contains the persona id and the user id
            # JWT token is base64 encoded, so we need to decode it
            try:
                user_id, persona_id, user_name = await self.get_identity()
                self._user_id = user_id
                self._persona_id = persona_id
            except Exception as exc:
                logger.error(f"Failed to get identity: {exc}")
                raise AuthenticationRequired("Failed to get identity from token")

            self._store_tokens(self._access_token, self._refresh_token)
            logger.info("Access token set successfully")

            if not self._user_id or not user_name:
                logger.error("user_id or user_name is None après authentification")
                raise AuthenticationRequired("user_id or user_name is None")
            return Authentication(self._user_id, user_name)
        except (AccessDenied, InvalidCredentials, AuthenticationRequired) as e:
            logger.exception(f"Failed to authenticate: {repr(e)}")
            raise InvalidCredentials()
        
    @staticmethod
    def _offer_id_from_game_id(game_id: GameId) -> OfferId:
        return OfferId(game_id.split('@')[0])

    async def _get_offers(self, offer_ids: Iterable[OfferId]) -> Dict[OfferId, Json]:
        """Retrieves offer data from a list of offer IDs.
        First checks the local cache, then makes requests for missing offers."""
        offers = {}
        missing_offers = []
        
        # First check offers in the cache
        for offer_id in offer_ids:
            cached_offer = self._offer_id_cache.get(offer_id)
            if isinstance(cached_offer, dict):
                offers[offer_id] = cached_offer
            else:
                missing_offers.append(offer_id)
        
        if missing_offers:
            # Make batch requests for missing offers
            requests = [self._backend_client.get_offer(offer_id) for offer_id in missing_offers]
            new_offers = await asyncio.gather(*requests, return_exceptions=True)
            
            for i, offer in enumerate(new_offers):
                if isinstance(offer, Exception):
                    logger.error(f"Error retrieving offer {missing_offers[i]}: {repr(offer)}")
                    continue
                
                if isinstance(offer, dict):
                    offer_id = offer.get("originOfferId")
                    if offer_id:
                        offers[offer_id] = offer
                        self._offer_id_cache[offer_id] = offer
                else:
                    logger.warning(f"Data for offer {missing_offers[i]} not found.")
            
            # Save cache updates
            if any(isinstance(new_offers[i], dict) for i in range(len(new_offers))):
                self.push_cache()
        
        return {k: v for k, v in offers.items() if isinstance(v, dict)}

    async def _get_owned_offers(self) -> Dict[GameId, Json]:
        """Retrieves all offers owned by the user and returns a dictionary with GameId as keys and offer data as values."""

        entitlements = await self._backend_client.get_entitlements()
        basegames = [
            e for e in entitlements
            if e.get("product") and e.get("product", {}).get("baseItem", {}).get("gameType") == "BASE_GAME"
        ]
        offer_ids = [e["originOfferId"] for e in basegames]
        offers = await self._get_offers(offer_ids)

        result = {}
        for offer in offers.values():
            result[GameId(offer["offerId"])] = offer
        return result

    async def get_owned_games(self) -> List[Game]:
        self._check_authenticated()

        owned_offers = await self._get_owned_offers()
        games = []
        for game_id, offer in owned_offers.items():
            if game_id and offer is not None:
                if "displayName" in offer:
                    game = Game(
                        game_id,
                        offer.get("displayName") or "",
                        None,
                        LicenseInfo(LicenseType.SinglePurchase, None)
                    )
                    games.append(game)
                else:
                    continue
        return games

    async def prepare_achievements_context(self, game_ids: List[GameId]) -> AchievementsImportContext:
        self._check_authenticated()
        achievement_sets: Dict[GameSlug, AchievementSet] = dict()
        achievements: Dict[AchievementSet, List[Achievement]] = dict()
        for game_id in game_ids:
            try:
                offer = self._offer_id_from_game_id(game_id)
                offer_data = self._offer_id_cache.get(offer)
                if not offer_data or "gameSlug" not in offer_data:
                    continue
                game_slug = GameSlug(offer_data["gameSlug"])
                if self._persona_id is None:
                    logger.error("Persona ID is None, user might not be properly authenticated")
                    raise AuthenticationRequired("User not properly authenticated")
                achievement_set = await self._backend_client.get_achievement_set(offer, self._persona_id)
                if achievement_set is not None:
                    achievement_set_obj = AchievementSet(achievement_set)
                    achievement_sets[game_slug] = achievement_set_obj
                    ach_dict = await self._backend_client.get_achievements(offer, self._persona_id)
                    if isinstance(ach_dict, dict) and achievement_set_obj in ach_dict:
                        achievements[achievement_set_obj] = ach_dict[achievement_set_obj]
                    else:
                        achievements[achievement_set_obj] = []
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
        offer_data = self._offer_id_cache.get(offer)
        if not offer_data or "gameSlug" not in offer_data:
            logger.warning("Game '{}' doesn't have achievements.".format(game_id))
            return []
        game_slug = GameSlug(offer_data["gameSlug"])
        if game_slug not in context.owned_games:
            logger.warning("Game '{}' doesn't have achievements.".format(game_id))
            return []
        else:
            achievements_set = context.owned_games[game_slug]
            achievements = context.achievements.get(achievements_set)
            if achievements is not None:
                return achievements
            if self._persona_id is None:
                logger.error("Persona ID is None, user might not be properly authenticated")
                raise AuthenticationRequired("User not properly authenticated")
            ach_dict = await self._backend_client.get_achievements(offer, self._persona_id)
            if isinstance(ach_dict, dict) and achievements_set in ach_dict:
                return ach_dict[achievements_set]
            return []

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
        game_slugs = [GameSlug(self._offer_id_cache[offer_id]["gameSlug"]) for offer_id in offer_ids if offer_id in self._offer_id_cache and "gameSlug" in self._offer_id_cache[offer_id]]

        try:
            _, last_played_games = await asyncio.gather(
                self._get_offers(offer_ids),  # update local cache ignoring return value
                self._backend_client.get_lastplayed_games(game_slugs)
            )
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
                logger.exception("Internal cache out of sync")
                raise UnknownError()
            if "gameSlug" in offer:
                game_slug = GameSlug(offer["gameSlug"])
            else:
                # Specific case in which offer data's in the other format
                game_slug = GameSlug(offer["gameNameFacetKey"])

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
            uri = "origin2://game/launch?offerId={}".format(master_title_id)
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
            uri = "origin2://game/launch?offerId={}&autoDownload=1".format(master_title_id)
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
            logger.debug("Local games are being updated, returning cached values")
            if isinstance(self._local_games, list):
                return self._local_games
            return []
        loop = asyncio.get_running_loop()
        try:
            self._local_games_update_in_progress = True
            local_games = await loop.run_in_executor(None, partial(self._local_game_status))
            self._local_games_last_update = time.time()
        finally:
            self._local_games_update_in_progress = False
        return local_games

    def handle_local_game_update_notifications(self):
        async def notify_local_games_changed():
            notify_list = []
            try:
                self._local_games_update_in_progress = True
                notify_list = await loop.run_in_executor(None, partial(self._local_game_status))
                self._local_games_last_update = time.time()
            finally:
                self._local_games_update_in_progress = False

            for local_games_notify in notify_list:
                self.update_local_game_status(local_games_notify)

        # don't overlap update operations
        if self._local_games_update_in_progress:
            logger.debug("Local games are being updated, skipping cache update")
            return

        if time.time() - self._local_games_last_update < LOCAL_GAMES_CACHE_VALID_PERIOD:
            logger.debug("Local games cache is fresh enough")
            return

        loop = asyncio.get_running_loop()
        asyncio.create_task(notify_local_games_changed())

    async def prepare_local_size_context(self, game_ids: List[GameId]) -> Dict[str, pathlib.PurePath]:
        game_id_manifest_map: Dict[str, pathlib.PurePath] = {}
        for game_id in game_ids:
            game = self._offer_id_cache.get(self._offer_id_from_game_id(game_id))
            if not game:
                continue
            if ("installCheckOverride" in game or "executePathOverride" in game):
                path = game.get("installCheckOverride", None) or game.get("executePathOverride", None)
                if path and path.startswith('[') and ']' in path:
                    reg_parts = path.split(']', 1)
                    reg_key = reg_parts[0][1:]
                    reg_components = reg_key.split('\\')
                    if len(reg_components) >= 3:
                        try:
                            hive_name = reg_components[0]
                            hive = getattr(winreg, hive_name)
                            value_name = reg_components[-1]
                            key_path = "\\".join(reg_components[1:-1])
                            install_location = get_install_location(hive, key_path, value_name)
                            if install_location and os.path.exists(install_location):
                                manifest_path = pathlib.Path(install_location) / "Support" / "mnfst.txt"
                                game_id_manifest_map[str(game_id)] = manifest_path
                                logger.debug(f"Manifest path for {game_id}: {manifest_path}")
                        except Exception as e:
                            logger.error(f"Error accessing registry key {path}: {e}")
                    else:
                        logger.error(f"Invalid registry key format: {path}")
        return game_id_manifest_map

    async def get_local_size(self, game_id: GameId, context: Dict[str, pathlib.PurePath]) -> Optional[int]:
        try:
            return parse_total_size(context[game_id])
        except FileNotFoundError:
            return None
        except KeyError:
            raise UnknownError("Manifest not found")

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
