import json
import logging
import time
from collections import namedtuple
from datetime import datetime
from typing import Dict, List, NewType, Optional, Any, Tuple

import aiohttp
from galaxy.api.errors import (
    AccessDenied, AuthenticationRequired, BackendError, BackendNotAvailable, BackendTimeout, NetworkError,
    UnknownBackendResponse
)
from galaxy.api.types import Achievement, SubscriptionGame, Subscription
from galaxy.http import HttpClient
from yarl import URL
from datetime import datetime


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


MasterTitleId = NewType("MasterTitleId", str)
AchievementSet = NewType("AchievementSet", str)
OfferId = NewType("OfferId", str)
Timestamp = NewType("Timestamp", int)
GameSlug = NewType("GameSlug", str)
Json = Dict[str, Any]  # helper alias for general purpose

SubscriptionDetails = namedtuple('SubscriptionDetails', ['tier', 'end_time'])


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
        self._last_access_token_success = None
        self._save_lats_callback = None
        super().__init__(cookie_jar=self._cookie_jar)

    def set_auth_lost_callback(self, callback):
        self._auth_lost_callback = callback

    def set_cookies_updated_callback(self, callback):
        self._cookie_jar.set_cookies_updated_callback(callback)

    async def authenticate(self, cookies):
        self._cookie_jar.update_cookies(cookies)
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

    async def _authorized_get(self, *args, **kwargs):
        headers = kwargs.setdefault("headers", {})
        headers["Authorization"] = "Bearer {}".format(self._access_token)

        return await super().request("GET", *args, **kwargs)

    async def _refresh_token(self):
        try:
            if self._access_token is not None:
                # diff method, once you have one access_token, you can get another one on refresh.
                url = "https://accounts.ea.com/connect/auth"
                params = {
                    "client_id": "JUNO_PC_CLIENT",
                    "scope": "signin dp.client.default",
                    "access_token": self._access_token,
                }
                response = await super().request("GET", url, params=params, allow_redirects=False)
                if "access_token" in response.headers["Location"]:
                    data = response.headers["Location"]
                    # should look like qrc:/html/login_successful.html#access_token=
                    # note that there's some other parameters afterwards, so we need to isolate the variable well
                    self._access_token = data.split("#")[1].split("=")[1].split("&")[0]
            else:
                await self._get_access_token()
        except (BackendNotAvailable, BackendTimeout, BackendError, NetworkError):
            logger.warning("Failed to refresh token for independent reasons")
            raise
        except Exception:
            logger.exception("Failed to refresh token")
            self._access_token = None
            if self._auth_lost_callback:
                self._auth_lost_callback()
            raise AccessDenied("Failed to refresh token")

    async def _get_access_token(self):
        url = "https://accounts.ea.com/connect/auth"
        params = {
            "client_id": "JUNO_PC_CLIENT",
            "display": "junoWeb/login",
            "response_type": "token",
            "redirectUri": "nucleus:rest"
        }
        response = await super().request("GET", url, params=params, allow_redirects=False)

        # upd 18.09.2023 : the access_token is in the "Location" header. It's a Bearer token.
        if "access_token" in response.headers["Location"]:
            data = response.headers["Location"]
            # should look like qrc:/html/login_successful.html#access_token=
            # note that there's some other parameters afterwards, so we need to isolate the variable well
            self._access_token = data.split("#")[1].split("=")[1].split("&")[0]
        elif "access_token" not in response.headers["Location"] and "error=login_required" in response.headers["Location"]:
            self._log_session_details()
            raise AuthenticationRequired("Error parsing access token. Must reauthenticate.")
        else:
            self._save_lats()

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


class EABackendClient:
    def __init__(self, http_client):
        self._http_client = http_client

    # Juno API
    @staticmethod
    def _get_api_host():
        return "https://service-aggregation-layer.juno.ea.com/graphql"

    async def get_identity(self) -> Tuple[str, str, str]:
        url = "{}?query=query{{me{{player{{pd psd displayName}}}}}}".format(self._get_api_host())
        pid_response = await self._http_client.get(url)
        data = await pid_response.json()

        try:
            user_id = data["data"]["me"]["player"]["pd"]
            persona_id = data["data"]["me"]["player"]["psd"]
            user_name = data["data"]["me"]["player"]["displayName"]

            return str(user_id), str(persona_id), str(user_name)
        except (AttributeError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", data, repr(e))
            raise UnknownBackendResponse()

    async def get_entitlements(self) -> List[Json]:
        # Step 1 = get all Origin product IDs
        u1 = "{}?query=query{{me{{ownedGameProducts(locale:\"en\" entitlementEnabled:true storefronts:[EA,STEAM,EPIC] type:[DIGITAL_FULL_GAME,PACKAGED_FULL_GAME] platforms:[PC] paging:{{limit:9999}}){{items{{originOfferId product{{gameSlug baseItem {{gameType}} gameProductUser{{ownershipMethods entitlementId}}}}}}}}}}}}".format(self._get_api_host())
        r1 = await self._http_client.get(u1)
        try:
            d1 = await r1.json()
            return d1['data']['me']['ownedGameProducts']['items']
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", await d1.text(), repr(e))
            raise UnknownBackendResponse()
    
    async def get_offer(self, offer_id) -> Json:
        u2 = "{}?query=query{{legacyOffers(offerIds: [\"{}\"], locale: \"en\"){{offerId: id contentId basePlatform primaryMasterTitleId mdmTitleIds achievementSetOverride multiplayerId installCheckOverride executePathOverride displayName displayType metadataInstallLocation softwarePlatform softwareId}} gameProducts(offerIds: [\"{}\"], locale: \"en\"){{items{{name originOfferId baseItem{{title}} gameSlug}}}}}}".format(
                self._get_api_host(),
                offer_id,
                offer_id
            )
        u2 = u2.replace(' ', '%20').replace('+', '%20')
        response = await self._http_client.get(u2)
        try:
            r2 = await response.json()
            return r2['data']['legacyOffers'][0], r2['data']['gameProducts']['items'][0]
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", await response.text(), repr(e))
            raise UnknownBackendResponse()
        

    async def get_achievements(self, offer: OfferId, persona: str) -> Dict[str, List[Achievement]]:
        url = "{}?query=query{{achievements(offerId:\"{}\",playerPsd:\"{}\",showHidden:true){{id achievements{{id name awardCount date}}}}}}".format(
            self._get_api_host(),
            str(offer),
            str(persona)
        )
        response = await self._http_client.get(url)
        def parser(json_data: Dict) -> List[Achievement]:
            achievements = []
            try:
                for achievement in json_data["achievements"]:
                    if achievement["awardCount"] == 1:
                        date_obj = datetime.strptime(achievement["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                        unix_timestamp = int(date_obj.timestamp())
                        achievement_data = Achievement(
                            achievement_id=achievement["id"],
                            achievement_name=achievement["name"],
                            unlock_time=unix_timestamp
                        )
                        achievements.append(achievement_data)
            except KeyError as e:
                logger.exception("Can not parse achievements from backend response %s", repr(e))
                raise UnknownBackendResponse()
            return achievements

        try:
            json = await response.json()
            achievement_sets = {}
            for achievement_set in json["data"]["achievements"]:
                achievements = parser(achievement_set)
                achievement_sets[achievement_set["id"]] = achievements
            return achievement_sets

        except (ValueError, KeyError) as e:
            logger.exception("Can not parse achievements from backend response %s", repr(e))
            raise UnknownBackendResponse()

    async def get_achievement_set(self, offer_id: OfferId, persona_id: str) -> str:
        url = "{}?query=query{{achievements(offerId:\"{}\",playerPsd:\"{}\"){{id}}}}".format(self._get_api_host(), offer_id, persona_id)
        response = await self._http_client.get(url)
    
        try:
            json = await response.json()
            achievements = json["data"]["achievements"]
            if achievements:
                return achievements[0]["id"] if "id" in achievements[0] else None
            else:
                return None

        except (ValueError, KeyError) as e:
            logger.exception("Can not parse achievements from backend response %s", repr(e))
            raise UnknownBackendResponse()

    async def get_game_time(self, game_slug):
        url = "{}?query=query{{me{{recentGames(gameSlugs:{}){{items{{lastSessionEndDate totalPlayTimeSeconds}}}}}}}}".format(
            self._get_api_host(),
            json.dumps(game_slug)
        )

        response = await self._http_client.get(url)

        """
        example response:
        {
            "data": {
                "me": {
                "recentGames": {
                    "items": [
                        {
                            "lastSessionEndDate": "2024-02-29T16:00:23.000Z",
                            "totalPlayTimeSeconds": 791005,
                        }
                    ],
                },
                }
            }
        }
        """
        try:
            def parse_last_played_time(lastplayed_timestamp) -> Optional[int]:
                try:
                    time_delta = datetime.strptime(lastplayed_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime(1970, 1, 1)
                except ValueError:
                    raise ValueError(f"time data '{lastplayed_timestamp}' does not match with the expected format")
                        
                return int(time_delta.total_seconds())

            content = await response.json()
            # assuming this is just EA's way of saying we never played a game.
            if not content['data']['me']['recentGames']['items']:
                return 0, None
            else:
                total_play_time = round(int(content['data']['me']['recentGames']['items'][0]['totalPlayTimeSeconds']) / 60)  # response is in seconds
                last_played_time = parse_last_played_time(content['data']['me']['recentGames']['items'][0]['lastSessionEndDate'])

            return total_play_time, last_played_time
        except (AttributeError, ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, %s", await response.text(), repr(e))
            raise UnknownBackendResponse()

    async def get_friends(self):
        response = await self._http_client.get(
            "{}?query=query{{me{{friends{{items{{player{{pd psd displayName}}}}}}}}}}".format(
                self._get_api_host()
            )
        )

        """
        {
            "data": {
                "me": {
                    "friends": {
                        "items": [
                            {
                                "player": {
                                    "pd": "...",
                                    "psd": "...",
                                    "displayName": "User"
                                }
                            }
                        ]
                    }
                }
            }
        }
        """

        try:
            content = await response.json()
            return {
                user_json['player']['pd']: user_json["player"]["displayName"]
                for user_json in content["data"]["me"]["friends"]["items"]
            }
        except (AttributeError, KeyError):
            logger.exception("Can not parse backend response: %s", await response.text())
            raise UnknownBackendResponse()

    async def get_lastplayed_games(self, game_slugs) -> Dict[GameSlug, Timestamp]:
        url = "{}?query=query{{me{{recentGames(gameSlugs:{}){{items{{gameSlug lastSessionEndDate}}}}}}}}".format(
            self._get_api_host(),
            json.dumps(game_slugs)
        )

        response = await self._http_client.get(url.replace(' ', '%20').replace('+', '%20'))

        '''
        {
            "data": {
                "me": {
                "recentGames": {
                    "items": [
                        {
                            "gameSlug": "the-sims-4",
                            "lastSessionEndDate": "2024-02-29T16:00:23.000Z"
                        }
                    ],
                },
                }
            }
        }
        '''

        def parse_last_session_end_date(date) -> int:
            try:
                time_delta = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime(1970, 1, 1)
            except ValueError:
                raise ValueError(f"time data '{date}' does not match with the expected format")
                    
            return int(time_delta.total_seconds())


        try:
            content = await response.json()
            games = content["data"]["me"]["recentGames"]["items"]
            return {
                game["gameSlug"]: parse_last_session_end_date(game["lastSessionEndDate"])
                for game in games
            }
        except (KeyError, ValueError) as e:
            logger.exception("Can not parse backend response: %s", await response.text())
            raise UnknownBackendResponse(e)


    async def _get_active_subscription(self, sub_json) -> Optional[SubscriptionDetails]:
        def parse_timestamp(timestamp: str) -> Timestamp:
            return Timestamp(
                int((datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime(1970, 1, 1)).total_seconds()))
        try:
            if sub_json and sub_json['status'].startswith('ACTIVE'):
                return SubscriptionDetails(
                    tier=sub_json['level'].lower(),
                    end_time=parse_timestamp(sub_json['end'])
                )
            else:
                logger.debug(f"Subscription status is not 'ACTIVE': {sub_json}")
                return None
        except (ValueError, KeyError) as e:
            logger.exception("Quack ! Seems like there's an issue involving subscriptions: %s, error %s", await sub_json.text(), repr(e))
            raise UnknownBackendResponse()

    async def _get_subscription_uris(self) -> List[str]:
        url = "{}?query=query{{me{{subscriptions{{offerId recurring start end level status offer{{offerName duration}} platform type statusReasonCode acquisitionMethod}}}}}}".format(self._get_api_host())
        response = await self._http_client.get(url)
        try:
            data = await response.json()
            return data['data']['me']['subscriptions']
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response while getting subs uri: %s, error %s", await response.text(), repr(e))
            raise UnknownBackendResponse()

    async def get_subscriptions(self) -> List[Subscription]:
        subs = {'standard': Subscription(subscription_name='EA Play', owned=False),
                'premium': Subscription(subscription_name='EA Play Pro', owned=False)}
        for sub in await self._get_subscription_uris():
            user_sub = await self._get_active_subscription(sub)
            if user_sub:
                break
        else:
            user_sub = None
        logger.debug(f'user_sub: {user_sub}')
        try:
            if user_sub:
                subs[user_sub.tier].owned = True
                subs[user_sub.tier].end_time = user_sub.end_time
        except (ValueError, KeyError) as e:
            logger.exception("Unknown subscription tier, error %s", repr(e))
            raise UnknownBackendResponse()
        return [subs['standard'], subs['premium']]

    async def get_games_in_subscription(self, tier) -> List[SubscriptionGame]:
        if tier == 'standard':
            tier = "ORIGIN_ACCESS_BASIC"
            check = "ea-play"
        elif tier == 'premium':
            tier = "ORIGIN_ACCESS_PREMIER"
            check = "ea-play-pro"
            check2 = "ea-play"

        url = "{}?query=query{{gameSearch(filter:{{gameTypes:[BASE_GAME],productLifecycleFilter:{{lifecycleTypes:[{}]}}}},paging:{{limit:9999}}){{items{{slug}}}}}}".format(self._get_api_host(), tier)
        response = await self._http_client.get(url)
        try:
            slugs = await response.json()
            slugs = [game['slug'] for game in slugs['data']['gameSearch']['items']]
            # we'll only get slugs, now get entitlement data
            subscription_games = []  # Create an empty list to accumulate the subscription games
            url2 = "{}?query=query{{games(slugs:{}){{items{{slug products{{items{{id name originOfferId}}}}}}}}}}".format(
                self._get_api_host(),
                json.dumps(slugs)
            )
            res2 = await self._http_client.get(url2.replace(' ', '%20').replace('+', '%20'))
            try:
                games = await res2.json()
                # verify product info, and take the correct Origin offer ID (some games have multiple offers)
                for game in games['data']['games']['items']:
                    if len(game['products']['items']) == 1:
                        subscription_games.append(
                            SubscriptionGame(
                                game_title=game['products']['items'][0]['name'],
                                game_id=game['products']['items'][0]['originOfferId'] + '@subscription'
                            )
                        )
                    for product in game['products']['items']:
                        if tier == "ORIGIN_ACCESS_BASIC":
                            verif = product['id'].find(check)
                            if verif != -1:
                                subscription_games.append(
                                    SubscriptionGame(
                                        game_title=product['name'],
                                        game_id=product['originOfferId'] + '@subscription'
                                    )
                                )
                        elif tier == "ORIGIN_ACCESS_PREMIER":
                            verif = product['id'].find(check)
                            verif2 = product['id'].find(check2)
                            if verif != -1 or verif2 != -1:
                                subscription_games.append(
                                    SubscriptionGame(
                                        game_title=product['name'],
                                        game_id=product['originOfferId'] + '@subscription'
                                    )
                                )
            except (ValueError, KeyError) as e:
                logger.exception("Can not parse backend response while getting subs games: %s, error %s", await res2.text(), repr(e))
                raise UnknownBackendResponse()
            return subscription_games  # Return the list of subscription games
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response while getting subs games: %s, error %s", await response.text(), repr(e))
            raise UnknownBackendResponse()
