import json
import logging
from collections import namedtuple
from datetime import datetime
from urllib.parse import quote
from typing import Dict, List, NewType, Optional, Any, Tuple

from galaxy.api.errors import UnknownBackendResponse
from galaxy.api.types import Achievement, SubscriptionGame, Subscription

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MasterTitleId = NewType("MasterTitleId", str)
AchievementSet = NewType("AchievementSet", str)
OfferId = NewType("OfferId", str)
Timestamp = NewType("Timestamp", int)
GameSlug = NewType("GameSlug", str)
Json = Dict[str, Any]

SubscriptionDetails = namedtuple('SubscriptionDetails', ['tier', 'end_time'])

class EABackendClient:
    def __init__(self, http_client):
        self._http_client = http_client

    # Juno API
    @staticmethod
    def _get_api_host():
        return "https://service-aggregation-layer.juno.ea.com/graphql"

    async def get_identity(self) -> Tuple[str, str, str]:
        query = "query{me{player{pd psd displayName}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        pid_response = await self._http_client.get(url)

        try:
            user_id = pid_response["data"]["me"]["player"]["pd"]
            persona_id = pid_response["data"]["me"]["player"]["psd"]
            user_name = pid_response["data"]["me"]["player"]["displayName"]

            return str(user_id), str(persona_id), str(user_name)
        except (AttributeError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", pid_response, repr(e))
            raise UnknownBackendResponse()

    async def get_entitlements(self) -> List[Json]:
        query = """query {
                    me {
                        ownedGameProducts(
                            locale: "DEFAULT"
                            entitlementEnabled: true
                            storefronts: [EA]
                            type: [DIGITAL_FULL_GAME, PACKAGED_FULL_GAME, DIGITAL_EXTRA_CONTENT, PACKAGED_EXTRA_CONTENT]
                            platforms: [PC]
                            paging: { limit: 9999 }
                        ) {
                            items {
                                originOfferId
                                product {
                                    id
                                    name
                                    gameSlug
                                    baseItem {
                                        gameType
                                    }
                                    gameProductUser {
                                        ownershipMethods
                                        entitlementId
                                    }
                                }
                            }
                        }
                    }
                }"""
        
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        
        try:
            return response['data']['me']['ownedGameProducts']['items']
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()

    async def get_offers(self, offer_ids: List[str]) -> Dict[str, Json]:
        query = (
            "query{"
            f"legacyOffers(offerIds: {json.dumps(offer_ids)}, locale: \"DEFAULT\")" "{"
            "offerId: id contentId basePlatform primaryMasterTitleId mdmTitleIds "
            "achievementSetOverride multiplayerId installCheckOverride executePathOverride "
            "displayName displayType metadataInstallLocation softwarePlatform softwareId"
            "}"
            f"gameProducts(offerIds: {json.dumps(offer_ids)}, locale: \"DEFAULT\")" "{"
            "items{id name originOfferId baseItem {title gameType} gameSlug}"
            "}"
            "}"
        )

        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)

        try:
            if not isinstance(response, dict):
                raise ValueError("Response is not a dict")
            data = response.get('data') or {}

            legacy_offers = data.get('legacyOffers') or []
            game_products = (data.get('gameProducts') or {}).get('items', [])

            by_origin_offer = {p.get('originOfferId'): p for p in game_products if isinstance(p, dict) and p.get('originOfferId')}
            by_product_id = {p.get('id'): p for p in game_products if isinstance(p, dict) and p.get('id')}

            result: Dict[str, Json] = {}

            for legacy_offer in legacy_offers:
                if not isinstance(legacy_offer, dict):
                    continue
                offer_id = legacy_offer.get('offerId')
                content_id = legacy_offer.get('contentId')
                if not offer_id:
                    continue

                product = (
                    by_origin_offer.get(offer_id)
                    or (content_id and by_product_id.get(content_id))
                    or by_product_id.get(offer_id)
                    or {}
                )

                display_type = str(legacy_offer.get('displayType', '')).replace('_', '').lower()
                game_type = str(product.get('baseItem', {}).get('gameType', '')).lower()
                is_full_or_base = display_type in {"fullgame", "basegame", "game"}
                is_base_game = game_type == 'base_game'
                if not (is_full_or_base or is_base_game):
                    logger.debug("Offer %s filtered out (displayType=%s gameType=%s)", offer_id, display_type, game_type)
                    continue

                if not legacy_offer.get('displayName'):
                    legacy_offer['displayName'] = product.get('name') or f"Unknown Game ({offer_id})"
                if product.get('gameSlug'):
                    legacy_offer['gameSlug'] = product['gameSlug']

                legacy_offer['game_product'] = product  # trace/debug

                key = product.get('originOfferId') or offer_id
                result[key] = legacy_offer

            return result
        except Exception as e:
            logger.exception("Can not parse backend response: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()
        

    async def get_achievements(self, achievement_sets: List[AchievementSet], persona: str) -> Tuple[Optional[str], List[Achievement]]:
        query = f"query{{achievements(achievementSetIds:{json.dumps([str(x) for x in achievement_sets])},playerPsd:\"{str(persona)}\",showHidden:true){{id achievements{{id name awardCount date}}}}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        
        def parser(json_data: Dict) -> List[Achievement]:
            achievements = []
            try:
                for achievement in json_data["achievements"]:
                    if achievement.get("awardCount") == 1:
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
            achievement_sets = response["data"]["achievements"]
            if not achievement_sets:
                return None, []
            
            all_achievements = []
            achievement_set_id = None
            
            for achievement_set in achievement_sets:
                if isinstance(achievement_set, dict) and "id" in achievement_set and not achievement_set_id:
                    achievement_set_id = achievement_set["id"]
                if isinstance(achievement_set, dict):
                    achievements = parser(achievement_set)
                    all_achievements.extend(achievements)
            
            return achievement_set_id, all_achievements

        except (ValueError, KeyError) as e:
            logger.exception("Can not parse achievements from backend response %s", repr(e))
            raise UnknownBackendResponse()

    async def get_game_time(self, game_slug):
        query = f"query{{me{{recentGames(gameSlugs:{json.dumps(game_slug)}){{items{{lastSessionEndDate totalPlayTimeSeconds}}}}}}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)

        """
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

            if not response['data']['me']['recentGames']['items']:
                return 0, None
            else:
                total_play_time = round(int(response['data']['me']['recentGames']['items'][0]['totalPlayTimeSeconds']) / 60)  # response is in seconds
                last_played_time = parse_last_played_time(response['data']['me']['recentGames']['items'][0]['lastSessionEndDate'])

            return total_play_time, last_played_time
        except (AttributeError, ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, %s", response, repr(e))
            raise UnknownBackendResponse()

    async def get_friends(self):
        query = "query{me{friends{items{player{pd psd displayName avatar{large{path}}}}}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
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
                                    "displayName": "User",
                                    "avatar": {
                                        "large": {
                                            "path": "..."
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        """
        try:
            return {
                user_json['player']['pd']: (user_json["player"]["displayName"], user_json["player"]["avatar"]["large"]["path"])
                for user_json in response["data"]["me"]["friends"]["items"]
            }
        except (AttributeError, KeyError):
            logger.exception("Can not parse backend response: %s", response)
            raise UnknownBackendResponse()

    async def get_lastplayed_games(self, game_slugs) -> Dict[GameSlug, Timestamp]:
        query = f"query{{me{{recentGames(gameSlugs:{json.dumps(game_slugs)}){{items{{gameSlug lastSessionEndDate}}}}}}}}"
        response = await self._http_client.get(f"{self._get_api_host()}?query={quote(query)}")

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
            me = response.get("data", {}).get("me", {})
            recent_games = me.get("recentGames")
            if not recent_games:
                logger.info("no data in recentGames: %s", response)
                return {}
            items = recent_games.get("items", [])
            if not items:
                logger.info("No recent games found in the response: %s", response)
                return {}
            return {
                GameSlug(game["gameSlug"]): Timestamp(parse_last_session_end_date(game["lastSessionEndDate"]))
                for game in items if "gameSlug" in game and "lastSessionEndDate" in game
            }
        except Exception as e:
            logger.exception("Can not parse backend response in get_lastplayed_games: %s", response)
            return {}

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
            logger.exception("Quack ! Seems like there's an issue involving subscriptions: %s, error %s", sub_json, repr(e))
            raise UnknownBackendResponse()

    async def _get_subscription_uris(self) -> List[str]:
        query = "query{me{subscriptions{offerId recurring start end level status offer{offerName duration} platform type statusReasonCode acquisitionMethod}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        try:
            return response['data']['me']['subscriptions']
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response while getting subs uri: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()

    async def get_active_subscription(self) -> Optional[SubscriptionDetails]:
        """
        Returns the active subscription for the user, if any.
        """
        for sub in await self._get_subscription_uris():
            user_sub = await self._get_active_subscription(sub)
            if user_sub:
                return user_sub
        return None

    async def get_user_subscriptions(self) -> List[Subscription]:
        """
        Returns the list of Galaxy subscriptions (EA Play, EA Play Pro) with their status for the user.
        """
        subs = {'standard': Subscription(subscription_name='EA Play', owned=False),
                'premium': Subscription(subscription_name='EA Play Pro', owned=False)}
        user_sub = await self.get_active_subscription()
        if user_sub:
            try:
                subs[user_sub.tier].owned = True
                subs[user_sub.tier].end_time = user_sub.end_time
            except (ValueError, KeyError) as e:
                logger.exception("Unknown subscription tier, error %s", repr(e))
                raise UnknownBackendResponse()
        return [subs['standard'], subs['premium']]

    async def get_games_in_subscription(self, tier) -> List[SubscriptionGame]:
        if tier == 'standard':
            api_tier = "origin-access-basic"
        elif tier == 'premium':
            api_tier = "origin-access-premier"

        query = f"query{{gameSearch(filter: {{gameTypes: [BASE_GAME, COLLECTION], subscriptionAvailabilitiesWithFreeToPlay: [{api_tier}]}} paging: {{limit: 9999}}) {{items {{slug}}}}}}"
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        try:
            slugs = [game['slug'] for game in response['data']['gameSearch']['items']]
            subscription_games = []
            query2 = f"query{{games(slugs:{json.dumps(slugs)}){{items{{slug products{{items{{id name originOfferId}}}}}}}}}}"
            url2 = f"{self._get_api_host()}?query={quote(query2)}"
            games = await self._http_client.get(url2)
            try:
                for game in games['data']['games']['items']:
                    for game_product in game['products']['items']:
                        if (tier == 'premium' and 'ea-play-pro' in game_product['id']) or ('ea-play' in game_product['id']):
                            subscription_games.append(
                                SubscriptionGame(
                                    game_title=game_product['name'],
                                    game_id=game_product['originOfferId'] + '@subscription'
                                )
                            )
                            break
            except (ValueError, KeyError) as e:
                logger.exception("Can not parse backend response while getting subs games: %s, error %s", games, repr(e))
                raise UnknownBackendResponse()
            return subscription_games
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response while getting subs games: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()

    async def get_subscription_games_for_tier(self, tier: str) -> List[SubscriptionGame]:
        """
        Returns the list of games available in the specified subscription tier.
        Valid tiers are 'standard' for EA Play and 'premium' for EA Play Pro.
        """
        return await self.get_games_in_subscription(tier)