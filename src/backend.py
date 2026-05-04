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

# The EA servers do not accept over 100 slugs/IDs, so we need to take that into account by batching.
BATCH_SIZE = 100
all_games = []

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
        """Fetch owned games"""
        query = """query getPreloadedOwnedGames($isMac: Boolean = false, $storefronts: [UserGameProductStorefront!], $processorArchitectures: [ProcessorArchitecture!]) {
            me {
                ownedGameProducts(
                storefronts: [EA]
                locale: "DEFAULT"
                paging: {limit: 9999, next: null}
                productFound: true
                orderBy: {field: NAME, direction: ASC}
                ownershipMethod: [PURCHASE, REDEMPTION, ENTITLEMENT_GRANT]
                processorArchitectures: $processorArchitectures
                type: [DIGITAL_FULL_GAME, PACKAGED_FULL_GAME]
                downloadableOnly: false
                entitlementEnabled: true
                platforms: [PC]
                ) {
                items {
                    id: originOfferId
                    status
                    product {
                    id
                    name
                    downloadable
                    gameSlug
                    trialDetails {
                        trialType
                    }
                    baseItem(availabilities: [VISIBLE]) {
                        title
                        id
                        baseGameSlug
                        gameType
                    }
                    gamePlatformDetails @include(if: $isMac) {
                        gamePlatform
                    }
                    processorArchitectureDetails @include(if: $isMac) {
                        processorArchitecture
                        platform
                    }
                    gameProductUser(storefronts: $storefronts) {
                        ownershipMethods
                        initialEntitlementDate
                        entitlementId
                        gameProductUserTrial {
                        trialTimeRemainingSeconds
                        }
                        status
                    }
                    purchaseStatus {
                        repurchasable
                    }
                    }
                }
                }
            }
        }"""
        
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        
        try:
            items = response['data']['me']['ownedGameProducts']['items']
            return items
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()

    async def get_offers(self, offer_ids: List[str]) -> Dict[str, Json]:
        ids_json = json.dumps(offer_ids)
        query = f"""query{{
            legacyOffers(offerIds:{ids_json},locale:"DEFAULT"){{
                offerId:id contentId basePlatform primaryMasterTitleId mdmTitleIds
                achievementSetOverride multiplayerId installCheckOverride executePathOverride
                displayName displayType metadataInstallLocation softwarePlatform softwareId
            }}
            gameProducts(offerIds:{ids_json},locale:"DEFAULT"){{
                items{{id name originOfferId baseItem{{title gameType}} gameSlug}}
            }}
        }}"""
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)

        try:
            if not isinstance(response, dict):
                raise ValueError("Response is not a dict")
            data = response.get('data') or {}
            errors = response.get('errors')
            if errors:
                logger.warning("GraphQL errors in get_offers: %s", errors)

            legacy_offers = data.get('legacyOffers') or []
            game_products = (data.get('gameProducts') or {}).get('items', [])

            # Build lookup maps in single pass
            by_origin_offer = {}
            by_product_id = {}
            for p in game_products:
                if isinstance(p, dict):
                    if p.get('originOfferId'):
                        by_origin_offer[p['originOfferId']] = p
                    if p.get('id'):
                        by_product_id[p['id']] = p

            result: Dict[str, Json] = {}

            for legacy_offer in legacy_offers:
                if not isinstance(legacy_offer, dict):
                    continue
                
                offer_id = legacy_offer.get('offerId')
                if not offer_id:
                    continue

                # Find matching product with priority order
                product = (
                    by_origin_offer.get(offer_id)
                    or by_product_id.get(legacy_offer.get('contentId'))
                    or by_product_id.get(offer_id)
                    or {}
                )

                # Check if DLC/expansion
                display_type = legacy_offer.get('displayType', '').replace('_', '').lower()
                game_type = (product.get('baseItem') or {}).get('gameType', '').lower() if product else ''
                
                if display_type in {"addon", "expansion", "dlc"} or game_type in {"extra_content", "expansion"}:
                    logger.debug("Offer %s filtered out as DLC (displayType=%s gameType=%s)", offer_id, display_type, game_type)
                    continue

                # Set display name if missing
                if not legacy_offer.get('displayName'):
                    legacy_offer['displayName'] = (product.get('name') if product else None) or f"Unknown Game ({offer_id})"
                
                # Set game slug if available
                if product and product.get('gameSlug'):
                    legacy_offer['gameSlug'] = product['gameSlug']

                if product:
                    legacy_offer['game_product'] = product

                result[product.get('originOfferId') or offer_id if product else offer_id] = legacy_offer

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
        slugs = [game_slug] if not isinstance(game_slug, list) else game_slug
        query = f"query{{me{{recentGames(gameSlugs:{json.dumps(slugs)}){{items{{lastSessionEndDate totalPlayTimeSeconds}}}}}}}}"
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

    async def _get_active_subscription(self, sub_json) -> Optional[Subscription]:
        def parse_timestamp(timestamp: str) -> Timestamp:
            return Timestamp(
                int((datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime(1970, 1, 1)).total_seconds()))
        try:
            if sub_json and sub_json['status'].startswith('ACTIVE'):
                return Subscription(
                    subscription_name=sub_json['level'].lower(),
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

    async def get_active_subscription(self) -> Optional[Subscription]:
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
                subs[user_sub.subscription_name].owned = True
                subs[user_sub.subscription_name].end_time = user_sub.end_time
            except (ValueError, KeyError) as e:
                logger.exception("Unknown subscription tier, error %s", repr(e))
                raise UnknownBackendResponse()
        return [subs['standard'], subs['premium']]

    async def get_games_in_subscription(self, tier: str) -> List[SubscriptionGame]:
        if tier == 'standard':
            api_tier = "origin-access-basic"
        elif tier == 'premium':
            api_tier = "origin-access-premier"

        query = f"""query {{
            gameSearch(filter: {{gameTypes: [BASE_GAME, COLLECTION], subscriptionAvailabilitiesWithFreeToPlay: ["{api_tier}"]}} paging: {{limit: 9999}}) {{
                items {{ slug }}
            }}
        }}
        """
        
        url = f"{self._get_api_host()}?query={quote(query)}"
        response = await self._http_client.get(url)
        
        try:
            slugs = [game['slug'] for game in response['data']['gameSearch']['items']]
            subscription_games = []
            
            for i in range(0, len(slugs), BATCH_SIZE):
                batch = slugs[i:i+BATCH_SIZE]
                
                query = f"""query {{
                    games(slugs: {json.dumps(batch)}, locale: "DEFAULT") {{
                        items {{
                            slug
                            products {{
                                items {{
                                    id
                                    name
                                    originOfferId
                                    availableInSubscription {{
                                        slug
                                    }}
                                    trialDetails {{
                                        trialType
                                    }}
                                    baseItem {{
                                        gameType
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                """
                
                url = f"{self._get_api_host()}?query={quote(query)}"
                games_batch = await self._http_client.get(url)
                
                for game in games_batch.get('data', {}).get('games', {}).get('items', []):
                    for game_product in game.get('products', {}).get('items', []):
                        if game_product.get('trialDetails'):
                            continue
                        else:
                            if self._match_subscription_tier(game_product, tier):
                                subscription_games.append(
                                    SubscriptionGame(
                                        game_title=game_product.get('name'),
                                        game_id=(game_product.get('originOfferId') or '') + '@subscription'
                                    )
                                )
            
            return subscription_games
        except (ValueError, KeyError) as e:
            logger.exception("Can not parse backend response while getting subs games: %s, error %s", response, repr(e))
            raise UnknownBackendResponse()

    def _match_subscription_tier(self, product: Dict, tier: str) -> bool:
        """Check if product matches the subscription tier."""
        tier_slug = "origin-access-premier" if tier == 'premium' else "origin-access-basic"
        for avail in product.get('availableInSubscription') or []:
            slug_val = str(avail.get('slug', '')).lower()
            if isinstance(avail.get('slug'), list):
                slug_val = ','.join(str(s).lower() for s in avail['slug'])
            if tier_slug in slug_val:
                return True
        return False

    async def get_subscription_games_for_tier(self, tier: str) -> List[SubscriptionGame]:
        """
        Returns the list of games available in the specified subscription tier.
        Valid tiers are 'standard' for EA Play and 'premium' for EA Play Pro.
        """
        return await self.get_games_in_subscription(tier)