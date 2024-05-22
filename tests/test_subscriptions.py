from unittest.mock import Mock
import pytest
from galaxy.api.types import SubscriptionGame, Subscription
from galaxy.api.errors import AuthenticationRequired, BackendError

from backend import EABackendClient


SUBSCRIPTION_OWNED_ID = "EA Play Pro"
SUBSCRIPTIONS_NOT_OWNED = [
    Subscription(subscription_name="EA Play", owned=False, end_time=None),
    Subscription(subscription_name="EA Play Pro", owned=False, end_time=None),
]
SUBSCRIPTIONS_OWNED = [
    Subscription(subscription_name="EA Play", owned=False, end_time=None),
    Subscription(subscription_name="EA Play Pro", owned=True, end_time=1581331712),
]

SUBSCRIPTION_GAMES_BACKEND_RESPONSE = {
    "game": [
        {
            "displayName": "Mass Effect 3 N7 Digital Deluxe Edition - PC - WW (Origin/3PDD)",
            "masterTitleIds": {"masterTitleId": ["69317"]},
            "basegames": {"basegame": ["DR:230773600"]},
            "extracontents": {
                "extracontent": [
                    "OFB-EAST:46112",
                    "OFB-MASS:46482",
                    "OFB-MASS:46483",
                    "OFB-MASS:46484",
                    "OFB-MASS:51074",
                ]
            },
            "offerInfos": {
                "offerInfo": [
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "91057bd2-0fa6-4d1f-936b-4389b615f0b7",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "OFB-MASS:51074",
                        "itemId": "ITM-MASS:41362",
                    },
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "9fc16845-87fe-4bef-84af-629498b734a4",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "DR:230773600",
                        "itemId": "ITM-EAST:34520",
                    },
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "1cac5243-7bdc-4d2d-aaf9-7360cd923a54",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "OFB-MASS:46484",
                        "itemId": "ITM-MASS:39212",
                    },
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "743cda51-203a-47f4-86aa-267ded87e23d",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "OFB-EAST:46112",
                        "itemId": "ITM-MASS:38705",
                    },
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "99814ee6-fae4-43b6-a2fc-f18da55dd0f6",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "OFB-MASS:46482",
                        "itemId": "ITM-MASS:39210",
                    },
                    {
                        "softwareIds": {
                            "softwareId": [
                                {
                                    "id": "bb88d063-4898-43f7-bbb0-41b257cb1319",
                                    "platform": "PCWIN",
                                }
                            ]
                        },
                        "offerId": "OFB-MASS:46483",
                        "itemId": "ITM-MASS:39211",
                    },
                ]
            },
            "offerId": "DR:230773600",
            "gameEditionTypeFacetKeyRankDesc": "7000",
        },
        {
            "displayName": "LEGRAND LEGACY: Tale of the Fatebounds - PC - WW - (Origin)",
            "masterTitleIds": {"masterTitleId": ["198199"]},
            "basegames": {"basegame": ["Origin.OFR.50.0003727"]},
            "extracontents": {},
            "offerInfos": {
                "offerInfo": [
                    {
                        "softwareIds": {
                            "softwareId": [{"id": "Origin.SFT.50.0001170", "platform": "PCWIN"}]
                        },
                        "offerId": "Origin.OFR.50.0003727",
                        "itemId": "Origin.ITM.50.0003195",
                    }
                ]
            },
            "offerId": "Origin.OFR.50.0003727",
            "gameEditionTypeFacetKeyRankDesc": "3000",
        },
        {
            "displayName": "Mable & the Wood - PC - WW - (Origin)",
            "masterTitleIds": {"masterTitleId": ["198204"]},
            "basegames": {"basegame": ["Origin.OFR.50.0003777"]},
            "extracontents": {},
            "offerInfos": {
                "offerInfo": [
                    {
                        "softwareIds": {
                            "softwareId": [{"id": "Origin.SFT.50.0001180", "platform": "PCWIN"}]
                        },
                        "offerId": "Origin.OFR.50.0003777",
                        "itemId": "Origin.ITM.50.0003237",
                    }
                ]
            },
            "offerId": "Origin.OFR.50.0003777",
            "gameEditionTypeFacetKeyRankDesc": "3000",
        },
        {
            "displayName": "Worms W.M.D - PC - WW - (Origin)",
            "masterTitleIds": {"masterTitleId": ["198167"]},
            "basegames": {"basegame": ["Origin.OFR.50.0003802"]},
            "extracontents": {},
            "offerInfos": {
                "offerInfo": [
                    {
                        "softwareIds": {
                            "softwareId": [{"id": "Origin.SFT.50.0001186", "platform": "PCWIN"}]
                        },
                        "offerId": "Origin.OFR.50.0003802",
                        "itemId": "Origin.ITM.50.0003254",
                    }
                ]
            },
            "offerId": "Origin.OFR.50.0003802",
            "gameEditionTypeFacetKeyRankDesc": "3000",
        },
    ]
}

SUBSCRIPTION_GAMES = [
    SubscriptionGame(
        game_title="Mass Effect 3 N7 Digital Deluxe Edition - PC - WW (Origin/3PDD)",
        game_id="DR:230773600@subscription",
        start_time=None,
        end_time=None,
    ),
    SubscriptionGame(
        game_title="LEGRAND LEGACY: Tale of the Fatebounds - PC - WW - (Origin)",
        game_id="Origin.OFR.50.0003727@subscription",
        start_time=None,
        end_time=None,
    ),
    SubscriptionGame(
        game_title="Mable & the Wood - PC - WW - (Origin)",
        game_id="Origin.OFR.50.0003777@subscription",
        start_time=None,
        end_time=None,
    ),
    SubscriptionGame(
        game_title="Worms W.M.D - PC - WW - (Origin)",
        game_id="Origin.OFR.50.0003802@subscription",
        start_time=None,
        end_time=None,
    ),
]


@pytest.mark.asyncio
async def test_backend_client_subscription_games(http_client, create_json_response):
    http_client.get.return_value = create_json_response(SUBSCRIPTION_GAMES_BACKEND_RESPONSE)
    tier = Mock(str)
    assert SUBSCRIPTION_GAMES == await EABackendClient(http_client).get_games_in_subscription(tier)


@pytest.mark.asyncio
async def test_subscription_not_owned(authenticated_plugin, backend_client):
    backend_client.get_subscriptions.return_value = SUBSCRIPTIONS_NOT_OWNED
    assert SUBSCRIPTIONS_NOT_OWNED == await authenticated_plugin.get_subscriptions()


@pytest.mark.asyncio
async def test_subscription_owned(authenticated_plugin, backend_client):
    backend_client.get_subscriptions.return_value = SUBSCRIPTIONS_OWNED
    assert SUBSCRIPTIONS_OWNED == await authenticated_plugin.get_subscriptions()


@pytest.mark.asyncio
async def test_subscription_games_unauthorized(plugin, http_client):
    """Error raised from prepare_context method"""
    http_client.is_authenticated.return_value = False

    with pytest.raises(AuthenticationRequired):
        await plugin.prepare_subscription_games_context(None)


@pytest.mark.asyncio
async def test_subscription_games_error(authenticated_plugin, backend_client):
    backend_client.get_games_in_subscription.side_effect = BackendError()

    context = await authenticated_plugin.prepare_subscription_games_context([SUBSCRIPTION_OWNED_ID])
    with pytest.raises(BackendError):
        async for _ in authenticated_plugin.get_subscription_games(SUBSCRIPTION_OWNED_ID, context):
            pass


@pytest.mark.asyncio
async def test_subscription_games(authenticated_plugin, backend_client):
    backend_client.get_games_in_subscription.return_value = SUBSCRIPTION_GAMES

    context = await authenticated_plugin.prepare_subscription_games_context([SUBSCRIPTION_OWNED_ID])
    all_sub_games = []
    async for sub_games in authenticated_plugin.get_subscription_games(SUBSCRIPTION_OWNED_ID, context):
        all_sub_games.extend(sub_games)
    assert all_sub_games == SUBSCRIPTION_GAMES
