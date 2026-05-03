from galaxy.api.types import Game, LicenseInfo
from galaxy.api.consts import LicenseType
from galaxy.api.errors import AuthenticationRequired, AccessDenied, BackendError, UnknownError
import pytest

from tests.async_mock import AsyncMock


@pytest.mark.asyncio
async def test_not_authenticated(plugin, http_client):
    http_client.is_authenticated.return_value = False
    with pytest.raises(AuthenticationRequired):
        await plugin.get_owned_games()


@pytest.mark.asyncio
async def test_no_games(authenticated_plugin, backend_client, user_id):
    backend_client.get_entitlements.return_value = []

    result = await authenticated_plugin.get_owned_games()
    backend_client.get_entitlements.assert_called_with(user_id)
    assert result == []


@pytest.mark.asyncio
@pytest.mark.parametrize('entitlements, offers, expected', [
    pytest.param(
        [
            {
                "offerId" : "DR:119971300",
                "offerType" : "basegame"
            },
            {
                "offerId" : "DR:113311",
                "offerType": "dlc"
            },
        ],
        [
            {
                "offerId" : "DR:119971300",
                "i18n": {
                    "displayName": "Need for Speed SHIFT"
                }
            },
        ],
        [
            Game(
                "DR:119971300",
                "Need for Speed SHIFT",
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            )
        ],
        id='single game - dlc should be ignored'
    ),
    pytest.param(
        [
            {
                "offerId" : "DR:0001",
                "offerType": "basegame"
            },
            {
                "offerId" : "DR:119971300",
                "offerType" : "basegame"
            },
        ], 
        [
            UnknownError("404"),
            {
                "offerId" : "DR:119971300",
                "i18n": {
                    "displayName": "Need for Speed SHIFT"
                }
            },
        ], 
        [
            Game(
                "DR:119971300",
                "Need for Speed SHIFT",
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            )
        ],
        id="2 owned entitlements with one offer details not accessible"
    ),
    pytest.param(
        [
            {
                "offerId": "Origin.OFR.50.0003211",
                "grantDate": "2020-04-01T13:23:00Z",
                "terminationDate": "2020-04-01T13:24:00Z",
                "status": "DISABLED",
                "statusReasonCode": "SUNSET",
                "externalType": "SUBSCRIPTION",
                "offerType": "basegame",
            }, 
        ],
        [
            {
                "offerId": "Origin.OFR.50.0003211",
                "i18n": {
                    "displayName": "Blackguards"
                }
            },
        ],
        [
            Game(
                "Origin.OFR.50.0003211@subscription",
                "Blackguards",
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            )
        ],
        id="expired subscription game (still shown in Origin)"
    ),
    pytest.param(
        [
            {
                "offerId" : "DR:119971300",
                "offerType" : "basegame"
            },
            {
                "offerId" : "DR:113311",
                "offerType": "dlc"
            },
            {
                "offerId" : "Origin.OFR.50.000252",
                "offerType" : "basegame",
                "externalType" : 'STEAM',
            },
        ],
        [
            {
                "offerId" : "DR:119971300",
                "i18n": {
                    "displayName": "Need for Speed SHIFT"
                }
            },
            {
                "offerId" : "Origin.OFR.50.000252",
                "i18n": {
                    "displayName": "Unravel Two"
                }
            },
        ],
        [
            Game(
                "DR:119971300",
                "Need for Speed SHIFT",
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            ),
            Game(
                "Origin.OFR.50.000252@steam",
                "Unravel Two",
                None,
                LicenseInfo(LicenseType.SinglePurchase, None)
            )
        ],
    id="mixed: 2 games, one from steam, one dlc"
    ) 
])
async def test_own_games(
    authenticated_plugin, backend_client,
    entitlements, offers, expected
):
    backend_client.get_entitlements.return_value = entitlements
    backend_client.get_offer.side_effect = offers

    result = await authenticated_plugin.get_owned_games()
    assert result == expected


@pytest.mark.asyncio
async def test_access_denied(authenticated_plugin, backend_client, user_id):
    backend_client.get_entitlements.side_effect = [AccessDenied]
    with pytest.raises(AccessDenied):
        await authenticated_plugin.get_owned_games()
    backend_client.get_entitlements.assert_called_with(user_id)


@pytest.mark.asyncio
async def test_cache(authenticated_plugin, backend_client, mocker):
    ENTITLEMENTS = [
        {
            "offerId" : "DR:119971300",
            "offerType" : "basegame"
        },
        {
            "offerId" : "Origin.OFR.50.000252",
            "offerType" : "basegame",
            "externalType" : "STEAM",
        },
    ]
    OFFERS = [
        {
            "offerId" : "DR:119971300",
            "i18n": {
                "displayName": "Need for Speed SHIFT"
            },
        },
        {
            "offerId" : "Origin.OFR.50.000252",
            "i18n": {
                "displayName": "Unravel Two"
            }
        }
    ]
    offer_id_cache = {
        offer["offerId"]: offer for offer in OFFERS
    }
    mocker.patch.object(
        type(authenticated_plugin),
        "persistent_cache",
        new_callable=mocker.PropertyMock,
        return_value={"offers": offer_id_cache}
    )
    backend_client.get_entitlements.return_value = ENTITLEMENTS
    await authenticated_plugin.get_owned_games()
    backend_client.get_entitlements.assert_called_once()
    backend_client.get_offer.assert_not_called()


@pytest.mark.asyncio
async def test_get_offers_uses_smaller_batches_and_splits_failures(authenticated_plugin, backend_client):
    async def get_offers_side_effect(chunk):
        if len(chunk) > 2:
            raise BackendError("Request timed out")

        return {
            offer_id: {
                "offerId": offer_id,
                "displayName": f"Game {offer_id}"
            }
            for offer_id in chunk
        }

    backend_client.get_offers = AsyncMock(side_effect=get_offers_side_effect)

    offer_ids = [f"OFFER-{index}" for index in range(5)]

    result = await authenticated_plugin._get_offers(offer_ids)

    assert set(result.keys()) == set(offer_ids)
    assert [len(call[0][0]) for call in backend_client.get_offers.call_args_list] == [5, 2, 3, 1, 2]
