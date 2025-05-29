import asyncio
from unittest.mock import call

import pytest
from galaxy.api.errors import AuthenticationRequired
from galaxy.api.types import GameTime
from galaxy.unittest.mock import async_return_value

from plugin import EABackendClient

# only fields important for the logic are specified

OFFER_IDS = ["Origin.OFR.50.0000941", "OFB-EAST:109551006", "DR:119971300", "Origin.OFR.50.0002694"]
MASTER_TITLE_IDS = ["192140", "190077", "54856", "194908"]
MULTIPLAYER_IDS = ["11", None, None, None]

BACKEND_ENTITLEMENTS_RESPONSE = [{"offerId": offer_id, "offerType": "basegame"} for offer_id in OFFER_IDS]

BACKEND_OFFER_RESPONSES = [{
    "offerId": OFFER_IDS[0],
    "masterTitleId": MASTER_TITLE_IDS[0],
    "platforms": [
        {
            "platform": "PCWIN",
            "multiPlayerId": None  # intentionally None
        },
        {
            "platform": "OTHER_PLATFORM",
            "multiPlayerId": MULTIPLAYER_IDS[0]
        }
    ],
    "i18n": {"displayName": "STAR WARS™ Battlefront™"}
}, {
    "offerId": OFFER_IDS[1],
    "masterTitleId": MASTER_TITLE_IDS[1],
    "platforms": [
        {
            "platform": "PCWIN",
            "multiPlayerId": MULTIPLAYER_IDS[1]
        }
    ],
    "i18n": {"displayName": "FIFA World"}
}, {
    "offerId": OFFER_IDS[2],
    "masterTitleId": MASTER_TITLE_IDS[2],
    "platforms": [
        {
            "platform": "PCWIN",
            "multiPlayerId": MULTIPLAYER_IDS[2]
        }
    ],
    "i18n": {"displayName": "Need For Speed™ Shift"}
}, {
    "offerId": OFFER_IDS[3],
    "masterTitleId": MASTER_TITLE_IDS[3],
    "platforms": [
        {
            "platform": "PCWIN",
            "multiPlayerId": MULTIPLAYER_IDS[3]
        },
        {
            "platform": "PCMAC",
            "multiPlayerId": MULTIPLAYER_IDS[3]
        }
    ],
    "i18n": {"displayName": "Apex Legends™"}
}]

BACKEND_GAME_USAGE_RESPONSES = [
    (10, 1451288960),
    (120, 1551288960),
    (120, 1551288960),
    (0, None)
]

NEW_BACKEND_GAME_USAGE_RESPONSES = [
    (10, 1451288960),  # same time, no update
    (125, 1551288965),  # newer endtime, updated
    (120, 1551288960),  # missing in new 'last_played' response
    (5, 1551288965)  # previously missing
]

LASTPLAYED_GAMES = {
    MASTER_TITLE_IDS[0]: NEW_BACKEND_GAME_USAGE_RESPONSES[0][1],
    MASTER_TITLE_IDS[1]: NEW_BACKEND_GAME_USAGE_RESPONSES[1][1],
    # MASTER_TITLE_IDS[2] previously existed, now missing
    MASTER_TITLE_IDS[3]: NEW_BACKEND_GAME_USAGE_RESPONSES[3][1]
}

GAME_TIMES = [GameTime(OFFER_IDS[i], *BACKEND_GAME_USAGE_RESPONSES[i]) for i in range(len(OFFER_IDS))]


@pytest.mark.asyncio
async def test_not_authenticated(plugin, http_client):
    http_client.is_authenticated.return_value = False
    with pytest.raises(AuthenticationRequired):
        await plugin.prepare_game_times_context([])


BACKEND_LASTPLAYED_RESPONSE = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<lastPlayedGames>
    <userId>1008620950926</userId>
    <lastPlayed>
        <masterTitleId>''' + MASTER_TITLE_IDS[1] + '''</masterTitleId>
        <timestamp>2019-05-17T14:45:48.001Z</timestamp>
    </lastPlayed>
    <lastPlayed>
        <masterTitleId>''' + MASTER_TITLE_IDS[0] + '''</masterTitleId>
        <timestamp>2019-04-12T14:00:02.573Z</timestamp>
    </lastPlayed>
    <lastPlayed>
        <masterTitleId>''' + MASTER_TITLE_IDS[2] + '''</masterTitleId>
        <timestamp>2019-02-27T14:55:30Z</timestamp>
    </lastPlayed>
</lastPlayedGames>
'''  # intentionally missing MASTER_TITLE_IDS[3]

BACKEND_LASTPLAYED_PARSED = {
    MASTER_TITLE_IDS[0]: 1555077602,
    MASTER_TITLE_IDS[1]: 1558104348,
    MASTER_TITLE_IDS[2]: 1551279330
    # intentionally missing MASTER_TITLE_IDS[3]
}


@pytest.mark.asyncio
async def test_lastplayed_parsing(persona_id, http_client, create_xml_response):
    http_client.get.return_value = create_xml_response(BACKEND_LASTPLAYED_RESPONSE)

    assert BACKEND_LASTPLAYED_PARSED == await EABackendClient(http_client).get_lastplayed_games(persona_id)

    http_client.get.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("total, last_played_time, game_time", [
    ("<total>30292</total>", "<lastSessionEndTimeStamp>0</lastSessionEndTimeStamp>", (505, None)),
    ("<total>59</total>", "", (1, None)),
    ("<total>128</total>", "<lastSessionEndTimeStamp>1497190184759</lastSessionEndTimeStamp>", (2, 1497190185)),
])
async def test_game_time_parsing(total, last_played_time, game_time, persona_id, http_client, create_xml_response):
    http_client.get.return_value = create_xml_response('''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <usage>
            {total}
            {last_played_time}
        </usage>
    '''.format(total=total, last_played_time=last_played_time))

    assert game_time == await EABackendClient(http_client).get_game_time(persona_id, None, None)

    http_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_prepare_game_times_context(
    authenticated_plugin,
    backend_client,
    user_id,
    mocker
):
    get_offers_mock = mocker.patch(
        "plugin.OriginPlugin._get_offers",
        return_value=async_return_value(mocker.patch("plugin.OriginPlugin._get_offers"))
    )
    backend_client.get_lastplayed_games.return_value = async_return_value(LASTPLAYED_GAMES)

    assert LASTPLAYED_GAMES == await authenticated_plugin.prepare_game_times_context(OFFER_IDS)

    get_offers_mock.assert_called_once_with(OFFER_IDS)
    backend_client.get_lastplayed_games.assert_called_once_with(user_id)


@pytest.mark.asyncio
async def test_game_time_import_cached_entries(
    authenticated_plugin,
    backend_client,
    user_id,
    mocker
):
    mocker.patch.object(
        type(authenticated_plugin),
        "persistent_cache",
        new_callable=mocker.PropertyMock,
        return_value={
            "game_time": {game_time.game_id: game_time for game_time in GAME_TIMES}
        }
    )

    backend_client.get_offer.side_effect = BACKEND_OFFER_RESPONSES
    backend_client.get_lastplayed_games.return_value = async_return_value(LASTPLAYED_GAMES)
    backend_client.get_game_time.side_effect = NEW_BACKEND_GAME_USAGE_RESPONSES[1:]  # updates only

    await authenticated_plugin.prepare_game_times_context(OFFER_IDS)
    # wait for all `get_game_time` requests to be called
    for i in range(3):
        await asyncio.sleep(0)

    backend_client.get_offer.assert_has_calls([call(offer_id) for offer_id in OFFER_IDS], any_order=True)
    backend_client.get_lastplayed_games.assert_called_once_with(user_id)

    for offer_id, response in zip(OFFER_IDS, NEW_BACKEND_GAME_USAGE_RESPONSES):
        assert GameTime(offer_id, *response) == await authenticated_plugin.get_game_time(offer_id, LASTPLAYED_GAMES)

    backend_client.get_entitlements.assert_not_called()
    backend_client.get_game_time.assert_has_calls(
        [call(user_id, master_title_id, None) for master_title_id in MASTER_TITLE_IDS[1:]],
        any_order=True
    )


@pytest.mark.asyncio
async def test_game_time_import_empty_cache(
    authenticated_plugin,
    backend_client,
    user_id
):
    backend_client.get_lastplayed_games.return_value = async_return_value(LASTPLAYED_GAMES)
    backend_client.get_offer.side_effect = BACKEND_OFFER_RESPONSES
    backend_client.get_game_time.side_effect = BACKEND_GAME_USAGE_RESPONSES

    assert LASTPLAYED_GAMES == await authenticated_plugin.prepare_game_times_context(OFFER_IDS)
    # wait for all `get_game_time` requests to be called
    for i in range(3):
        await asyncio.sleep(0)

    backend_client.get_entitlements.assert_not_called()
    backend_client.get_lastplayed_games.assert_called_once_with(user_id)
    backend_client.get_offer.assert_has_calls([call(offer_id) for offer_id in OFFER_IDS], any_order=True)

    for game_time in GAME_TIMES:
        assert game_time == await authenticated_plugin.get_game_time(game_time.game_id, LASTPLAYED_GAMES)

    backend_client.get_game_time.assert_has_calls(
        [call(user_id, MASTER_TITLE_IDS[i], MULTIPLAYER_IDS[i]) for i in range(len(OFFER_IDS))],
        any_order=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_game_time_cache, game_time_cache", [
    ("{}", {}),
    (
        '''{
            "DR:119971300": {"game_id": "DR:119971300", "time_played": 41, "last_played_time": 1551279330},
            "OFB-EAST:109552409": {"game_id": "OFB-EAST:109552409", "time_played": 0},
            "OFB-EAST:48217": {"game_id": "OFB-EAST:48217", "time_played": 116, "last_played_time": 1564660289},
            "Origin.OFR.0002694": {"game_id": "Origin.OFR.50.0002694", "time_played": 1, "last_played_time": 1555077603}
        }''',
        {
            "DR:119971300": GameTime(game_id="DR:119971300", time_played=41, last_played_time=1551279330),
            "OFB-EAST:109552409": GameTime(game_id="OFB-EAST:109552409", time_played=0, last_played_time=None),
            "OFB-EAST:48217": GameTime(game_id="OFB-EAST:48217", time_played=116, last_played_time=1564660289),
            "Origin.OFR.0002694": GameTime(game_id="Origin.OFR.50.0002694", time_played=1, last_played_time=1555077603)
        }
    ),
    pytest.param(
        '''{
            "DR:119971300" :{ "game_id" : "DR:119971300", "last_played_time" : 1579099613, "time_played" : 43},
            "DR:224766400" :{ "game_id" : "DR:224766400", "time_played" : 0},
            "DR:119971300@subscription" : {"game_id" : "DR:119971300@subscription", "last_played_time" : 1579099813, "time_played" : 44}
        }''',
        {
            "DR:224766400": GameTime(game_id="DR:224766400", time_played=0, last_played_time=None),
            "DR:119971300@subscription": GameTime(game_id="DR:119971300@subscription", time_played=44, last_played_time=1579099813),
        },
        id="removing old entires after offerId -> gameId migration"
    )
])
async def test_game_time_cache_decoding(raw_game_time_cache, game_time_cache, plugin, mocker):
    persistent_cache_mock = mocker.patch.object(
        type(plugin),
        "persistent_cache",
        new_callable=mocker.PropertyMock,
        return_value={"game_time": raw_game_time_cache}
    )

    plugin.handshake_complete()
    assert persistent_cache_mock.return_value["game_time"] == game_time_cache


@pytest.mark.asyncio
@pytest.mark.parametrize("game_id,", [
    "OFR.1234",
    "OFR.1234@subscription"
])
async def test_game_time_import(
    authenticated_plugin,
    backend_client,
    game_id,
    mocker
):
    offer_id = game_id.split('@')[0]
    master_title_id = "12345"
    backend_times_response = (10, 1451288960)
    offer_cache = {
        offer_id: {
            "offerId": offer_id,
            "masterTitleId": master_title_id,
            "platforms": [
                {
                    "platform": "PCWIN",
                    "multiPlayerId": None
                }
            ],
        }
    }
    context = {master_title_id: backend_times_response[1]}
    expected = GameTime(game_id, *backend_times_response)

    mocker.patch.object(
        type(authenticated_plugin),
        "persistent_cache",
        new_callable=mocker.PropertyMock,
        return_value={"offers": offer_cache}
    )
    backend_client.get_game_time.return_value = backend_times_response

    assert expected == await authenticated_plugin.get_game_time(game_id, context)
