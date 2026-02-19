import pytest
from galaxy.api.errors import AuthenticationRequired
from galaxy.api.types import Achievement

from backend import EABackendClient
from plugin import AchievementsImportContext

from tests.async_mock import AsyncMock

SIMPLE_ACHIEVEMENTS_SETS ={
    "DR:119971300": None,
    "OFB-EAST:109552409": None,
    "OFB-EAST:48217": None,
    "OFB-EAST:50885": "50563_52657_50844",
    "Origin.OFR.50.0001672": "50318_194188_50844",
    "Origin.OFR.50.0001452": "193634_192492_50844"
}

SPECIAL_ACHIEVEMENTS_SETS = {
    "DR:225064100": "BF_BF3_PC",
    "OFB-EAST:48217@subscription": None
}

ACHIEVEMENT_SETS = {**SIMPLE_ACHIEVEMENTS_SETS, **SPECIAL_ACHIEVEMENTS_SETS}



ACHIEVEMENTS = {
    "DR:119971300": [],
    "OFB-EAST:109552409": [],
    "OFB-EAST:48217": [],
    "Origin.OFR.50.0001672": [],
    "OFB-EAST:50885": [
        Achievement(1376676315, "1", "Stranger in a Strange Land"),
        Achievement(1376684053, "2", "Space Odyssey"),
        Achievement(1377464844, "3", "Critical Mass"),
        Achievement(1377467003, "4", "Snow Crash"),
        Achievement(1383078508, "5", "Intestinal Fortitude"),
        Achievement(1403554953, "31", "Full House"),
        Achievement(1377897989, "34", "From the Jaws"),
        Achievement(1377459297, "50", "Overpowered Healing")
    ],
    "DR:225064100": [
        Achievement(1371064136, "XP2ACH02_00", "Man of Calibre"),
        Achievement(1362857404, "ACH36_00", "M.I.A"),
        Achievement(1371066037, "XP2ACH01_00", "Dominator"),
        Achievement(1362857404, "ACH33_00", "Support Efficiency"),
    ],
    "Origin.OFR.50.0001452": [
        Achievement(1480870347, "1", "The Student..."),
        Achievement(1480870977, "3", "The Graduate"),
        Achievement(1483373394, "50", "Free Association")
    ],
    "OFB-EAST:48217@subscription": [],
}

MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_RESPONSE = {
    "50318_194188_50844": {
        "achievements": {},
        "name": "THE WITCHER® 3: WILD HUNT"
    },
    "50563_52657_50844": {
        "achievements": {
            "1": {
                "complete": True,
                "u": 1376676315,
                "name": "Stranger in a Strange Land",
            },
            "2": {
                "complete": True,
                "u": 1376684053,
                "name": "Space Odyssey",
            },
            "3": {
                "complete": True,
                "u": 1377464844,
                "name": "Critical Mass",
            },
            "4": {
                "complete": True,
                "u": 1377467003,
                "name": "Snow Crash",
            },
            "5": {
                "complete": True,
                "u": 1383078508,
                "name": "Intestinal Fortitude",
            },
            "31": {
                "complete": True,
                "u": 1403554953,
                "name": "Full House",
            },
            "34": {
                "complete": True,
                "u": 1377897989,
                "name": "From the Jaws",
            },
            "50": {
                "complete": True,
                "u": 1377459297,
                "name": "Overpowered Healing",
            }
        },
        "name": "Dead Space™ 3"
    },
    "193634_192492_50844": {
        "achievements": {
            "1": {
                "complete": True,
                "u": 1480870347,
                "name": "The Student...",
            },
            "3": {
                "complete": True,
                "u": 1480870977,
                "name": "The Graduate",
            },
            "50": {
                "complete": True,
                "u": 1483373394,
                "name": "Free Association",
            }
        },
        "name": "Titanfall® 2"
    }
}

MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_PARSED = {
    "50318_194188_50844": ACHIEVEMENTS["Origin.OFR.50.0001672"],
    "50563_52657_50844": ACHIEVEMENTS["OFB-EAST:50885"],
    "193634_192492_50844": ACHIEVEMENTS["Origin.OFR.50.0001452"]
}

SINGLE_ACHIEVEMENTS_SET_BACKEND_PARSED = {
    "BF_BF3_PC": ACHIEVEMENTS["DR:225064100"]
}

SINGLE_ACHIEVEMENTS_SET_BACKEND_RESPONSE = {
    "XP2ACH02_00": {
        "complete": True,
        "u": 1371064136,
        "name": "Man of Calibre",
    },
    "ACH36_00": {
        "complete": True,
        "u": 1362857404,
        "name": "M.I.A",
    },
    "XP2ACH01_00": {
        "complete": True,
        "u": 1371066037,
        "name": "Dominator",
    },
    "ACH33_00": {
        "complete": True,
        "u": 1362857404,
        "name": "Support Efficiency",
    }
}

@pytest.mark.asyncio
async def test_not_authenticated(plugin, http_client):
    http_client.is_authenticated.return_value = False

    with pytest.raises(AuthenticationRequired):
        await plugin.prepare_achievements_context(None)


@pytest.mark.asyncio
async def test_achievements_context_preparation(
    authenticated_plugin,
    persona_id,
    backend_client
):
    authenticated_plugin._get_owned_offers = AsyncMock()
    authenticated_plugin._get_owned_offers.return_value = {}
    await authenticated_plugin.prepare_achievements_context(None)


    authenticated_plugin._get_owned_offers.assert_called_once_with()
    backend_client.get_achievements.assert_called_once_with(persona_id)


@pytest.mark.asyncio
async def test_get_unlocked_achievements_simple(
    authenticated_plugin,
    backend_client,
):
    for game_id in SIMPLE_ACHIEVEMENTS_SETS.keys():
        assert ACHIEVEMENTS[game_id] == await authenticated_plugin.get_unlocked_achievements(
            game_id,
            context=AchievementsImportContext(
                owned_games=SIMPLE_ACHIEVEMENTS_SETS,
                achievements=MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_PARSED
            )
        )

    backend_client.get_achievements.assert_not_called()


@pytest.mark.asyncio
async def test_get_unlocked_achievements_explicit_call(
    authenticated_plugin,
    backend_client,
    persona_id
):
    backend_client.get_achievements.return_value = SINGLE_ACHIEVEMENTS_SET_BACKEND_PARSED

    for game_id in ACHIEVEMENT_SETS.keys():
        assert ACHIEVEMENTS[game_id] == await authenticated_plugin.get_unlocked_achievements(
            game_id,
            context=AchievementsImportContext(
                owned_games=ACHIEVEMENT_SETS,
                achievements=MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_PARSED
            )
        )

    backend_client.get_achievements.assert_called_once_with(persona_id, "BF_BF3_PC")

@pytest.mark.asyncio
@pytest.mark.parametrize("backend_response, parsed, explicit_set", [
    ({}, {}, None),
    (MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_RESPONSE, MULTIPLE_ACHIEVEMENTS_SETS_BACKEND_PARSED, None),
    (SINGLE_ACHIEVEMENTS_SET_BACKEND_RESPONSE, SINGLE_ACHIEVEMENTS_SET_BACKEND_PARSED, "BF_BF3_PC"),
])
async def test_achievements_parsing(
    backend_response,
    parsed,
    explicit_set,
    http_client,
    user_id,
    persona_id,
    create_json_response
):
    http_client.get.return_value = create_json_response(backend_response)

    assert parsed == await EABackendClient(http_client).get_achievements([explicit_set] if explicit_set else [], persona_id)

    http_client.get.assert_called_once_with(
        "https://achievements.gameservices.ea.com/achievements/personas/{user_id}{specific_set}/all".format(
            user_id=user_id, specific_set="/" + explicit_set if explicit_set else ""
        ),
        params={'lang': 'en_US', 'metadata': 'true'}
    )
