from backend import EABackendClient
from galaxy.api.types import FriendInfo
from galaxy.api.errors import AuthenticationRequired
import pytest

FRIEND_LIST = [
    FriendInfo("1003118773678", "crak"),
    FriendInfo("1008880909879", "Danpire")
]

PARSED_FRIEND_LIST_RESPONSE = {
    "1003118773678": "crak",
    "1008880909879": "Danpire"
}

BACKEND_FRIENDS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <users>
        <user>
            <userId>1003118773678</userId>
            <personaId>1781965055</personaId>
            <EAID>crak</EAID>
        </user>
        <user>
            <userId>1008880909879</userId>
            <personaId>1004303509879</personaId>
            <EAID>Danpire</EAID>
        </user>
    </users>
"""

@pytest.mark.asyncio
async def test_not_authenticated(plugin, http_client):
    http_client.is_authenticated.return_value = False
    with pytest.raises(AuthenticationRequired):
        await plugin.get_friends()


@pytest.mark.asyncio
async def test_no_friends(authenticated_plugin, backend_client, user_id):
    backend_client.get_friends.return_value = {}

    assert [] == await authenticated_plugin.get_friends()
    backend_client.get_friends.assert_called_once_with(user_id)


@pytest.mark.asyncio
async def test_multiple_friends(authenticated_plugin, backend_client, user_id):
    backend_client.get_friends.return_value = PARSED_FRIEND_LIST_RESPONSE

    assert FRIEND_LIST == await authenticated_plugin.get_friends()
    backend_client.get_friends.assert_called_once_with(user_id)


@pytest.mark.asyncio
async def test_profile_parsing(http_client, user_id, create_xml_response):
    http_client.get.return_value = create_xml_response(BACKEND_FRIENDS_RESPONSE)

    assert PARSED_FRIEND_LIST_RESPONSE == await EABackendClient(http_client).get_friends()
    http_client.get.assert_called_once_with(f"https://friends.gs.ea.com/api/users/{user_id}/friends")
