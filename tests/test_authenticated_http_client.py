from unittest.mock import patch, MagicMock, ANY

import pytest
from galaxy.api.errors import AccessDenied, BackendNotAvailable
from galaxy.unittest.mock import AsyncMock

from http_client import AuthenticatedHttpClient



@pytest.fixture()
async def http_client():
    client = AuthenticatedHttpClient()
    yield client
    await client.close()

@pytest.fixture()
def http_request():
    with patch("backend.HttpClient.request", new_callable=AsyncMock) as http_request_:
        yield http_request_

@pytest.mark.asyncio
async def test_not_authenticated(http_client):
    with pytest.raises(AccessDenied):
        await http_client.get("http://test.com")

@pytest.mark.asyncio
async def test_authenticate(http_request, http_client, create_json_response):
    access_token = "token"
    http_request.return_value = create_json_response({"access_token": access_token})
    cookies = {
        "cookie": "value"
    }
    await http_client.authenticate(cookies)
    http_request.assert_called_once_with("GET", "https://accounts.ea.com/connect/auth", params=ANY)

    http_request.reset_mock()
    http_request.return_value = create_json_response({"e": "f"})

    url = "http://test.com"
    headers = {
        "a": "b"
    }
    params = {
        "c": "d"
    }
    await http_client.get(url, params=params, headers=headers)

    headers["Authorization"] = "Bearer {}".format(access_token)
    headers["AuthToken"] = access_token
    headers["X-AuthToken"] = access_token
    http_request.assert_called_once_with("GET", url, params=params, headers=headers)

@pytest.mark.asyncio
async def test_refresh_access_token_success(http_request, http_client, create_json_response):
    http_request.side_effect = [
        create_json_response({"access_token": "token"})
    ]
    await http_client.authenticate({})
    assert http_request.call_count == 1

    http_request.reset_mock()
    http_request.side_effect = [
        AccessDenied(),
        create_json_response({"access_token": "new_token"}),
        create_json_response({})
    ]
    await http_client.get("http://test.com")
    assert http_request.call_count == 3
    headers = http_request.call_args_list[2][1]["headers"]
    assert headers.get("AuthToken") == "new_token"

@pytest.mark.asyncio
async def test_refresh_token_failure(http_request, http_client, create_json_response):
    auth_lost = MagicMock()
    http_client.set_auth_lost_callback(auth_lost)
    http_request.return_value = create_json_response({"access_token": "token"})
    await http_client.authenticate({})
    http_request.assert_called_once()
    http_request.reset_mock()

    http_request.side_effect = [
        AccessDenied(),
        AccessDenied()
    ]
    with pytest.raises(AccessDenied):
        await http_client.get("http://test.com")

    assert http_request.call_count == 2

    auth_lost.assert_called_once_with()

@pytest.mark.asyncio
async def test_refresh_token_independent_failure(http_request, http_client, create_json_response):
    auth_lost = MagicMock()
    http_client.set_auth_lost_callback(auth_lost)
    http_request.return_value = create_json_response({"access_token": "token"})
    await http_client.authenticate({})
    http_request.assert_called_once()
    http_request.reset_mock()

    http_request.side_effect = [
        AccessDenied(),
        BackendNotAvailable()
    ]
    with pytest.raises(BackendNotAvailable):
        await http_client.get("http://test.com")

    assert http_request.call_count == 2
    auth_lost.assert_not_called()