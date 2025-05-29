import asyncio
from http.cookies import Morsel
from unittest.mock import patch

from galaxy.api.types import Authentication, NextStep

from plugin import JS

AUTH_PARAMS = {
    "window_title": "Login to EA Desktop",
    "window_width": 495,
    "window_height": 850,
    "start_uri": "https://accounts.ea.com/connect/auth"
                "?response_type=code&client_id=JUNO_PC_CLIENT&display=junoClient/login"
                "&redirect_uri=qrc:///html/login_successful.html"
                "&locale=en_US&pc_sign=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmVhLmNvbSIsInN1YiI6IjE0MTQwMDAwMDAwMDAwMDAwMCIsImF1ZCI6IkpVTk9fUENfQ0xJRU5UIiwiZXhwIjoxNjg4MjYyMDY2LCJpYXQiOjE2ODgyNTk0NjYsIm5iZiI6MTY4ODI1OTQ2NiwianRpIjoiZTAyYjA3MzAtZDYxNy00YjQzLTg3NzItZDIzYjA5Mjc0N2E3Iiwic2NvcGUiOiJwcm9maWxlIG9mZmxpbmVzIHByb2ZpbGU6cmVhZCBwcm9maWxlOnVwbG9hZCBwcm9maWxlOmRvd25sb2FkIHByb2ZpbGU6dXBsb2FkIGFjY291bnRzOmFjY291bnRzOmFjY291bnRzOnJlYWQgYWNjZXNzX3Rva2VuIGFjY291bnRzOnJlYWQgYWNjZXNzX3Rva2VuOnVwbG9hZCBhY2Nlc3NfdG9rZW46dXBsb2FkIGFjY291bnRzOnVwbG9hZCBhY2Nlc3NfdG9rZW46ZG93bmxvYWQifQ",
    "end_uri_regex": "qrc:/html/login_successful.html.*"
}


def test_no_stored_credentials(plugin, http_client, backend_client):
    loop = asyncio.get_event_loop()
    
    cookies = {
        "cookie": "value"
    }
    user_id = "13"
    persona_id = "19"
    user_name = "Jan"

    http_client.authenticate.return_value = None
    backend_client.get_identity.return_value = user_id, persona_id, user_name

    with patch.object(plugin, "store_credentials") as store_credentials:
        result = loop.run_until_complete(plugin.authenticate())
        assert result == NextStep("web_session", AUTH_PARAMS, js=JS)

        credentials = {
            "cookies": cookies,
        }

        result = loop.run_until_complete(plugin.pass_login_credentials(
            "whatever step",
            "whatever credentials",
            [{"name": key, "value": value} for key, value in cookies.items()]
        ))
        assert result == Authentication(user_id, user_name)
        store_credentials.assert_called_with(credentials)

    http_client.authenticate.assert_called_with(cookies)
    backend_client.get_identity.assert_called_with()


def test_stored_credentials(plugin, http_client, backend_client):
    loop = asyncio.get_event_loop()

    user_id = "13"
    persona_id = "19"
    user_name = "Jan"

    cookies = {
        "cookie": "value"
    }
    credentials = {
        "cookies": cookies
    }

    http_client.authenticate.return_value = None
    backend_client.get_identity.return_value = user_id, persona_id, user_name

    with patch.object(plugin, "store_credentials") as store_credentials:
        result = loop.run_until_complete(plugin.authenticate(credentials))
        assert result == Authentication(user_id, user_name)
        store_credentials.assert_not_called()

    http_client.authenticate.assert_called_with(cookies)
    backend_client.get_identity.assert_called_with()


def test_updated_cookies(plugin, http_client, backend_client):
    loop = asyncio.get_event_loop()

    user_id = "13"
    persona_id = "19"
    user_name = "Jan"

    cookies = {
        "cookie": "value"
    }
    credentials = {
        "cookies": cookies
    }

    new_cookies = {
        "new_cookie": "new_value"
    }
    morsel = Morsel()
    morsel.set("new_cookie", "new_value", "new_value")
    new_credentials = {
        "cookies": new_cookies
    }

    http_client.authenticate.return_value = None

    def get_identity():
        callback = http_client.set_cookies_updated_callback.call_args[0][0]
        callback([morsel])
        return user_id, persona_id, user_name

    backend_client.get_identity.side_effect = get_identity

    with patch.object(plugin, "store_credentials") as store_credentials:
        result = loop.run_until_complete(plugin.authenticate(credentials))
        assert result == Authentication(user_id, user_name)
        store_credentials.assert_called_with(new_credentials)

    http_client.authenticate.assert_called_with(cookies)
    backend_client.get_identity.assert_called_with()


def test_auth_lost(authenticated_plugin, http_client):
    callback = http_client.set_auth_lost_callback.call_args[0][0]
    with patch.object(authenticated_plugin, "lost_authentication") as lost_authentication:
        callback()
        lost_authentication.assert_called_with()
