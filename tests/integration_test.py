import json
import os
import platform
import socket
import subprocess

import pytest

TIMEOUT = 5


class TCPServer:
    def __init__(self, bind_interface="0.0.0.0", bind_port=0):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((bind_interface, bind_port))
        self.port = self._sock.getsockname()[1]
        self._sock.listen(1)

    def accept_connection(self, timeout):
        '''Returns connected socket'''
        self._sock.settimeout(timeout)
        return self._sock.accept()[0]


@pytest.mark.integration
def test_integration():
    with open(os.path.join("output", "manifest.json"), "r") as file_:
        manifest = json.load(file_)

    plugin_path = os.path.join("output", manifest["script"])

    request = {
        "id": "3",
        "jsonrpc": "2.0",
        "method": "get_capabilities"
    }
    token = "token"
    server = TCPServer()
    result = subprocess.Popen(
        ["python", plugin_path, token, str(server.port), "plugin.log"]
    )

    plugin_socket = server.accept_connection(TIMEOUT)
    plugin_socket.settimeout(TIMEOUT)
    plugin_socket.sendall((json.dumps(request, default=str) + "\n").encode("utf-8"))
    response = json.loads(plugin_socket.recv(4096))
    response["result"]["features"] = set(response["result"]["features"])
    print(response)
    expected_response = {
        "id": "3",
        "jsonrpc": "2.0",
        "result": {
            "platform_name": "ea",
            "token": token,
            "features": {
                "ImportOwnedGames",
                "ImportAchievements",
                "ImportInstalledGames",
                "ImportSubscriptions",
                "ImportSubscriptionGames",
                "ImportGameLibrarySettings",
                "LaunchGame",
                "InstallGame",
                "ShutdownPlatformClient",
                "ImportFriends",
                "ImportGameTime",
                "ImportLocalSize"
            }
        }
    }
    if platform.system().lower() == "windows":
        expected_response["result"]["features"].add("UninstallGame")

    assert response == expected_response, "Response differs from expected"

    plugin_socket.close()
    result.wait(TIMEOUT)
    assert result.returncode == 0
