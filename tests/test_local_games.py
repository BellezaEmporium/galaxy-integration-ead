import asyncio

import pytest
from galaxy.api.types import LocalGame, LocalGameState

from lgames_manifests import get_state_changes


def _sorted_games(games):
    return sorted(games, key=lambda x: x.game_id)


@pytest.fixture()
def local_games_object(process_iter_mock, tmpdir):
    return LocalGames(tmpdir)


def test_non_existent_dir(process_iter_mock):
    local_games_object = LocalGames("some non-existent directory")
    games, changed = local_games_object.update()
    assert games == []
    assert changed == []


def test_empty(local_games_object):
    games, changed = local_games_object.update()
    assert games == []
    assert changed == []


def test_bad_manifest_format(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName").join("gameid.mfst")
    mfst_file.write("?currentstate=kInstalling&previousstate=kPostTransfer")
    local_games, _ = local_games_object.update()
    assert local_games == []


@pytest.mark.parametrize('invalid_manifest_content', [
    "?currentstate=kInstalling&previousstate=kPostTransfer",
    "?currentstate=UNKNOWN_STATE&previousstate=kPostTransfer&id=some_id",
    "?currentstate=kInstalling&previousstate=UNKNOWN_STATE&id=some_id",
    "?currentstate=UNKNOWN_STATE&previousstate=UNKNOWN_STATE&id=some_id",
    "?currentstate=&previousstate=UNKNOWN_STATE&id=some_id",
    "?currentstate=UNKNOWN_STATE&previousstate=&id=some_id",
    "?currentstate=&previousstate=&id=some_id",
    "INVALID_CONTENT",
    b'\0',
    "",
])
def test_valid_and_invalid_manifest_format(local_games_object, tmpdir, invalid_manifest_content):
    """Invalid manifest file shouldn't break parsing rest of manifest files."""

    TEST_GAME_ID = 'test_id'
    game_dir = tmpdir.mkdir("GameName")
    valid_mfst_file, invalid_mfst_file = game_dir.join("valid.mfst"), game_dir.join("invalid.mfst")
    invalid_mfst_file.write(invalid_manifest_content)
    valid_mfst_file.write("?currentstate=kInstalling&previousstate=kPostTransfer&id=" + TEST_GAME_ID)

    local_games, _ = local_games_object.update()

    assert len(local_games) == 1
    assert local_games[0].game_id == TEST_GAME_ID



@pytest.mark.parametrize("game_id_raw, game_id_expected", [
    pytest.param("OFB-EAST%3a48217", "OFB-EAST:48217", id="origin offer id"),
    pytest.param("OFB-EAST%3a48217%40epic", "OFB-EAST:48217@epic", id="id with external type"),
])
def test_installing(local_games_object, tmpdir, game_id_raw, game_id_expected):
    mfst_file = tmpdir.mkdir("GameName").join("gameid.mfst")
    mfst_file.write(f"?currentstate=kInstalling&id={game_id_raw}&previousstate=kPostTransfer&ddinitialdownload=1")

    expected = [LocalGame(game_id_expected, LocalGameState.None_)]

    games, changed = local_games_object.update()
    assert games == expected
    assert changed == expected


def test_installing_dynamic_download(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName").join("gameid.mfst")
    mfst_file.write(
        "?currentstate=kTransferring&ddinitialdownload=1&id=Origin.OFR.50.0002694&ddinstallalreadycompleted=0&previousstate=kPendingEula&repairstate=repairing")

    expected = [LocalGame("Origin.OFR.50.0002694", LocalGameState.None_)]

    games, changed = local_games_object.update()
    assert games == expected
    assert changed == expected


def test_installed_dynamic_download(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName").join("gameid.mfst")
    mfst_file.write(
        "?currentstate=kTransferring&ddinitialdownload=1&id=Origin.OFR.50.0002694&ddinstallalreadycompleted=1&previousstate=kPendingEula&repairstate=")

    expected = [LocalGame("Origin.OFR.50.0002694", LocalGameState.Installed)]

    games, changed = local_games_object.update()
    assert games == expected
    assert changed == expected


def test_two_games(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName1").join("gameid.mfst")
    mfst_file.write("?currentstate=kReadyToStart&id=OFB-EAST%3a48217&previousstate=kCompleted")
    mfst_file = tmpdir.mkdir("GameName2").join("gameid.mfst")
    mfst_file.write("?currentstate=kReadyToStart&id=DR%3a119971300&previousstate=kCompleted")

    expected = [
        LocalGame("OFB-EAST:48217", LocalGameState.Installed),
        LocalGame("DR:119971300", LocalGameState.Installed)
    ]

    games, changed = local_games_object.update()
    assert _sorted_games(games) == _sorted_games(expected)
    assert _sorted_games(changed) == _sorted_games(expected)


def test_notify_removed(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName1").join("gameid.mfst")
    mfst_file.write("?currentstate=kReadyToStart&id=OFB-EAST%3a48217&previousstate=kCompleted")

    local_games_object.update()
    mfst_file.remove()
    games, changed = local_games_object.update()
    assert games == []
    assert changed == [LocalGame("OFB-EAST:48217", LocalGameState.None_)]


def test_notify_changed(local_games_object, tmpdir):
    mfst_file = tmpdir.mkdir("GameName1").join("gameid.mfst")
    mfst_file.write("?currentstate=kInstalling&id=OFB-EAST%3a48217&previousstate=kPostTransfer&ddinitialdownload=1")

    local_games_object.update()
    mfst_file.write("?currentstate=kReadyToStart&id=OFB-EAST%3a48217&previousstate=kCompleted")
    games, changed = local_games_object.update()
    assert games == [LocalGame("OFB-EAST:48217", LocalGameState.Installed)]
    assert changed == [LocalGame("OFB-EAST:48217", LocalGameState.Installed)]


def test_get_state_changes_added():
    old = []
    new = [LocalGame("1", LocalGameState.Installed)]
    result = [LocalGame("1", LocalGameState.Installed)]
    assert get_state_changes(old, new) == result


def test_get_state_changes_removed():
    old = [LocalGame("1", LocalGameState.Installed)]
    new = []
    result = [LocalGame("1", LocalGameState.None_)]
    assert get_state_changes(old, new) == result


def test_get_state_changes_changed():
    old = [LocalGame("1", LocalGameState.Installed)]
    new = [LocalGame("1", LocalGameState.Running)]
    result = [LocalGame("1", LocalGameState.Running)]
    assert get_state_changes(old, new) == result


def test_get_state_changes_unchanged():
    old = [LocalGame("1", LocalGameState.Installed)]
    new = [LocalGame("1", LocalGameState.Installed)]
    result = []
    assert get_state_changes(old, new) == result


def test_plugin_empty_dir(plugin):
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(plugin.get_local_games())
    assert result == []


def test_plugin_game_installed(tmpdir, plugin):
    mfst_file = tmpdir.mkdir("GameName1").join("gameid.mfst")
    mfst_file.write("?currentstate=kReadyToStart&id=OFB-EAST%3a48217&previousstate=kCompleted")

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(plugin.get_local_games())
    assert result == [LocalGame("OFB-EAST:48217", LocalGameState.Installed)]


@pytest.mark.asyncio
@pytest.mark.parametrize("proc_name, state", [
    (
        r"C:\Program Files (x86)\Origin Games\FIFA 12\fifa.exe",
        LocalGameState.Installed | LocalGameState.Running
    ),
    (
        None,
        LocalGameState.Installed
    ),
])
async def test_plugin_game_running(proc_name, state, tmpdir, plugin, process_iter_mock):
    mfst_file = tmpdir.mkdir("GameName1").join("gameid.mfst")
    mfst_file.write(
        r"?currentstate=kReadyToStart"
        r"&id=OFB-EAST:48217"
        r"&previousstate=kCompleted"
        r"&dipinstallpath=C:\Program Files (x86)\Origin Games\FIFA 12"
    )

    process_iter_mock.side_effect = [[(2077, proc_name)]]
    assert [LocalGame("OFB-EAST:48217", state)] == await plugin.get_local_games()
