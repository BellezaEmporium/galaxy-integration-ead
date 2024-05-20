import json
import logging
import os
import platform
import tempfile
import winreg

if platform.system() == "Windows":
    from ctypes import byref, sizeof, windll, create_unicode_buffer, FormatError, WinError
    from ctypes.wintypes import DWORD
    from typing import Optional, Set, List
else:
    import psutil

from enum import Flag
from typing import Iterator, Tuple

from galaxy.api.errors import FailedParsingManifest
from galaxy.api.types import LocalGame, LocalGameState


logger = logging.getLogger(__name__)


class EAGameState(Flag):
    None_ = 0
    Installed = 1
    Playable = 2

def parse_total_size(filepath) -> int:
    # get folder size
    total_size = 0
    if filepath is not None:
        for dirpath, _, filenames in os.walk(filepath):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
    return total_size

if platform.system() == "Windows":
    def get_process_info(pid) -> Tuple[int, Optional[str]]:
        _MAX_PATH = 260
        _PROC_QUERY_LIMITED_INFORMATION = 0x1000
        _WIN32_PATH_FORMAT = 0x0000

        h_process = windll.kernel32.OpenProcess(_PROC_QUERY_LIMITED_INFORMATION, False, pid)
        if not h_process:
            return pid, None

        def get_process_file_name() -> Optional[str]:
            try:
                file_name_buffer = create_unicode_buffer(_MAX_PATH)
                file_name_len = DWORD(len(file_name_buffer))

                return file_name_buffer[:file_name_len.value] if windll.kernel32.QueryFullProcessImageNameW(
                    h_process, _WIN32_PATH_FORMAT, file_name_buffer, byref(file_name_len)
                ) else None

            finally:
                windll.kernel32.CloseHandle(h_process)

        return pid, get_process_file_name()


    def get_process_ids() -> Set[int]:
        _PROC_ID_T = DWORD
        list_size = 4096

        def try_get_info_list(list_size) -> Tuple[int, List[int]]:
            result_size = DWORD()
            proc_id_list = (_PROC_ID_T * list_size)()

            if not windll.psapi.EnumProcesses(byref(proc_id_list), sizeof(proc_id_list), byref(result_size)):
                raise WinError(descr="Failed to get process ID list: %s" % FormatError())

            size = int(result_size.value / sizeof(_PROC_ID_T()))
            return proc_id_list[:size]

        while True:
            proc_id_list = try_get_info_list(list_size)
            if len(proc_id_list) < list_size:
                return proc_id_list
            # if returned collection is not smaller than list size it indicates that some pids have not fitted
            list_size *= 2

        return set(proc_id_list)


    def process_iter() -> Iterator[Tuple[int, str]]:
        try:
            for pid in get_process_ids():
                yield get_process_info(pid)
        except OSError:
            logger.exception("Failed to iterate over the process list")
            pass

else:
    def process_iter() -> Iterator[Tuple[int, str]]:
        for pid in psutil.pids():
            try:
                yield pid, psutil.Process(pid=pid).as_dict(attrs=["exe"])["exe"]
            except psutil.NoSuchProcess:
                pass
            except StopIteration:
                raise
            except Exception:
                logger.exception("Failed to get information for PID=%s" % pid)


def get_install_location(base_key, regkey_path, part):
    try:
        with winreg.OpenKey(base_key, regkey_path) as key:
            install_location, _ = winreg.QueryValueEx(key, part)
            return install_location
    except FileNotFoundError:
        logger.warning(f"Registry key not found: {base_key}\\{regkey_path}")
        return None
    except Exception as e:
        logger.error(f"Error accessing registry key {base_key}\\{regkey_path}: {str(e)}")
        return None


def get_local_games_from_manifests():
    local_games = []

    running_processes = set(exe for _, exe in process_iter() if exe)

    def is_game_running(game_exe):
        return any(game_exe in exe for exe in running_processes)

    offer_cache_path = os.path.join(tempfile.gettempdir(), "offer_cache.json")
    if not os.path.exists(offer_cache_path):
        logger.error(f"Offer cache file does not exist: {offer_cache_path}")
        return local_games


    with open(offer_cache_path, "r") as f:
        offer_cache = json.load(f)

    for offer_id, game_data in offer_cache.items():
        state = LocalGameState.None_
        if "executePathOverride" in game_data and game_data["executePathOverride"] != "":
            regkey_full = game_data["executePathOverride"]
            regkey_path, part = regkey_full.split(']')
            game_name = regkey_full.split(']')[1].split('\\')[-1]
            regkey_path = regkey_path.replace('[', '')
            regkey_parts = regkey_path.split("\\")
            if len(regkey_parts) < 2:
                logger.error(f"Invalid registry key format: {regkey_full}")
                continue

            hive = getattr(winreg, regkey_parts[0]) # Convert hive to integer
            regkey_path = "\\".join(regkey_parts[1:-1]) # Join the rest of the path excluding the last part
            part = regkey_parts[-1] # The last part of the path is the part you want to get the value of

            install_location = get_install_location(hive, regkey_path, part)

            if install_location:
                logger.info(f"Game file name: {game_name}")
                logger.debug(f"Install location found: {install_location}")
                # get last part of the registry now that we have the install location to trigger the last part of the registry
                if os.path.exists(install_location):
                    state = LocalGameState.Installed
                    if is_game_running(game_name):
                        state = LocalGameState.Installed | LocalGameState.Running
                local_games.append(LocalGame(offer_id, state))
            else:
                local_games.append(LocalGame(offer_id, state))
        else:
            local_games.append(LocalGame(offer_id, state))

    return local_games

def get_state_changes(old_list, new_list):
    old_dict = {x.game_id: x.local_game_state for x in old_list}
    new_dict = {x.game_id: x.local_game_state for x in new_list}
    result = []
    # removed games
    result.extend(LocalGame(game_id, LocalGameState.None_) for game_id in old_dict.keys() - new_dict.keys())
    # added games
    result.extend(local_game for local_game in new_list if local_game.game_id in new_dict.keys() - old_dict.keys())
    # state changed
    result.extend(
        LocalGame(game_id, new_dict[game_id])
        for game_id in new_dict.keys() & old_dict.keys()
        if new_dict[game_id] != old_dict[game_id]
    )
    return result


def get_python_path():
    platform_id = platform.system()
    python_path = ""
    if platform_id == "Windows":
        reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)

        keyname = winreg.OpenKey(reg, r'SOFTWARE\WOW6432Node\GOG.com\GalaxyClient\paths')
        for i in range(1024):
            try:
                valname = winreg.EnumKey(keyname, i)
                open_key = winreg.OpenKey(keyname, valname)
                python_path = winreg.QueryValueEx(open_key, "client")
            except EnvironmentError:
                break
    else:
        python_path = ""  # fallback for testing on another platform
        # raise NotImplementedError("Not implemented on {}".format(platform_id))

    return python_path


def get_local_content_path():
    platform_id = platform.system()
    if platform_id == "Windows":
        local_content_path = os.path.join(os.environ.get("ProgramData", os.environ.get("SystemDrive", "C:") + R"\ProgramData"), "EA Desktop", "InstallData")
    elif platform_id == "Darwin":
        local_content_path = os.path.join(os.sep, "Library", "Application Support", "EA Desktop", "InstallData")
    else:
        local_content_path = "."  # fallback for testing on another platform
        # raise NotImplementedError("Not implemented on {}".format(platform_id))

    return local_content_path


class LocalGames:
    def __init__(self):
        try:
            self._local_games = get_local_games_from_manifests()
        except FailedParsingManifest:
            logger.warning("Failed to parse manifest. Most likely there's no offer cache.")
            self._local_games = []

    @property
    def local_games(self):
        return self._local_games

    def update(self):
        '''
        returns list of changed games (added, removed, or changed)
        updated local_games property
        '''
        new_local_games = get_local_games_from_manifests()
        notify_list = get_state_changes(self._local_games, new_local_games)
        self._local_games = new_local_games

        return self._local_games, notify_list
