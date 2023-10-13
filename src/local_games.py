import json
import re
import functools
import logging
import os
import time
import platform
import subprocess
import tempfile
import winreg

if platform.system() == "Windows":
    from ctypes import byref, sizeof, windll, create_unicode_buffer, FormatError, WinError
    from ctypes.wintypes import DWORD
    from typing import Optional, Set, List
else:
    import psutil

from enum import Enum, auto, Flag
from typing import Iterator, Tuple

from galaxy.api.errors import FailedParsingManifest
from galaxy.api.types import LocalGame, LocalGameState


logger = logging.getLogger(__name__)


class _State(Enum):
    kInvalid = auto()
    kError = auto()
    kPaused = auto()
    kPausing = auto()
    kCanceling = auto()
    kReadyToStart = auto()
    kInitializing = auto()
    kResuming = auto()
    kPreTransfer = auto()
    kPendingInstallInfo = auto()
    kPendingEulaLangSelection = auto()
    kPendingEula = auto()
    kEnqueued = auto()
    kTransferring = auto()
    kPendingDiscChange = auto()
    kPostTransfer = auto()
    kMounting = auto()
    kUnmounting = auto()
    kUnpacking = auto()
    kDecrypting = auto()
    kReadyToInstall = auto()
    kPreInstall = auto()
    kInstalling = auto()  # This status is used for games which are installing or updating
    kPostInstall = auto()
    kFetchLicense = auto()
    kCompleted = auto()

    @classmethod
    def _missing_(cls, value):
        logging.warning('Unrecognized state: %s' % value)
        return cls.kInvalid


class OriginGameState(Flag):
    None_ = 0
    Installed = 1
    Playable = 2


def parse_map_crc_for_total_size(filepath) -> int:
    with open(filepath, 'r', encoding='utf-16-le') as f:
        content = f.read()
    pattern = r'size=(\d+)'
    sizes = re.findall(pattern, content)
    return functools.reduce(lambda a, b : a + int(b), sizes, 0)


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


def get_local_games_from_manifests():
    local_games = []

    # since the awakening of EA Desktop, the logic has changed concerning the verification of installed games.
    # manifests are no longer necessary in order to verify if a game is installed or not.
    running_processes = [exe for pid, exe in process_iter() if exe is not None]

    def is_game_running(game_folder_name):
        for exe in running_processes:
            if game_folder_name in exe:
                return True
        return False
    
    is_file = os.path.join(tempfile.gettempdir(), "is.json")

    if os.path.exists(is_file):
        if platform.system() == "Windows":
            is_decrypt_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "is_decryption_galaxy.py")
            python_path = os.path.join(get_python_path(), "python.exe")
            if not os.path.exists(python_path):
                python_path = "python.exe"
            if os.path.exists(is_decrypt_path):
                subprocess.check_output("Powershell -Command \"Start-Process \'" + python_path + "\' -ArgumentList \'" + is_decrypt_path + "\' -Verb RunAs\"", shell=True)
                time.sleep(10)
        
            installed_games = [json.loads(line) for line in open(is_file, 'r', encoding='utf-8')]
            logger.info(f"Opening manifest file {is_file} ...")
            for game in installed_games[0]['installInfos']:
                    logger.info(f"Found installed game: ", game['softwareId'])
                    if game['executablePath'] != "":
                        local_games.append(LocalGame(game['softwareId'], LocalGameState.Installed))
                    else:
                        local_games.append(LocalGame(game['softwareId'], LocalGameState.None_))
    else:
        logger.warning("%TEMP%\is.json file not found. Local games won't be checked. We strongly suggest to use the is_decryption_galaxy.py file to generate the decrypted IS file.")


    for local_game in local_games:
        if is_game_running(local_game.game_id):
            local_game.local_game_state = LocalGameState.Running

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
            logger.warning("Failed to parse manifest. Most likely there's no presence of the IS JSON file.")
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
