from asyncio.log import logger
from enum import Flag
import os
import platform
if platform.system() == "Windows":
    from ctypes import byref, sizeof, windll, create_unicode_buffer, FormatError, WinError
    from ctypes.wintypes import DWORD
    from typing import Optional, Set, List
else:
    import psutil
from typing import Iterator, List, Optional, Set, Tuple
import winreg

from galaxy.api.types import LocalGame, LocalGameState

# Helpers for the Local Games data

class EAGameState(Flag):
    None_ = 0
    Installed = 1
    Playable = 2

def parse_total_size(filepath) -> int:
    total_size = 0
    if filepath is not None and os.path.isfile(filepath):
        base_path = os.path.dirname(os.path.dirname(filepath))
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    rel_path = line.strip().strip('"\'')
                    if not rel_path:
                        continue
                    rel_path = os.path.normpath(rel_path)
                    abs_path = os.path.join(base_path, rel_path)
                    if os.path.isfile(abs_path):
                        total_size += os.path.getsize(abs_path)
                    else:
                        continue
        except Exception as e:
            logger.warning(f"Error while reading {filepath}: {e}")
    return total_size


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

                return file_name_buffer.value[:file_name_len.value] if windll.kernel32.QueryFullProcessImageNameW(
                    h_process, _WIN32_PATH_FORMAT, file_name_buffer, byref(file_name_len)
                ) else None

            finally:
                windll.kernel32.CloseHandle(h_process)

        return pid, get_process_file_name()


    def get_process_ids() -> Set[int]:
        _PROC_ID_T = DWORD

        def try_get_info_list(list_size) -> List[int]:
            result_size = DWORD()
            proc_id_list = (_PROC_ID_T * list_size)()

            if not windll.psapi.EnumProcesses(byref(proc_id_list), sizeof(proc_id_list), byref(result_size)):
                raise WinError(descr="Failed to get process ID list: %s" % FormatError())

            size = int(result_size.value / sizeof(_PROC_ID_T()))
            return proc_id_list[:size]

        list_size = 4096
        while True:
            proc_id_list = try_get_info_list(list_size)
            if len(proc_id_list) < list_size:
                return set(proc_id_list)
            # if returned collection is not smaller than list size it indicates that some pids have not fitted
            list_size *= 2


    def process_iter() -> Iterator[Tuple[int, Optional[str]]]:
        try:
            for pid in get_process_ids():
                yield get_process_info(pid)
        except OSError:
            logger.exception("Failed to iterate over the process list")
            pass

else:
    def process_iter() -> Iterator[Tuple[int, Optional[str]]]:
        for pid in psutil.pids():
            try:
                yield pid, psutil.Process(pid=pid).as_dict(attrs=["exe"])["exe"]
            except psutil.NoSuchProcess:
                pass
            except StopIteration:
                raise
            except Exception:
                logger.exception("Failed to get information for PID=%s" % pid)


def get_install_location(base_key, regkey_path, part) -> Optional[str]:
    try:
        with winreg.OpenKey(base_key, regkey_path) as key:
            install_location, _ = winreg.QueryValueEx(key, part)
            # If the registry value is a list, take the first element
            if isinstance(install_location, list):
                if not install_location:  # If list is empty
                    return None
                install_location = install_location[0]
            # Handle None case
            if install_location is None:
                return None
            # Ensure we always return a string
            return str(install_location)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error accessing registry key {base_key}\\{regkey_path}: {str(e)}")
        return None