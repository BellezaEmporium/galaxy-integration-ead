import logging
import os
import platform
import winreg
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from enum import Flag
from functools import lru_cache
from typing import Final

if platform.system() == "Windows":
    from ctypes import byref, sizeof, windll, create_unicode_buffer, FormatError, WinError
    from ctypes.wintypes import DWORD
else:
    import psutil

logger = logging.getLogger(__name__)

from galaxy.api.types import LocalGame, LocalGameState


class EAGameState(Flag):
    None_ = 0
    Installed = 1
    Playable = 2


class RegistryManager:
    HIVE_MAPPING: Final[dict[str, int]] = {
        "HKEY_LOCAL_MACHINE": winreg.HKEY_LOCAL_MACHINE,
        "HKEY_CURRENT_USER": winreg.HKEY_CURRENT_USER,
        "HKEY_CLASSES_ROOT": winreg.HKEY_CLASSES_ROOT,
        "HKEY_USERS": winreg.HKEY_USERS,
        "HKEY_CURRENT_CONFIG": winreg.HKEY_CURRENT_CONFIG,
    }

    @staticmethod
    def parse_registry_path(registry_path: str) -> tuple[int, str, str] | None:
        """Parse a registry path like [HKEY_LOCAL_MACHINE\\SOFTWARE\\EA Games\\Game\\Install Dir]"""
        if not (registry_path.startswith("[") and "]" in registry_path):
            return None

        reg_key = registry_path[1 : registry_path.index("]")]
        components = reg_key.split("\\")

        if len(components) < 3:
            logger.error("Invalid registry key format: %s", registry_path)
            return None

        hive_name = components[0]
        if hive_name not in RegistryManager.HIVE_MAPPING:
            logger.error("Unknown registry hive: %s", hive_name)
            return None

        hive = RegistryManager.HIVE_MAPPING[hive_name]
        value_name = components[-1]
        key_path = "\\".join(components[1:-1])

        return hive, key_path, value_name

    @staticmethod
    def get_registry_value(hive: int, key_path: str, value_name: str) -> str | None:
        return _cached_reg_value(hive, key_path, value_name)


@lru_cache(maxsize=512)
def _cached_reg_value(hive: int, key_path: str, value_name: str) -> str | None:
    """Cached registry value lookup that:
    - tries 64-bit and 32-bit views
    - supports default values
    - expands REG_EXPAND_SZ and %VAR% variables
    """
    views = [winreg.KEY_WOW64_32KEY, winreg.KEY_WOW64_64KEY] if "WOW6432Node" in key_path else [winreg.KEY_WOW64_64KEY, winreg.KEY_WOW64_32KEY]

    for view in views:
        try:
            with winreg.OpenKeyEx(hive, key_path, 0, winreg.KEY_READ | view) as key:
                if value_name in ("", "@", "(Default)"):
                    try:
                        value, val_type = winreg.QueryValueEx(key, "")
                    except OSError:
                        value = winreg.QueryValue(hive, key_path)
                        val_type = winreg.REG_SZ
                else:
                    value, val_type = winreg.QueryValueEx(key, value_name)

                if isinstance(value, list):
                    value = value[0] if value else None

                if value is None:
                    return None

                if val_type == getattr(winreg, "REG_EXPAND_SZ", None) or (isinstance(value, str) and "%" in value):
                    try:
                        value = os.path.expandvars(value)
                    except Exception:
                        pass

                return str(value)
        except (FileNotFoundError, OSError):
            continue

    return None


class GamePathResolver:
    @staticmethod
    def find_executable_in_directory(directory: str) -> str | None:
        """Find the first .exe file in a directory."""
        if not directory or not os.path.isdir(directory):
            return None

        try:
            for entry in os.listdir(directory):
                if entry.lower().endswith(".exe"):
                    return os.path.join(directory, entry)
        except OSError:
            logger.debug("Cannot access directory: %s", directory)

        return None

    @staticmethod
    def resolve_registry_path(registry_path: str) -> str | None:
        parsed = RegistryManager.parse_registry_path(registry_path)
        if not parsed:
            return None
        hive, key_path, value_name = parsed
        return RegistryManager.get_registry_value(hive, key_path, value_name)


def parse_registry_expression(expr: str) -> tuple[int, str, str, str] | None:
    """Parse an expression like [HIVE\\Key\\Value]relative\\path and return
    (hive, key_path, value_name, tail_after_bracket).
    """
    if not (expr.startswith("[") and "]" in expr):
        return None
    bracket_end = expr.index("]")
    head = expr[: bracket_end + 1]
    tail = expr[bracket_end + 1 :].lstrip("\\/")
    parsed = RegistryManager.parse_registry_path(head)
    if not parsed:
        return None
    hive, key_path, value_name = parsed
    return hive, key_path, value_name, tail


def resolve_registry_expression(expr: str, base_fallback: str | None = None) -> str | None:
    """Resolve a [HIVE\\...\\Value]tail expression to a full absolute path."""
    parsed = parse_registry_expression(expr)
    if not parsed:
        return base_fallback
    hive, key_path, value_name, tail = parsed
    base = RegistryManager.get_registry_value(hive, key_path, value_name) or base_fallback
    if not base:
        return None
    full = os.path.join(base, tail) if tail else base
    try:
        return os.path.expandvars(full)
    except Exception:
        return full


def parse_total_size(filepath: str | None) -> int:
    total_size = 0
    if filepath is not None and os.path.isfile(filepath):
        base_path = os.path.dirname(os.path.dirname(filepath))
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    rel_path = line.strip().strip("\"'")
                    if not rel_path:
                        continue
                    abs_path = os.path.join(base_path, os.path.normpath(rel_path))
                    if os.path.isfile(abs_path):
                        total_size += os.path.getsize(abs_path)
        except Exception as e:
            logger.warning("Error while reading %s: %s", filepath, e)
    return total_size


def get_state_changes(old_list: list[LocalGame], new_list: list[LocalGame]) -> list[LocalGame]:
    old_dict = {x.game_id: x.local_game_state for x in old_list}
    new_dict = {x.game_id: x.local_game_state for x in new_list}
    result: list[LocalGame] = []

    # removed
    result.extend(LocalGame(gid, LocalGameState.None_) for gid in old_dict.keys() - new_dict.keys())
    # added
    result.extend(lg for lg in new_list if lg.game_id in new_dict.keys() - old_dict.keys())
    # changed
    result.extend(
        LocalGame(gid, new_dict[gid])
        for gid in new_dict.keys() & old_dict.keys()
        if new_dict[gid] != old_dict[gid]
    )
    return result


def get_python_path() -> str:
    if platform.system() != "Windows":
        return ""

    reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
    keyname = winreg.OpenKey(reg, r"SOFTWARE\WOW6432Node\GOG.com\GalaxyClient\paths")

    for i in range(1024):
        try:
            valname = winreg.EnumKey(keyname, i)
            open_key = winreg.OpenKey(keyname, valname)
            return winreg.QueryValueEx(open_key, "client")[0]
        except EnvironmentError:
            break

    return ""


def get_local_content_path() -> str:
    platform_id = platform.system()
    if platform_id == "Windows":
        return os.path.join(os.environ.get("ProgramData", os.environ.get("SystemDrive", "C:") + r"\ProgramData"), "EA Desktop", "InstallData")
    if platform_id == "Darwin":
        return os.path.join(os.sep, "Library", "Application Support", "EA Desktop", "InstallData")
    return "."


if platform.system() == "Windows":
    def get_process_info(pid: int) -> tuple[int, str | None]:
        _MAX_PATH = 260
        _PROC_QUERY_LIMITED_INFORMATION = 0x1000
        _WIN32_PATH_FORMAT = 0x0000

        h_process = windll.kernel32.OpenProcess(_PROC_QUERY_LIMITED_INFORMATION, False, pid)
        if not h_process:
            return pid, None

        try:
            file_name_buffer = create_unicode_buffer(_MAX_PATH)
            file_name_len = DWORD(len(file_name_buffer))

            if windll.kernel32.QueryFullProcessImageNameW(h_process, _WIN32_PATH_FORMAT, file_name_buffer, byref(file_name_len)):
                return pid, file_name_buffer.value[: file_name_len.value]
            return pid, None
        finally:
            windll.kernel32.CloseHandle(h_process)


    def get_process_ids() -> set[int]:
        _PROC_ID_T = DWORD

        def try_get_info_list(list_size: int) -> list[int]:
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
            list_size *= 2


    def process_iter() -> Iterator[tuple[int, str | None]]:
        try:
            for pid in get_process_ids():
                yield get_process_info(pid)
        except OSError:
            logger.exception("Failed to iterate over the process list")

else:
    def process_iter() -> Iterator[tuple[int, str | None]]:
        for pid in psutil.pids():
            try:
                yield pid, psutil.Process(pid=pid).as_dict(attrs=["exe"])["exe"]
            except psutil.NoSuchProcess:
                pass
            except StopIteration:
                raise
            except Exception:
                logger.exception("Failed to get information for PID=%s", pid)


def update_local_games(self) -> list[LocalGame]:
    local_games: list[LocalGame] = []
    running_exes = {os.path.basename(exe).lower() for _, exe in process_iter() if exe}

    for offer_id, game_data in self._offer_id_cache.items():
        if not isinstance(game_data, dict) or "displayName" not in game_data:
            continue

        state = LocalGameState.None_
        install_path = None

        if path := game_data.get("installCheckOverride") or game_data.get("executePathOverride"):
            base_path = get_install_path_from_xml(game_data, path)
            install_path = GamePathResolver.find_executable_in_directory(base_path) if base_path else path

        if install_path and os.path.exists(install_path):
            state = LocalGameState.Installed
            if os.path.basename(install_path).lower() in running_exes:
                state |= LocalGameState.Running

        local_games.append(LocalGame(offer_id, state))

    return local_games


def local_game_status(self) -> list[LocalGame]:
    new_local_games = update_local_games(self)
    notify_list = get_state_changes(self._local_games, new_local_games)
    self._local_games = new_local_games
    return notify_list


def get_install_path_from_xml(game_data: dict, xml_path: str) -> str | None:
    return ManifestResolver(game_data).resolve_install_path(xml_path)


class ManifestResolver:
    def __init__(self, game_data: dict):
        self.game_data = game_data
        self.display_name = (
            game_data.get("displayName", "").lower()
            or game_data.get("i18n", {}).get("displayName", "").lower()
        )
        self.is_trial_game = "demo" in self.display_name or "trial" in self.display_name
        self.is_64bit = platform.machine().endswith("64")

    def resolve_install_path(self, xml_path: str) -> str | None:
        try:
            if xml_path.startswith("[") and "]" in xml_path:
                return self._resolve_registry_based_path(xml_path)
            if os.path.exists(xml_path):
                return self._resolve_file_based_path(xml_path)
            return self._resolve_directory_based_path(xml_path)
        except Exception as e:
            logger.info("Error resolving install path: %s", e)
            return None

    def _resolve_registry_based_path(self, xml_path: str) -> str | None:
        reg_path, xml_relative_path = xml_path.split("]", 1)
        base_install_location = GamePathResolver.resolve_registry_path(reg_path + "]")

        if not base_install_location:
            return None

        full_xml_path = os.path.join(base_install_location, "__Installer", "installerdata.xml")
        if not os.path.exists(full_xml_path) and xml_relative_path:
            full_xml_path = os.path.join(base_install_location, xml_relative_path)

        return self._parse_manifest_xml(full_xml_path, base_install_location)

    def _resolve_file_based_path(self, xml_path: str) -> str | None:
        return self._parse_manifest_xml(xml_path, os.path.dirname(xml_path))

    def _resolve_directory_based_path(self, xml_path: str) -> str | None:
        if os.path.isdir(xml_path):
            installer_xml = os.path.join(xml_path, "__Installer", "installerdata.xml")
            if os.path.exists(installer_xml):
                return self._parse_manifest_xml(installer_xml, xml_path)
            return xml_path
        return None

    def _parse_manifest_xml(self, xml_path: str, base_install_location: str) -> str | None:
        if not os.path.exists(xml_path):
            logger.debug("XML file not found: %s", xml_path)
            return base_install_location

        try:
            root = ET.parse(xml_path).getroot()

            if root.tag != "DiPManifest":
                legacy_path = self._extract_legacy_executable(root, base_install_location)
                return legacy_path or self._handle_legacy_game()

            return self._find_best_launcher(root, base_install_location)

        except ET.ParseError as e:
            logger.error("Failed to parse XML file %s: %s", xml_path, e)
            return base_install_location
        except Exception as e:
            logger.error("Error parsing installerdata.xml: %s", e)
            return base_install_location

    def _handle_legacy_game(self) -> str | None:
        logger.warning("Potentially old game %s, not a DiPManifest", self.display_name)

        install_location = self.game_data.get("installCheckOverride") or self.game_data.get("executePathOverride")

        if install_location and install_location.endswith(".exe") and install_location.startswith("[") and "]" in install_location:
            return GamePathResolver.resolve_registry_path(install_location[: install_location.index("]") + 1])

        return None

    def _extract_legacy_executable(self, root: ET.Element, base_install_location: str) -> str | None:
        try:
            for node in root.findall(".//ignore"):
                if node is not None and node.text:
                    val = node.text.strip()
                    if val.lower().endswith(".exe"):
                        full = val if os.path.isabs(val) else os.path.join(base_install_location, val)
                        try:
                            full = os.path.expandvars(full)
                        except Exception:
                            pass
                        if os.path.exists(full):
                            logger.debug("Found legacy executable via <ignore>: %s", full)
                            return full
        except Exception as e:
            logger.debug("Legacy executable extraction failed: %s", e)
        return None

    def _find_best_launcher(self, root: ET.Element, base_install_location: str) -> str | None:
        launchers = root.findall(".//runtime/launcher")

        if not launchers:
            logger.debug("No launcher elements found in manifest")
            return base_install_location

        selected = self._select_launcher(launchers)
        return self._extract_launcher_path(selected, base_install_location) if selected is not None else base_install_location

    def _select_launcher(self, launchers: list[ET.Element]) -> ET.Element | None:
        selected = None
        fallback = None

        for launcher in launchers:
            trial_attr = launcher.get("trial", "0")
            is_trial_launcher = trial_attr == "1"
            requires_64bit = launcher.get("requires64BitOS", "0") == "1"

            names = [n.text.strip().lower() for n in launcher.findall("name") if n is not None and n.text]
            if any("trial" in n or "demo" in n for n in names):
                is_trial_launcher = True

            trial_match = (self.is_trial_game and is_trial_launcher) or (not self.is_trial_game and not is_trial_launcher)
            arch_match = (self.is_64bit and requires_64bit) or not requires_64bit

            if trial_match and arch_match:
                selected = launcher
                break
            if trial_match:
                fallback = launcher

        if selected is None:
            if fallback is not None:
                selected = fallback
                logger.debug("Using fallback launcher due to architecture mismatch")
            elif not self.is_trial_game:
                for launcher in launchers:
                    if launcher.get("trial", "0") != "1":
                        selected = launcher
                        break

            if selected is None and launchers:
                selected = launchers[0]
                logger.debug("No specific launcher match found, using first available launcher")

        return selected

    def _extract_launcher_path(self, launcher: ET.Element, base_install_location: str) -> str | None:
        file_path_element = launcher.find("filePath")
        launcher_path = None

        if file_path_element is not None and file_path_element.text:
            launcher_path = file_path_element.text.strip()
        elif launcher.text:
            launcher_path = launcher.text.strip()

        if not launcher_path:
            return base_install_location

        if launcher_path.startswith("[") and "]" in launcher_path:
            return self._resolve_launcher_registry_path(launcher_path, base_install_location)

        full_launcher_path = launcher_path if os.path.isabs(launcher_path) else os.path.join(base_install_location, launcher_path)

        if os.path.exists(full_launcher_path):
            logger.debug("Found launcher at: %s", full_launcher_path)
            return full_launcher_path

        logger.debug("Launcher path does not exist: %s", full_launcher_path)
        return base_install_location

    def _resolve_launcher_registry_path(self, launcher_path: str, base_install_location: str) -> str | None:
        try:
            full = resolve_registry_expression(launcher_path, base_fallback=base_install_location)
            return full if full and os.path.exists(full) else base_install_location
        except Exception as e:
            logger.error("Failed to parse registry-based launcher path: %s", e)
            return base_install_location


def parse_installerdata_xml(game_data: dict, xml_path: str, base_install_location: str) -> str | None:
    """Legacy function - use ManifestResolver instead."""
    return ManifestResolver(game_data)._parse_manifest_xml(xml_path, base_install_location)