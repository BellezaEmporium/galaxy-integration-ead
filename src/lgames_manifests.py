import logging
import os
import platform
import winreg
import xml.etree.ElementTree as ET
from enum import Flag
from typing import Iterator, List, Optional, Set, Tuple
from functools import lru_cache
from urllib.parse import unquote, parse_qs

if platform.system() == "Windows":
    from ctypes import byref, sizeof, windll, create_unicode_buffer, FormatError, WinError
    from ctypes.wintypes import DWORD
else:
    import psutil

logger = logging.getLogger(__name__)

from galaxy.api.types import LocalGame, LocalGameState

# Helpers for the Local Games data

class EAGameState(Flag):
    None_ = 0
    Installed = 1
    Playable = 2


class RegistryManager:
    """Manages Windows registry operations for game detection."""
    
    HIVE_MAPPING = {
        'HKEY_LOCAL_MACHINE': winreg.HKEY_LOCAL_MACHINE,
        'HKEY_CURRENT_USER': winreg.HKEY_CURRENT_USER,
        'HKEY_CLASSES_ROOT': winreg.HKEY_CLASSES_ROOT,
        'HKEY_USERS': winreg.HKEY_USERS,
        'HKEY_CURRENT_CONFIG': winreg.HKEY_CURRENT_CONFIG
    }
    
    @staticmethod
    def parse_registry_path(registry_path: str) -> Optional[Tuple[int, str, str]]:
        """Parse a registry path like [HKEY_LOCAL_MACHINE\\SOFTWARE\\EA Games\\Game\\Install Dir]"""
        if not (registry_path.startswith('[') and ']' in registry_path):
            return None
            
        reg_key = registry_path[1:registry_path.index(']')]
        components = reg_key.split('\\')
        
        if len(components) < 3:
            logger.error(f"Invalid registry key format: {registry_path}")
            return None
            
        hive_name = components[0]
        if hive_name not in RegistryManager.HIVE_MAPPING:
            logger.error(f"Unknown registry hive: {hive_name}")
            return None
            
        hive = RegistryManager.HIVE_MAPPING[hive_name]
        value_name = components[-1]
        key_path = "\\".join(components[1:-1])
        
        return hive, key_path, value_name
    
    @staticmethod
    def get_registry_value(hive: int, key_path: str, value_name: str) -> Optional[str]:
        """Get a value from the Windows registry with caching, view fallbacks and env expansion."""
        return _cached_reg_value(hive, key_path, value_name)


@lru_cache(maxsize=512)
def _cached_reg_value(hive: int, key_path: str, value_name: str) -> Optional[str]:
    """Cached registry value lookup that:
    - tries 64-bit and 32-bit views
    - supports default values
    - expands REG_EXPAND_SZ and %VAR% variables
    """
    try:
        access = winreg.KEY_READ
        # Prefer view based on path hint, else try both
        views: List[int] = []
        if 'WOW6432Node' in key_path:
            views = [winreg.KEY_WOW64_32KEY, winreg.KEY_WOW64_64KEY]
        else:
            views = [winreg.KEY_WOW64_64KEY, winreg.KEY_WOW64_32KEY]

        last_error: Optional[Exception] = None

        for view in views:
            try:
                with winreg.OpenKeyEx(hive, key_path, 0, access | view) as key:
                    # Default value handling
                    if value_name in ("", "@", "(Default)"):
                        try:
                            # Try unnamed value via QueryValueEx with empty name
                            value, val_type = winreg.QueryValueEx(key, "")
                        except OSError:
                            # Fallback to QueryValue which reads the default value
                            value = winreg.QueryValue(hive, key_path)
                            val_type = winreg.REG_SZ
                    else:
                        value, val_type = winreg.QueryValueEx(key, value_name)

                    # Normalize multi-string values
                    if isinstance(value, list):
                        value = value[0] if value else None

                    if value is None:
                        return None

                    # Expand environment variables if necessary
                    if val_type == getattr(winreg, 'REG_EXPAND_SZ', None) or (
                        isinstance(value, str) and '%' in value
                    ):
                        try:
                            value = os.path.expandvars(value)
                        except Exception:
                            pass

                    return str(value)
            except FileNotFoundError as e:
                last_error = e
                continue
            except OSError as e:
                last_error = e
                continue

        # Not found in any view
        if last_error:
            return None
        return None
    except Exception as e:
        logger.error(f"Error accessing registry: {e}")
        return None


class GamePathResolver:
    """Resolves game installation paths from various sources."""
    
    @staticmethod
    def find_executable_in_directory(directory: str) -> Optional[str]:
        """Find the first .exe file in a directory."""
        if not directory or not os.path.isdir(directory):
            return None
            
        try:
            for entry in os.listdir(directory):
                if entry.lower().endswith('.exe'):
                    return os.path.join(directory, entry)
        except OSError:
            logger.debug(f"Cannot access directory: {directory}")
            
        return None
    
    @staticmethod
    def resolve_registry_path(registry_path: str) -> Optional[str]:
        """Resolve a registry-based path to an actual file path."""
        parsed = RegistryManager.parse_registry_path(registry_path)
        if not parsed:
            return None
            
        hive, key_path, value_name = parsed
        return RegistryManager.get_registry_value(hive, key_path, value_name)


def parse_registry_expression(expr: str) -> Optional[Tuple[int, str, str, str]]:
    """Parse an expression like [HIVE\\Key\\Value]relative\\path and return
    (hive, key_path, value_name, tail_after_bracket).
    """
    if not (expr.startswith('[') and ']' in expr):
        return None
    bracket_end = expr.index(']')
    head = expr[: bracket_end + 1]
    tail = expr[bracket_end + 1 :].lstrip('\\/')
    parsed = RegistryManager.parse_registry_path(head)
    if not parsed:
        return None
    hive, key_path, value_name = parsed
    return hive, key_path, value_name, tail


def resolve_registry_expression(expr: str, base_fallback: Optional[str] = None) -> Optional[str]:
    """Resolve a [HIVE\\...\\Value]tail expression to a full absolute path.
    - Reads the registry value (with 32/64-bit handling and env expansion)
    - Joins the optional tail path
    - Falls back to base_fallback + tail if registry lookup fails
    """
    parsed = parse_registry_expression(expr)
    if not parsed:
        return base_fallback
    hive, key_path, value_name, tail = parsed
    base = RegistryManager.get_registry_value(hive, key_path, value_name)
    if not base:
        base = base_fallback
    if not base:
        return None
    full = os.path.join(base, tail) if tail else base
    try:
        return os.path.expandvars(full)
    except Exception:
        return full

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

def update_local_games(self):
    local_games: List[LocalGame] = []
    running_exes = set(os.path.basename(exe).lower() for _, exe in process_iter() if exe)

    # 1) Detection based on offers cache (installCheckOverride/executePathOverride)
    for offer_id, game_data in self._offer_id_cache.items():
        if not isinstance(game_data, dict):
            continue
        if "displayName" not in game_data:
            continue

        state = LocalGameState.None_
        install_path = None

        path = game_data.get("installCheckOverride") or game_data.get("executePathOverride")
        if path:
            base_path = get_install_path_from_xml(game_data, path)
            if base_path:
                exe = GamePathResolver.find_executable_in_directory(base_path)
                install_path = exe or base_path
            else:
                install_path = path

        if install_path and os.path.exists(install_path):
            state = LocalGameState.Installed
            exe_name = os.path.basename(install_path).lower()
            if exe_name in running_exes:
                state |= LocalGameState.Running

        local_games.append(LocalGame(offer_id, state))

    return local_games

def local_game_status(self):
    '''
    returns list of changed games (added, removed, or changed)
    updated local_games property
    '''
    new_local_games = update_local_games(self)
    notify_list = get_state_changes(self._local_games, new_local_games)
    self._local_games = new_local_games

    return notify_list

def get_install_path_from_xml(game_data, xml_path):
    """Extract the installation path from the XML file, parsing DiPManifest for launcher info."""
    resolver = ManifestResolver(game_data)
    return resolver.resolve_install_path(xml_path)


class ManifestResolver:
    """Handles XML manifest parsing and game path resolution."""
    
    def __init__(self, game_data: dict):
        self.game_data = game_data
        self.display_name = (
            game_data.get('displayName', '').lower() or 
            game_data.get('i18n', {}).get('displayName', '').lower()
        )
        self.is_trial_game = 'demo' in self.display_name or 'trial' in self.display_name
        self.is_64bit = platform.machine().endswith('64')
    
    def resolve_install_path(self, xml_path: str) -> Optional[str]:
        """Main entry point to resolve installation path from XML or registry."""
        try:
            if xml_path.startswith('[') and ']' in xml_path:
                return self._resolve_registry_based_path(xml_path)
            elif os.path.exists(xml_path):
                return self._resolve_file_based_path(xml_path)
            else:
                return self._resolve_directory_based_path(xml_path)
        except Exception as e:
            logger.info(f"Error resolving install path: {e}")
            return None
    
    def _resolve_registry_based_path(self, xml_path: str) -> Optional[str]:
        """Resolve path when it starts with registry reference."""
        reg_path, xml_relative_path = xml_path.split(']', 1)
        base_install_location = GamePathResolver.resolve_registry_path(reg_path + ']')
        
        if not base_install_location:
            return None
            
        # Always try to find __Installer/installerdata.xml first
        full_xml_path = os.path.join(base_install_location, "__Installer", "installerdata.xml")
        if not os.path.exists(full_xml_path) and xml_relative_path:
            full_xml_path = os.path.join(base_install_location, xml_relative_path)
        
        return self._parse_manifest_xml(full_xml_path, base_install_location)
    
    def _resolve_file_based_path(self, xml_path: str) -> Optional[str]:
        """Resolve path when XML file exists directly."""
        base_install_location = os.path.dirname(xml_path)
        return self._parse_manifest_xml(xml_path, base_install_location)
    
    def _resolve_directory_based_path(self, xml_path: str) -> Optional[str]:
        """Resolve path when given a directory."""
        if os.path.isdir(xml_path):
            installer_xml = os.path.join(xml_path, "__Installer", "installerdata.xml")
            if os.path.exists(installer_xml):
                return self._parse_manifest_xml(installer_xml, xml_path)
            return xml_path  # Return directory if no XML found
        return None
    
    def _parse_manifest_xml(self, xml_path: str, base_install_location: str) -> Optional[str]:
        """Parse installerdata.xml to find the correct launcher."""
        if not os.path.exists(xml_path):
            logger.debug(f"XML file not found: {xml_path}")
            return base_install_location
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            if root.tag != 'DiPManifest':
                # Legacy style manifest (e.g., <game ...>)
                legacy_path = self._extract_legacy_executable(root, base_install_location)
                if legacy_path:
                    return legacy_path
                return self._handle_legacy_game()
            
            return self._find_best_launcher(root, base_install_location)
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML file {xml_path}: {e}")
            return base_install_location
        except Exception as e:
            logger.error(f"Error parsing installerdata.xml: {e}")
            return base_install_location
    
    def _handle_legacy_game(self) -> Optional[str]:
        """Handle older games that don't use DiPManifest format."""
        logger.warning(f"Potentially old game {self.display_name}, not a DiPManifest")
        
        install_location = (
            self.game_data.get('installCheckOverride') or 
            self.game_data.get('executePathOverride')
        )
        
        if install_location and install_location.endswith('.exe'):
            if install_location.startswith('[') and ']' in install_location:
                reg_part = install_location[:install_location.index(']') + 1]
                return GamePathResolver.resolve_registry_path(reg_part)
        
        return None

    def _extract_legacy_executable(self, root: ET.Element, base_install_location: str) -> Optional[str]:
        """In legacy manifests (<game> root), some executables are listed in <ignore> nodes."""
        try:
            # Look for any <ignore> elements containing .exe
            ignore_nodes = root.findall('.//ignore')
            candidates: List[str] = []
            for node in ignore_nodes:
                if node is not None and node.text:
                    val = node.text.strip()
                    if val and val.lower().endswith('.exe'):
                        candidates.append(val)
            for rel in candidates:
                # Absolute or relative
                full = rel if os.path.isabs(rel) else os.path.join(base_install_location, rel)
                try:
                    full = os.path.expandvars(full)
                except Exception:
                    pass
                if os.path.exists(full):
                    logger.debug(f"Found legacy executable via <ignore>: {full}")
                    return full
        except Exception as e:
            logger.debug(f"Legacy executable extraction failed: {e}")
        return None
    
    def _find_best_launcher(self, root, base_install_location: str) -> Optional[str]:
        """Find the most appropriate launcher from XML manifest."""
        launchers = root.findall(".//runtime/launcher")
        
        if not launchers:
            logger.debug(f"No launcher elements found in manifest")
            return base_install_location
        
        selected_launcher = self._select_launcher(launchers)
        
        if selected_launcher is not None:
            return self._extract_launcher_path(selected_launcher, base_install_location)
        
        return base_install_location
    
    def _select_launcher(self, launchers):
        """Select the best launcher based on trial status and architecture."""
        selected_launcher = None
        fallback_launcher = None
        
        for launcher in launchers:
            trial_attr = launcher.get('trial', '0')
            is_trial_launcher = trial_attr == '1'
            requires_64bit = launcher.get('requires64BitOS', '0') == '1'
            # Also infer trial/demo from localized names
            names = [n.text.strip().lower() for n in launcher.findall('name') if n is not None and n.text]
            name_says_trial = any(('trial' in n or 'demo' in n) for n in names)
            if name_says_trial:
                is_trial_launcher = True
            
            trial_match = (self.is_trial_game and is_trial_launcher) or (not self.is_trial_game and not is_trial_launcher)
            arch_match = (self.is_64bit and requires_64bit) or (not requires_64bit)
            
            if trial_match and arch_match:
                selected_launcher = launcher
                break
            elif trial_match:
                fallback_launcher = launcher
        
        # Fallback logic
        if selected_launcher is None:
            if fallback_launcher is not None:
                selected_launcher = fallback_launcher
                logger.debug("Using fallback launcher due to architecture mismatch")
            elif not self.is_trial_game:
                # Find first non-trial launcher for non-trial games
                for launcher in launchers:
                    if launcher.get('trial', '0') != '1':
                        selected_launcher = launcher
                        break
            
            # Last resort: take the first launcher
            if selected_launcher is None and launchers:
                selected_launcher = launchers[0]
                logger.debug("No specific launcher match found, using first available launcher")
        
        return selected_launcher
    
    def _extract_launcher_path(self, launcher, base_install_location: str) -> Optional[str]:
        """Extract the actual file path from a launcher element."""
        file_path_element = launcher.find('filePath')
        launcher_path = None
        
        if file_path_element is not None and file_path_element.text:
            launcher_path = file_path_element.text.strip()
        elif launcher.text:
            launcher_path = launcher.text.strip()
        
        if not launcher_path:
            return base_install_location
        
        # Handle registry-based paths
        if launcher_path.startswith('[') and ']' in launcher_path:
            return self._resolve_launcher_registry_path(launcher_path, base_install_location)
        else:
            # Convert relative path to absolute path
            if not os.path.isabs(launcher_path):
                full_launcher_path = os.path.join(base_install_location, launcher_path)
            else:
                full_launcher_path = launcher_path
            
            if os.path.exists(full_launcher_path):
                logger.debug(f"Found launcher at: {full_launcher_path}")
                return full_launcher_path
            else:
                logger.debug(f"Launcher path does not exist: {full_launcher_path}")
                return base_install_location
    
    def _resolve_launcher_registry_path(self, launcher_path: str, base_install_location: str) -> Optional[str]:
        """Resolve registry-based launcher paths."""
        try:
            full_launcher_path = resolve_registry_expression(launcher_path, base_fallback=base_install_location)
            if full_launcher_path and os.path.exists(full_launcher_path):
                return full_launcher_path
            return base_install_location
        except Exception as e:
            logger.error(f"Failed to parse registry-based launcher path: {e}")
            return base_install_location

def parse_installerdata_xml(game_data, xml_path, base_install_location):
    """Legacy function - use ManifestResolver instead."""
    resolver = ManifestResolver(game_data)
    return resolver._parse_manifest_xml(xml_path, base_install_location)