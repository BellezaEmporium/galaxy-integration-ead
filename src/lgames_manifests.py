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

def find_executable_in_dir(directory):
    """Cherche un exécutable dans le dossier donné (retourne le premier .exe trouvé)."""
    if not directory or not os.path.isdir(directory):
        return None
    for entry in os.listdir(directory):
        if entry.lower().endswith('.exe'):
            return os.path.join(directory, entry)
    return None

def update_local_games(self):
    local_games = []
    running_exes = set(os.path.basename(exe).lower() for _, exe in process_iter() if exe)

    for offer_id, game_data in self._offer_id_cache.items():
        if "displayName" in game_data:
            logger.info(f"Checking local game status for {offer_id}, game name is {game_data.get('displayName')}")
            state = LocalGameState.None_
            install_path = None

            path = game_data.get("installCheckOverride") or game_data.get("executePathOverride")
            if path:
                base_path = get_install_path_from_xml(game_data, path)
                if base_path:
                    exe = find_executable_in_dir(base_path)
                    install_path = exe or base_path
                else:
                    install_path = path

            if install_path and os.path.exists(install_path):
                state = LocalGameState.Installed
                exe_name = os.path.basename(install_path).lower()
                if exe_name in running_exes:
                    state |= LocalGameState.Running
                logger.info(f"{offer_id} is installed at {install_path}")

            local_games.append(LocalGame(offer_id, state))
        else:
            continue

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
        
        try:
            # Always look for __Installer/installerdata.xml relative to the base path
            if xml_path.startswith('[') and ']' in xml_path:
                reg_path, xml_relative_path = xml_path.split(']', 1)
                reg_key = reg_path[1:]  # Remove the [ at the beginning
                
                # Divide the registry key into its components
                reg_components = reg_key.split('\\')
                if len(reg_components) < 3:
                    logger.error(f"Invalid registry key format: {xml_path}")
                    return None
                
                try:
                    hive_name = reg_components[0]
                    # Map registry hive names to their constants
                    hive_mapping = {
                        'HKEY_LOCAL_MACHINE': winreg.HKEY_LOCAL_MACHINE,
                        'HKEY_CURRENT_USER': winreg.HKEY_CURRENT_USER,
                        'HKEY_CLASSES_ROOT': winreg.HKEY_CLASSES_ROOT,
                        'HKEY_USERS': winreg.HKEY_USERS,
                        'HKEY_CURRENT_CONFIG': winreg.HKEY_CURRENT_CONFIG
                    }
                    
                    if hive_name not in hive_mapping:
                        logger.error(f"Unknown registry hive: {hive_name}")
                        return None
                    
                    hive = hive_mapping[hive_name]
                    value_name = reg_components[-1]
                    key_path = "\\".join(reg_components[1:-1])
                    
                    # Get the base installation location from the registry
                    base_install_location = get_install_location(hive, key_path, value_name)
                    
                    if base_install_location:
                        # Always try to find __Installer/installerdata.xml
                        full_xml_path = os.path.join(base_install_location, "__Installer", "installerdata.xml")
                        if not os.path.exists(full_xml_path):
                            # Fallback to the original XML path if specified differently
                            full_xml_path = os.path.join(base_install_location, xml_relative_path)
                        
                        return parse_installerdata_xml(game_data, full_xml_path, base_install_location)
                except Exception as e:
                    logger.error(f"Error processing registry path: {e}")
                    return None
            elif os.path.exists(xml_path):
                base_install_location = os.path.dirname(xml_path)
                return parse_installerdata_xml(game_data, xml_path, base_install_location)
            else:
                # Try to find __Installer/installerdata.xml in the path
                if os.path.isdir(xml_path):
                    installer_xml = os.path.join(xml_path, "__Installer", "installerdata.xml")
                    if os.path.exists(installer_xml):
                        return parse_installerdata_xml(game_data, installer_xml, xml_path)                
                    return xml_path  # Return the path as-is if no XML found
                
        except Exception as e:
            logger.info(f"Error while parsing installerdata.xml file: {e}")

        return None

def parse_installerdata_xml(game_data, xml_path, base_install_location):
    """Parse installerdata.xml to find the correct launcher based on trial/demo detection and system architecture."""
    import xml.etree.ElementTree as ET
    import platform
    
    try:
        if not os.path.exists(xml_path):
            logger.debug(f"XML file not found: {xml_path}")
            return base_install_location
        
        tree = ET.parse(xml_path)
        root = tree.getroot()


        if root.tag != 'DiPManifest':
            logger.error(f"Potentially old game {game_data.get('displayName', '') or game_data.get('i18n', {}).get('displayName', '')}, not a DiPManifest")
            install_location = game_data.get('installCheckOverride') or game_data.get('executePathOverride')
            if install_location.endswith('.exe'):
                # Parse registry path like [HKEY_LOCAL_MACHINE\SOFTWARE\EA Games\Battlefield 4\Install Dir]BFLauncher.exe
                reg_part, exe_part = install_location.split(']', 1)
                reg_key = reg_part[1:]  # Remove the [
                
                # Split registry key into components
                reg_components = reg_key.split('\\')
                if len(reg_components) >= 3:
                    hive_name = reg_components[0]
                    value_name = reg_components[-1]
                    key_path = "\\".join(reg_components[1:-1])
                    
                    hive = getattr(winreg, hive_name)
                    return  get_install_location(hive, key_path, value_name)
                else:
                    logger.error(f"Invalid registry key format: {install_location}")
                    return base_install_location
                # Look for runtime/launcher elements
        launchers = root.findall(".//runtime/launcher")
        
        if not launchers:
            logger.debug(f"No launcher elements found in {xml_path}")
            return base_install_location
        
        # Detect if this is a Demo/Trial game by checking displayName
        display_name = game_data.get('displayName', '').lower() or game_data.get('i18n', {}).get('displayName', '').lower()
        is_trial_game = 'demo' in display_name or 'trial' in display_name
        
        # Detect system architecture
        is_64bit = platform.machine().endswith('64')
        
        # Find the appropriate launcher
        selected_launcher = None
        fallback_launcher = None
        
        for launcher in launchers:
            trial_attr = launcher.get('trial', '0')
            is_trial_launcher = trial_attr == '1'
            requires_64bit = launcher.get('requires64BitOS', '0') == '1'
            
            # Check if this launcher matches trial requirements
            trial_match = (is_trial_game and is_trial_launcher) or (not is_trial_game and not is_trial_launcher)
            
            # Check if this launcher matches architecture requirements
            arch_match = (is_64bit and requires_64bit) or (not requires_64bit)
            
            if trial_match and arch_match:
                selected_launcher = launcher
                break
            elif trial_match:
                # Keep as fallback if trial matches but architecture doesn't
                fallback_launcher = launcher
        
        # Use fallback if no perfect match
        if selected_launcher is None and fallback_launcher is not None:
            selected_launcher = fallback_launcher
            logger.debug(f"Using fallback launcher due to architecture mismatch")
        
        # If still no match, take the first non-trial launcher for non-trial games
        if selected_launcher is None and launchers and not is_trial_game:
            for launcher in launchers:
                trial_attr = launcher.get('trial', '0')
                if trial_attr != '1':
                    selected_launcher = launcher
                    break
        
        # Last resort: take the first launcher
        if selected_launcher is None and launchers:
            selected_launcher = launchers[0]
            logger.debug(f"No specific launcher match found, using first available launcher")
        
        if selected_launcher is not None:
            # Look for filePath element within the launcher
            file_path_element = selected_launcher.find('filePath')
            launcher_path = None
            
            if file_path_element is not None and file_path_element.text:
                launcher_path = file_path_element.text.strip()
            elif selected_launcher.text:
                # Fallback to launcher text if no filePath element
                launcher_path = selected_launcher.text.strip()
            
            if launcher_path:
                # Handle registry-based paths
                if launcher_path.startswith('[') and ']' in launcher_path:
                    try:
                        # Parse registry path like [HKEY_LOCAL_MACHINE\SOFTWARE\EA Games\Battlefield 4\Install Dir]BFLauncher.exe
                        reg_part, exe_part = launcher_path.split(']', 1)
                        reg_key = reg_part[1:]  # Remove the [
                        
                        # Split registry key into components
                        reg_components = reg_key.split('\\')
                        if len(reg_components) >= 3:
                            hive_name = reg_components[0]
                            value_name = reg_components[-1]
                            key_path = "\\".join(reg_components[1:-1])
                            
                            hive = getattr(winreg, hive_name)
                            install_dir = get_install_location(hive, key_path, value_name)
                            
                            if install_dir:
                                full_launcher_path = os.path.join(install_dir, exe_part)
                            else:
                                # Fallback to base installation location
                                full_launcher_path = os.path.join(base_install_location, exe_part)
                        else:
                            logger.error(f"Invalid registry key format: {launcher_path}")
                            return base_install_location
                    except Exception as e:
                        logger.error(f"Failed to parse registry-based launcher path: {e}")
                        return base_install_location
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
        
        # If no valid launcher found, return the base installation directory
        return base_install_location
        
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML file {xml_path}: {e}")
        return base_install_location
    except Exception as e:
        logger.error(f"Error parsing installerdata.xml: {e}")
        return base_install_location