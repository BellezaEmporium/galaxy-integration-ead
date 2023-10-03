from hashlib import sha1, sha3_256
import json
import re
import functools
import logging
import os
import platform
import subprocess
from cryptography.hazmat.primitives.ciphers import algorithms, modes
from cryptography.hazmat.primitives.ciphers.base import Cipher

if platform.system() == "Windows":
    from wmi import WMI
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
    # 530c11479fe252fc5aabc24935b9776d4900eb3ba58fdc271e0d6229413ad40e = allUsersGenericId
    iv = "allUsersGenericIdIS".encode('ascii')
    iv_hash = sha3_256(iv).digest()

    if platform.system() == "Windows":
        data_path = os.path.join(os.environ.get("ProgramData", os.environ.get("SystemDrive", "C:") + R"\ProgramData"), "EA Desktop", "530c11479fe252fc5aabc24935b9776d4900eb3ba58fdc271e0d6229413ad40e")
    elif platform.system() == "Darwin":
        data_path = os.path.join(os.sep, "Library", "Application Support", "EA Desktop", "530c11479fe252fc5aabc24935b9776d4900eb3ba58fdc271e0d6229413ad40e")
    else:
        data_path = "."
    
    running_processes = [exe for pid, exe in process_iter() if exe is not None]

    def is_game_running(game_folder_name):
        for exe in running_processes:
            if game_folder_name in exe:
                return True
        return False
    
    if platform.system() == "Windows":
        # Retrieve system information
        sheesh = WMI()

        for baseboard in sheesh.Win32_BaseBoard():
            baseboard_manufacturer = baseboard.Manufacturer
            baseboard_serial_number = baseboard.SerialNumber
        for bios in sheesh.Win32_BIOS():
            bios_manufacturer = bios.Manufacturer
            bios_serial_number = bios.SerialNumber
        for disk in sheesh.Win32_LogicalDisk():
            if disk.Caption == "C:":
                volume_serial_number = disk.VolumeSerialNumber
        for video in sheesh.Win32_VideoController():
            video_controller_pnp_device_id = video.PNPDeviceID
            # get the processor video controller, not the gpu one (if one exists)
            if video.VideoProcessor.startswith("Intel") or video.VideoProcessor.startswith("AMD"):
                break
        for processor in sheesh.Win32_Processor():
            processor_manufacturer = processor.Manufacturer
            processor_id = processor.ProcessorId
            processor_name = processor.Name

    elif platform.system() == "Darwin":
        # Retrieve system information
        baseboard_manufacturer = ''
        baseboard_serial_number = ''
        bios_manufacturer = ''
        bios_serial_number = ''
        volume_serial_number = ''
        processor_manufacturer = ''
        processor_id = ''
        processor_name = ''

        # Baseboard information
        baseboard_manufacturer = subprocess.check_output(["system_profiler", "SPHardwareDataType"])
        baseboard_manufacturer = baseboard_manufacturer.decode('utf-8').split(':')[-1].strip()

        # BIOS information (Mac doesn't have BIOS in the traditional sense)
        bios_manufacturer = "Apple"
        bios_serial_number = subprocess.check_output(["system_profiler", "SPHardwareDataType"])
        bios_serial_number = bios_serial_number.decode('utf-8').split(':')[-1].strip()

        # Disk information
        volume_serial_number = subprocess.check_output(["system_profiler", "SPStorageDataType"])
        volume_serial_number = volume_serial_number.decode('utf-8').split('Serial Number (system):')[-1].strip()

        # Video controller information
        # to be determined

        # Processor information
        processor_info = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"])
        processor_info = processor_info.decode('utf-8').strip()

        processor_manufacturer = "Intel"  # Assuming Mac uses Intel processors
        processor_name = processor_info

    
    elif platform.system() == "Linux":
        # Retrieve system information
        baseboard_manufacturer = ''
        baseboard_serial_number = ''
        bios_manufacturer = ''
        bios_serial_number = ''
        volume_serial_number = ''
        video_controller_pnp_device_id = ''
        processor_manufacturer = ''
        processor_id = ''
        processor_name = ''

        # Baseboard information
        with open('/sys/class/dmi/id/board_vendor', 'r') as f:
            baseboard_manufacturer = f.read().strip()

        with open('/sys/class/dmi/id/board_serial', 'r') as f:
            baseboard_serial_number = f.read().strip()

        # BIOS information
        with open('/sys/class/dmi/id/bios_vendor', 'r') as f:
            bios_manufacturer = f.read().strip()

        with open('/sys/class/dmi/id/bios_version', 'r') as f:
            bios_serial_number = f.read().strip()

        # Disk information (assuming the root partition is mounted at /)
        partition = psutil.disk_partitions(all=False)[0]
        volume_serial_number = psutil.disk_usage(partition.mountpoint).serial

        # Video controller information (you may need to adjust this)
        for device in psutil.pids():
            try:
                cmdline = psutil.Process(device).cmdline()
                if "Xorg" in cmdline or "xorg" in cmdline:
                    # Extract video controller information from Xorg process
                    # TEST THIS
                    video_controller_pnp_device_id = cmdline[cmdline.index("Xorg") + 1]
                    pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        # Processor information
        processor_info = os.popen("lscpu").read()
        lines = processor_info.split('\n')
        for line in lines:
            if "Vendor ID:" in line:
                processor_manufacturer = line.split(':')[1].strip()
            elif "Model name:" in line:
                processor_name = line.split(':')[1].strip()

    # sha1 string
    hw_info = baseboard_manufacturer + ";" + baseboard_serial_number + ";" + bios_manufacturer + ";" + bios_serial_number + ";" + volume_serial_number + ";" + video_controller_pnp_device_id + ";" + processor_manufacturer + ";" + processor_id + ";" + processor_name + ';'
    # Calculate SHA1 Hash of hardware string
    hw_info_bytes = hw_info.encode('ascii')
    hw_hash = sha1(hw_info_bytes).digest()
    hash_str = 'allUsersGenericIdIS' + hw_hash.hex().lower()

    print("Got hardware info: %s", hw_info)

    # Calculate SHA3 256 Hash of full string
    hash_bytes = hash_str.encode('ascii')
    key_hash = sha3_256(hash_bytes).digest()

    print("Got key hash: %s", key_hash.hex())

    cipher = Cipher(algorithms.AES(key_hash), modes.CBC(iv_hash[0:16]))

    # Create a decryptor object
    decryptor = cipher.decryptor()

    # Open input and output files
    with open(data_path, 'rb') as infile, open("IS.json", 'wb') as outfile:
        # Read the first 64 bytes and discard them
        infile.read(64)
        block_size = 16

        while True:
            # Read a block from the input file
            block = infile.read(block_size)
            if not block:
                break  # Reached end of file

            # Decrypt the block and write to the output file
            decrypted_block = decryptor.update(block)
            outfile.write(decrypted_block)

    # verifying the JSON part
    # seems like there's undescribed characters () in the end of the json file
    # so we need to remove them

    with open("IS.json", "r+") as f:
        # remove  from the end of the file
        json_string = f.read().replace("", "")
        f.write(json_string)

    print("IS decrypted successfully.")
    
    installed_games = [json.loads(line) for line in open(data_path + "/is.json", 'r', encoding='utf-8')]
    logger.info(f"Opening manifest file ", data_path + "/is.json ...")
    for game in installed_games[0]['installInfos']:
            logger.info(f"Found installed game: ", game['softwareId'])
            if game['executablePath'] != "":
                local_games.append(LocalGame(game['softwareId'], LocalGameState.Installed))
            else:
                local_games.append(LocalGame(game['softwareId'], LocalGameState.None_))
    else:
        logger.warning("is.json file not found. Local games won't be checked. We strongly suggest to use the is_decryption_galaxy.py file to generate the decrypted IS file.")


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
