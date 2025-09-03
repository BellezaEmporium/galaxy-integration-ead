###
#----------------------------------------------------PC SIGN----------------------------------------------------#
# EA Desktop's way of linking your login info to your PC. This is a hash of your hardware info and a timestamp.
# The hash is signed with a secret key to prevent tampering. The server can verify the hash with the secret key.
# The server can also generate the hash itself and compare it to the one sent by the client.
# It, then, can decide if the client is allowed to log in.
#---------------------------------------------------------------------------------------------------------------#
# Kudos to @imLinguin for the necessary info.
###

import platform
import random
import subprocess
import datetime
import base64
import hmac
import hashlib
import json
import threading
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PCSignVersion(Enum):
    V1 = "v1"
    V2 = "v2"

class HardwareInfoCache:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._cache = None
            self._cache_time = None
            self._cache_duration = 3600
            self._initialized = True
    
    def set_cache_duration(self, duration_seconds: int):
        """Permits to modify the cache duration"""
        with self._lock:
            self._cache_duration = duration_seconds
            if (self._cache_time is not None and 
                time.time() - self._cache_time >= duration_seconds):
                self._cache = None
                self._cache_time = None
    
    def clear_cache(self):
        """Forces cache invalidation"""
        with self._lock:
            self._cache = None
            self._cache_time = None

    def get_hardware_info(self) -> Tuple[str, str, int, str, str, str, str, str, str]:
        """Get hardware information with caching and optimized methods"""
        current_time = time.time()
        
        if (self._cache is not None and 
            self._cache_time is not None and 
            current_time - self._cache_time < self._cache_duration):
            return self._cache
        
        with self._lock:
            if (self._cache is not None and 
                self._cache_time is not None and 
                current_time - self._cache_time < self._cache_duration):
                return self._cache
            
            if platform.system() == "Windows":
                self._cache = self._gather_windows_info()
            elif platform.system() == "Darwin":
                self._cache = self._gather_macos_info()
            else:
                raise OSError("Unsupported OS")
            
            self._cache_time = current_time
            if self._cache is None:
                raise RuntimeError("Failed to gather hardware information")
            return self._cache
    
    def _run_cmd(self, cmd):
        """Execute a command with improved error handling and logging"""
        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=15)
            return output.decode('utf-8', errors='ignore').strip()
        except subprocess.TimeoutExpired:
            logger.warning(f"Command timed out: {cmd[:50]}...")
            return ""
        except subprocess.CalledProcessError as e:
            logger.warning(f"Command failed with return code {e.returncode}: {cmd[:50]}...")
            return ""
        except Exception as e:
            logger.warning(f"Unexpected error running command: {e}")
            return ""
    
    def _gather_windows_info(self):
        """Get hardware information on Windows with CIM commands (PowerShell)"""
        try:
            result = subprocess.check_output(
                ['powershell', '-ExecutionPolicy', 'Bypass', '-File', 'pc_sign_ps.ps1'],
                stderr=subprocess.STDOUT,
                timeout=15,
                text=True,
                encoding='utf-8'
            ).strip()
                    
            if result:
                data = json.loads(result)
                return (
                    data.get('mbm', ''),
                    data.get('bbm', ''),
                    data.get('gid', 0),
                    data.get('bsn', ''),
                    data.get('hsn', ''),
                    data.get('msn', ''),
                    data.get('mac', ''),
                    data.get('osn', ''),
                    data.get('osi', ''),
                )
        except (json.JSONDecodeError, KeyError, subprocess.CalledProcessError, subprocess.TimeoutExpired, Exception) as e:
            logger.warning(f"PowerShell optimized method failed: {e}.")
            return "", "", 0, "", "", "", "", "", ""

    def _gather_macos_info(self):
        """Get hardware information on macOS with improved error handling"""
        try:
            mbm = self._run_cmd("system_profiler SPHardwareDataType | awk '/Model Name/ {print $NF}'")
            bsn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Serial Number/ {print $NF}'")
            gid_str = self._run_cmd("system_profiler SPDisplaysDataType | awk '/Device ID:/ {print $NF}'")
            gid = int(gid_str, 16) if gid_str and gid_str.strip() else 0
            hsn = self._run_cmd("diskutil info /dev/disk0 | awk '/Device Identifier:/ {print $NF}'")
            msn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Hardware UUID:/ {print $NF}'")
            
            # Try to get MAC address from different interfaces and format like EA Desktop
            mac = ""
            for interface in ["en0", "en1", "en2"]:
                mac_addr = self._run_cmd(f"ifconfig {interface} | awk '/ether/ {{print $2}}'")
                if mac_addr and mac_addr.strip():
                    # Format MAC address like EA Desktop: add $ prefix and remove separators
                    mac_clean = mac_addr.replace(':', '').replace('-', '').lower()
                    if mac_clean and len(mac_clean) == 12:
                        mac = f"${mac_clean}"
                    break
                    
            # Default values in case of failure
            if not bsn or bsn == "N/A":
                bsn = "macOS-Unknown"
            if not hsn:
                hsn = "disk0"
            if not msn:
                msn = "macOS-Unknown-UUID"
            return mbm, "", gid, bsn, hsn, msn, mac, "", ""
        except Exception as e:
            # In case of error, use default values
            logger.error(f"Error gathering macOS hardware info: {str(e)}")
            return "", "macOS-Unknown", 0, "", "disk0", "macOS-Unknown-UUID", "", "", ""

@dataclass
class PCSign:
    """
    PCSign represents a unique signature for a PC based on its hardware information.

    Attributes:
        av (str): Application version. Always set to "v1".
        bsn (str): BIOS Serial Number.
        gid (int): GPU device ID.
        hsn (str): Disk Serial Number.
        mac (str): MAC Address. May be None if not available.
        mid (str): FNV-1a hash of hardware info, serving as a machine identifier.
        msb (str): Motherboard Manufacturer.
        msn (str): Motherboard Serial Number.
        sv (PCSignVersion): Secret Key Version, determines the signing key used.
        ts (str): Timestamp of signature generation.

    Methods:
        __post_init__(): Initializes hardware information and computes the machine ID and timestamp.
        calculate_fnv1a_hash() -> str: Calculates the FNV-1a hash of hardware information.
        preload_hardware_cache(): Preloads hardware information cache.
        generate_fast(sv: PCSignVersion) -> str: Quickly generates a PC signature for a given version.
        sign_key(): Returns the secret key corresponding to the current PCSignVersion.
        to_dict(): Serializes the PCSign instance to a dictionary.
        base64url_encode(data: bytes) -> str: Encodes bytes using URL-safe base64 encoding without padding.
        generate_pc_sign() -> str: Generates the final PC signature as a base64url-encoded payload and signature.
    """
    bbm: str = field(init=False) # BIOS Baseboard Manufacturer
    bsn: str = field(init=False) # BIOS Serial Number
    gid: int = field(init=False) # GPU Device ID
    hsn: str = field(init=False) # Hard Disk Serial Number
    mbm: str = field(init=False) # Motherboard Manufacturer
    msn: str = field(init=False) # Motherboard Serial Number
    mac: str = field(init=False) # MAC Address
    mid: str = field(init=False) # Machine ID
    osn: str = field(init=False) # Operating System Serial Number
    osi: str = field(init=False) # Operating System ID
    ts: str = field(init=False) # Timestamp
    av: str = "v1"                                          
    sv: PCSignVersion = random.choice(list(PCSignVersion))

    def __post_init__(self):
        cache = HardwareInfoCache()
        self.mbm, self.bbm, self.gid, self.bsn, self.hsn, self.msn, self.mac, self.osn, self.osi = cache.get_hardware_info()
        self.mid = self.calculate_fnv1a_hash()
        now = datetime.datetime.now(datetime.timezone.utc)
        self.ts = f"{now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second}:{int(now.microsecond/1000)}"

    def calculate_fnv1a_hash(self) -> str:
        buffer = (
            str(self.mbm)
            + str(self.msn)
            + str(self.bbm)
            + str(self.bsn)
            + str(self.osi)
            + str(self.osn)
        )

        if self.mac is not None:
            buffer += str(self.mac)

        # FNV-1a 64-bit hash
        offset = 0xcbf29ce484222325
        prime = 0x100000001b3
        for b in buffer.encode("utf-8"):
            offset ^= b
            offset = (offset * prime) & 0xFFFFFFFFFFFFFFFF

        return str(offset)

    @staticmethod
    def preload_hardware_cache():
        cache = HardwareInfoCache()
        try:
            cache.get_hardware_info()
        except Exception:
            pass

    @classmethod
    def generate_fast(cls, sv: PCSignVersion) -> str:
        instance = cls()
        instance.sv = sv
        return instance.generate_pc_sign()

    def sign_key(self):
        keys = {
            PCSignVersion.V1: b"ISa3dpGOc8wW7Adn4auACSQmaccrOyR2",
            PCSignVersion.V2: b"nt5FfJbdPzNcl2pkC3zgjO43Knvscxft"
        }
        key = keys.get(self.sv)
        if key is None:
            raise ValueError(f"Invalid PCSignVersion: {self.sv}")
        return key

    def to_dict(self):
        d = {
            "av":self.av,"bsn":self.bsn,"gid":self.gid,
            "hsn":self.hsn,"mac":self.mac,"mid":self.mid,"msn":self.msn,
            "sv":self.sv.value,"ts":self.ts
        }
        return d

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

    def generate_pc_sign(self) -> str:
        payload = self.base64url_encode(json.dumps(self.to_dict(), separators=(',', ':')).encode())
        signature = hmac.new(self.sign_key(), payload.encode(), hashlib.sha256).digest()
        return f"{payload}.{self.base64url_encode(signature)}"


def extract_user_info_from_jwt(jwt_token: str) -> Tuple[str, str, str]:
    """Extracts user & persona ID and the username from a JWT token."""
    try:
        _, payload, _ = jwt_token.split('.')
        decoded_payload = base64.urlsafe_b64decode(payload + '==').decode('utf-8')
        data = json.loads(decoded_payload)['nexus']

        return data.get('pid', ''), data.get('psid', ''), data.get('psif', [{}])[0].get('dis', '')
    except Exception as e:
        logger.error(f"Failed to extract user info from JWT: {e}")
        return '', '', ''

def generate_pc_sign_fast(sv: PCSignVersion = random.choice(list(PCSignVersion))) -> str:
    return PCSign.generate_fast(sv)

def preload_pc_sign_cache():
    PCSign.preload_hardware_cache()
