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
from typing import Tuple, Dict, Any, Optional
from pathlib import Path

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
        
        # Check cache outside lock first
        if (self._cache is not None and 
            self._cache_time is not None and 
            current_time - self._cache_time < self._cache_duration):
            return self._cache
        
        with self._lock:
            # Double-check pattern
            if (self._cache is not None and 
                self._cache_time is not None and 
                current_time - self._cache_time < self._cache_duration):
                return self._cache
            
            try:
                if platform.system() == "Windows":
                    self._cache = self._gather_windows_info()
                elif platform.system() == "Darwin":
                    self._cache = self._gather_macos_info()
                else:
                    logger.error("Unsupported OS: %s", platform.system())
                    self._cache = ("", "", 0, "", "", "", "", "", "")
                
                self._cache_time = current_time
                return self._cache
                
            except Exception as e:
                logger.error("Failed to gather hardware information: %s", e)
                self._cache = ("", "", 0, "", "", "", "", "", "")
                self._cache_time = current_time
                return self._cache
    
    def _run_cmd(self, cmd: str, timeout: int = 10) -> str:
        """Execute a command with improved error handling and logging"""
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=timeout,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode != 0:
                logger.warning("Command failed (code %d): %s", result.returncode, cmd[:50])
                return ""
                
            return result.stdout.strip()
            
        except subprocess.TimeoutExpired:
            logger.warning("Command timed out: %s", cmd[:50])
            return ""
        except Exception as e:
            logger.warning("Command error: %s", e)
            return ""
    
    def _parse_json_safely(self, json_str: str) -> Optional[Dict[str, Any]]:
        """Safely parse JSON with better error handling"""
        if not json_str.strip():
            return None
            
        try:
            data = json.loads(json_str)
            
            # Handle list responses
            if isinstance(data, list):
                if not data:
                    logger.warning("PowerShell returned empty list")
                    return None
                data = data[0]  # This was the source of the error
            
            if not isinstance(data, dict):
                logger.warning("PowerShell data is not a dict: %r", type(data))
                return None
                
            return data
            
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON from PowerShell: %s", e)
            return None
        except (IndexError, TypeError) as e:
            logger.warning("Error processing PowerShell data: %s", e)
            return None
    
    def _safe_int_convert(self, value: Any, default: int = 0) -> int:
        """Safely convert value to int with fallback"""
        if value is None:
            return default
            
        try:
            # Handle hex strings
            if isinstance(value, str) and value.startswith(('0x', '0X')):
                return int(value, 16)
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def _gather_windows_info(self) -> Tuple[str, str, int, str, str, str, str, str, str]:
        """Get hardware information on Windows with improved error handling"""
        
        # Check if PowerShell script exists
        ps_script_path = Path(__file__).parent / 'pc_sign_ps.ps1'
        if not ps_script_path.exists():
            logger.warning("PowerShell script not found: %s", ps_script_path)
            return self._fallback_windows_info()
        
        try:
            result = subprocess.run(
                ['powershell', '-ExecutionPolicy', 'Bypass', '-File', str(ps_script_path)],
                capture_output=True,
                timeout=15,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode != 0:
                logger.warning("PowerShell script failed (code %d): %s", 
                             result.returncode, result.stderr[:200])
                return self._fallback_windows_info()
            
            data = self._parse_json_safely(result.stdout)
            if not data:
                return self._fallback_windows_info()
            
            gid = self._safe_int_convert(data.get('gid'))
            
            return (
                data.get('mbm', ''),
                data.get('bbm', ''),
                gid,
                data.get('bsn', ''),
                data.get('hsn', ''),
                data.get('msn', ''),
                data.get('mac', ''),
                data.get('osn', ''),
                data.get('osi', ''),
            )
            
        except subprocess.TimeoutExpired:
            logger.warning("PowerShell script timed out")
            return self._fallback_windows_info()
        except Exception as e:
            logger.warning("PowerShell execution failed: %s", e)
            return self._fallback_windows_info()
    
    def _fallback_windows_info(self) -> Tuple[str, str, int, str, str, str, str, str, str]:
        """Fallback method using direct wmic commands"""
        logger.info("Using fallback Windows info gathering")
        
        mbm = self._run_cmd('wmic baseboard get manufacturer /value | findstr "="').split('=', 1)[-1] if '=' in self._run_cmd('wmic baseboard get manufacturer /value | findstr "="') else ""
        bbm = self._run_cmd('wmic baseboard get product /value | findstr "="').split('=', 1)[-1] if '=' in self._run_cmd('wmic baseboard get product /value | findstr "="') else ""
        bsn = self._run_cmd('wmic bios get serialnumber /value | findstr "="').split('=', 1)[-1] if '=' in self._run_cmd('wmic bios get serialnumber /value | findstr "="') else ""
        
        return (mbm, bbm, 0, bsn, "", "", "", "", "")

    def _gather_macos_info(self) -> Tuple[str, str, int, str, str, str, str, str, str]:
        """Get hardware information on macOS with improved error handling"""
        try:
            mbm = self._run_cmd("system_profiler SPHardwareDataType | awk '/Model Name/ {for(i=3;i<=NF;i++) printf \"%s \", $i; print \"\"}'").strip()
            bsn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Serial Number/ {print $NF}'")
            
            gid_str = self._run_cmd("system_profiler SPDisplaysDataType | awk '/Device ID:/ {print $NF}'")
            gid = self._safe_int_convert(gid_str, 0) if gid_str else 0
            
            hsn = self._run_cmd("diskutil info /dev/disk0 | awk '/Device Identifier:/ {print $NF}'")
            msn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Hardware UUID:/ {print $NF}'")
            
            # Get MAC address with improved formatting
            mac = self._get_macos_mac_address()
                    
            # Set defaults for missing values
            bsn = bsn if bsn and bsn != "N/A" else "macOS-Unknown"
            hsn = hsn if hsn else "disk0"
            msn = msn if msn else "macOS-Unknown-UUID"
            
            return (mbm, "", gid, bsn, hsn, msn, mac, "", "")
            
        except Exception as e:
            logger.error("Error gathering macOS hardware info: %s", e)
            return ("", "", 0, "macOS-Unknown", "disk0", "macOS-Unknown-UUID", "", "", "")
    
    def _get_macos_mac_address(self) -> str:
        """Get MAC address on macOS with proper formatting"""
        for interface in ["en0", "en1", "en2"]:
            mac_addr = self._run_cmd(f"ifconfig {interface} | awk '/ether/ {{print $2}}'")
            if mac_addr and len(mac_addr.replace(':', '')) == 12:
                mac_clean = mac_addr.replace(':', '').replace('-', '').lower()
                return f"${mac_clean}"
        return ""

@dataclass
class PCSign:
    """
    PCSign represents a unique signature for a PC based on its hardware information.
    """
    bbm: str = field(init=False)
    bsn: str = field(init=False)
    gid: int = field(init=False)
    hsn: str = field(init=False)
    mbm: str = field(init=False)
    msn: str = field(init=False)
    mac: str = field(init=False)
    mid: str = field(init=False)
    osn: str = field(init=False)
    osi: str = field(init=False)
    ts: str = field(init=False)
    av: str = "v1"
    sv: PCSignVersion = field(default_factory=lambda: random.choice(list(PCSignVersion)))

    def __post_init__(self):
        cache = HardwareInfoCache()
        (self.mbm, self.bbm, self.gid, self.bsn, 
         self.hsn, self.msn, self.mac, self.osn, self.osi) = cache.get_hardware_info()
        
        self.mid = self.calculate_fnv1a_hash()
        self.ts = self._generate_timestamp()

    def _generate_timestamp(self) -> str:
        """Generate timestamp in EA Desktop format"""
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"{now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second}:{int(now.microsecond/1000)}"

    def calculate_fnv1a_hash(self) -> str:
        """Calculate FNV-1a hash of hardware information"""
        buffer_parts = [
            str(self.mbm), str(self.msn), str(self.bbm),
            str(self.bsn), str(self.osi), str(self.osn)
        ]
        
        if self.mac:
            buffer_parts.append(str(self.mac))
        
        buffer = ''.join(buffer_parts)

        # FNV-1a 64-bit hash
        offset = 0xcbf29ce484222325
        prime = 0x100000001b3
        
        for byte in buffer.encode("utf-8"):
            offset ^= byte
            offset = (offset * prime) & 0xFFFFFFFFFFFFFFFF

        return str(offset)

    @staticmethod
    def preload_hardware_cache():
        """Preload hardware information cache"""
        cache = HardwareInfoCache()
        try:
            cache.get_hardware_info()
        except Exception:
            pass

    @classmethod
    def generate_fast(cls, sv: PCSignVersion) -> str:
        """Quickly generate a PC signature for a given version"""
        instance = cls()
        instance.sv = sv
        return instance.generate_pc_sign()

    def sign_key(self) -> bytes:
        """Get the secret key for the current PCSignVersion"""
        keys = {
            PCSignVersion.V1: b"ISa3dpGOc8wW7Adn4auACSQmaccrOyR2",
            PCSignVersion.V2: b"nt5FfJbdPzNcl2pkC3zgjO43Knvscxft"
        }
        key = keys.get(self.sv)
        if key is None:
            raise ValueError(f"Invalid PCSignVersion: {self.sv}")
        return key

    def to_dict(self) -> Dict[str, Any]:
        """Convert PCSign to dictionary"""
        return {
            "av": self.av, "bsn": self.bsn, "gid": self.gid,
            "hsn": self.hsn, "mac": self.mac, "mid": self.mid, 
            "msn": self.msn, "sv": self.sv.value, "ts": self.ts
        }

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        """Encode bytes using URL-safe base64 without padding"""
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')

    def generate_pc_sign(self) -> str:
        """Generate the final PC signature as a base64url-encoded payload and signature"""
        payload_json = json.dumps(self.to_dict(), separators=(',', ':'))
        payload = self.base64url_encode(payload_json.encode('utf-8'))
        signature = hmac.new(self.sign_key(), payload.encode('ascii'), hashlib.sha256).digest()
        return f"{payload}.{self.base64url_encode(signature)}"


def extract_user_info_from_jwt(jwt_token: str) -> Tuple[str, str, str]:
    """Extract user & persona ID and username from JWT token"""
    try:
        _, payload, _ = jwt_token.split('.')
        # Add padding if needed
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding
            
        decoded_payload = base64.urlsafe_b64decode(payload).decode('utf-8')
        data = json.loads(decoded_payload)['nexus']

        return (
            data.get('pid', ''), 
            data.get('psid', ''), 
            data.get('psif', [{}])[0].get('dis', '')
        )
    except Exception as e:
        logger.error("Failed to extract user info from JWT: %s", e)
        return '', '', ''

def generate_pc_sign_fast(sv: Optional[PCSignVersion] = None) -> str:
    """Generate PC signature quickly with optional version"""
    if sv is None:
        sv = random.choice(list(PCSignVersion))
    return PCSign.generate_fast(sv)

def preload_pc_sign_cache():
    """Preload PC sign cache"""
    PCSign.preload_hardware_cache()
