###
#----------------------------------------------------PC SIGN----------------------------------------------------#
# EA Desktop's way of linking your login info to your PC. This is a hash of your hardware info and a timestamp.
# The hash is signed with a secret key to prevent tampering. The server can verify the hash with the secret key.
# The server can also generate the hash itself and compare it to the one sent by the client.
# It, then, can decide if the client is allowed to log in.
#---------------------------------------------------------------------------------------------------------------#
# Kudos to @imLinguin for the necessary info.
###

import tempfile
import os
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
    
    def get_hardware_info(self) -> Tuple[str, str, int, str, str, str, str, str]:
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
        ps_script = '''
        try {
            $ErrorActionPreference = "SilentlyContinue"
            
            $bios = Get-CimInstance Win32_BIOS | Select-Object -First 1
            $baseBoard = Get-CimInstance Win32_BaseBoard | Select-Object -First 1
            $os = Get-CimInstance Win32_OperatingSystem | Select-Object -First 1
            $videoControllers = Get-CimInstance Win32_VideoController | Where-Object { $_.PNPDeviceID -match "DEV_[0-9A-F]+" } | Select-Object -First 1
            $diskDrive = Get-CimInstance Win32_DiskDrive | Select-Object -First 1
            $networkAdapter = Get-CimInstance Win32_NetworkAdapter | Where-Object { $_.PhysicalAdapter -eq $true -and $_.NetEnabled -eq $true -and $_.ServiceName -notmatch "vmnetadapter|vboxnetadp|ndisip|tap|hyperv|loopback" -and $_.MACAddress } | Select-Object -First 1
            
            $gid = 0
            if ($videoControllers -and $videoControllers.PNPDeviceID) {
                if ($videoControllers.PNPDeviceID -match "DEV_([0-9A-F]+)") {
                    try {
                        $gid = [Convert]::ToInt32($matches[1], 16)
                    } catch {
                        $gid = 0
                    }
                }
            }

            $mac = ""
            if ($networkAdapter -and $networkAdapter.MACAddress) {
                $macClean = $networkAdapter.MACAddress -replace "[:-]", ""
                if ($macClean) {
                    $mac = "`$" + $macClean.ToLower()
                }
            }
            
            $osiTimestamp = ""
            if ($os -and $os.InstallDate) {
                try {
                    $epoch = Get-Date "1970-01-01"
                    $osiTimestamp = [int64](($os.InstallDate - $epoch).TotalSeconds)
                } catch {
                    $osiTimestamp = ""
                }
            }
            
            $result = @{
                bbm = if ($bios -and $bios.Manufacturer) { $bios.Manufacturer.ToString() } else { "" }
                bsn = if ($bios -and $bios.SerialNumber) { $bios.SerialNumber.ToString() } else { "" }
                gid = $gid
                hsn = if ($diskDrive -and $diskDrive.SerialNumber) { $diskDrive.SerialNumber.ToString().Trim() } else { "" }
                msn = if ($baseBoard -and $baseBoard.SerialNumber) { $baseBoard.SerialNumber.ToString() } else { "" }
                mac = $mac
                osn = if ($os -and $os.SerialNumber) { $os.SerialNumber.ToString() } else { "" }
                osi = $osiTimestamp.ToString()
            }
            
            $result | ConvertTo-Json -Compress
        } catch {
            $errorResult = @{
                bbm = ""
                bsn = ""
                gid = 0
                hsn = ""
                msn = ""
                mac = ""
                osn = ""
                osi = ""
            }
            $errorResult | ConvertTo-Json -Compress
        }
        '''
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.ps1', delete=False, encoding='utf-8') as f:
                f.write(ps_script)
                script_path = f.name
            
            try:
                result = subprocess.check_output(
                    ['powershell', '-ExecutionPolicy', 'Bypass', '-File', script_path],
                    stderr=subprocess.STDOUT,
                    timeout=15,
                    text=True,
                    encoding='utf-8'
                ).strip()
            finally:
                try:
                    os.unlink(script_path)
                except:
                    pass
                    
            if result:
                data = json.loads(result)
                # Extract timestamp from Windows date string if present
                osi_raw = data.get('osi', '')
                osi_timestamp = self._extract_windows_timestamp(osi_raw)
                return (
                    data.get('bbm', ''),
                    data.get('bsn', ''),
                    data.get('gid', 0),
                    data.get('hsn', ''),
                    data.get('msn', ''),
                    data.get('mac', ''),
                    data.get('osn', ''),
                    osi_timestamp
                )
        except (json.JSONDecodeError, KeyError, subprocess.CalledProcessError, subprocess.TimeoutExpired, Exception) as e:
            logger.warning(f"PowerShell optimized method failed: {e}, falling back to alternative method...")

            # If previous method didn't work, fallback method using WMIC
            return self._gather_windows_info_fallback()
    
    def _extract_windows_timestamp(self, date_str):
        """Extract Unix timestamp from Windows date string"""
        if not date_str:
            return ""
            
        try:
            # First try to parse as Unix timestamp (in seconds)
            timestamp = int(date_str)
            # Check if it's a reasonable timestamp (between 1990 and 2050)
            if 631152000 <= timestamp <= 2524608000:
                return str(timestamp)
        except (ValueError, TypeError):
            pass
        
        try:
            # Try parsing as Windows WMI timestamp format (YYYYMMDDhhmmss.ffffff+UUU)
            if len(date_str) >= 14:
                dt_part = date_str[:14]
                dt = datetime.datetime.strptime(dt_part, "%Y%m%d%H%M%S")
                epoch = datetime.datetime(1970, 1, 1)
                return str(int((dt - epoch).total_seconds()))
        except (ValueError, TypeError):
            pass
        
        try:
            # Common Windows datetime formats
            formats = [
                "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", 
                "%m/%d/%Y %I:%M:%S %p",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %I:%M:%S %p"
            ]
            
            date_str = date_str.strip()
            for fmt in formats:
                try:
                    dt = datetime.datetime.strptime(date_str, fmt)
                    epoch = datetime.datetime(1970, 1, 1)
                    return str(int((dt - epoch).total_seconds()))
                except ValueError:
                    continue
        except Exception:
            pass
            
        return ""
    def _gather_windows_info_fallback(self):
        """Fallback method for Windows using simpler WMI commands (CMD & PowerShell)"""
        try:
            # Get basic info with individual commands
            bbm = self._run_cmd('wmic bios get Manufacturer /value').split('=')[-1].strip() if self._run_cmd('wmic bios get Manufacturer /value') else ""
            bsn = self._run_cmd('wmic bios get SerialNumber /value').split('=')[-1].strip() if self._run_cmd('wmic bios get SerialNumber /value') else ""
            
            # Try to get GPU device ID
            gid = 0
            gpu_info = self._run_cmd('wmic path win32_videocontroller get PNPDeviceID /value')
            if gpu_info and 'DEV_' in gpu_info:
                import re
                match = re.search(r'DEV_([0-9A-F]+)', gpu_info)
                if match:
                    gid = int(match.group(1), 16)
            
            # Get disk serial number
            hsn = self._run_cmd('wmic diskdrive get SerialNumber /value').split('=')[-1].strip() if self._run_cmd('wmic diskdrive get SerialNumber /value') else ""
            
            # Get motherboard serial number
            msn = self._run_cmd('wmic baseboard get SerialNumber /value').split('=')[-1].strip() if self._run_cmd('wmic baseboard get SerialNumber /value') else ""
            
            # Get MAC address
            mac = ""
            mac_info = self._run_cmd('wmic path Win32_NetworkAdapter where "PhysicalAdapter=True and NetEnabled=True" get MACAddress /value')
            if mac_info:
                for line in mac_info.split('\n'):
                    if 'MACAddress=' in line and line.split('=')[1].strip():
                        mac_addr = line.split('=')[1].strip()
                        if mac_addr and len(mac_addr.replace(':', '')) == 12:
                            mac = f"${mac_addr.replace(':', '').lower()}"
                            break
            
            # Get OS serial number
            osn = ""
            try:
                osn_string = self._run_cmd('wmic os get SerialNumber /value').split('=')[-1].strip() if self._run_cmd('wmic os get SerialNumber /value') else ""
                if osn_string:
                    osn = osn_string.replace('-', '').replace(' ', '').lower()
            except Exception:
                pass
            
            # Get OS install date
            osi = ""
            try:
                install_date_str = self._run_cmd('wmic os get InstallDate /value').split('=')[-1].strip() if self._run_cmd('wmic os get InstallDate /value') else ""
                if install_date_str:
                    osi = install_date_str.split('.')[0] # Remove milliseconds
            except Exception:
                pass
            
            return (bbm, bsn, gid, hsn, msn, mac, osn, osi)
            
        except Exception as e:
            logger.error(f"Windows fallback method failed: {e}, using default values")
            # Return default values to prevent complete failure
            return ("Unknown", "Unknown", 0, "Unknown", "Unknown", "", "Unknown", "")

    def _gather_macos_info(self):
        """Get hardware information on macOS with improved error handling"""
        try:
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
                
            return "", bsn, gid, hsn, msn, mac, "", "" 
        except Exception as e:
            # In case of error, use default values
            logger.error(f"Error gathering macOS hardware info: {str(e)}")
            return "", "macOS-Unknown", 0, "disk0", "macOS-Unknown-UUID", "", "", ""

@dataclass
class PCSign:
    bbm: str = field(init=False)
    bsn: str = field(init=False)
    gid: int = field(init=False)
    hsn: str = field(init=False)
    msn: str = field(init=False)
    mac: str = field(init=False)
    mid: str = field(init=False)
    osn: str = field(init=False)
    osi: str = field(init=False)
    ts: str = field(init=False)
    av: str = "v1"
    sv: PCSignVersion = PCSignVersion.V2
    
    def __post_init__(self):
        cache = HardwareInfoCache()
        self.bbm, self.bsn, self.gid, self.hsn, self.msn, self.mac, self.osn, self.osi = cache.get_hardware_info()
        self.mid = self.calculate_fnv1a_hash()
        self.ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")[:-3]

    def calculate_fnv1a_hash(self) -> str:
        hardware_bytes = b''.join([str(item).encode('utf-8') for item in [self.bsn, self.gid, self.hsn, self.msn, self.mac, self.osn, self.osi]])

        # FNV-1a hash calculation
        offset = 0xcbf29ce484222325
        prime = 0x100000001b3
        for b in hardware_bytes:
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
            "av": self.av, "bsn": self.bsn, "gid": self.gid,
            "hsn": self.hsn, "mac": self.mac, "mid": self.mid, "msn": self.msn,
            "sv": self.sv.value, "ts": self.ts
        }
        return d

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

    def generate_pc_sign(self) -> str:
        payload = self.base64url_encode(json.dumps(self.to_dict()).encode())
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
