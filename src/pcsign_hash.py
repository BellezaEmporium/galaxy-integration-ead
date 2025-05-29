###
#----------------------------------------------------PC SIGN----------------------------------------------------#
# EA Desktop way of linking your login info to your PC. This is a hash of your hardware info and a timestamp.
# The hash is signed with a secret key to prevent tampering. The server can verify the hash with the secret key.
# The server can also generate the hash itself and compare it to the one sent by the client.
# It, then, can decide if the client is allowed to log in.
#---------------------------------------------------------------------------------------------------------------#
# Kudos to @imLinguin for the necessary info.
###

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
from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple

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
            self._cache_duration = 3600  # 1 heure en secondes
            self._initialized = True
    
    def set_cache_duration(self, duration_seconds: int):
        """Permet de modifier la durée du cache (utile pour les tests)"""
        with self._lock:
            self._cache_duration = duration_seconds
            # Invalider le cache actuel si on raccourcit la durée
            if (self._cache_time is not None and 
                time.time() - self._cache_time >= duration_seconds):
                self._cache = None
                self._cache_time = None
    
    def clear_cache(self):
        """Force l'invalidation du cache"""
        with self._lock:
            self._cache = None
            self._cache_time = None
    
    def get_hardware_info(self) -> Tuple[str, str, int, str, str, str, str, str]:
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
            
            if os.name == "nt":
                self._cache = self._gather_windows_info_optimized()
            elif os.name == "posix" and platform.system() == "Darwin":
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
            import logging
            logging.warning(f"Command timed out: {cmd[:50]}...")
            return ""
        except subprocess.CalledProcessError as e:
            import logging
            logging.warning(f"Command failed with return code {e.returncode}: {cmd[:50]}...")
            return ""
        except Exception as e:
            import logging
            logging.warning(f"Unexpected error running command: {e}")
            return ""
    
    def _gather_windows_info_optimized(self):
        """Get hardware information on Windows with improved error handling"""
        ps_script = '''
        try {
            $bios = Get-CimInstance -ClassName Win32_BIOS
            $baseBoard = Get-CimInstance -ClassName Win32_BaseBoard
            $os = Get-CimInstance -ClassName Win32_OperatingSystem
            $videoControllers = Get-CimInstance -ClassName Win32_VideoController
            $diskDrive = Get-WmiObject -Class Win32_DiskDrive | Select-Object -Index 0
            $networkAdapter = Get-CimInstance Win32_NetworkAdapter | Where-Object { $_.PhysicalAdapter -and $_.NetEnabled -and $_.ServiceName -notmatch 'vmnetadapter|vboxnetadp|ndisip|tap|hyperv|loopback' } | Select-Object -First 1
            
            $gid = 0
            foreach ($gpu in $videoControllers) {
                if ($gpu.PNPDeviceID -match "DEV_([0-9A-F]+)") {
                    $gid = [Convert]::ToInt32($matches[1], 16)
                    break
                }
            }
            
            $mac = ""
            if ($networkAdapter -and $networkAdapter.MACAddress) {
                $mac = "$" + ($networkAdapter.MACAddress -replace ':', '' -replace '-', '').ToLower()
            }
            
            $osiTimestamp = ""
            if ($os.InstallDate) {
                $osiTimestamp = $os.InstallDate.ToString()
            }
            
            @{
                bbm = if ($bios.Manufacturer) { $bios.Manufacturer } else { "" }
                bsn = if ($bios.SerialNumber) { $bios.SerialNumber } else { "" }
                gid = $gid
                hsn = if ($diskDrive.SerialNumber) { $diskDrive.SerialNumber } else { "" }
                msn = if ($baseBoard.SerialNumber) { $baseBoard.SerialNumber } else { "" }
                mac = $mac
                osn = if ($os.SerialNumber) { $os.SerialNumber } else { "" }
                osi = $osiTimestamp
            } | ConvertTo-Json -Compress
        } catch {
            @{
                bbm = ""
                bsn = ""
                gid = 0
                hsn = ""
                msn = ""
                mac = ""
                osn = ""
                osi = ""
            } | ConvertTo-Json -Compress
        }
        '''
        
        try:
            result = self._run_cmd(f'powershell -Command "{ps_script}"')
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
        except (json.JSONDecodeError, KeyError, Exception) as e:
            import logging
            logging.warning(f"PowerShell optimized method failed: {e}, falling back to alternative method...")
        
        # Fallback method using simpler commands
        return self._gather_windows_info_fallback()
    
    def _extract_windows_timestamp(self, date_str):
        """Extract Unix timestamp from Windows date string"""
        import re
        import datetime
        
        if not date_str:
            return ""
            
        try:
            # Check if it's in /Date(timestamp)/ format
            match = re.search(r'/Date\((\d+)\)/', date_str)
            if match:
                # Convert from milliseconds to seconds
                timestamp_ms = int(match.group(1))
                return str(timestamp_ms)
            
            # Try to parse as regular datetime string
            # Common Windows datetime formats (based on the actual output we saw)
            formats = [
                "%m/%d/%Y %H:%M:%S",  # 03/03/2025 14:26:45
                "%d/%m/%Y %H:%M:%S",  # 03/03/2025 14:26:45 (day/month)
                "%m/%d/%Y %I:%M:%S %p",  # 12/31/2024 3:00:00 PM
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %I:%M:%S %p"
            ]
            
            # Clean the date string
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
        """Fallback method for Windows using simpler WMI commands"""
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
                install_date_str = self._run_cmd('powershell -Command "(Get-CimInstance -ClassName Win32_OperatingSystem).InstallDate.ToString()"')
                if install_date_str:
                    osi = self._extract_windows_timestamp(install_date_str)
            except Exception:
                pass
            
            return (bbm, bsn, gid, hsn, msn, mac, osn, osi)
            
        except Exception as e:
            import logging
            logging.error(f"Windows fallback method failed: {e}, using default values")
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
            import logging
            logging.error(f"Error gathering macOS hardware info: {str(e)}")
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


def generate_pc_sign_fast(sv: PCSignVersion = random.choice(list(PCSignVersion))) -> str:
    return PCSign.generate_fast(sv)

def preload_pc_sign_cache():
    PCSign.preload_hardware_cache()
