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
import subprocess
import datetime
import base64
import hmac
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

class PCSignVersion(Enum):
    V1 = "v1"
    V2 = "v2"

@dataclass
class PCSign:
    bsn: str = field(init=False)
    gid: int = field(init=False)
    hsn: str = field(init=False)
    msn: str = field(init=False)
    mac: Optional[str] = None
    mid: str = field(init=False)
    ts: str = field(init=False)
    av: str = "v1"
    sv: PCSignVersion = PCSignVersion.V1
    
    def __post_init__(self):
        self.bsn, self.gid, self.hsn, self.msn, self.mac = self.gather_hardware_info()
        self.mid = self.calculate_fnv1a_hash(self.bsn, self.gid, self.hsn, self.msn)
        self.ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")[:-3]

    def gather_hardware_info(self):
        if os.name == "nt":
            return self._gather_windows_info()
        elif os.name == "posix":
            return self._gather_macos_info()
        else:
            raise OSError("Unsupported OS")

    def _run_cmd(self, cmd):
        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            return output.decode().strip()
        except subprocess.CalledProcessError:
            return ""

    def _gather_windows_info(self):
        bsn = self._run_cmd("powershell -Command \"(Get-CimInstance -ClassName Win32_BIOS).SerialNumber\"").strip()
        gpu_line = self._run_cmd("powershell -Command \"(Get-CimInstance -ClassName Win32_VideoController).PNPDeviceID\"").strip()
        gid = int(gpu_line.split('DEV_')[1].split('&')[0], 16) if "DEV_" in gpu_line else 0
        hsn = self._run_cmd("powershell -Command \"(Get-CimInstance -ClassName Win32_DiskDrive).SerialNumber\"").strip()
        msn = self._run_cmd("powershell -Command \"(Get-CimInstance -ClassName Win32_BaseBoard).SerialNumber\"").strip()
        mac_line = self._run_cmd("powershell -Command \"(Get-CimInstance -ClassName Win32_NetworkAdapter | Where-Object {$_.PhysicalAdapter}) | Select-Object -First 1 -ExpandProperty MACAddress\"").strip()
        mac = mac_line if mac_line else None
        return bsn, gid, hsn, msn, mac

    def _gather_macos_info(self):
        """Get hardware information on macOS with improved error handling"""
        try:
            bsn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Serial Number/ {print $NF}'")
            gid_str = self._run_cmd("system_profiler SPDisplaysDataType | awk '/Device ID:/ {print $NF}'")
            gid = int(gid_str, 16) if gid_str and gid_str.strip() else 0
            hsn = self._run_cmd("diskutil info /dev/disk0 | awk '/Device Identifier:/ {print $NF}'")
            msn = self._run_cmd("system_profiler SPHardwareDataType | awk '/Hardware UUID:/ {print $NF}'")
            
            # Try to get MAC address from different interfaces
            mac = None
            for interface in ["en0", "en1", "en2"]:
                mac_addr = self._run_cmd(f"ifconfig {interface} | awk '/ether/ {{print $2}}'")
                if mac_addr and mac_addr.strip():
                    mac = mac_addr
                    break
                    
            # Default values in case of failure
            if not bsn or bsn == "N/A":
                bsn = "macOS-Unknown"
            if not hsn:
                hsn = "disk0"
            if not msn:
                msn = "macOS-Unknown-UUID"
                
            return bsn, gid, hsn, msn, mac
        except Exception as e:
            # In case of error, use default values
            import logging
            logging.error(f"Error gathering macOS hardware info: {str(e)}")
            return "macOS-Unknown", 0, "disk0", "macOS-Unknown-UUID", None
    
    @staticmethod
    def calculate_fnv1a_hash(bsn, gid, hsn, msn):
        hardware_bytes = f"{bsn}{gid}{hsn}{msn}".encode()
        offset = 0xcbf29ce484222325
        prime = 0x100000001b3
        for b in hardware_bytes:
            offset ^= b
            offset = (offset * prime) & 0xFFFFFFFFFFFFFFFF
        return f"{offset:016x}"

    def sign_key(self):
        keys = {
            PCSignVersion.V1: b"ISa3dpGOc8wW7Adn4auACSQmaccrOyR2",
            PCSignVersion.V2: b"nt5FfJbdPzNcl2pkC3zgjO43Knvscxft"
        }
        return keys.get(self.sv)

    def to_dict(self):
        d = {
            "av": self.av, "bsn": self.bsn, "gid": self.gid,
            "hsn": self.hsn, "mid": self.mid, "msn": self.msn,
            "sv": self.sv.value, "ts": self.ts
        }
        if self.mac: d["mac"] = self.mac
        return d

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

    def generate_pc_sign(self) -> str:
        payload = self.base64url_encode(json.dumps(self.to_dict()).encode())
        signature = hmac.new(self.sign_key(), payload.encode(), hashlib.sha256).digest()
        return f"{payload}.{self.base64url_encode(signature)}"
