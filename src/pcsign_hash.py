###
#----------------------------------------------------PC SIGN----------------------------------------------------#
# EA Desktop's way of linking your login info to your PC. This is a hash of your hardware info and a timestamp.
# The hash is signed with a secret key to prevent tampering. The server can verify the hash with the secret key.
# The server can also generate the hash itself and compare it to the one sent by the client.
# It, then, can decide if the client is allowed to log in.
#---------------------------------------------------------------------------------------------------------------#
# Kudos to @imLinguin & ArmchairDevelopers for the necessary info.
###

import base64
import datetime
import hashlib
import hmac
import json
import logging
import platform
import random
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

_SIGN_KEY_V1 = base64.b64decode("SVNhM2RwR09jOHdXN0FkbjRhdUFDU1FtYWNjck95UjI=")
_SIGN_KEY_V2 = base64.b64decode("bnQ1RmZKYmRQek5jbDJwa0MzemdqTzQzS252c2N4ZnQ=")

HASH_SCHEMAS: Dict[int, List[str]] = {
    0: [
        "board_manufacturer", "board_sn", "hostname",
        "bios_manufacturer", "bios_sn",
        "os_install_date", "os_sn",
    ],
    1: [
        "board_manufacturer", "board_sn", "hostname",
        "bios_manufacturer", "bios_sn",
        "os_install_date", "os_sn",
    ],
    2: [
        "board_manufacturer", "board_sn",
        "bios_manufacturer", "bios_sn",
        "os_install_date", "os_sn",
        "volume_sn", "gpu_pnp_id",
        "cpu_manufacturer", "cpu_edx", "cpu_ecx",
    ],
    3: [
        "board_manufacturer", "board_sn",
        "bios_manufacturer", "bios_sn",
        "volume_sn", "gpu_pnp_id",
        "cpu_manufacturer", "cpu_edx", "cpu_ecx",
    ],
    4: [
        "board_manufacturer", "board_sn",
        "bios_manufacturer", "bios_sn",
        "volume_sn", "gpu_pnp_id",
        "cpu_manufacturer", "cpu_edx_eax",
    ],
}

_LATEST_SCHEMA_VERSION = max(HASH_SCHEMAS.keys())


# =============================================================================
# Exceptions
# =============================================================================

class HardwareProbeError(Exception):
    """Raised when hardware probing fails critically."""
    pass


class UnsupportedPlatformError(HardwareProbeError):
    """Raised on unsupported operating systems."""
    pass


# =============================================================================
# Hash Utilities
# =============================================================================

def hash_fnv1a(data: bytes) -> int:
    """64-bit FNV-1a hash."""
    offset = 0xcbf29ce484222325
    prime = 0x100000001b3
    for byte in data:
        offset ^= byte
        offset = (offset * prime) & 0xFFFFFFFFFFFFFFFF
    return offset


def _b64url_encode(data: bytes) -> str:
    """Base64url encoding without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


# =============================================================================
# CPU Information (No Shellcode)
# =============================================================================

@dataclass(frozen=True)
class CpuDetails:
    """Immutable CPU details gathered without shellcode."""
    eax: int = 0
    ebx: int = 0
    ecx: int = 0
    edx: int = 0
    manufacturer: str = ""
    brand_name: str = ""

    @classmethod
    def gather(cls) -> "CpuDetails":
        """Gather CPU info using platform-native methods."""
        system = platform.system()

        eax, ebx, ecx, edx = 0, 0, 0, 0
        manufacturer = ""
        brand_name = platform.processor() or ""

        if system == "Windows":
            manufacturer, brand_name, eax, ebx, ecx, edx = cls._from_windows()
        elif system == "Linux":
            manufacturer, brand_name, eax, ebx, ecx, edx = cls._from_linux()
        elif system == "Darwin":
            manufacturer, brand_name = cls._from_macos()

        return cls(
            eax=eax, ebx=ebx, ecx=ecx, edx=edx,
            manufacturer=manufacturer, brand_name=brand_name,
        )

    @classmethod
    def _from_windows(cls) -> Tuple[str, str, int, int, int, int]:
        """Get CPU info on Windows via PowerShell Get-CimInstance."""
        try:
            ps_cmd = (
                "Get-CimInstance Win32_Processor | "
                "Select-Object -First 1 Name,Manufacturer,ProcessorId | "
                "ConvertTo-Json -Compress"
            )
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True, text=True, timeout=10,
                encoding="utf-8", errors="ignore",
            )
            if result.returncode != 0:
                return cls._from_windows_com()

            data = json.loads(result.stdout.strip())
            name = data.get("Name", platform.processor())
            manufacturer = data.get("Manufacturer", "")
            processor_id = data.get("ProcessorId", "")

            eax, ebx, ecx, edx = 0, 0, 0, 0
            if processor_id and len(processor_id) >= 16:
                try:
                    pid_val = int(processor_id, 16)
                    edx = (pid_val >> 96) & 0xFFFFFFFF
                    ecx = (pid_val >> 64) & 0xFFFFFFFF
                    ebx = (pid_val >> 32) & 0xFFFFFFFF
                    eax = pid_val & 0xFFFFFFFF
                except ValueError:
                    pass

            return manufacturer, name, eax, ebx, ecx, edx
        except Exception as exc:
            logger.debug("PowerShell CPU probe failed: %s", exc)
            return cls._from_windows_com()

    @classmethod
    def _from_windows_com(cls) -> Tuple[str, str, int, int, int, int]:
        """Fallback: WMI COM interface via ctypes (no PowerShell)."""
        try:
            import comtypes
            from comtypes.client import CreateObject
            locator = CreateObject("WbemScripting.SWbemLocator")
            service = locator.ConnectServer(".", "root\\cimv2")
            processors = service.ExecQuery("SELECT Name,Manufacturer,ProcessorId FROM Win32_Processor")
            for proc in processors:
                name = getattr(proc, "Name", platform.processor())
                manufacturer = getattr(proc, "Manufacturer", "")
                processor_id = getattr(proc, "ProcessorId", "")
                eax, ebx, ecx, edx = 0, 0, 0, 0
                if processor_id and len(processor_id) >= 16:
                    try:
                        pid_val = int(processor_id, 16)
                        edx = (pid_val >> 96) & 0xFFFFFFFF
                        ecx = (pid_val >> 64) & 0xFFFFFFFF
                        ebx = (pid_val >> 32) & 0xFFFFFFFF
                        eax = pid_val & 0xFFFFFFFF
                    except ValueError:
                        pass
                return manufacturer, name, eax, ebx, ecx, edx
        except Exception as exc:
            logger.debug("COM CPU probe failed: %s", exc)
        return "", platform.processor(), 0, 0, 0, 0

    @classmethod
    def _from_linux(cls) -> Tuple[str, str, int, int, int, int]:
        """Parse /proc/cpuinfo for CPU details."""
        manufacturer = ""
        brand_name = ""
        eax, ebx, ecx, edx = 0, 0, 0, 0

        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("vendor_id\t:"):
                        manufacturer = line.split(":", 1)[1].strip()
                    elif line.startswith("model name\t:"):
                        brand_name = line.split(":", 1)[1].strip()
                    elif line.startswith("cpu family\t:"):
                        try:
                            eax |= int(line.split(":", 1)[1].strip()) << 8
                        except ValueError:
                            pass
                    elif line.startswith("model\t\t:"):
                        try:
                            eax |= int(line.split(":", 1)[1].strip()) << 4
                        except ValueError:
                            pass
                    elif line.startswith("stepping\t:"):
                        try:
                            eax |= int(line.split(":", 1)[1].strip())
                        except ValueError:
                            pass
                    elif line.startswith("flags\t\t:"):
                        flags = line.split(":", 1)[1].strip().split()
                        if "lm" in flags:
                            edx |= 1 << 29
                        if "sse2" in flags:
                            edx |= 1 << 26
        except Exception as exc:
            logger.debug("Linux CPU probe failed: %s", exc)

        return manufacturer, brand_name, eax, ebx, ecx, edx

    @classmethod
    def _from_macos(cls) -> Tuple[str, str]:
        """Get CPU info on macOS via sysctl."""
        manufacturer = ""
        brand_name = ""
        try:
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                brand_name = result.stdout.strip()
                if "Intel" in brand_name:
                    manufacturer = "GenuineIntel"
                elif "AMD" in brand_name:
                    manufacturer = "AuthenticAMD"
                else:
                    manufacturer = "Apple"
        except Exception as exc:
            logger.debug("macOS CPU probe failed: %s", exc)
        return manufacturer, brand_name


# =============================================================================
# GPU PNP ID Generation
# =============================================================================

def generate_pci_pnp_id(
    version: int,
    vendor: Optional[int],
    device: Optional[int],
    revision: Optional[int],
) -> str:
    """Build a PCI PNP ID string matching EA's format."""
    sections = [
        f"VEN_{vendor or 0:04X}",
        f"DEV_{device or 0:04X}",
        f"SUBSYS_{0:08X}",
    ]
    if version < 4:
        sections.append(f"REV_{revision or 0:02X}")
    else:
        sections.append(f"REV_{revision or 0:02X}\\0")
        sections.append(f"{0xDEADBEEF:08X}")
        sections.append(f"{0:X}")
        sections.append(f"{0xDEAD:04X}")

    return "PCI\\" + "&".join(sections)


# =============================================================================
# MAC Address
# =============================================================================

def _get_ea_mac_address() -> Optional[str]:
    """Return MAC as '$<hex>' matching EA's format."""
    try:
        import uuid
        raw = uuid.getnode()
        if raw >> 40 & 1:  # multicast / locally administered
            return None
        mac_hex = f"{raw:012x}"
        return f"${mac_hex}"
    except Exception:
        return None


# =============================================================================
# Windows Hardware Probing
# =============================================================================

_PS_HW_PROBE = r"""
$ErrorActionPreference = "SilentlyContinue"

try {
    $cimCmdlet = "Get-CimInstance"

    $bios = & $cimCmdlet Win32_BIOS | Select-Object -First 1
    $biosMan = if ($bios -and $bios.Manufacturer) { $bios.Manufacturer.ToString() } else { "" }
    $biosSerial = if ($bios -and $bios.SerialNumber) { $bios.SerialNumber.ToString() } else { "" }

    $bb = & $cimCmdlet Win32_BaseBoard | Select-Object -First 1
    $bbMan = if ($bb -and $bb.Manufacturer) { $bb.Manufacturer.ToString() } else { "" }
    $bbSerial = if ($bb -and $bb.SerialNumber) { $bb.SerialNumber.ToString() } else { "" }

    $os = & $cimCmdlet Win32_OperatingSystem | Select-Object -First 1
    $osSerial = if ($os -and $os.SerialNumber) { $os.SerialNumber.ToString() } else { "" }

    # EA uses a specific WMI object for the OS Install Date
    $osWmi = Get-WmiObject -Class Win32_OperatingSystem
    $installDate = if ($osWmi -and $osWmi.InstallDate) { $osWmi.InstallDate.ToString() } else { "0" }

    $videoControllers = & $cimCmdlet Win32_VideoController |
        Where-Object { $_.PNPDeviceID -match "DEV_[0-9A-F]+" } | Select-Object -First 1
    $diskDrive = & $cimCmdlet Win32_DiskDrive | Select-Object -First 1
    $networkAdapter = & $cimCmdlet Win32_NetworkAdapter |
        Where-Object { $_.PhysicalAdapter -eq $true -and $_.NetEnabled -eq $true -and
            $_.ServiceName -notmatch "vmnetadapter|vboxnetadp|ndisip|tap|hyperv|loopback" -and
            $_.MACAddress } | Select-Object -First 1

    # Extract GPU device ID (hex -> int)
    $gid = 0
    if ($videoControllers -and $videoControllers.PNPDeviceID) {
        if ($videoControllers.PNPDeviceID -match "DEV_([0-9A-F]+)") {
            try { $gid = [Convert]::ToInt32($matches[1], 16) } catch { $gid = 0 }
        }
    }

    # Format MAC as before
    $mac = ""
    if ($networkAdapter -and $networkAdapter.MACAddress) {
        $macClean = $networkAdapter.MACAddress -replace "[:-]", ""
        if ($macClean) { $mac = "$" + $macClean.ToLower() }
    }

    $result = @{
        bbm = $biosMan
        bsn = $biosSerial
        gid = $gid
        hsn = if ($diskDrive -and $diskDrive.SerialNumber) { $diskDrive.SerialNumber.ToString().Trim() } else { "" }
        mbm = $bbMan
        msn = $bbSerial
        mac = $mac
        osi = $installDate
        osn = $osSerial
    }

    $result | ConvertTo-Json -Compress
} catch {
    $errorResult = @{
        bbm = ""
        bsn = ""
        gid = 0
        hsn = ""
        mbm = ""
        msn = ""
        mac = ""
        osn = ""
        osi = "0"
    }
    $errorResult | ConvertTo-Json -Compress
}
""".strip()


def _probe_windows_all() -> Dict[str, Any]:
    """Single PowerShell invocation for all Windows hardware info."""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", _PS_HW_PROBE],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="ignore",
        )
        if result.returncode != 0:
            logger.warning("PowerShell hardware probe failed: %s", result.stderr[:200])
            return _probe_windows_fallback()

        raw = result.stdout.strip()
        if not raw:
            return _probe_windows_fallback()

        data = json.loads(raw)
        if not isinstance(data, dict):
            return _probe_windows_fallback()

        # Map PS keys to our internal keys
        install_date = data.get("osi", "0")
        if install_date == "0" or not install_date:
            install_date = "1970-01-0100:00:00.000000000+0000"

        return {
            "board_manufacturer": data.get("mbm", "Microsoft Corporation") or "Microsoft Corporation",
            "board_sn": data.get("msn", "None") or "None",
            "bios_manufacturer": data.get("bbm", "Microsoft Corporation") or "Microsoft Corporation",
            "bios_sn": data.get("bsn", "None") or "None",
            "os_sn": data.get("osn", "None") or "None",
            "os_install_date": install_date,
            "disk_sn": data.get("hsn", "None") or "None",
            "volume_sn": _get_windows_volume_sn(),
            "gpu_pnp_id": None,  # Will be generated from gid
            "gid": data.get("gid", 0),
            "mac": data.get("mac") or None,
        }
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse PowerShell output: %s", exc)
        return _probe_windows_fallback()
    except Exception as exc:
        logger.warning("PowerShell probe exception: %s", exc)
        return _probe_windows_fallback()


def _probe_windows_fallback() -> Dict[str, Any]:
    """Fallback using individual CIM queries if the monolithic PS fails."""
    logger.debug("Using fallback Windows probing")

    def cim_query(class_name: str, properties: List[str], where: Optional[str] = None) -> List[Dict[str, Any]]:
        prop_str = ",".join(properties)
        where_clause = f"| Where-Object {{ {where} }}" if where else ""
        ps_cmd = (
            f"Get-CimInstance {class_name} {where_clause} | "
            f"Select-Object -First 1 {prop_str} | "
            f"ConvertTo-Json -Compress"
        )
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True, text=True, timeout=15,
                encoding="utf-8", errors="ignore",
            )
            if result.returncode != 0 or not result.stdout.strip():
                return []
            data = json.loads(result.stdout.strip())
            return [data] if isinstance(data, dict) else data if isinstance(data, list) else []
        except Exception:
            return []

    baseboard = cim_query("Win32_BaseBoard", ["Manufacturer", "SerialNumber"])
    bios = cim_query("Win32_BIOS", ["Manufacturer", "SerialNumber"])
    os_info = cim_query("Win32_OperatingSystem", ["SerialNumber", "InstallDate"])
    disk = cim_query("Win32_DiskDrive", ["SerialNumber"], where="Index -eq 0")

    # GPU with DEV_ pattern matching
    gpus = cim_query("Win32_VideoController", ["PNPDeviceID", "DeviceID"])
    gpu_pnp = None
    gid = 0
    for gpu in gpus:
        pnp = gpu.get("PNPDeviceID", "")
        if pnp and "DEV_" in pnp:
            gpu_pnp = pnp
            m = re.search(r"DEV_(\w+)", pnp)
            if m:
                try:
                    gid = int(m.group(1), 16)
                except ValueError:
                    pass
            break

    bb = baseboard[0] if baseboard else {}
    b = bios[0] if bios else {}
    o = os_info[0] if os_info else {}
    d = disk[0] if disk else {}

    install_date = o.get("InstallDate", "")
    if install_date and len(install_date) >= 14:
        install_date = (
            f"{install_date[:4]}-{install_date[4:6]}-{install_date[6:8]}"
            f"{install_date[8:10]}:{install_date[10:12]}:{install_date[12:14]}"
            f"{install_date[14:]}"
        )
    else:
        install_date = "1970-01-0100:00:00.000000000+0000"

    return {
        "board_manufacturer": bb.get("Manufacturer", "Microsoft Corporation") or "Microsoft Corporation",
        "board_sn": bb.get("SerialNumber", "None") or "None",
        "bios_manufacturer": b.get("Manufacturer", "Microsoft Corporation") or "Microsoft Corporation",
        "bios_sn": b.get("SerialNumber", "None") or "None",
        "os_sn": o.get("SerialNumber", "None") or "None",
        "os_install_date": install_date,
        "disk_sn": d.get("SerialNumber", "None") or "None",
        "volume_sn": _get_windows_volume_sn(),
        "gpu_pnp_id": gpu_pnp,
        "gid": gid,
        "mac": _get_ea_mac_address(),
    }


def _get_windows_volume_sn() -> str:
    """Get C: drive volume serial number via ctypes."""
    try:
        import ctypes
        vol_serial = ctypes.c_uint32(0)
        ctypes.windll.kernel32.GetVolumeInformationW(
            "C:\\", None, 0, ctypes.byref(vol_serial),
            None, None, None, 0,
        )
        return f"{vol_serial.value:08x}"
    except Exception:
        return "00000000"


# =============================================================================
# macOS Hardware Probing
# =============================================================================

def _probe_macos_all() -> Dict[str, Any]:
    """Probe hardware info on macOS."""
    def run(*args: str, timeout: int = 10) -> str:
        try:
            r = subprocess.run(
                list(args), capture_output=True, text=True,
                timeout=timeout, encoding="utf-8", errors="ignore",
            )
            return r.stdout if r.returncode == 0 else ""
        except Exception:
            return ""

    ioreg = run("ioreg", "-d2", "-c", "IOPlatformExpertDevice")

    def extract_ioreg(key: str) -> str:
        m = re.search(rf'"{key}"\s*=\s*"([^"]+)"', ioreg)
        return m.group(1) if m else ""

    board_sn = extract_ioreg("IOPlatformSerialNumber") or "None"
    os_sn = extract_ioreg("IOPlatformUUID") or "None"

    disk_sn = "None"
    diskutil_out = run("diskutil", "info", "/")
    for line in diskutil_out.split("\n"):
        if line.strip().startswith("Volume UUID:"):
            parts = line.split()
            if len(parts) >= 3:
                disk_sn = parts[2]
                break

    gpu_pnp_id = None
    device_id = 0
    revision_id = 0
    sp_json = run("system_profiler", "SPDisplaysDataType", "-json")
    if sp_json:
        try:
            sp_data = json.loads(sp_json)
            items = sp_data.get("SPDisplaysDataType", [])
            if items:
                gpu = items[0]
                device_id = int(gpu.get("spdisplays_device-id", "0x0000"), 16)
                revision_id = int(gpu.get("spdisplays_revision-id", "0x00"), 16)
        except Exception as exc:
            logger.debug("system_profiler GPU parse failed: %s", exc)

    return {
        "board_manufacturer": "Apple Inc.",
        "board_sn": board_sn,
        "bios_manufacturer": "Apple Inc.",
        "bios_sn": board_sn,
        "os_sn": os_sn,
        "os_install_date": "1970010100:00:00.000000000+0000",
        "disk_sn": disk_sn,
        "volume_sn": "43000000",
        "gpu_pnp_id": None,
        "gid": device_id,
        "gpu_device_id": device_id,
        "gpu_revision_id": revision_id,
    }


# =============================================================================
# Hardware Info Dataclass
# =============================================================================

@dataclass(frozen=True)
class HardwareInfo:
    """Immutable hardware fingerprint data."""
    version: int
    board_manufacturer: str = ""
    board_sn: str = ""
    bios_manufacturer: str = ""
    bios_sn: str = ""
    os_install_date: str = ""
    os_sn: str = ""
    disk_sn: str = ""
    volume_sn: str = ""
    gpu_pnp_id: Optional[str] = None
    mac: Optional[str] = None
    cpu_details: CpuDetails = field(default_factory=CpuDetails)
    hostname: str = ""

    cpu_manufacturer: str = field(init=False, repr=False)
    cpu_edx: str = field(init=False, repr=False)
    cpu_ecx: str = field(init=False, repr=False)
    cpu_edx_eax: str = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "cpu_manufacturer", self.cpu_details.manufacturer or "")
        object.__setattr__(self, "cpu_edx", f"{self.cpu_details.edx:08x}")
        object.__setattr__(self, "cpu_ecx", f"{self.cpu_details.ecx:08x}")
        object.__setattr__(self, "cpu_edx_eax", f"{self.cpu_details.edx:08X}{self.cpu_details.eax:08X}")

    @classmethod
    def new(cls, version: int) -> "HardwareInfo":
        """Factory: probe hardware for the given hash version."""
        system = platform.system()

        if system == "Windows":
            data = _probe_windows_all()
        elif system == "Darwin":
            data = _probe_macos_all()
            data["gpu_pnp_id"] = generate_pci_pnp_id(
                version, None, data.pop("gpu_device_id", 0), data.pop("gpu_revision_id", 0)
            )
        else:
            raise UnsupportedPlatformError(f"Unsupported OS: {system}")

        mac = data.get("mac") or _get_ea_mac_address()
        cpu = CpuDetails.gather()

        # Generate GPU PNP ID from gid if not already set
        gpu_pnp = data.get("gpu_pnp_id")
        gid = data.get("gid", 0)
        if gpu_pnp is None and gid:
            gpu_pnp = generate_pci_pnp_id(version, None, gid, None)

        return cls(
            version=version,
            board_manufacturer=data.get("board_manufacturer", "Microsoft Corporation"),
            board_sn=data.get("board_sn", "None"),
            bios_manufacturer=data.get("bios_manufacturer", "Microsoft Corporation"),
            bios_sn=data.get("bios_sn", "None"),
            os_install_date=data.get("os_install_date", "1970-01-0100:00:00.000000000+0000"),
            os_sn=data.get("os_sn", "None"),
            disk_sn=data.get("disk_sn", "None"),
            volume_sn=data.get("volume_sn", "00000000"),
            gpu_pnp_id=gpu_pnp,
            mac=mac,
            cpu_details=cpu,
            hostname=platform.node(),
        )

    def get_gpu_id(self) -> int:
        """Parse DEV_XXXX from PNP ID string."""
        if not self.gpu_pnp_id:
            return 0
        m = re.search(r"DEV_(\w+)", self.gpu_pnp_id)
        if m:
            try:
                return int(m.group(1), 16)
            except ValueError:
                pass
        return 0

    def generate_mid(self) -> str:
        """FNV-1a 64-bit hash over identity fields + MAC."""
        buffer = (
            self.board_manufacturer +
            self.board_sn +
            self.bios_manufacturer +
            self.bios_sn +
            self.os_install_date +
            self.os_sn
        )
        if self.mac:
            buffer += self.mac
        return str(hash_fnv1a(buffer.encode("utf-8")))

    def generate_hardware_hash(self) -> str:
        """Version-branching SHA-1 hash using schema lookup."""
        schema = HASH_SCHEMAS.get(self.version)
        if schema is None:
            logger.warning("Unknown version %d, using schema %d", self.version, _LATEST_SCHEMA_VERSION)
            schema = HASH_SCHEMAS[_LATEST_SCHEMA_VERSION]

        parts: List[str] = []
        for field_name in schema:
            value = getattr(self, field_name, None)
            parts.append(value if value is not None else "None")

        final_data = ";".join(parts) + ";"
        if self.version >= 2:
            final_data += self.cpu_details.brand_name + ";"

        logger.debug("Hardware hash string: %r", final_data)
        digest = hashlib.sha1(final_data.encode("utf-8")).digest()

        if self.version < 4:
            return "".join(f"{b:x}" for b in digest)
        else:
            return digest.hex()


# =============================================================================
# TTL Cache with Thread Safety
# =============================================================================

class HardwareInfoCache:
    """Thread-safe TTL cache for HardwareInfo."""

    def __init__(self, ttl_seconds: int = 3600):
        self._ttl = ttl_seconds
        self._cache: Dict[int, Tuple[HardwareInfo, float]] = {}
        self._lock = threading.Lock()

    def set_ttl(self, seconds: int) -> None:
        with self._lock:
            self._ttl = seconds
            now = time.time()
            expired = [
                v for v, (_, ts) in self._cache.items()
                if now - ts >= seconds
            ]
            for v in expired:
                del self._cache[v]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def get(self, version: int) -> HardwareInfo:
        now = time.time()

        with self._lock:
            if version in self._cache:
                hw, ts = self._cache[version]
                if now - ts < self._ttl:
                    return hw

            hw = HardwareInfo.new(version)
            self._cache[version] = (hw, now)
            return hw


_default_cache: Optional[HardwareInfoCache] = None
_default_cache_lock = threading.Lock()


def _get_cache() -> HardwareInfoCache:
    global _default_cache
    if _default_cache is None:
        with _default_cache_lock:
            if _default_cache is None:
                _default_cache = HardwareInfoCache()
    return _default_cache


# =============================================================================
# PCSign Protocol
# =============================================================================

class PCSignVersion(Enum):
    V1 = "v1"
    V2 = "v2"


@dataclass(frozen=True)
class PCSign:
    """Immutable PC Sign token."""
    av: str = "v1"
    sv: PCSignVersion = field(default_factory=lambda: random.choice(list(PCSignVersion)))
    hardware_version: int = 4

    board_manufacturer: str = field(init=False)
    board_sn: str = field(init=False)
    bios_manufacturer: str = field(init=False)
    bios_sn: str = field(init=False)
    os_install_date: str = field(init=False)
    os_sn: str = field(init=False)
    disk_sn: str = field(init=False)
    volume_sn: str = field(init=False)
    gpu_pnp_id: Optional[str] = field(init=False)
    gid: int = field(init=False)
    mac: Optional[str] = field(init=False)
    mid: str = field(init=False)
    hostname: str = field(init=False)
    ts: str = field(init=False)
    hardware_hash: str = field(init=False)

    def __post_init__(self):
        hw = _get_cache().get(self.hardware_version)

        object.__setattr__(self, "board_manufacturer", hw.board_manufacturer)
        object.__setattr__(self, "board_sn", hw.board_sn)
        object.__setattr__(self, "bios_manufacturer", hw.bios_manufacturer)
        object.__setattr__(self, "bios_sn", hw.bios_sn)
        object.__setattr__(self, "os_install_date", hw.os_install_date)
        object.__setattr__(self, "os_sn", hw.os_sn)
        object.__setattr__(self, "disk_sn", hw.disk_sn)
        object.__setattr__(self, "volume_sn", hw.volume_sn)
        object.__setattr__(self, "gpu_pnp_id", hw.gpu_pnp_id)
        object.__setattr__(self, "gid", hw.get_gpu_id())
        object.__setattr__(self, "mac", hw.mac)
        object.__setattr__(self, "hostname", hw.hostname)
        object.__setattr__(self, "mid", hw.generate_mid())
        object.__setattr__(self, "hardware_hash", hw.generate_hardware_hash())
        object.__setattr__(self, "ts", self._generate_timestamp())

    @staticmethod
    def _generate_timestamp() -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        ms = now.microsecond // 1000
        return (
            f"{now.year:04d}-{now.month:02d}-{now.day:02d} "
            f"{now.hour:02d}:{now.minute:02d}:{now.second:02d}:{ms:03d}"
        )

    def _sign_key(self) -> bytes:
        keys = {
            PCSignVersion.V1: _SIGN_KEY_V1,
            PCSignVersion.V2: _SIGN_KEY_V2,
        }
        key = keys.get(self.sv)
        if key is None:
            raise ValueError(f"Invalid PCSignVersion: {self.sv}")
        return key

    def to_dict(self) -> Dict[str, Any]:
        return {
            "av": self.av,
            "bsn": self.bios_sn,
            "gid": self.gid,
            "hsn": self.disk_sn,
            "mac": self.mac,
            "mid": self.mid,
            "msn": self.board_sn,
            "sv": self.sv.value,
            "ts": self.ts,
        }

    def generate_pc_sign(self) -> str:
        payload_json = json.dumps(self.to_dict(), separators=(",", ":"))
        payload = _b64url_encode(payload_json.encode("utf-8"))
        signature = hmac.new(
            self._sign_key(), payload.encode("ascii"), hashlib.sha256
        ).digest()
        return f"{payload}.{_b64url_encode(signature)}"

    @classmethod
    def generate_fast(cls, sv: PCSignVersion, hardware_version: int = 4) -> str:
        instance = cls(hardware_version=hardware_version)
        object.__setattr__(instance, "sv", sv)
        return instance.generate_pc_sign()

    @staticmethod
    def preload_cache(hardware_version: int = 4) -> None:
        try:
            _get_cache().get(hardware_version)
        except Exception:
            pass


# =============================================================================
# Convenience Functions
# =============================================================================

def generate_pc_sign_fast(
    sv: Optional[PCSignVersion] = None,
    hardware_version: int = 4,
) -> str:
    if sv is None:
        sv = random.choice(list(PCSignVersion))
    return PCSign.generate_fast(sv, hardware_version=hardware_version)


def preload_pc_sign_cache(hardware_version: int = 4) -> None:
    PCSign.preload_cache(hardware_version)


def extract_user_info_from_jwt(jwt_token: str) -> Tuple[str, str, str]:
    """Extract user & persona ID and display name from a JWT token."""
    try:
        _, payload, _ = jwt_token.split(".")
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        data = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))["nexus"]
        return (
            data.get("pid", ""),
            data.get("psid", ""),
            data.get("psif", [{}])[0].get("dis", ""),
        )
    except Exception as exc:
        logger.error("Failed to extract user info from JWT: %s", exc)
        return "", "", ""


def set_cache_ttl(seconds: int) -> None:
    """Change the hardware info cache TTL."""
    _get_cache().set_ttl(seconds)


def clear_cache() -> None:
    """Clear the hardware info cache."""
    _get_cache().clear()