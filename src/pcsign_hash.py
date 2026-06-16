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
import ctypes
import datetime
import hashlib
import hmac
import json
import logging
import platform
import random
import re
import struct
import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hash_fnv1a(data: bytes) -> int:
    offset = 0xcbf29ce484222325
    prime = 0x100000001b3
    for byte in data:
        offset ^= byte
        offset = (offset * prime) & 0xFFFFFFFFFFFFFFFF
    return offset


# Pre-compile CPUID shellcode once per architecture
_IS_64BIT: Final = struct.calcsize("P") == 8

_CPUID_CODE_64: Final = bytes([
    0x53, 0x57, 0x89, 0xC8, 0x48, 0x89, 0xD7, 0x31, 0xC9,
    0x0F, 0xA2, 0x89, 0x07, 0x89, 0x5F, 0x04, 0x89, 0x4F,
    0x08, 0x89, 0x57, 0x0C, 0x5F, 0x5B, 0xC3,
])

_CPUID_CODE_32: Final = bytes([
    0x53, 0x57, 0x8B, 0x44, 0x24, 0x0C, 0x31, 0xC9, 0x0F,
    0xA2, 0x8B, 0x7C, 0x24, 0x10, 0x89, 0x07, 0x89, 0x5F,
    0x04, 0x89, 0x4F, 0x08, 0x89, 0x57, 0x0C, 0x5F, 0x5B,
    0xC3,
])


def _cpuid(leaf: int) -> tuple[int, int, int, int]:
    """Execute the x86 CPUID instruction via ctypes shellcode (Windows only)."""
    kernel32 = ctypes.windll.kernel32
    kernel32.VirtualAlloc.restype = ctypes.c_void_p
    kernel32.VirtualAlloc.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32, ctypes.c_uint32]
    kernel32.VirtualFree.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32]

    MEM_COMMIT = 0x1000
    MEM_RESERVE = 0x2000
    PAGE_EXECUTE_READWRITE = 0x40
    MEM_RELEASE = 0x8000

    code = _CPUID_CODE_64 if _IS_64BIT else _CPUID_CODE_32

    addr = kernel32.VirtualAlloc(None, len(code), MEM_COMMIT | MEM_RESERVE, PAGE_EXECUTE_READWRITE)
    if not addr:
        raise OSError("VirtualAlloc failed")
    try:
        ctypes.memmove(addr, code, len(code))
        result = (ctypes.c_uint32 * 4)()
        func_type = ctypes.CFUNCTYPE(None, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32 * 4))
        func = func_type(addr)
        func(leaf, result)
        return (result[0], result[1], result[2], result[3])
    finally:
        kernel32.VirtualFree(addr, 0, MEM_RELEASE)


@dataclass(frozen=True, slots=True)
class CpuDetails:
    eax: int = 0
    ebx: int = 0
    ecx: int = 0
    edx: int = 0
    manufacturer: str = ""
    brand_name: str = ""

    @classmethod
    def gather(cls) -> "CpuDetails":
        m_eax, m_ebx, m_ecx, m_edx = _cpuid(0)
        f_eax, f_ebx, f_ecx, f_edx = _cpuid(1)

        vendor_bytes = bytearray()
        for val in (m_ebx, m_edx, m_ecx):
            vendor_bytes += val.to_bytes(4, "little")
        manufacturer = vendor_bytes.decode("ascii", errors="replace")

        brand_bytes = bytearray()
        stop = False
        for leaf in (0x80000002, 0x80000003, 0x80000004):
            if stop:
                break
            b_eax, b_ebx, b_ecx, b_edx = _cpuid(leaf)
            for val in (b_eax, b_ebx, b_ecx, b_edx):
                for byte in val.to_bytes(4, "little"):
                    if byte == 0:
                        stop = True
                        break
                    brand_bytes.append(byte)
                if stop:
                    break
        brand_bytes += b"\x00" * (47 - len(brand_bytes))
        brand_name = brand_bytes.rstrip(b"\x00").decode("ascii", errors="replace")

        return cls(eax=f_eax, ebx=f_ebx, ecx=f_ecx, edx=f_edx,
                   manufacturer=manufacturer, brand_name=brand_name)


@dataclass(slots=True)
class HardwareInfo:
    version: int
    board_manufacturer: str = ""
    board_sn: str = ""
    bios_manufacturer: str = ""
    bios_sn: str = ""
    os_install_date: str = ""
    os_sn: str = ""
    disk_sn: str = ""
    volume_sn: str = ""
    gpu_pnp_id: str | None = None
    mac: str | None = None
    cpu_details: CpuDetails = field(default_factory=CpuDetails)
    hostname: str = ""

    @classmethod
    def new(cls, version: int) -> "HardwareInfo":
        system = platform.system()
        try:
            if system == "Windows":
                return cls._from_windows(version)
            if system == "Darwin":
                return cls._from_macos(version)
            logger.error("Unsupported OS: %s", system)
        except Exception as exc:
            logger.warning("HardwareInfo.new failed (%s), returning defaults: %s", system, exc)
        return cls(version=version)

    @classmethod
    def _from_windows(cls, version: int) -> "HardwareInfo":
        ps_script = Path(__file__).parent / "pc_sign_ps.ps1"
        data: dict[str, Any] = {}

        if ps_script.exists():
            try:
                result = subprocess.run(
                    ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(ps_script)],
                    capture_output=True, timeout=15, text=True,
                    encoding="utf-8", errors="ignore",
                )
                if result.returncode == 0:
                    raw = json.loads(result.stdout.strip())
                    data = raw[0] if isinstance(raw, list) else raw
            except Exception as exc:
                logger.warning("PowerShell script failed: %s", exc)

        if not data:
            data = cls._wmic_fallback()

        gid_raw = data.get("gid", 0)
        try:
            gid = int(gid_raw, 16) if isinstance(gid_raw, str) and gid_raw.startswith(("0x", "0X")) else int(gid_raw or 0)
        except (ValueError, TypeError):
            gid = 0

        gpu_pnp_id = generate_pci_pnp_id(version, None, gid if gid else None, None) if gid else data.get("gpu_pnp_id")
        volume_sn = data.get("volume_sn") or cls._get_win_volume_sn()

        return cls(
            version=version,
            board_manufacturer=data.get("board_manufacturer") or data.get("mbm", "Microsoft Corporation"),
            board_sn=data.get("board_sn") or data.get("bbm", "None"),
            bios_manufacturer=data.get("bios_manufacturer") or data.get("bim", "Microsoft Corporation"),
            bios_sn=data.get("bios_sn") or data.get("bsn", "None"),
            os_install_date=data.get("osi", "1970-01-0100:00:00.000000000+0000"),
            os_sn=data.get("osn", "None"),
            disk_sn=data.get("disk_sn") or data.get("hsn", "None"),
            volume_sn=volume_sn,
            gpu_pnp_id=gpu_pnp_id,
            mac=_get_ea_mac_address(),
            cpu_details=CpuDetails.gather(),
            hostname=platform.node(),
        )

    @classmethod
    def _from_macos(cls, version: int) -> "HardwareInfo":
        @lru_cache(maxsize=8)
        def run(*args: str, timeout: int = 10) -> str:
            try:
                r = subprocess.run(list(args), capture_output=True, text=True,
                                   timeout=timeout, encoding="utf-8", errors="ignore")
                return r.stdout if r.returncode == 0 else ""
            except Exception:
                return ""

        board_sn = "None"
        os_sn = "None"

        ioreg = run("ioreg", "-d2", "-c", "IOPlatformExpertDevice")

        def extract_ioreg(key: str) -> str:
            if m := re.search(rf'"{key}"\s*=\s*"([^"]+)"', ioreg):
                return m.group(1)
            return ""

        board_sn = extract_ioreg("IOPlatformSerialNumber") or "None"
        os_sn = extract_ioreg("IOPlatformUUID") or "None"

        disk_sn = "None"
        for line in run("diskutil", "info", "/").splitlines():
            if line.strip().startswith("Volume UUID:"):
                parts = line.split()
                if len(parts) >= 3:
                    disk_sn = parts[2]
                    break

        gpu_pnp_id = None
        sp_json = run("system_profiler", "SPDisplaysDataType", "-json")
        if sp_json:
            try:
                sp_data = json.loads(sp_json)
                items = sp_data.get("SPDisplaysDataType", [])
                if items:
                    gpu = items[0]
                    device_id = int(gpu.get("spdisplays_device-id", "0x0000"), 16)
                    revision_id = int(gpu.get("spdisplays_revision-id", "0x00"), 16)
                    gpu_pnp_id = generate_pci_pnp_id(version, None, device_id, revision_id)
            except Exception as exc:
                logger.debug("system_profiler GPU parse failed: %s", exc)

        return cls(
            version=version,
            board_manufacturer="Apple Inc.",
            board_sn=board_sn,
            bios_manufacturer="Apple Inc.",
            bios_sn=board_sn,
            os_install_date="1970010100:00:00.000000000+0000",
            os_sn=os_sn,
            disk_sn=disk_sn,
            volume_sn="43000000",
            gpu_pnp_id=gpu_pnp_id,
            mac=_get_ea_mac_address(),
            cpu_details=CpuDetails.gather(),
            hostname=platform.node(),
        )

    def get_gpu_id(self) -> int:
        if not self.gpu_pnp_id:
            return 0
        if m := re.search(r"DEV_(\w+)", self.gpu_pnp_id):
            try:
                return int(m.group(1), 16)
            except ValueError:
                pass
        return 0

    def generate_mid(self) -> str:
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
        gpu = self.gpu_pnp_id or "None"
        cpu_edx = f"{self.cpu_details.edx:08x}"
        cpu_edx_eax = f"{self.cpu_details.edx:08X}{self.cpu_details.eax:08X}"
        cpu_ecx = f"{self.cpu_details.ecx:08x}"

        parts = [self.board_manufacturer, self.board_sn]

        if self.version in (0, 1):
            parts += [
                self.hostname, self.bios_manufacturer, self.bios_sn,
                self.os_install_date, self.os_sn,
            ]
        elif self.version == 2:
            parts += [
                self.bios_manufacturer, self.bios_sn,
                self.os_install_date, self.os_sn,
                self.volume_sn, gpu,
                self.cpu_details.manufacturer, cpu_edx, cpu_ecx,
            ]
        elif self.version == 3:
            parts += [
                self.bios_manufacturer, self.bios_sn,
                self.volume_sn, gpu,
                self.cpu_details.manufacturer, cpu_edx, cpu_ecx,
            ]
        else:
            parts += [
                self.bios_manufacturer, self.bios_sn,
                self.volume_sn, gpu,
                self.cpu_details.manufacturer, cpu_edx_eax,
            ]

        final_data = ";".join(parts) + ";"
        if self.version >= 2:
            final_data += self.cpu_details.brand_name + ";"

        logger.debug('Hardware hash string "%s"', final_data)
        digest = hashlib.sha1(final_data.encode("utf-8")).digest()

        if self.version < 4:
            return "".join(f"{b:x}" for b in digest)
        return digest.hex()

    @staticmethod
    def _wmic_fallback() -> dict[str, Any]:
        def wmic(query: str) -> str:
            out = subprocess.run(query, shell=True, capture_output=True,
                                 text=True, timeout=10, encoding="utf-8", errors="ignore")
            return out.stdout.strip()

        def extract(query: str) -> str:
            raw = wmic(query)
            return raw.split("=", 1)[-1].strip() if "=" in raw else ""

        return {
            "board_manufacturer": extract('wmic baseboard get manufacturer /value | findstr "="'),
            "board_sn": extract('wmic baseboard get serialnumber /value | findstr "="'),
            "bios_manufacturer": extract('wmic bios get manufacturer /value | findstr "="'),
            "bios_sn": extract('wmic bios get serialnumber /value | findstr "="'),
            "osn": extract('wmic os get serialnumber /value | findstr "="'),
            "osi": extract('wmic os get installdate /value | findstr "="'),
            "disk_sn": extract('wmic diskdrive where Index=0 get serialnumber /value | findstr "="'),
        }

    @staticmethod
    def _get_win_volume_sn() -> str:
        try:
            vol_serial = ctypes.c_uint32(0)
            ctypes.windll.kernel32.GetVolumeInformationW(
                "C:\\", None, 0, ctypes.byref(vol_serial),
                None, None, None, 0,
            )
            return f"{vol_serial.value:08x}"
        except Exception:
            return "00000000"


def _get_ea_mac_address() -> str | None:
    """Return MAC as '$<hex>' matching Rust's get_ea_mac_address."""
    try:
        raw = __import__("uuid").getnode()
        if raw >> 40 & 1:
            return None
        return f"${raw:012x}"
    except Exception:
        return None


def generate_pci_pnp_id(
    version: int,
    vendor: int | None,
    device: int | None,
    revision: int | None,
) -> str:
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


class PCSignVersion(Enum):
    V1 = "v1"
    V2 = "v2"


class HardwareInfoCache:
    _instance: "HardwareInfoCache | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "HardwareInfoCache":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._hw: HardwareInfo | None = None
            self._cache_time: float | None = None
            self._cache_duration: int = 3600
            self._initialized = True

    def set_cache_duration(self, seconds: int) -> None:
        with self._lock:
            self._cache_duration = seconds
            if self._cache_time is not None and time.time() - self._cache_time >= seconds:
                self._hw = None
                self._cache_time = None

    def clear_cache(self) -> None:
        with self._lock:
            self._hw = None
            self._cache_time = None

    def get(self, version: int) -> HardwareInfo:
        now = time.time()
        if (self._hw is not None and
                self._cache_time is not None and
                now - self._cache_time < self._cache_duration):
            return self._hw
        with self._lock:
            if (self._hw is not None and
                    self._cache_time is not None and
                    now - self._cache_time < self._cache_duration):
                return self._hw
            self._hw = HardwareInfo.new(version)
            self._cache_time = now
            return self._hw


@dataclass(slots=True)
class PCSign:
    board_manufacturer: str = field(init=False)
    board_sn: str = field(init=False)
    bios_manufacturer: str = field(init=False)
    bios_sn: str = field(init=False)
    os_install_date: str = field(init=False)
    os_sn: str = field(init=False)
    disk_sn: str = field(init=False)
    volume_sn: str = field(init=False)
    gpu_pnp_id: str | None = field(init=False)
    gid: int = field(init=False)
    mac: str | None = field(init=False)
    mid: str = field(init=False)
    hostname: str = field(init=False)
    ts: str = field(init=False)

    hardware_version: int = 4
    av: str = "v1"
    sv: PCSignVersion = field(default_factory=lambda: random.choice(list(PCSignVersion)))

    def __post_init__(self):
        hw = HardwareInfoCache().get(self.hardware_version)
        self.board_manufacturer = hw.board_manufacturer
        self.board_sn = hw.board_sn
        self.bios_manufacturer = hw.bios_manufacturer
        self.bios_sn = hw.bios_sn
        self.os_install_date = hw.os_install_date
        self.os_sn = hw.os_sn
        self.disk_sn = hw.disk_sn
        self.volume_sn = hw.volume_sn
        self.gpu_pnp_id = hw.gpu_pnp_id
        self.gid = hw.get_gpu_id()
        self.mac = hw.mac
        self.hostname = hw.hostname
        self.mid = hw.generate_mid()
        self.ts = self._generate_timestamp()

    def _generate_timestamp(self) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"{now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second}:{now.microsecond // 1000}"

    @lru_cache(maxsize=2)
    def sign_key(self) -> bytes:
        keys = {
            PCSignVersion.V1: b"ISa3dpGOc8wW7Adn4auACSQmaccrOyR2",
            PCSignVersion.V2: b"nt5FfJbdPzNcl2pkC3zgjO43Knvscxft",
        }
        if self.sv not in keys:
            raise ValueError(f"Invalid PCSignVersion: {self.sv}")
        return keys[self.sv]

    def to_dict(self) -> dict[str, Any]:
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

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    def generate_pc_sign(self) -> str:
        payload_json = json.dumps(self.to_dict(), separators=(",", ":"))
        payload = self.base64url_encode(payload_json.encode("utf-8"))
        signature = hmac.new(self.sign_key(), payload.encode("ascii"), hashlib.sha256).digest()
        return f"{payload}.{self.base64url_encode(signature)}"

    @classmethod
    def generate_fast(cls, sv: PCSignVersion, hardware_version: int = 4) -> str:
        instance = cls(hardware_version=hardware_version)
        instance.sv = sv
        return instance.generate_pc_sign()

    @staticmethod
    def preload_hardware_cache(hardware_version: int = 4) -> None:
        try:
            HardwareInfoCache().get(hardware_version)
        except Exception:
            pass


def extract_user_info_from_jwt(jwt_token: str) -> tuple[str, str, str]:
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


def generate_pc_sign_fast(
    sv: PCSignVersion | None = None,
    hardware_version: int = 4,
) -> str:
    if sv is None:
        sv = random.choice(list(PCSignVersion))
    return PCSign.generate_fast(sv, hardware_version=hardware_version)


def preload_pc_sign_cache(hardware_version: int = 4) -> None:
    PCSign.preload_hardware_cache(hardware_version)