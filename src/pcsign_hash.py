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
from pathlib import Path
from typing import Any, Final

logger = logging.getLogger(__name__)


def hash_fnv1a(data: bytes) -> int:
    h = 0xcbf29ce484222325
    prime = 0x100000001b3
    for byte in data:
        h = ((h ^ byte) * prime) & 0xFFFFFFFFFFFFFFFF
    return h

try:
    import cpuinfo as _cpuinfo_lib
    _CPUINFO_AVAILABLE = True
except ImportError:
    _CPUINFO_AVAILABLE = False


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
        if platform.system() != "Windows":
            return 0, 0, 0, 0
        kernel32 = ctypes.windll.kernel32
        kernel32.VirtualAlloc.restype = ctypes.c_void_p
        kernel32.VirtualAlloc.argtypes = [
            ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32, ctypes.c_uint32
        ]
        kernel32.VirtualFree.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32]

        code = _CPUID_CODE_64 if _IS_64BIT else _CPUID_CODE_32
        addr = kernel32.VirtualAlloc(None, len(code), 0x3000, 0x40)
        if not addr:
            raise OSError("VirtualAlloc failed")
        try:
            ctypes.memmove(addr, code, len(code))
            result = (ctypes.c_uint32 * 4)()
            func = ctypes.CFUNCTYPE(None, ctypes.c_uint32,
                                    ctypes.POINTER(ctypes.c_uint32 * 4))(addr)
            func(leaf, result)
            return tuple(result)  # type: ignore[return-value]
        finally:
            kernel32.VirtualFree(addr, 0, 0x8000)


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
        eax = ebx = ecx = edx = 0
        manufacturer = ""
        brand_name = ""

        if platform.system() == "Windows":
            try:
                _, m_ebx, m_edx, m_ecx = _cpuid(0)
                f_eax, _, f_ecx, f_edx = _cpuid(1)
                eax, ecx, edx = f_eax, f_ecx, f_edx

                vendor_bytes = bytearray()
                for val in (m_ebx, m_edx, m_ecx):
                    vendor_bytes += val.to_bytes(4, "little")
                manufacturer = vendor_bytes.decode("ascii", errors="replace")

                brand_bytes = bytearray()
                stop = False
                for leaf in (0x80000002, 0x80000003, 0x80000004):
                    if stop:
                        break
                    for val in _cpuid(leaf):
                        for byte in val.to_bytes(4, "little"):
                            if byte == 0:
                                stop = True
                                break
                            brand_bytes.append(byte)
                        if stop:
                            break
                brand_bytes += b"\x00" * max(0, 47 - len(brand_bytes))
                brand_name = brand_bytes.rstrip(b"\x00").decode("ascii", errors="replace")
            except Exception as exc:
                logger.warning("CPUID shellcode failed: %s", exc)

        if _CPUINFO_AVAILABLE and (not manufacturer or not brand_name):
            try:
                info = _cpuinfo_lib.get_cpu_info()
                manufacturer = manufacturer or info.get("vendor_id_raw", "")
                brand_name   = brand_name   or info.get("brand_raw", "")
            except Exception as exc:
                logger.debug("py-cpuinfo fallback failed: %s", exc)

        return cls(eax=eax, ebx=ebx, ecx=ecx, edx=edx,
                   manufacturer=manufacturer, brand_name=brand_name)

def _get_ea_mac_address() -> str | None:
    """Return MAC as '$<hex>' — omit if multicast/locally-administered bit set."""
    try:
        import uuid
        raw = uuid.getnode()
        if (raw >> 40) & 1:
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
        sections += [
            f"REV_{revision or 0:02X}\\0",
            f"{0xDEADBEEF:08X}",
            "0",
            f"{0xDEAD:04X}",
        ]
    return "PCI\\" + "&".join(sections)


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
            logger.warning("HardwareInfo.new failed (%s): %s", system, exc)
        return cls(version=version)

    @classmethod
    def _from_windows(cls, version: int) -> "HardwareInfo":
        ps_script = Path(__file__).parent / "pc_sign_ps.ps1"
        data: dict[str, Any] = {}

        if ps_script.exists():
            try:
                result = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-NonInteractive",
                        "-ExecutionPolicy", "Bypass",
                        "-File", str(ps_script),
                    ],
                    capture_output=True, timeout=15,
                    text=True, encoding="utf-8", errors="ignore",
                )
                if result.returncode == 0 and result.stdout.strip():
                    raw = json.loads(result.stdout.strip())
                    data = raw[0] if isinstance(raw, list) else raw
            except Exception as exc:
                logger.warning("PowerShell script failed: %s", exc)

        if not data:
            data = cls._wmic_fallback()

        gid_raw = data.get("gid", 0)
        try:
            gid = (
                int(gid_raw, 16)
                if isinstance(gid_raw, str) and gid_raw.startswith(("0x", "0X"))
                else int(gid_raw or 0)
            )
        except (ValueError, TypeError):
            gid = 0

        gpu_pnp_id = (
            generate_pci_pnp_id(version, None, gid, None) if gid
            else data.get("gpu_pnp_id")
        )

        return cls(
            version=version,
            board_manufacturer=data.get("mbm") or data.get("board_manufacturer", "Microsoft Corporation"),
            board_sn=data.get("msn") or data.get("board_sn", "None"),
            bios_manufacturer=data.get("bbm") or data.get("bios_manufacturer", "Microsoft Corporation"),
            bios_sn=data.get("bsn") or data.get("bios_sn", "None"),
            os_install_date=data.get("osi", "1970-01-0100:00:00.000000000+0000"),
            os_sn=data.get("osn") or data.get("os_sn", "None"),
            disk_sn=data.get("hsn") or data.get("disk_sn", "None"),
            volume_sn=data.get("volume_sn") or cls._get_win_volume_sn(),
            gpu_pnp_id=gpu_pnp_id,
            mac=data.get("mac") or _get_ea_mac_address(),
            cpu_details=CpuDetails.gather(),
            hostname=platform.node(),
        )

    @classmethod
    def _from_macos(cls, version: int) -> "HardwareInfo":
        def run(*args: str, timeout: int = 10) -> str:
            try:
                r = subprocess.run(
                    list(args), capture_output=True, text=True,
                    timeout=timeout, encoding="utf-8", errors="ignore",
                )
                return r.stdout if r.returncode == 0 else ""
            except Exception:
                return ""

        def extract_ioreg(key: str, text: str) -> str:
            if m := re.search(rf'"{key}"\s*=\s*"([^"]+)"', text):
                return m.group(1)
            return ""

        ioreg = run("ioreg", "-d2", "-c", "IOPlatformExpertDevice")
        board_sn = extract_ioreg("IOPlatformSerialNumber", ioreg) or "None"
        os_sn = extract_ioreg("IOPlatformUUID", ioreg) or "None"

        disk_sn = "None"
        for line in run("diskutil", "info", "/").splitlines():
            if "Volume UUID:" in line:
                parts = line.split()
                if len(parts) >= 3:
                    disk_sn = parts[2]
                break

        gpu_pnp_id = None
        sp_json = run("system_profiler", "SPDisplaysDataType", "-json")
        if sp_json:
            try:
                items = json.loads(sp_json).get("SPDisplaysDataType", [])
                if items:
                    gpu = items[0]
                    device_id  = int(gpu.get("spdisplays_device-id",   "0x0000"), 16)
                    revision_id = int(gpu.get("spdisplays_revision-id", "0x00"),   16)
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
        if self.gpu_pnp_id and (m := re.search(r"DEV_([0-9A-Fa-f]+)", self.gpu_pnp_id)):
            try:
                return int(m.group(1), 16)
            except ValueError:
                pass
        return 0

    def generate_mid(self) -> str:
        parts = [
            self.board_manufacturer,
            self.board_sn,
            self.bios_manufacturer,
            self.bios_sn,
            self.os_install_date,
            self.os_sn,
        ]
        if self.mac:
            parts.append(self.mac)
        return str(hash_fnv1a("".join(parts).encode("utf-8")))

    def generate_hardware_hash(self) -> str:
        cpu = self.cpu_details
        cpu_edx      = f"{cpu.edx:08x}"
        cpu_edx_eax  = f"{cpu.edx:08X}{cpu.eax:08X}"
        cpu_ecx      = f"{cpu.ecx:08x}"
        gpu          = self.gpu_pnp_id or "None"

        parts = [self.board_manufacturer, self.board_sn]

        if self.version in (0, 1):
            parts += [self.hostname, self.bios_manufacturer, self.bios_sn,
                      self.os_install_date, self.os_sn]
        elif self.version == 2:
            parts += [self.bios_manufacturer, self.bios_sn,
                      self.os_install_date, self.os_sn,
                      self.volume_sn, gpu, cpu.manufacturer, cpu_edx, cpu_ecx]
        elif self.version == 3:
            parts += [self.bios_manufacturer, self.bios_sn,
                      self.volume_sn, gpu, cpu.manufacturer, cpu_edx, cpu_ecx]
        else:
            parts += [self.bios_manufacturer, self.bios_sn,
                      self.volume_sn, gpu, cpu.manufacturer, cpu_edx_eax]

        final = ";".join(parts) + ";"
        if self.version >= 2:
            final += cpu.brand_name + ";"

        logger.debug('Hardware hash string: "%s"', final)
        digest = hashlib.sha1(final.encode("utf-8")).digest()
        return digest.hex() if self.version >= 4 else "".join(f"{b:x}" for b in digest)

    @staticmethod
    def _wmic_fallback() -> dict[str, Any]:
        def extract(query: str) -> str:
            try:
                out = subprocess.run(
                    query, shell=True, capture_output=True,
                    text=True, timeout=10, encoding="utf-8", errors="ignore",
                )
                raw = out.stdout.strip()
                return raw.split("=", 1)[-1].strip() if "=" in raw else ""
            except Exception:
                return ""

        return {
            "mbm":  extract('wmic baseboard get manufacturer /value | findstr "="'),
            "msn":  extract('wmic baseboard get serialnumber /value | findstr "="'),
            "bbm":  extract('wmic bios get manufacturer /value | findstr "="'),
            "bsn":  extract('wmic bios get serialnumber /value | findstr "="'),
            "osn":  extract('wmic os get serialnumber /value | findstr "="'),
            "osi":  extract('wmic os get installdate /value | findstr "="'),
            "hsn":  extract('wmic diskdrive where Index=0 get serialnumber /value | findstr "="'),
        }

    @staticmethod
    def _get_win_volume_sn() -> str:
        try:
            vol = ctypes.c_uint32(0)
            ctypes.windll.kernel32.GetVolumeInformationW(
                "C:\\", None, 0, ctypes.byref(vol), None, None, None, 0,
            )
            return f"{vol.value:08x}"
        except Exception:
            return "00000000"


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
            self._hw:            HardwareInfo | None = None
            self._cache_time:    float | None        = None
            self._cache_duration: int               = 3600
            self._initialized = True

    def get(self, version: int) -> HardwareInfo:
        now = time.monotonic()
        if (self._hw is not None
                and self._cache_time is not None
                and now - self._cache_time < self._cache_duration):
            return self._hw
        with self._lock:
            if (self._hw is not None
                    and self._cache_time is not None
                    and now - self._cache_time < self._cache_duration):
                return self._hw
            self._hw = HardwareInfo.new(version)
            self._cache_time = now
            return self._hw

    def clear(self) -> None:
        with self._lock:
            self._hw = None
            self._cache_time = None


class PCSignVersion(Enum):
    V1 = "v1"
    V2 = "v2"

_SIGN_KEYS: Final[dict[PCSignVersion, bytes]] = {
    PCSignVersion.V1: b"ISa3dpGOc8wW7Adn4auACSQmaccrOyR2",
    PCSignVersion.V2: b"nt5FfJbdPzNcl2pkC3zgjO43Knvscxft",
}

@dataclass(slots=True)
class PCSign:
    hardware_version: int      = 4
    av:               str      = "v1"
    sv:               PCSignVersion = field(
        default_factory=lambda: random.choice(list(PCSignVersion))
    )

    board_manufacturer: str       = field(init=False)
    board_sn:           str       = field(init=False)
    bios_manufacturer:  str       = field(init=False)
    bios_sn:            str       = field(init=False)
    os_install_date:    str       = field(init=False)
    os_sn:              str       = field(init=False)
    disk_sn:            str       = field(init=False)
    volume_sn:          str       = field(init=False)
    gpu_pnp_id:         str | None = field(init=False)
    gid:                int        = field(init=False)
    mac:                str | None = field(init=False)
    mid:                str        = field(init=False)
    hostname:           str        = field(init=False)
    ts:                 str        = field(init=False)

    def __post_init__(self):
        hw = HardwareInfoCache().get(self.hardware_version)
        self.board_manufacturer = hw.board_manufacturer
        self.board_sn           = hw.board_sn
        self.bios_manufacturer  = hw.bios_manufacturer
        self.bios_sn            = hw.bios_sn
        self.os_install_date    = hw.os_install_date
        self.os_sn              = hw.os_sn
        self.disk_sn            = hw.disk_sn
        self.volume_sn          = hw.volume_sn
        self.gpu_pnp_id         = hw.gpu_pnp_id
        self.gid                = hw.get_gpu_id()
        self.mac                = hw.mac
        self.mid                = hw.generate_mid()
        self.hostname           = hw.hostname
        self.ts                 = self._make_timestamp()

    @staticmethod
    def _make_timestamp() -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        ms  = now.microsecond // 1000
        return (
            f"{now.year}-{now.month:02d}-{now.day:02d} "
            f"{now.hour:02d}:{now.minute:02d}:{now.second:02d}:{ms:03d}"
        )

    def sign_key(self) -> bytes:
        return _SIGN_KEYS[self.sv]

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "av":  self.av,
            "bsn": self.bios_sn,
            "gid": self.gid,
            "hsn": self.disk_sn,
            "mid": self.mid,
            "msn": self.board_sn,
            "sv":  self.sv.value,
            "ts":  self.ts,
        }
        if self.mac is not None:
            d["mac"] = self.mac
        return d

    @staticmethod
    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    def generate_pc_sign(self) -> str:
        payload = self._b64(json.dumps(self.to_dict(), separators=(",", ":")).encode())
        sig     = hmac.new(self.sign_key(), payload.encode("ascii"), hashlib.sha256).digest()
        return f"{payload}.{self._b64(sig)}"

    @classmethod
    def generate_fast(cls, sv: PCSignVersion, hardware_version: int = 4) -> str:
        return cls(hardware_version=hardware_version, sv=sv).generate_pc_sign()

    @staticmethod
    def preload_hardware_cache(hardware_version: int = 4) -> None:
        try:
            HardwareInfoCache().get(hardware_version)
        except Exception:
            pass


def extract_user_info_from_jwt(jwt_token: str) -> tuple[str, str, str]:
    try:
        _, payload, _ = jwt_token.split(".")
        rem = len(payload) % 4
        if rem:
            payload += "=" * (4 - rem)
        data = json.loads(base64.urlsafe_b64decode(payload))["nexus"]
        return (
            data.get("pid",  ""),
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