"""
core/drives.py — Safe external-only drive detection + DriveMonitor thread.

Exports:
  list_removable_drives(cfg)  -> List[str]
  DriveMonitor                -> background thread watching for hot-plug events
"""

from __future__ import annotations

import os
import platform
import string
import threading
import time
from typing import Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from config import AppConfig

try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False

try:
    import win32file
    _WIN32 = True
except ImportError:
    _WIN32 = False

# Drive letters NEVER returned regardless of type
_BLOCKED: set = {"C"}


def _os_drive() -> str:
    """Letter of the drive hosting Windows (usually C, but may differ)."""
    try:
        root = os.path.splitdrive(os.environ.get("SystemRoot", "C:\\"))[0]
        return root.upper().strip(":\\/")
    except Exception:
        return "C"


def list_removable_drives(cfg: Optional["AppConfig"] = None) -> List[str]:
    """
    Return removable/external drive root paths (e.g. ['E:/', 'F:/']).
    C:/ and the OS system drive are always excluded.
    cfg is accepted for API compatibility but not currently used.
    """
    blocked = _BLOCKED | {_os_drive()}

    if platform.system() == "Windows":
        if _PSUTIL:
            return _psutil_windows(blocked)
        if _WIN32:
            return _win32(blocked)
        return _fallback_windows(blocked)
    else:
        return _linux_mac()


def _psutil_windows(blocked: set) -> List[str]:
    drives: List[str] = []
    try:
        for p in psutil.disk_partitions(all=False):
            letter = p.device.rstrip("\\/").upper().rstrip(":")
            if letter in blocked:
                continue
            if "removable" in p.opts.lower():
                mp = p.mountpoint.replace("\\", "/")
                if not mp.endswith("/"):
                    mp += "/"
                drives.append(mp)
    except Exception:
        pass
    return sorted(drives)


def _win32(blocked: set) -> List[str]:
    drives: List[str] = []
    for letter in string.ascii_uppercase:
        if letter in blocked:
            continue
        d = f"{letter}:/"
        try:
            if win32file.GetDriveType(d) == win32file.DRIVE_REMOVABLE:
                drives.append(d)
        except Exception:
            pass
    return drives


def _fallback_windows(blocked: set) -> List[str]:
    """
    Last-resort fallback when neither psutil nor win32file is available.
    Uses a size heuristic: drives < 512 GB are likely USB.
    Never includes blocked letters.
    """
    import shutil
    drives: List[str] = []
    for letter in string.ascii_uppercase:
        if letter in blocked:
            continue
        d = f"{letter}:/"
        if os.path.exists(d):
            try:
                total_gb = shutil.disk_usage(d).total / (1024 ** 3)
                if total_gb < 512:
                    drives.append(d)
            except Exception:
                pass
    return drives


def _linux_mac() -> List[str]:
    drives: List[str] = []
    if _PSUTIL:
        try:
            bad_fs = {"tmpfs", "devtmpfs", "squashfs", "overlay", "sysfs", "proc"}
            for p in psutil.disk_partitions(all=False):
                mp = p.mountpoint
                if p.fstype.lower() in bad_fs:
                    continue
                if mp.startswith("/media") or mp.startswith("/run/media") or mp.startswith("/Volumes"):
                    drives.append(mp if mp.endswith("/") else mp + "/")
        except Exception:
            pass
    else:
        for base in ("/media", "/run/media", "/Volumes"):
            if os.path.isdir(base):
                try:
                    for entry in os.scandir(base):
                        if entry.is_dir():
                            drives.append(entry.path + "/")
                except Exception:
                    pass
    return sorted(drives)


# ── DriveMonitor ──────────────────────────────────────────────────────────────

class DriveMonitor:
    """
    Background daemon thread that polls for drive changes every DRIVE_MONITOR_MS ms.

    Callbacks:
      on_change(added, removed)  — when the set of available drives changes
      on_missing(missing)        — when a *selected* drive disappears
    """

    def __init__(
        self,
        cfg: "AppConfig",
        on_change:  Callable[[List[str], List[str]], None],
        on_missing: Callable[[List[str]], None],
        interval_ms: int = 2000,
    ):
        self._cfg        = cfg
        self._on_change  = on_change
        self._on_missing = on_missing
        self._interval   = interval_ms / 1000.0
        self._selected:  List[str] = []
        self._last_seen: set       = set()
        self._stop_evt   = threading.Event()
        self._thread     = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._last_seen = set(list_removable_drives(self._cfg))
        self._thread.start()

    def stop(self) -> None:
        self._stop_evt.set()

    def set_selected(self, drives: List[str]) -> None:
        self._selected = list(drives)

    def _run(self) -> None:
        while not self._stop_evt.wait(self._interval):
            try:
                current = set(list_removable_drives(self._cfg))
                added   = sorted(current - self._last_seen)
                removed = sorted(self._last_seen - current)
                if added or removed:
                    self._last_seen = current
                    try:
                        self._on_change(added, removed)
                    except Exception:
                        pass
                if self._selected:
                    missing = [d for d in self._selected if d not in current]
                    if missing:
                        try:
                            self._on_missing(missing)
                        except Exception:
                            pass
            except Exception:
                pass
