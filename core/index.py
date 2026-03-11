"""
core/index.py — IndexManager class + build_entry helper.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

from config import INDEX_DIR_NAME, INDEX_FILE_NAME, INDEX_VERSION, Status

IndexEntry   = Dict[str, Any]
VirtualIndex = Dict[str, IndexEntry]


# ── Standalone helpers ────────────────────────────────────────────────────────

def build_entry(
    parts:      List[str],
    compressed: bool,
    fmt:        str,
    orig_name:  str,
    orig_size:  int,
    status:     Status                   = Status.COMPLETE,
    checksums:  Optional[Dict[str, str]] = None,
    verified:   bool                     = True,
    part_sizes: Optional[List[int]]      = None,
) -> IndexEntry:
    entry: IndexEntry = {
        "version":    INDEX_VERSION,
        "parts":      parts,
        "compressed": compressed,
        "format":     fmt,
        "orig_name":  orig_name,
        "size":       orig_size,
        "status":     status.value,
        "created_at": time.time(),
    }
    if checksums is not None:
        entry["checksums"] = checksums
        entry["verified"]  = verified
    if part_sizes is not None:
        entry["part_sizes"] = part_sizes
    return entry


def entry_parts(val: Any) -> List[str]:
    if isinstance(val, list):
        return val
    if isinstance(val, dict):
        return val.get("parts", [])
    return []


def _meta_dir(drive: str) -> str:
    return os.path.join(drive, INDEX_DIR_NAME)


def _index_file(drive: str) -> str:
    return os.path.join(_meta_dir(drive), INDEX_FILE_NAME)


def _hide(path: str) -> None:
    """Mark a file/folder as hidden on Windows. Safe no-op on other OSes."""
    try:
        subprocess.call(
            ["attrib", "+h", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _ensure_meta(drive: str) -> str:
    meta = _meta_dir(drive)
    os.makedirs(meta, exist_ok=True)
    _hide(meta)
    return meta


MAX_HISTORY = 10


def _rotate_history(drive: str, current_data: str) -> None:
    """
    Copy current_data (already serialised JSON string) into a timestamped
    snapshot file.  We pass the string — not the path — so we never read a
    potentially-stale or empty file.
    Uses millisecond timestamp to avoid collisions when two saves happen
    within the same second.
    """
    meta        = _meta_dir(drive)
    history_dir = os.path.join(meta, "history")
    os.makedirs(history_dir, exist_ok=True)

    ts   = int(time.time() * 1000)          # milliseconds → unique per save
    dest = os.path.join(history_dir, f"index_{ts}.json")
    try:
        with open(dest, "w", encoding="utf-8") as f:
            f.write(current_data)
    except Exception as e:
        logging.warning(f"vdrive index history write failed: {e}")

    # Prune old snapshots
    try:
        snaps = sorted(
            os.path.join(history_dir, x)
            for x in os.listdir(history_dir)
            if x.endswith(".json")
        )
        while len(snaps) > MAX_HISTORY:
            try:
                os.remove(snaps.pop(0))
            except Exception:
                pass
    except Exception:
        pass


def list_history(drive: str) -> List[str]:
    history_dir = os.path.join(_meta_dir(drive), "history")
    if not os.path.isdir(history_dir):
        return []
    return sorted(
        os.path.join(history_dir, f)
        for f in os.listdir(history_dir)
        if f.endswith(".json")
    )


def _read_index_file(path: str) -> Optional[VirtualIndex]:
    """
    Read and parse an index JSON file.
    Returns None (not {}) if the file is missing, empty, or unparseable.
    """
    if not os.path.exists(path):
        return None
    try:
        size = os.path.getsize(path)
        if size == 0:
            logging.warning(f"vdrive: index file is empty: {path}")
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logging.warning(f"vdrive: index file is not a dict: {path}")
            return None
        return data
    except Exception as e:
        logging.warning(f"vdrive: failed to read index {path}: {e}")
        return None


# ── IndexManager ──────────────────────────────────────────────────────────────

class IndexManager:
    """Thread-safe virtual index manager."""

    def __init__(self, drives: List[str]):
        self._drives = drives
        self._index:  VirtualIndex = {}
        self._lock    = threading.Lock()

    # ── IO ────────────────────────────────────────────────────────────────────

    def load(self) -> None:
        """
        Load index from the first readable drive.
        Falls back to each replica in order.
        Does NOT overwrite memory with {} if all files are missing/empty —
        it only replaces memory when a valid non-empty dict is found.
        """
        for drive in self._drives:
            path = _index_file(drive)
            data = _read_index_file(path)
            if data is not None:
                with self._lock:
                    self._index = data
                logging.info(f"vdrive: index loaded from {path} ({len(data)} entries)")
                return

        # No readable index found on any drive — keep whatever is in memory.
        # This is normal on first use; don't wipe existing in-memory state.
        logging.info("vdrive: no existing index found on any drive (first use or drives empty)")

    def save(self) -> None:
        """
        Atomically write the current index to all drives.
        Uses a temp file + rename for atomicity so a crash mid-write never
        produces an empty index.json.
        """
        with self._lock:
            snapshot = dict(self._index)

        serialised = json.dumps(snapshot, indent=2)

        for drive in self._drives:
            try:
                meta = _ensure_meta(drive)
                path = _index_file(drive)

                # Write to a temp file first, then rename — atomic on Windows too
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(serialised)
                    f.flush()
                    os.fsync(f.fileno())

                # Rename is atomic on NTFS
                if os.path.exists(path):
                    os.replace(tmp, path)
                else:
                    os.rename(tmp, path)

                _hide(path)
                _rotate_history(drive, serialised)

            except Exception as e:
                logging.error(f"vdrive: index save failed on {drive}: {e}")

    def primary_index_path(self) -> str:
        if not self._drives:
            raise RuntimeError("No drives selected")
        _ensure_meta(self._drives[0])
        return _index_file(self._drives[0])

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def get(self, key: str) -> Optional[IndexEntry]:
        with self._lock:
            return self._index.get(key)

    def set(self, key: str, value: IndexEntry) -> None:
        with self._lock:
            self._index[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self._index.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._index.clear()

    def snapshot(self) -> VirtualIndex:
        with self._lock:
            return dict(self._index)

    def update_status(self, key: str, status: Status) -> None:
        with self._lock:
            if key in self._index and isinstance(self._index[key], dict):
                self._index[key]["status"] = status.value

    @staticmethod
    def entry_parts(val: Any) -> List[str]:
        return entry_parts(val)

    def list_history(self) -> List[str]:
        if not self._drives:
            return []
        return list_history(self._drives[0])

    def restore_snapshot(self, snapshot_path: str) -> bool:
        data = _read_index_file(snapshot_path)
        if data is None:
            return False
        with self._lock:
            self._index = data
        self.save()
        return True