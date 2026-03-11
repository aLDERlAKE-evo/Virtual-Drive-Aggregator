"""
config.py — AppConfig dataclass, Status enum, constants, config.json persistence.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List

CONFIG_FILE = "config.json"

# ── Constants (also importable directly) ──────────────────────────────────────
INDEX_DIR_NAME      = ".vdrive_meta"
INDEX_FILE_NAME     = "index.json"
INDEX_VERSION       = 2
LOG_FILE            = "vdrive_log.txt"
DEFAULT_CHUNK_MB    = 8
CHUNK_CHOICES_MB    = [4, 8, 16, 32, 64]
COMPRESSION_CHOICES = ["store", "zip", "lzma"]
DRIVE_MONITOR_MS    = 2_000
MAX_WORKERS         = 4
SPEED_HISTORY_LEN   = 30


# ── Status enum ───────────────────────────────────────────────────────────────
class Status(str, Enum):
    UPLOADING  = "uploading"
    COMPLETE   = "complete"
    INCOMPLETE = "incomplete"
    CANCELLED  = "cancelled"
    MISSING    = "missing"


# ── AppConfig dataclass ───────────────────────────────────────────────────────
@dataclass
class AppConfig:
    chunk_mb:       int         = DEFAULT_CHUNK_MB
    compression:    str         = "store"
    theme:          str         = "darkly"
    backup_nodes:   List[str]   = field(default_factory=list)
    backup_enabled: bool        = False
    max_workers:    int         = MAX_WORKERS

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "chunk_mb":       self.chunk_mb,
            "compression":    self.compression,
            "theme":          self.theme,
            "backup_nodes":   self.backup_nodes,
            "backup_enabled": self.backup_enabled,
            "max_workers":    self.max_workers,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AppConfig":
        return cls(
            chunk_mb       = int(d.get("chunk_mb",       DEFAULT_CHUNK_MB)),
            compression    = str(d.get("compression",    "store")),
            theme          = str(d.get("theme",          "darkly")),
            backup_nodes   = list(d.get("backup_nodes",  [])),
            backup_enabled = bool(d.get("backup_enabled", False)),
            max_workers    = int(d.get("max_workers",    MAX_WORKERS)),
        )

    @classmethod
    def load(cls) -> "AppConfig":
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    return cls.from_dict(json.load(f))
            except Exception:
                pass
        return cls()

    def save(self) -> None:
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.to_dict(), f, indent=2)
        except Exception:
            pass


# ── Legacy helpers kept for dialogs.py ────────────────────────────────────────
def save_config(d: dict) -> None:
    """Save a plain dict as config (used by SettingsDialog)."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
    except Exception:
        pass
