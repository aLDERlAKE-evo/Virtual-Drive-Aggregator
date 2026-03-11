"""
ui/dialogs.py — SettingsDialog, RepairDialog, HistoryDialog.

SettingsDialog accepts an AppConfig object and calls on_save(AppConfig).
RepairDialog accepts broken entries and two callbacks: on_reupload, on_remove.
"""

from __future__ import annotations

import os
import time
import tkinter as tk
from tkinter import messagebox
from typing import Any, Callable, Dict, List

import ttkbootstrap as tb

from config import (
    AppConfig,
    CHUNK_CHOICES_MB,
    COMPRESSION_CHOICES,
)


class SettingsDialog(tb.Toplevel):
    """
    Modal settings dialog.
    Calls on_save(AppConfig) when the user clicks Save.
    """

    def __init__(
        self,
        parent:   tk.Misc,
        cfg:      AppConfig,
        on_save:  Callable[[AppConfig], None],
    ):
        super().__init__(parent)
        self.title("Settings")
        self.resizable(False, False)
        self.grab_set()
        self._on_save = on_save
        self._cfg     = cfg

        pad = {"padx": 12, "pady": 6}

        tb.Label(self, text="Settings", font=("Segoe UI", 13, "bold")).grid(
            row=0, column=0, columnspan=2, pady=(12, 4))

        # Chunk size
        tb.Label(self, text="Chunk size (MB):").grid(row=1, column=0, sticky="w", **pad)
        self._chunk = tk.IntVar(value=cfg.chunk_mb)
        tb.Combobox(self, textvariable=self._chunk, values=CHUNK_CHOICES_MB,
                    width=8, state="readonly").grid(row=1, column=1, **pad)

        # Compression
        tb.Label(self, text="Compression:").grid(row=2, column=0, sticky="w", **pad)
        self._comp = tk.StringVar(value=cfg.compression)
        tb.Combobox(self, textvariable=self._comp, values=COMPRESSION_CHOICES,
                    width=8, state="readonly").grid(row=2, column=1, **pad)

        # Max workers
        tb.Label(self, text="Max workers:").grid(row=3, column=0, sticky="w", **pad)
        self._workers = tk.IntVar(value=cfg.max_workers)
        tb.Spinbox(self, from_=1, to=8, textvariable=self._workers,
                   width=6).grid(row=3, column=1, **pad)

        # Theme
        tb.Label(self, text="Theme:").grid(row=4, column=0, sticky="w", **pad)
        self._theme = tk.StringVar(value=cfg.theme)
        themes = ["darkly", "flatly", "cyborg", "journal", "litera", "minty", "solar"]
        tb.Combobox(self, textvariable=self._theme, values=themes,
                    width=10, state="readonly").grid(row=4, column=1, **pad)

        # Backup nodes
        tb.Label(self, text="Backup node IPs\n(one per line):").grid(
            row=5, column=0, sticky="nw", **pad)
        self._nodes_box = tk.Text(self, width=28, height=4)
        self._nodes_box.grid(row=5, column=1, **pad)
        self._nodes_box.insert("1.0", "\n".join(cfg.backup_nodes))

        # Buttons
        btn_row = tb.Frame(self)
        btn_row.grid(row=6, column=0, columnspan=2, pady=10)
        tb.Button(btn_row, text="Save",   bootstyle="success",   command=self._save).pack(side="left", padx=6)
        tb.Button(btn_row, text="Cancel", bootstyle="secondary", command=self.destroy).pack(side="left", padx=6)

    def _save(self):
        nodes_raw = self._nodes_box.get("1.0", "end").strip()
        nodes     = [n.strip() for n in nodes_raw.splitlines() if n.strip()]
        new_cfg   = AppConfig(
            chunk_mb       = self._chunk.get(),
            compression    = self._comp.get(),
            max_workers    = self._workers.get(),
            theme          = self._theme.get(),
            backup_nodes   = nodes,
            backup_enabled = self._cfg.backup_enabled,
        )
        new_cfg.save()
        self._on_save(new_cfg)
        self.destroy()


class RepairDialog(tb.Toplevel):
    """
    Shows files with incomplete/missing status.
    on_reupload(key) — called when user clicks Re-upload.
    on_remove(key)   — called when user clicks Remove from index.
    """

    def __init__(
        self,
        parent:      tk.Misc,
        broken:      Dict[str, Any],
        on_reupload: Callable[[str], None],
        on_remove:   Callable[[str], None],
    ):
        super().__init__(parent)
        self.title("Repair Index")
        self.geometry("540x360")
        self.grab_set()

        tb.Label(self, text="Files with incomplete / missing status:",
                 font=("Segoe UI", 11, "bold")).pack(pady=(12, 4))

        frame = tb.Frame(self)
        frame.pack(fill="both", expand=True, padx=12)
        self._lb = tk.Listbox(frame, selectmode="single")
        self._lb.pack(side="left", fill="both", expand=True)
        sb = tb.Scrollbar(frame, command=self._lb.yview)
        self._lb.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")

        self._keys: List[str] = list(broken.keys())
        for k in self._keys:
            status = broken[k].get("status", "?") if isinstance(broken[k], dict) else "?"
            self._lb.insert("end", f"{k}  [{status}]")

        if not self._keys:
            self._lb.insert("end", "✅  All files are complete.")

        btn_row = tb.Frame(self)
        btn_row.pack(pady=10)
        tb.Button(btn_row, text="Re-upload",         bootstyle="primary",
                  command=lambda: self._act(on_reupload)).pack(side="left", padx=6)
        tb.Button(btn_row, text="Remove from index", bootstyle="danger",
                  command=lambda: self._act(on_remove)).pack(side="left", padx=6)
        tb.Button(btn_row, text="Close",             bootstyle="secondary",
                  command=self.destroy).pack(side="left", padx=6)

    def _act(self, fn: Callable[[str], None]) -> None:
        sel = self._lb.curselection()
        if not sel:
            messagebox.showinfo("Select", "Select a file first.", parent=self)
            return
        idx = sel[0]
        if idx < len(self._keys):
            fn(self._keys[idx])
            self._lb.delete(idx)
            self._keys.pop(idx)


class HistoryDialog(tb.Toplevel):
    """Browse and restore index snapshots."""

    def __init__(
        self,
        parent:     tk.Misc,
        snapshots:  List[str],
        on_restore: Callable[[str], None],
    ):
        super().__init__(parent)
        self.title("Index History")
        self.geometry("480x300")
        self.grab_set()
        self._on_restore = on_restore
        self._snaps = snapshots

        tb.Label(self, text="Select a snapshot to restore:",
                 font=("Segoe UI", 11, "bold")).pack(pady=(12, 4))

        frame = tb.Frame(self)
        frame.pack(fill="both", expand=True, padx=12)
        self._lb = tk.Listbox(frame)
        self._lb.pack(side="left", fill="both", expand=True)
        sb = tb.Scrollbar(frame, command=self._lb.yview)
        self._lb.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")

        for s in snapshots:
            ts = os.path.basename(s).replace("index_", "").replace(".json", "")
            try:
                label = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
            except Exception:
                label = ts
            self._lb.insert("end", label)

        if not snapshots:
            self._lb.insert("end", "No snapshots found.")

        btn_row = tb.Frame(self)
        btn_row.pack(pady=10)
        tb.Button(btn_row, text="Restore", bootstyle="warning",
                  command=self._restore).pack(side="left", padx=6)
        tb.Button(btn_row, text="Close",   bootstyle="secondary",
                  command=self.destroy).pack(side="left", padx=6)

    def _restore(self) -> None:
        sel = self._lb.curselection()
        if not sel or sel[0] >= len(self._snaps):
            return
        snap = self._snaps[sel[0]]
        if messagebox.askyesno("Confirm", "Restore this snapshot?\nThis overwrites the current index.", parent=self):
            self._on_restore(snap)
            self.destroy()
