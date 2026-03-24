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

        # Encryption toggle
        tb.Label(self, text="Encrypt parts (AES-256):").grid(row=5, column=0, sticky="w", **pad)
        self._encrypt = tk.BooleanVar(value=cfg.encrypt_enabled)
        tb.Checkbutton(self, variable=self._encrypt,
                       bootstyle="round-toggle").grid(row=5, column=1, sticky="w", **pad)

        # Backup nodes
        tb.Label(self, text="Backup node IPs\n(one per line):").grid(
            row=6, column=0, sticky="nw", **pad)
        self._nodes_box = tk.Text(self, width=28, height=4)
        self._nodes_box.grid(row=6, column=1, **pad)
        self._nodes_box.insert("1.0", "\n".join(cfg.backup_nodes))

        # Buttons
        btn_row = tb.Frame(self)
        btn_row.grid(row=7, column=0, columnspan=2, pady=10)
        tb.Button(btn_row, text="Save",   bootstyle="success",   command=self._save).pack(side="left", padx=6)
        tb.Button(btn_row, text="Cancel", bootstyle="secondary", command=self.destroy).pack(side="left", padx=6)

    def _save(self):
        nodes_raw = self._nodes_box.get("1.0", "end").strip()
        nodes     = [n.strip() for n in nodes_raw.splitlines() if n.strip()]
        new_cfg   = AppConfig(
            chunk_mb        = self._chunk.get(),
            compression     = self._comp.get(),
            max_workers     = self._workers.get(),
            theme           = self._theme.get(),
            backup_nodes    = nodes,
            backup_enabled  = self._cfg.backup_enabled,
            encrypt_enabled = self._encrypt.get(),
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


class PasswordDialog(tb.Toplevel):
    """
    Modal password prompt for session-level AES-256 encryption.
    Call PasswordDialog.ask(parent) -> str | None.
    Returns the entered password, or None if the user cancelled.
    """

    def __init__(self, parent: tk.Misc):
        super().__init__(parent)
        self.title("Encryption Password")
        self.resizable(False, False)
        self.grab_set()
        self._result: "str | None" = None

        tb.Label(self, text="Session Encryption",
                 font=("Segoe UI", 13, "bold")).pack(pady=(16, 4), padx=24)
        tb.Label(
            self,
            text="Enter a password to encrypt / decrypt all files\n"
                 "uploaded in this session.  You will need the same\n"
                 "password to download files later.",
            foreground="gray",
            justify="center",
        ).pack(padx=24, pady=(0, 12))

        tb.Label(self, text="Password:").pack(anchor="w", padx=24)
        self._var = tk.StringVar()
        entry1 = tb.Entry(self, textvariable=self._var, show="●", width=30)
        entry1.pack(padx=24, pady=(2, 8))
        entry1.focus_set()

        tb.Label(self, text="Confirm password:").pack(anchor="w", padx=24)
        self._var2 = tk.StringVar()
        tb.Entry(self, textvariable=self._var2, show="●", width=30).pack(padx=24, pady=(2, 6))

        self._err = tk.StringVar()
        tb.Label(self, textvariable=self._err,
                 foreground="red", font=("Segoe UI", 9)).pack()

        btn_row = tb.Frame(self)
        btn_row.pack(pady=14)
        tb.Button(btn_row, text="Unlock",  bootstyle="success",
                  command=self._ok).pack(side="left", padx=8)
        tb.Button(btn_row, text="Cancel",  bootstyle="secondary",
                  command=self.destroy).pack(side="left", padx=8)

        self.bind("<Return>", lambda _: self._ok())
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window()

    def _ok(self) -> None:
        pwd  = self._var.get()
        pwd2 = self._var2.get()
        if len(pwd) < 6:
            self._err.set("Password must be at least 6 characters.")
            return
        if pwd != pwd2:
            self._err.set("Passwords do not match.")
            return
        self._result = pwd
        self.destroy()

    @classmethod
    def ask(cls, parent: tk.Misc) -> "str | None":
        """Convenience factory — returns password string or None."""
        return cls(parent)._result


class PostProcessDialog(tb.Toplevel):
    """
    Dialog to compress and/or encrypt an already-stored file.

    result is a tuple (do_compress: bool, compress_fmt: str, do_encrypt: bool)
    or None if the user cancelled.
    """

    def __init__(
        self,
        parent:             tk.Misc,
        filename:           str,
        already_compressed: bool = False,
        already_encrypted:  bool = False,
    ):
        super().__init__(parent)
        self.title("Post-process File")
        self.resizable(False, False)
        self.grab_set()
        self.result = None

        tb.Label(self, text="Post-process stored file",
                 font=("Segoe UI", 13, "bold")).pack(pady=(16, 4), padx=24)
        tb.Label(self, text=f"File:  {filename}",
                 foreground="gray").pack(padx=24, pady=(0, 12))

        # ── Compression ───────────────────────────────────────────────────
        comp_frame = tb.Labelframe(self, text="Compression", padding=10)
        comp_frame.pack(fill="x", padx=20, pady=6)

        self._do_compress = tk.BooleanVar(value=False)
        comp_cb = tb.Checkbutton(
            comp_frame, text="Compress file",
            variable=self._do_compress, bootstyle="round-toggle",
            command=self._toggle_compress,
        )
        comp_cb.pack(anchor="w")

        if already_compressed:
            tb.Label(comp_frame, text="⚠ Already compressed — will re-compress",
                     foreground="orange").pack(anchor="w", pady=(2, 0))

        fmt_row = tb.Frame(comp_frame)
        fmt_row.pack(anchor="w", pady=(6, 0))
        tb.Label(fmt_row, text="Format:").pack(side="left")
        self._fmt = tk.StringVar(value="lzma")
        self._fmt_combo = tb.Combobox(
            fmt_row, textvariable=self._fmt,
            values=["zip", "lzma"], width=8, state="disabled",
        )
        self._fmt_combo.pack(side="left", padx=6)

        # ── Encryption ────────────────────────────────────────────────────
        enc_frame = tb.Labelframe(self, text="Encryption", padding=10)
        enc_frame.pack(fill="x", padx=20, pady=6)

        self._do_encrypt = tk.BooleanVar(value=False)
        tb.Checkbutton(
            enc_frame, text="Encrypt with AES-256 (uses current session key)",
            variable=self._do_encrypt, bootstyle="round-toggle",
        ).pack(anchor="w")

        if already_encrypted:
            tb.Label(enc_frame,
                     text="⚠ Already encrypted — will re-encrypt with current session key",
                     foreground="orange").pack(anchor="w", pady=(2, 0))
        else:
            tb.Label(enc_frame,
                     text="Requires encryption to be enabled and drives confirmed with a password.",
                     foreground="gray", font=("Segoe UI", 8)).pack(anchor="w", pady=(2, 0))

        # ── Buttons ───────────────────────────────────────────────────────
        btn_row = tb.Frame(self)
        btn_row.pack(pady=16)
        tb.Button(btn_row, text="Apply",  bootstyle="success",
                  command=self._apply).pack(side="left", padx=8)
        tb.Button(btn_row, text="Cancel", bootstyle="secondary",
                  command=self.destroy).pack(side="left", padx=8)

        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window()

    def _toggle_compress(self):
        state = "readonly" if self._do_compress.get() else "disabled"
        self._fmt_combo.configure(state=state)

    def _apply(self):
        do_compress  = self._do_compress.get()
        compress_fmt = self._fmt.get() if do_compress else "store"
        do_encrypt   = self._do_encrypt.get()

        if not do_compress and not do_encrypt:
            messagebox.showinfo("Nothing selected",
                                "Select at least one operation to apply.",
                                parent=self)
            return

        self.result = (do_compress, compress_fmt, do_encrypt)
        self.destroy()


class UploadOptionsDialog(tb.Toplevel):
    """
    Shown before every upload (file or folder).
    Lets the user pick compression mode per upload — not a global setting.

    result: str — "store" | "zip" | "lzma", or None if cancelled.
    """

    def __init__(self, parent: tk.Misc):
        super().__init__(parent)
        self.title("Upload Options")
        self.resizable(False, False)
        self.grab_set()
        self.result: "str | None" = None

        tb.Label(self, text="How should this file be stored?",
                 font=("Segoe UI", 12, "bold")).pack(pady=(18, 6), padx=24)

        desc = tk.StringVar(value="No compression — fastest upload, original size.")
        tb.Label(self, textvariable=desc, foreground="gray",
                 font=("Segoe UI", 9), justify="center").pack(padx=24, pady=(0, 12))

        self._choice = tk.StringVar(value="store")

        options = [
            ("store", "No Compression",  "Fastest upload. File stored at original size."),
            ("zip",   "ZIP (Deflate)",   "Moderate compression. Fast, widely compatible."),
            ("lzma",  "LZMA",            "Best compression ratio. Slower to compress."),
        ]

        frame = tb.Frame(self)
        frame.pack(padx=24, fill="x")

        def _update_desc(*_):
            for val, _, d in options:
                if self._choice.get() == val:
                    desc.set(d)

        for val, label, _ in options:
            tb.Radiobutton(
                frame, text=label, variable=self._choice, value=val,
                bootstyle="toolbutton",
                command=_update_desc,
            ).pack(fill="x", pady=2)

        btn_row = tb.Frame(self)
        btn_row.pack(pady=16)
        tb.Button(btn_row, text="Upload",  bootstyle="success",
                  command=self._ok).pack(side="left", padx=8)
        tb.Button(btn_row, text="Cancel",  bootstyle="secondary",
                  command=self.destroy).pack(side="left", padx=8)

        self.bind("<Return>", lambda _: self._ok())
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window()

    def _ok(self):
        self.result = self._choice.get()
        self.destroy()