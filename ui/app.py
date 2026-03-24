"""
ui/app.py — Main application window and transfer orchestration.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from tkinter import filedialog, messagebox
from typing import Callable, Dict, List, Optional
import tkinter as tk
from tkinter import ttk

import ttkbootstrap as tb
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from backup.client import BackupClient
from config import AppConfig, Status
from core.drives import DriveMonitor, list_removable_drives
from core.index import IndexManager, build_entry
from core.splitter import Splitter, sha256
from ui.dialogs import RepairDialog, SettingsDialog
from ui.widgets import FileTree, SpeedGraph, StatusBar

try:
    from tkinterdnd2 import DND_FILES
    _DND = True
except Exception:
    _DND = False


class App:
    def __init__(self, root: tb.Window):
        self.root   = root
        self.cfg    = AppConfig.load()

        self.root.title("vdrive — Virtual Aggregated Drive Manager")
        self.root.geometry("1300x860")

        # Apply saved theme
        try:
            self.root.style.theme_use(self.cfg.theme)
        except Exception:
            pass

        # State
        self.selected_drives: List[str] = []
        self.index    = IndexManager([])
        self.splitter: Optional[Splitter] = None

        self.user_pause   = threading.Event()
        self.drive_pause  = threading.Event()
        self.cancel_flag  = threading.Event()

        self.backup_client = BackupClient(list(self.cfg.backup_nodes))

        # Executor
        self.executor = ThreadPoolExecutor(max_workers=self.cfg.max_workers)
        self._futures: List[Future] = []
        self._fut_lock = threading.Lock()

        # Session encryption key (derived at Confirm Drives time)
        self._session_key:      bytes | None = None
        self._session_password: str   | None = None

        # Sync watcher
        self.sync_enabled  = True
        self.sync_observer: Optional[Observer] = None

        # Drive monitor
        self.drive_monitor = DriveMonitor(
            cfg=self.cfg,
            on_change=self._on_drive_change,
            on_missing=self._on_drive_missing,
        )
        self.drive_monitor.start()

        self._build_ui()

        self._update_storage_info()
        self._refresh_drives_ui()
        self.root.after(5000, lambda: self._submit(self._refresh_server_worker))

    class _SpeedProxy:
        _MAX = 30

        def __init__(self, canvas: tk.Canvas, history: list):
            self._c = canvas
            self._h = history

        def push(self, speed: float) -> None:
            self._h.append(max(0.0, speed))
            if len(self._h) > self._MAX:
                self._h.pop(0)
            try:
                self._c.after(0, self._draw)
            except Exception:
                pass

        def reset(self) -> None:
            self._h.clear()
            try:
                self._c.after(0, lambda: self._c.delete("all"))
            except Exception:
                pass

        def _draw(self) -> None:
            c = self._c
            c.delete("all")
            data = list(self._h)
            if len(data) < 2:
                return
            w  = c.winfo_width()  or 200
            h  = c.winfo_height() or 40
            pk = max(data) or 1.0
            pad = 4
            xs = [pad + i * (w - 2*pad) / (len(data)-1) for i in range(len(data))]
            ys = [h - pad - (v/pk) * (h - 2*pad) for v in data]
            pts = [coord for pair in zip(xs, ys) for coord in pair]
            c.create_line(*pts, fill="#00d2ff", width=2, smooth=True)
            c.create_text(w-4, 4, anchor="ne",
                          text=f"{data[-1]:.1f} MB/s",
                          fill="white", font=("Segoe UI", 8))

    def _build_ui(self):
        # Top bar
        top = tb.Frame(self.root, padding=8)
        top.pack(fill="x")
        tb.Label(top, text="vdrive — Virtual Aggregated Drive Manager",
                 font=("Segoe UI", 15, "bold")).pack(side="left")
        tb.Button(top, text="⚙ Settings", bootstyle="secondary",
                  command=self._open_settings).pack(side="right", padx=4)
        tb.Button(top, text="🔧 Repair", bootstyle="warning",
                  command=self._open_repair).pack(side="right", padx=4)
        tb.Button(top, text="🕘 History", bootstyle="secondary",
                  command=self._open_history).pack(side="right", padx=4)
        tb.Button(top, text="🌙 Theme", bootstyle="secondary",
                  command=self._toggle_theme).pack(side="right", padx=4)

        main = tb.Frame(self.root, padding=10)
        main.pack(fill="both", expand=True)

        # Drive selection panel
        drive_panel = tb.Labelframe(main, text="Drive Selection", padding=8)
        drive_panel.pack(fill="x", pady=6)

        self._drive_vars:   List[tk.StringVar] = []
        self._drive_combos: List[ttk.Combobox] = []

        drive_row = tb.Frame(drive_panel)
        drive_row.pack()
        for i in range(4):
            col = tb.Frame(drive_row)
            col.pack(side="left", padx=10)
            tb.Label(col, text=f"Drive {i+1}").pack()
            var = tk.StringVar(value="None")
            self._drive_vars.append(var)
            cb = tb.Combobox(col, textvariable=var, values=["None"],
                             width=12, state="readonly")
            cb.pack()
            self._drive_combos.append(cb)

        opts_row = tb.Frame(drive_panel)
        opts_row.pack(fill="x", pady=4)

        self._chunk_var = tk.IntVar(value=self.cfg.chunk_mb)
        tb.Label(opts_row, text="Chunk MB:").pack(side="left", padx=(6, 4))
        tb.Combobox(opts_row, textvariable=self._chunk_var,
                    values=[4, 8, 16, 32, 64], width=6, state="readonly").pack(side="left")

        self._backup_var    = tk.BooleanVar(value=self.cfg.backup_enabled)
        self._backup_status = tk.StringVar(value="Backup: OFF")
        tb.Checkbutton(opts_row, text="Enable Backup",
                       variable=self._backup_var, bootstyle="round-toggle",
                       command=self._toggle_backup).pack(side="left", padx=14)
        tb.Label(opts_row, textvariable=self._backup_status).pack(side="left")

        btn_row = tb.Frame(drive_panel)
        btn_row.pack(pady=6)
        tb.Button(btn_row, text="Confirm Drives", bootstyle="success",
                  command=self._confirm_drives).pack(side="left", padx=6)
        tb.Button(btn_row, text="Refresh", bootstyle="info",
                  command=self._refresh_drives_ui).pack(side="left", padx=6)
        tb.Button(btn_row, text="Purge All", bootstyle="danger",
                  command=self._purge_all).pack(side="left", padx=6)

        # Notebook
        nb = tb.Notebook(main)
        nb.pack(fill="both", expand=True, pady=6)

        files_tab  = tb.Frame(nb)
        server_tab = tb.Frame(nb)
        logs_tab   = tb.Frame(nb)
        nb.add(files_tab,  text="Files")
        nb.add(server_tab, text="Server")
        nb.add(logs_tab,   text="Logs")

        # Files tab
        self.file_tree = FileTree(files_tab)
        self.file_tree.pack(fill="both", expand=True, padx=4, pady=4)

        ec_row = tb.Frame(files_tab)
        ec_row.pack()
        tb.Button(ec_row, text="Expand All", bootstyle="secondary",
                  command=self.file_tree.expand_all).pack(side="left", padx=4)
        tb.Button(ec_row, text="Collapse All", bootstyle="secondary",
                  command=self.file_tree.collapse_all).pack(side="left", padx=4)

        bstyle = dict(width=16, padding=5)
        ctrl   = tb.Frame(files_tab)
        ctrl.pack(pady=6)

        row1 = tb.Frame(ctrl)
        row1.pack(pady=4)
        tb.Button(row1, text="Upload File",   bootstyle="info",
                  command=self._upload_file_dialog,   **bstyle).pack(side="left", padx=5)
        tb.Button(row1, text="Upload Folder", bootstyle="primary",
                  command=self._upload_folder_dialog, **bstyle).pack(side="left", padx=5)
        tb.Button(row1, text="Download",      bootstyle="warning",
                  command=self._download_item,        **bstyle).pack(side="left", padx=5)
        tb.Button(row1, text="Delete",        bootstyle="danger",
                  command=self._delete_item,          **bstyle).pack(side="left", padx=5)
        tb.Button(row1, text="Refresh",       bootstyle="success",
                  command=self._do_refresh_tree,      **bstyle).pack(side="left", padx=5)

        row2 = tb.Frame(ctrl)
        row2.pack(pady=4)
        tb.Button(row2, text="Pause",               bootstyle="warning",
                  command=self._pause,               **bstyle).pack(side="left", padx=5)
        tb.Button(row2, text="Resume",              bootstyle="success",
                  command=self._resume,              **bstyle).pack(side="left", padx=5)
        tb.Button(row2, text="Cancel",              bootstyle="danger",
                  command=self._cancel,              **bstyle).pack(side="left", padx=5)
        tb.Button(row2, text="Post-process",        bootstyle="info",
                  command=self._postprocess_item,    **bstyle).pack(side="left", padx=5)
        tb.Button(row2, text="Restore from Server", bootstyle="primary",
                  command=self._restore_from_server, **bstyle).pack(side="left", padx=5)

        # Status bar + speed graph
        status_row = tb.Frame(main)
        status_row.pack(fill="x", expand=False, pady=4)

        graph_frame = tk.Frame(status_row, width=210, height=56, bg="#1a1a2e")
        graph_frame.pack(side="right", padx=8)
        graph_frame.pack_propagate(False)
        self._speed_canvas = tk.Canvas(graph_frame, bg="#1a1a2e", highlightthickness=0)
        self._speed_canvas.pack(fill="both", expand=True)
        self._speed_history: list = []
        self.speed_graph = self._SpeedProxy(self._speed_canvas, self._speed_history)

        self.status_bar = StatusBar(status_row)
        self.status_bar.pack(side="left", fill="both", expand=True)

        # Server tab
        self.server_list = tk.Listbox(server_tab, height=18)
        self.server_list.pack(fill="both", expand=True, padx=10, pady=10)
        srv_btns = tb.Frame(server_tab)
        srv_btns.pack(pady=6)
        tb.Button(srv_btns, text="Refresh", bootstyle="info",
                  command=lambda: self._submit(self._refresh_server_worker)).pack(side="left", padx=6)
        tb.Button(srv_btns, text="Restore", bootstyle="success",
                  command=self._restore_from_server).pack(side="left", padx=6)
        tb.Button(srv_btns, text="Delete",  bootstyle="danger",
                  command=lambda: self._submit(self._delete_from_server_worker)).pack(side="left", padx=6)

        # Logs tab
        self.log_box = tk.Text(logs_tab, bg="#0d0d0d", fg="#39ff14",
                               wrap="none", font=("Consolas", 9))
        self.log_box.pack(fill="both", expand=True)

        if _DND:
            try:
                self.file_tree.tree.drop_target_register(DND_FILES)
                self.file_tree.tree.dnd_bind("<<Drop>>", self._on_drop)
            except Exception:
                pass

    def _refresh_drives_ui(self):
        drives = list_removable_drives(self.cfg)
        values = ["None"] + drives
        for var, cb in zip(self._drive_vars, self._drive_combos):
            cur = var.get()
            cb["values"] = values
            if cur not in values:
                var.set("None")

    def _confirm_drives(self):
        chosen = [v.get() for v in self._drive_vars if v.get() != "None"]
        if len(chosen) < 2:
            messagebox.showerror("Error", "Select at least two removable drives.")
            return

        self._session_key      = None
        self._session_password = None   # needed for per-file key derivation
        if self.cfg.encrypt_enabled:
            from ui.dialogs import PasswordDialog
            pwd = PasswordDialog.ask(self.root)
            if pwd is None:
                return
            from core.crypto import derive_key, new_salt
            salt_hex = getattr(self.cfg, "_session_salt_hex", None)
            if not salt_hex:
                salt     = new_salt()
                salt_hex = salt.hex()
                self.cfg._session_salt_hex = salt_hex
                self.cfg.save()
            else:
                salt = bytes.fromhex(salt_hex)
            self._session_key      = derive_key(pwd, salt)
            self._session_password = pwd
            self._log("Encryption enabled — session key derived ✓")

        self.selected_drives = [d if d.endswith("/") else d + "/" for d in chosen]
        self.index = IndexManager(self.selected_drives)
        self.drive_monitor.set_selected(self.selected_drives)

        self._build_splitter()
        self._reload_index_from_disk()
        self._start_sync_observer(self.selected_drives[0])
        self._auto_repair_on_startup()
        self._log(f"Drives confirmed: {self.selected_drives}")

    def _build_splitter(self):
        self.cancel_flag.clear()
        self.user_pause.clear()
        self.drive_pause.clear()
        self.cfg.chunk_mb = self._chunk_var.get()
        self.splitter = Splitter(
            drives         = self.selected_drives,
            cfg            = self.cfg,
            cancel_flag    = self.cancel_flag,
            user_pause     = self.user_pause,
            drive_pause    = self.drive_pause,
            on_drive_error = self._on_drive_error_cb,
        )

    def _on_drive_change(self, added: List[str], removed: List[str]):
        self.root.after(0, self._refresh_drives_ui)
        if added:
            self._log(f"Drives connected: {added}")
            if self.selected_drives:
                from core.drives import list_removable_drives
                current  = set(list_removable_drives(self.cfg))
                all_back = all(d in current for d in self.selected_drives)
                if all_back:
                    self._log("All selected drives reconnected — reloading index")
                    self.drive_pause.clear()
                    self.root.after(0, self._reload_index_from_disk)
        if removed:
            self._log(f"Drives disconnected: {removed}", level="warning")

    def _on_drive_missing(self, missing: List[str]):
        if not self.drive_pause.is_set():
            self.drive_pause.set()
            self._log(f"Selected drives missing: {missing} — paused", level="warning")
            self.root.after(0, lambda: messagebox.showwarning(
                "Drive Removed", f"Missing: {missing}\nReconnect to resume."))

    def _on_drive_error_cb(self, msg: str):
        self._log(f"Drive write error: {msg}", level="error")

    def _do_refresh_tree(self):
        self.file_tree.refresh(self.index.snapshot())

    def _reload_index_from_disk(self):
        if self.selected_drives:
            self.index.load()
        self._do_refresh_tree()

    def _update_storage_info(self):
        total, free = 0, 0
        try:
            for d in self.selected_drives:
                u = shutil.disk_usage(d)
                total += u.total
                free  += u.free
            t = total / 1e9; f = free / 1e9; u2 = t - f
            pct = (f / t * 100) if t else 0
            self.status_bar.storage_info.set(
                f"Total: {t:.2f} GB | Used: {u2:.2f} GB | Free: {f:.2f} GB ({pct:.1f}%)"
            )
        except Exception:
            self.status_bar.storage_info.set("Total: -- | Free: --")
        self.root.after(3000, self._update_storage_info)

    def _upload_file_dialog(self):
        if not self.selected_drives:
            messagebox.showerror("Error", "Confirm drives first.")
            return
        p = filedialog.askopenfilename()
        if not p:
            return
        from ui.dialogs import UploadOptionsDialog
        dlg = UploadOptionsDialog(self.root)
        if dlg.result is None:
            return
        self._submit(self._upload_single_file, p, None, dlg.result)

    def _upload_folder_dialog(self):
        if not self.selected_drives:
            messagebox.showerror("Error", "Confirm drives first.")
            return
        folder = filedialog.askdirectory()
        if not folder:
            return
        from ui.dialogs import UploadOptionsDialog
        dlg = UploadOptionsDialog(self.root)
        if dlg.result is None:
            return
        self._submit(self._upload_folder, folder, dlg.result)

    def _upload_single_file(
        self,
        path:         str,
        rel_override: str | None = None,
        mode:         str = "store",
    ):
        rel = rel_override or os.path.basename(path)

        file_hash = sha256(path)
        for entry in self.index.snapshot().values():
            if isinstance(entry, dict):
                if file_hash in entry.get("checksums", {}).values():
                    self._ui_info("Info", "File already exists in storage (duplicate skipped).")
                    return

        self._reset_flags()
        tmp_archive:   Optional[str] = None
        tmp_encrypted: Optional[str] = None
        placeholder = rel

        try:
            src        = path
            orig_size  = os.path.getsize(path)
            compressed = False
            fmt        = "store"

            if mode in ("zip", "lzma"):
                self.root.after_idle(lambda m=mode, r=rel: self.status_bar.current_file.set(
                    f"Compressing ({m}): {r}…"))
                self._log(f"Compressing {rel} ({mode})…")
                tmp_archive = self.splitter.compress(path, mode)
                src         = tmp_archive
                compressed  = True
                fmt         = mode
                placeholder = rel + f".{mode}bundle"

            enc_salt_hex: Optional[str] = None
            is_encrypted = bool(self._session_key)
            if is_encrypted:
                from core.crypto import new_iv, new_salt, derive_key
                enc_salt     = new_salt()
                enc_iv       = new_iv()
                enc_salt_hex = enc_salt.hex()
                per_file_key = derive_key(self._session_password, enc_salt)
                self.root.after_idle(lambda r=rel: self.status_bar.current_file.set(
                    f"Encrypting: {r}…"))
                self._log(f"Encrypting {rel}…")
                _enc_name     = f"vdrive_enc_{os.getpid()}_{os.path.basename(src)}.enc"
                tmp_encrypted = os.path.join(self.splitter._tmp_dir(), _enc_name)
                Splitter.encrypt_file(src, tmp_encrypted, per_file_key, enc_iv)
                if tmp_archive:
                    try: os.remove(tmp_archive)
                    except Exception: pass
                    tmp_archive = None
                src = tmp_encrypted

            existing     = self.index.get(placeholder)
            cached_sizes = existing.get("part_sizes") if isinstance(existing, dict) else None

            self.index.set(placeholder, build_entry(
                [], compressed, fmt, rel, orig_size,
                status=Status.UPLOADING, checksums={}, verified=False,
                part_sizes=cached_sizes,
                encrypted=is_encrypted, salt_hex=enc_salt_hex,
            ))
            self.index.save()
            self.root.after(0, self._do_refresh_tree)

            self.root.after_idle(lambda r=rel: self.status_bar.current_file.set(
                f"Splitting: {r}…"))

            def on_progress(done, total, eta_s, speed):
                self.root.after_idle(lambda: (
                    self.status_bar.update_transfer(f"Uploading: {rel}", done, total, eta_s, speed),
                    self.speed_graph.push(speed),
                ))

            parts, checksums, part_sizes = self.splitter.split(src, on_progress, cached_sizes)

            if self._backup_var.get() and self.backup_client.nodes:
                self.root.after_idle(lambda r=rel: self.status_bar.current_file.set(
                    f"Backing up: {r}…"))
                self._log("Sending to backup node…")
                self.backup_client.backup_all(parts, self.index.primary_index_path())

            self.index.set(placeholder, build_entry(
                parts, compressed, fmt, rel, orig_size,
                status=Status.COMPLETE, checksums=checksums, verified=True,
                part_sizes=part_sizes,
                encrypted=is_encrypted, salt_hex=enc_salt_hex,
            ))
            self.index.save()
            self.root.after(0, self._do_refresh_tree)
            self._ui_info("Success", f"Uploaded: {rel}")
            self._log(f"Uploaded {rel} → {len(parts)} part(s)")

        except KeyboardInterrupt:
            self.index.update_status(placeholder, Status.CANCELLED)
            self.index.save()
            self.root.after(0, self._do_refresh_tree)
            self._log(f"Upload cancelled: {rel}", level="warning")
        except ValueError as e:
            self._ui_error(str(e))
            self.index.update_status(placeholder, Status.INCOMPLETE)
            self.index.save()
        except Exception as e:
            self.index.update_status(placeholder, Status.INCOMPLETE)
            self.index.save()
            self.root.after(0, self._do_refresh_tree)
            self._ui_error(f"Upload failed: {e}")
            self._log(f"Upload error ({rel}): {e}", level="error")
        finally:
            for _tmp in [tmp_archive, tmp_encrypted]:
                if _tmp and os.path.exists(_tmp):
                    try: os.remove(_tmp)
                    except Exception: pass
            self.root.after(0, self.status_bar.reset)
            self.root.after(0, self.speed_graph.reset)

    def _upload_folder(self, folder: str, mode: str = "store"):
        files: List[tuple[str, str]] = []
        base = os.path.basename(folder.rstrip("/\\"))
        for root_dir, _, fnames in os.walk(folder):
            for fname in fnames:
                full = os.path.join(root_dir, fname)
                rel  = os.path.join(base, os.path.relpath(full, folder))
                files.append((full, rel))

        if not files:
            self._ui_info("Info", "Folder is empty.")
            return

        total = len(files)
        self._reset_flags()
        self.root.after_idle(lambda: self.status_bar.file_counter.set(f"0/{total}"))
        start = time.time()

        for i, (full, rel) in enumerate(files, 1):
            if self.cancel_flag.is_set():
                break
            self._upload_single_file(full, rel, mode)

            elapsed     = max(1e-9, time.time() - start)
            eta_overall = (elapsed / i) * (total - i)
            self.root.after_idle(lambda v=(i/total*100): self.status_bar.folder_progress.set(v))
            self.root.after_idle(lambda ii=i, t=total: self.status_bar.file_counter.set(f"{ii}/{t}"))
            self.root.after_idle(lambda e=eta_overall: self.status_bar.eta.set(f"ETA: {int(e)}s"))

        self.index.save()
        self.root.after(0, self._do_refresh_tree)
        if not self.cancel_flag.is_set():
            self._ui_info("Success", f"Folder '{base}' uploaded!")
        self.root.after(0, self.status_bar.reset)
        self.root.after(0, self.speed_graph.reset)

    def _download_item(self):
        rel = self.file_tree.selected_rel_path()
        if not rel:
            return
        snap = self.index.snapshot()
        is_folder = any(k.startswith(rel + os.sep) for k in snap)
        if is_folder:
            self._submit(self._download_folder, rel)
        else:
            self._submit(self._download_file, rel)

    def _download_file(self, rel: str):
        entry = self.index.get(rel)
        if not entry:
            self._ui_error(f"Not in index: {rel}")
            return
        if isinstance(entry, dict) and entry.get("status") != Status.COMPLETE.value:
            self._ui_info("Not ready", f"'{rel}' is not complete.")
            return
        save_path = filedialog.asksaveasfilename(initialfile=os.path.basename(rel))
        if not save_path:
            return
        ok = self._download_single(rel, entry, os.path.dirname(save_path),
                                   override_name=os.path.basename(save_path))
        if ok:
            self._ui_info("Downloaded", f"Saved to {save_path}")
        self.root.after(0, self.status_bar.reset)

    def _download_folder(self, folder_rel: str):
        target = filedialog.askdirectory(title=f"Save '{folder_rel}' to…")
        if not target:
            return
        snap  = self.index.snapshot()
        items = [(rel, snap[rel]) for rel in snap if rel.startswith(folder_rel + os.sep)]
        if not items:
            self._ui_info("Info", "No files found in that folder.")
            return
        for i, (rel, entry) in enumerate(items, 1):
            self._download_single(rel, entry, target)
            self.root.after_idle(lambda v=(i/len(items)*100): self.status_bar.folder_progress.set(v))
        self._ui_info("Success", f"Folder '{folder_rel}' downloaded!")
        self.root.after(0, self.status_bar.reset)

    def _download_single(
        self,
        rel:           str,
        entry:         dict,
        target_dir:    str,
        override_name: str | None = None,
    ) -> bool:
        parts = self.index.entry_parts(entry)
        if not parts:
            self._ui_error(f"No parts for {rel}.")
            return False

        meta      = entry if isinstance(entry, dict) else {}
        checksums = meta.get("checksums", {}) or {}
        is_comp   = meta.get("compressed", False)
        orig_name = meta.get("orig_name", os.path.basename(rel))
        out_name  = override_name or (orig_name if not is_comp else os.path.basename(rel))
        out_path  = os.path.join(target_dir, out_name)

        for p in parts:
            base     = os.path.basename(p)
            expected = checksums.get(base)
            if not expected:
                continue
            if not os.path.exists(p):
                if not self._ask_retry_until(
                    "Missing part", f"Part missing:\n{p}\nReconnect drive then click Retry."
                ):
                    return False
            verified = False
            while not verified:
                try:
                    got = sha256(p)
                    if got == expected:
                        verified = True
                    else:
                        self._log(f"Checksum mismatch: {p}", level="warning")
                        if not self._ask_retry_until(
                            "Checksum mismatch",
                            f"Expected: {expected[:12]}…\nGot:      {got[:12]}…\n"
                            f"Replace the part file then click Retry, or Cancel."
                        ):
                            return False
                        time.sleep(0.3)
                except Exception as e:
                    if not self._ask_retry_until("Verify error", str(e)):
                        return False

        try:
            self.sync_enabled = False

            def on_prog(done, tot, *_):
                pct = (done / tot * 100) if tot else 100
                self.root.after_idle(lambda: self.status_bar.file_progress.set(pct))

            ok = self.splitter.merge(parts, out_path, on_prog)
            if not ok:
                self._ui_error(f"Merge failed for {rel}")
                return False

            is_enc   = meta.get("encrypted", False)
            salt_hex = meta.get("salt_hex")
            if is_enc:
                if not self._session_password:
                    self._ui_error(
                        f"'{rel}' is encrypted but no session key is loaded.\n"
                        "Re-confirm drives and enter your password."
                    )
                    return False
                from core.crypto import derive_key
                per_file_key = derive_key(self._session_password,
                                          bytes.fromhex(salt_hex))
                dec_path = out_path + ".dec"
                try:
                    Splitter.decrypt_file(out_path, dec_path, per_file_key)
                    os.replace(dec_path, out_path)
                    self._log(f"Decrypted: {rel}")
                except Exception as e:
                    self._ui_error(f"Decryption failed for {rel}: {e}")
                    return False

            if is_comp:
                fmt      = meta.get("format", "store")
                ext      = ".lzma" if fmt == "lzma" else ".zip"
                tmp_arch = out_path + ext
                try:
                    os.replace(out_path, tmp_arch)
                    if not self.splitter.decompress(tmp_arch, target_dir, orig_name):
                        self._ui_error(f"Decompression failed for {rel}")
                        return False
                finally:
                    if os.path.exists(tmp_arch):
                        try: os.remove(tmp_arch)
                        except Exception: pass

            return True
        except Exception as e:
            self._ui_error(f"Download failed: {e}")
            self._log(f"Download error ({rel}): {e}", level="error")
            return False
        finally:
            self.sync_enabled = True

    def _ask_retry_until(self, title: str, msg: str) -> bool:
        from tkinter import messagebox as mb
        evt    = threading.Event()
        result = {"v": False}
        def ask():
            result["v"] = bool(mb.askretrycancel(title, msg))
            evt.set()
        self.root.after(0, ask)
        evt.wait()
        return result["v"]

    def _delete_item(self):
        rel = self.file_tree.selected_rel_path()
        if rel:
            self._submit(self._delete_worker, rel)

    def _delete_worker(self, rel: str):
        snap = self.index.snapshot()
        keys = [k for k in snap if k == rel or k.startswith(rel + os.sep)]
        total_parts = sum(len(self.index.entry_parts(snap[k])) for k in keys)
        done = 0
        for k in keys:
            entry = self.index.get(k)
            if not entry:
                continue
            for p in self.index.entry_parts(entry):
                if os.path.exists(p):
                    try: os.remove(p)
                    except Exception: pass
                done += 1
                pct = (done / total_parts * 100) if total_parts else 100
                self.root.after_idle(lambda v=pct: self.status_bar.file_progress.set(v))
            self.index.delete(k)
        self.index.save()
        self.root.after(0, self._do_refresh_tree)
        self._ui_info("Deleted", f"Removed: {rel}")
        self.root.after(0, self.status_bar.reset)

    def _postprocess_item(self):
        rel = self.file_tree.selected_rel_path()
        if not rel:
            messagebox.showinfo("Select a file", "Select a file in the tree first.")
            return
        if not self.selected_drives:
            messagebox.showerror("Error", "Confirm drives first.")
            return

        entry = self.index.get(rel)
        if not isinstance(entry, dict) or entry.get("status") != Status.COMPLETE.value:
            messagebox.showinfo("Not ready", f"'{rel}' is not complete — cannot post-process.")
            return

        from ui.dialogs import PostProcessDialog
        dlg = PostProcessDialog(
            self.root, rel,
            already_compressed = entry.get("compressed", False),
            already_encrypted  = entry.get("encrypted",  False),
        )
        if not dlg.result:
            return

        do_compress, compress_fmt, do_encrypt = dlg.result

        if do_encrypt and not self._session_key:
            messagebox.showerror(
                "No session key",
                "Encryption requires a session key.\n"
                "Re-confirm drives and enter your password first."
            )
            return

        self._submit(self._postprocess_worker, rel, do_compress, compress_fmt, do_encrypt)

    def _postprocess_worker(self, rel, do_compress, compress_fmt, do_encrypt):
        entry = self.index.get(rel)
        if not isinstance(entry, dict):
            self._ui_error(f"Index entry missing for {rel}")
            return

        self._reset_flags()   # clear any stale cancel/pause before merge

        old_parts  = self.index.entry_parts(entry)
        meta       = entry
        is_enc     = meta.get("encrypted", False)
        is_comp    = meta.get("compressed", False)
        salt_hex   = meta.get("salt_hex")
        orig_name  = meta.get("orig_name", os.path.basename(rel))
        orig_size  = meta.get("size", 0)
        fmt        = meta.get("format", "store")

        tmp_dir          = tempfile.mkdtemp(prefix="vdrive_pp_")
        tmp_merged       = os.path.join(tmp_dir, "merged.bin")
        tmp_decrypted    = os.path.join(tmp_dir, "decrypted.bin")
        tmp_decompressed = os.path.join(tmp_dir, orig_name)

        try:
            self.root.after_idle(lambda: self.status_bar.current_file.set(
                f"Post-processing: {rel}"))

            def on_prog(done, tot, *_):
                pct = (done / tot * 100) if tot else 50
                self.root.after_idle(lambda: self.status_bar.file_progress.set(pct * 0.3))

            ok = self.splitter.merge(old_parts, tmp_merged, on_prog)
            if not ok:
                self._ui_error(f"Post-process: merge failed for {rel}")
                return

            current = tmp_merged

            if is_enc:
                if not self._session_password or not salt_hex:
                    self._ui_error(
                        f"'{rel}' is encrypted but session key is missing.\n"
                        "Re-confirm drives with the correct password."
                    )
                    return
                from core.crypto import derive_key
                per_file_key = derive_key(self._session_password, bytes.fromhex(salt_hex))
                self._log(f"Post-process: decrypting {rel}…")
                Splitter.decrypt_file(current, tmp_decrypted, per_file_key)
                current = tmp_decrypted

            if is_comp:
                self._log(f"Post-process: decompressing {rel} ({fmt})…")
                ext      = ".lzma" if fmt == "lzma" else ".zip"
                tmp_arch = current + ext
                import shutil as _sh2
                _sh2.copy2(current, tmp_arch)
                try:
                    if not self.splitter.decompress(tmp_arch, tmp_dir, orig_name):
                        self._ui_error(f"Post-process: decompression failed for {rel}")
                        return
                finally:
                    if os.path.exists(tmp_arch):
                        try: os.remove(tmp_arch)
                        except Exception: pass
                current = tmp_decompressed

            tmp_processed  = current
            new_compressed = False
            new_fmt        = "store"
            new_encrypted  = False
            new_salt_hex   = None

            if do_compress:
                self._log(f"Post-process: compressing {rel} ({compress_fmt})…")
                tmp_processed  = self.splitter.compress(tmp_processed, compress_fmt)
                new_compressed = True
                new_fmt        = compress_fmt

            if do_encrypt:
                from core.crypto import derive_key, new_salt, new_iv
                enc_salt     = new_salt()
                enc_iv       = new_iv()
                new_salt_hex = enc_salt.hex()
                per_file_key = derive_key(self._session_password, enc_salt)
                self._log(f"Post-process: encrypting {rel}…")
                tmp_enc       = tmp_processed + ".enc"
                Splitter.encrypt_file(tmp_processed, tmp_enc, per_file_key, enc_iv)
                tmp_processed = tmp_enc
                new_encrypted = True

            self._log(f"Post-process: removing old parts for {rel}…")
            for p in old_parts:
                if os.path.exists(p):
                    try: os.remove(p)
                    except Exception: pass

            self._reset_flags()

            def on_split_prog(done, tot, eta_s, speed):
                pct = 30 + (done / tot * 70) if tot else 100
                self.root.after_idle(lambda: (
                    self.status_bar.file_progress.set(pct),
                    self.speed_graph.push(speed),
                ))

            new_parts, new_checksums, new_part_sizes = self.splitter.split(
                tmp_processed, on_split_prog
            )

            new_entry = build_entry(
                new_parts, new_compressed, new_fmt, orig_name, orig_size,
                status=Status.COMPLETE, checksums=new_checksums, verified=True,
                part_sizes=new_part_sizes,
                encrypted=new_encrypted, salt_hex=new_salt_hex,
            )
            self.index.set(rel, new_entry)
            self.index.save()
            self.root.after(0, self._do_refresh_tree)

            ops = []
            if do_compress: ops.append(f"compressed ({compress_fmt})")
            if do_encrypt:  ops.append("encrypted")
            if not ops:     ops.append("re-processed (no change)")
            self._ui_info("Post-process complete",
                          f"'{rel}' successfully {' + '.join(ops)}.")
            self._log(f"Post-process done: {rel} → {', '.join(ops)}")

        except KeyboardInterrupt:
            self.index.update_status(rel, Status.CANCELLED)
            self.index.save()
            self.root.after(0, self._do_refresh_tree)
            self._log(f"Post-process cancelled: {rel}", level="warning")
        except Exception as e:
            self._ui_error(f"Post-process failed: {e}")
            self._log(f"Post-process error ({rel}): {e}", level="error")
        finally:
            import shutil as _shutil
            try: _shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception: pass
            self.root.after(0, self.status_bar.reset)
            self.root.after(0, self.speed_graph.reset)

    def _purge_all(self):
        if not self.selected_drives:
            messagebox.showerror("Error", "Confirm drives first.")
            return
        if not messagebox.askyesno("Confirm", "Delete ALL parts and reset index?"):
            return
        self._submit(self._purge_worker)

    def _purge_worker(self):
        from config import INDEX_DIR_NAME
        for d in self.selected_drives:
            for root_dir, _, files in os.walk(d):
                for f in files:
                    if any(f.endswith(e) for e in (".part1", ".part2", ".part3", ".part4", ".temp")):
                        try: os.remove(os.path.join(root_dir, f))
                        except Exception: pass
            meta = os.path.join(d, INDEX_DIR_NAME)
            try:
                if os.path.exists(meta):
                    shutil.rmtree(meta)
            except Exception:
                pass
        self.index.clear()
        self.index.save()
        self.root.after(0, self._do_refresh_tree)
        self._ui_info("Purge Complete", "All data removed.")
        self._log("Purge All completed")

    def _auto_repair_on_startup(self):
        snap = self.index.snapshot()
        bad  = [k for k, v in snap.items()
                if isinstance(v, dict) and v.get("status") not in (Status.COMPLETE.value,)]
        if bad:
            self._log(f"Found {len(bad)} incomplete file(s) — opening repair dialog.")
            self.root.after(500, self._open_repair)

    def _open_repair(self):
        snap   = self.index.snapshot()
        broken = {k: v for k, v in snap.items()
                  if not isinstance(v, dict)
                  or v.get("status") != Status.COMPLETE.value}
        RepairDialog(
            self.root,
            broken,
            on_reupload=self._repair_files,
            on_remove=self._remove_index_entries,
        )

    def _open_history(self):
        if not self.selected_drives:
            messagebox.showinfo("No drives", "Confirm drives first.")
            return
        from ui.dialogs import HistoryDialog
        snaps = self.index.list_history()
        HistoryDialog(
            self.root,
            snaps,
            on_restore=self._restore_history_snapshot,
        )

    def _restore_history_snapshot(self, snapshot_path: str):
        ok = self.index.restore_snapshot(snapshot_path)
        if ok:
            self.root.after(0, self._do_refresh_tree)
            self._ui_info("Restored", "Index restored from snapshot.")
            self._log(f"Index restored from {snapshot_path}")
        else:
            self._ui_error(f"Failed to restore snapshot: {snapshot_path}")

    def _repair_files(self, keys: List[str]):
        for k in keys:
            entry = self.index.get(k)
            if not entry:
                continue
            orig = entry.get("orig_name", k) if isinstance(entry, dict) else k
            if os.path.exists(orig):
                self._submit(self._upload_single_file, orig, k)
            else:
                self._log(f"Repair: source file not found for {k}", level="warning")

    def _remove_index_entries(self, keys: List[str]):
        for k in keys:
            self.index.delete(k)
        self.index.save()
        self.root.after(0, self._do_refresh_tree)

    def _toggle_backup(self):
        self.cfg.backup_enabled = self._backup_var.get()
        if self.cfg.backup_enabled:
            self._backup_status.set("Backup: scanning…")
            self._submit(self._discover_backup)
        else:
            self.backup_client.nodes = []
            self._backup_status.set("Backup: OFF")
            self._log("Backup disabled")

    def _discover_backup(self):
        found = self.backup_client.discover()
        def upd():
            if found:
                self._backup_status.set(f"Backup: {len(found)} node(s)")
                self._log(f"Backup nodes: {found}")
            else:
                self._backup_status.set("Backup: OFFLINE")
        self.root.after(0, upd)

    def _refresh_server_worker(self):
        if not self.backup_client.nodes:
            return
        files = self.backup_client.list_files(self.backup_client.nodes[0])
        def upd():
            self.server_list.delete(0, tk.END)
            for f in files:
                self.server_list.insert(tk.END, f)
        self.root.after(0, upd)

    def _restore_from_server(self):
        self._submit(self._restore_worker)

    def _restore_worker(self):
        sel = self.server_list.curselection()
        if not sel or not self.backup_client.nodes:
            return
        filename = self.server_list.get(sel[0])
        node     = self.backup_client.nodes[0]
        try:
            all_parts  = self.backup_client.list_parts(node)
            part_names = sorted(p for p in all_parts if p.startswith(filename + ".part"))
            if not part_names:
                self._ui_error("No parts found on server.")
                return

            server_index = self.backup_client.get_index(node)
            server_entry = server_index.get(filename, {})
            checksums    = server_entry.get("checksums", {}) if isinstance(server_entry, dict) else {}

            tmp_dir      = tempfile.mkdtemp()
            local_parts: List[str] = []
            for name in part_names:
                p = self.backup_client.download_part(node, name, tmp_dir)
                if p:
                    local_parts.append(p)

            fake_entry = {
                "parts":      local_parts,
                "checksums":  checksums,
                "compressed": False,
                "format":     "store",
                "orig_name":  filename,
                "size":       sum(os.path.getsize(p) for p in local_parts),
                "status":     Status.COMPLETE.value,
            }
            ok = self._download_single(filename, fake_entry, tmp_dir, override_name=filename)
            merged = os.path.join(tmp_dir, filename)
            if not ok or not os.path.exists(merged):
                self._ui_error("Restore failed during merge.")
                return
            self._upload_single_file(merged)
            self._ui_info("Restore", "Restored successfully.")
        except Exception as e:
            self._ui_error(str(e))

    def _delete_from_server_worker(self):
        sel = self.server_list.curselection()
        if not sel or not self.backup_client.nodes:
            return
        filename = self.server_list.get(sel[0])
        node     = self.backup_client.nodes[0]
        self.backup_client.delete_file(node, filename)
        self._refresh_server_worker()
        self._ui_info("Done", "Deleted from server.")

    def _pause(self):
        self.user_pause.set()
        self.status_bar.current_file.set("Paused…")
        self._log("User paused")

    def _resume(self):
        self.user_pause.clear()
        self.status_bar.current_file.set("Resuming…")
        self._log("User resumed")

    def _cancel(self):
        self.cancel_flag.set()
        self.status_bar.current_file.set("Cancelling…")
        self._log("Cancel requested", level="warning")

    def _reset_flags(self):
        self.cancel_flag.clear()
        self.user_pause.clear()
        self.drive_pause.clear()

    class _SyncHandler(FileSystemEventHandler):
        _IGNORE_EXTS = {".json", ".temp", ".part1", ".part2", ".part3", ".part4"}
        _IGNORE_DIRS = {".vdrive_meta"}

        def __init__(self, app: "App"):
            self._app = app

        def on_modified(self, event):
            if event.is_directory or not self._app.sync_enabled:
                return
            p = event.src_path
            if any(seg in self._IGNORE_DIRS
                   for seg in p.replace("\\", "/").split("/")):
                return
            import os as _os
            ext = _os.path.splitext(p)[1].lower()
            if ext in self._IGNORE_EXTS:
                return
            base = _os.path.basename(p)
            if any(f".part{i}" in base for i in range(1, 9)):
                return
            self._app._submit(self._app._upload_single_file, p)

    def _start_sync_observer(self, watch_dir: str):
        if self.sync_observer:
            try:
                self.sync_observer.stop()
                self.sync_observer.join()
            except Exception:
                pass
        handler  = self._SyncHandler(self)
        observer = Observer()
        observer.schedule(handler, watch_dir, recursive=True)
        observer.start()
        self.sync_observer = observer
        self._log(f"File watcher active on {watch_dir}")

    def _on_drop(self, event):
        if not self.selected_drives:
            messagebox.showerror("Error", "Confirm drives first.")
            return
        try:
            paths = self.root.tk.splitlist(event.data)
        except Exception:
            paths = [event.data]
        for p in paths:
            if os.path.isdir(p):
                self._submit(self._upload_folder, p)
            else:
                self._submit(self._upload_single_file, p)

    def _open_settings(self):
        SettingsDialog(self.root, self.cfg, on_save=self._apply_settings)

    def _apply_settings(self, cfg: AppConfig):
        self.cfg = cfg
        self._chunk_var.set(cfg.chunk_mb)
        try:
            self.root.style.theme_use(cfg.theme)
        except Exception:
            pass
        if self.splitter:
            self._build_splitter()
        self._log("Settings saved and applied")

    def _toggle_theme(self):
        new = "flatly" if self.cfg.theme == "darkly" else "darkly"
        try:
            self.root.style.theme_use(new)
            self.cfg.theme = new
            self.cfg.save()
        except Exception as e:
            self._log(f"Theme toggle failed: {e}", level="error")

    def _log(self, msg: str, level: str = "info"):
        import logging
        getattr(logging, level, logging.info)(msg)
        def insert():
            t    = time.strftime("%H:%M:%S")
            line = f"[{t}] {msg}\n"
            try:
                self.log_box.insert("end", line)
                self.log_box.see("end")
            except Exception:
                pass
        try:
            self.root.after_idle(insert)
        except Exception:
            insert()

    def _ui_error(self, msg: str):
        self.root.after(0, lambda: messagebox.showerror("Error", msg))

    def _ui_info(self, title: str, msg: str):
        self.root.after(0, lambda: messagebox.showinfo(title, msg))

    def _submit(self, fn: Callable, *args, **kwargs) -> Optional[Future]:
        try:
            fut = self.executor.submit(fn, *args, **kwargs)
            with self._fut_lock:
                self._futures.append(fut)
            def _cb(f: Future):
                try:
                    f.result()
                except Exception as e:
                    self._log(f"Task error: {e}", level="error")
                with self._fut_lock:
                    self._futures[:] = [x for x in self._futures if not x.done()]
            fut.add_done_callback(_cb)
            return fut
        except Exception as e:
            self._log(f"Submit failed: {e}", level="error")
            return None

    def close(self):
        self.cancel_flag.set()
        self.drive_monitor.stop()
        if self.sync_observer:
            try:
                self.sync_observer.stop()
                self.sync_observer.join()
            except Exception:
                pass
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass
        self.cfg.save()
        self._log("Application closed")