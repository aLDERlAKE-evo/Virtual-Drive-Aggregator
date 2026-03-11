"""
ui/widgets.py — FileTree, SpeedGraph, StatusBar widgets.

Method signatures match exactly what ui/app.py calls:
  FileTree.refresh(index_dict)
  FileTree.selected_rel_path() -> str | None
  FileTree.expand_all() / collapse_all()
  SpeedGraph.push(speed_mb_s) / reset()
  StatusBar.update_transfer(label, done, total, eta_s, speed, file_num=1, file_total=1)
  StatusBar.reset()
  StatusBar.{current_file, file_progress, folder_progress, file_counter, eta, storage_info, speed_graph}
"""

from __future__ import annotations

import os
import tkinter as tk
from collections import deque
from tkinter import ttk
from typing import Callable, Dict, Optional

import ttkbootstrap as tb

from config import SPEED_HISTORY_LEN, Status

STATUS_ICONS: Dict[str, str] = {
    Status.COMPLETE.value:   "✅",
    Status.UPLOADING.value:  "⏳",
    Status.INCOMPLETE.value: "⚠️",
    Status.CANCELLED.value:  "❌",
    Status.MISSING.value:    "🔴",
}


# ── FileTree ──────────────────────────────────────────────────────────────────

class FileTree(tb.Frame):
    """
    Treeview with search bar, status icons, expand/collapse.
    """

    def __init__(self, parent: tk.Misc, on_select: Optional[Callable[[str], None]] = None):
        super().__init__(parent)
        self._on_select = on_select
        self._items: Dict[str, str] = {}  # rel_path -> status

        # Search bar
        bar = tb.Frame(self)
        bar.pack(fill="x", pady=(0, 4))
        tb.Label(bar, text="🔍").pack(side="left")
        self._search = tk.StringVar()
        self._search.trace_add("write", lambda *_: self._apply_filter())
        tb.Entry(bar, textvariable=self._search).pack(side="left", fill="x", expand=True, padx=4)
        tb.Button(bar, text="✕", bootstyle="secondary", width=3,
                  command=lambda: self._search.set("")).pack(side="left")

        # Tree
        tf = tb.Frame(self)
        tf.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(tf, columns=("status",), show="tree headings")
        self.tree.heading("status", text="")
        self.tree.column("status", width=36, anchor="center", stretch=False)
        self.tree.pack(side="left", fill="both", expand=True)
        ys = ttk.Scrollbar(tf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=ys.set)
        ys.pack(side="right", fill="y")
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

    # ── Public API ────────────────────────────────────────────────────────────

    def refresh(self, index: dict) -> None:
        """Rebuild the tree from a virtual index snapshot."""
        self._items = {
            rel: (entry.get("status", Status.COMPLETE.value) if isinstance(entry, dict)
                  else Status.COMPLETE.value)
            for rel, entry in index.items()
        }
        self._apply_filter()

    def selected_rel_path(self) -> Optional[str]:
        sel = self.tree.focus()
        if not sel:
            return None
        parts = []
        node  = sel
        while node:
            parts.insert(0, self.tree.item(node, "text"))
            node = self.tree.parent(node)
        return os.sep.join(parts)

    def expand_all(self) -> None:
        for iid in self.tree.get_children():
            self.tree.item(iid, open=True)

    def collapse_all(self) -> None:
        for iid in self.tree.get_children():
            self.tree.item(iid, open=False)

    # ── Private ───────────────────────────────────────────────────────────────

    def _apply_filter(self) -> None:
        query = self._search.get().strip().lower()
        self.tree.delete(*self.tree.get_children())
        filtered = {
            rel: st for rel, st in self._items.items()
            if not query or query in rel.lower()
        }
        for rel in sorted(filtered):
            self._insert(rel.split(os.sep), filtered[rel])

    def _insert(self, parts: list, status: str, parent: str = "") -> None:
        if not parts:
            return
        first = parts[0]
        child = None
        for cid in self.tree.get_children(parent):
            if self.tree.item(cid, "text") == first:
                child = cid
                break
        if not child:
            icon  = STATUS_ICONS.get(status, "") if len(parts) == 1 else ""
            child = self.tree.insert(parent, "end", text=first, values=(icon,), open=True)
        self._insert(parts[1:], status, child)

    def _on_tree_select(self, _=None) -> None:
        if self._on_select:
            p = self.selected_rel_path()
            if p:
                self._on_select(p)


# ── SpeedGraph ────────────────────────────────────────────────────────────────

class SpeedGraph(tk.Frame):
    """
    Sparkline showing the last SPEED_HISTORY_LEN MB/s samples.
    Subclasses tk.Frame (not Canvas) so .pack()/.grid()/.place() work
    normally even when the parent is a ttkbootstrap Labelframe with padding.
    The actual drawing is done on a plain tk.Canvas held inside.
    """

    def __init__(self, parent: tk.Misc, width: int = 200, height: int = 40, **kwargs):
        super().__init__(parent, width=width, height=height)
        self._history: deque = deque(maxlen=SPEED_HISTORY_LEN)
        self._canvas = tk.Canvas(self, width=width, height=height,
                                 bg="#1a1a2e", highlightthickness=0)
        self._canvas.pack(fill="both", expand=True)
        self._canvas.bind("<Configure>", self._on_resize)
        self._w = width
        self._h = height

    def push(self, speed: float) -> None:
        self._history.append(max(0.0, speed))
        try:
            self.after(0, self._redraw)
        except Exception:
            pass

    def reset(self) -> None:
        self._history.clear()
        try:
            self.after(0, lambda: self._canvas.delete("all"))
        except Exception:
            pass

    def _on_resize(self, event) -> None:
        self._w = event.width
        self._h = event.height
        self._redraw()

    def _redraw(self) -> None:
        self._canvas.delete("all")
        data = list(self._history)
        if len(data) < 2:
            return
        peak = max(data) or 1.0
        w, h = self._w, self._h
        pad  = 4
        xs   = [pad + i * (w - 2 * pad) / (len(data) - 1) for i in range(len(data))]
        ys   = [h - pad - (v / peak) * (h - 2 * pad) for v in data]
        pts  = [c for pair in zip(xs, ys) for c in pair]
        self._canvas.create_line(*pts, fill="#00d2ff", width=2, smooth=True)
        self._canvas.create_text(w - 4, 4, anchor="ne",
                                 text=f"{data[-1]:.1f} MB/s", fill="white",
                                 font=("Segoe UI", 8))


# ── StatusBar ─────────────────────────────────────────────────────────────────

class StatusBar(tb.Labelframe):
    """
    Status area: label + two progress bars + counters + ETA + storage info.
    The SpeedGraph is kept as a separate widget in app.py for layout flexibility,
    but StatusBar also exposes a speed_graph attribute for convenience.
    """

    def __init__(self, parent: tk.Misc):
        super().__init__(parent, text="Status", padding=8)
        # Make the labelframe itself expand horizontally
        self.columnconfigure(0, weight=1)

        # Public tk variables (app.py sets these directly)
        self.current_file    = tk.StringVar(value="Idle")
        self.file_progress   = tk.DoubleVar(value=0.0)
        self.folder_progress = tk.DoubleVar(value=0.0)
        self.file_counter    = tk.StringVar(value="0/0")
        self.eta             = tk.StringVar(value="--")
        self.storage_info    = tk.StringVar(value="Total: -- | Free: --")

        tb.Label(self, textvariable=self.current_file, anchor="w").pack(
            fill="x", padx=10, pady=(4, 0))
        tb.Progressbar(self, variable=self.file_progress).pack(
            fill="x", padx=10, pady=(4, 2))
        tb.Progressbar(self, variable=self.folder_progress).pack(
            fill="x", padx=10, pady=(0, 6))

        bottom = tb.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 4))
        tb.Label(bottom, textvariable=self.file_counter).pack(side="left")
        tb.Label(bottom, textvariable=self.eta).pack(side="left", padx=20)
        tb.Label(bottom, textvariable=self.storage_info,
                 font=("Segoe UI", 10, "bold")).pack(side="right")

    def reset(self) -> None:
        self.current_file.set("Idle")
        self.file_progress.set(0.0)
        self.folder_progress.set(0.0)
        self.file_counter.set("0/0")
        self.eta.set("--")

    def update_transfer(
        self,
        label:      str,
        done:       int,
        total:      int,
        eta_s:      float,
        speed:      float,
        file_num:   int = 1,
        file_total: int = 1,
    ) -> None:
        pct = (done / total * 100) if total else 100
        self.file_progress.set(pct)
        self.current_file.set(
            f"{label}  {done // 1_000_000}/{total // 1_000_000} MB"
            f"  |  ETA: {int(eta_s)}s  |  {speed:.1f} MB/s"
        )
        self.file_counter.set(f"{file_num}/{file_total}")
        self.eta.set(f"ETA: {int(eta_s)}s  |  {speed:.1f} MB/s")