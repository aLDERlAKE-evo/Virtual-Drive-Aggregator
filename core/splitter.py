"""
core/splitter.py — Splitter class, sha256 alias, compress/decompress helpers.

Exports:
  sha256(path)          -> str          (alias for compute_sha256)
  Splitter              -> main class used by ui/app.py
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from config import AppConfig

ProgressCB = Callable[[int, int, float, float], None]  # done, total, eta_s, speed_MB/s


def _hide_file(path: str) -> None:
    """Mark a file as hidden on Windows (attrib +h). Safe no-op elsewhere."""
    try:
        import subprocess
        subprocess.call(
            ["attrib", "+h", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


# ── Checksum ──────────────────────────────────────────────────────────────────

def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1_048_576), b""):
            h.update(chunk)
    return h.hexdigest()


# Alias expected by ui/app.py
sha256 = compute_sha256


def compute_checksums_parallel(paths: List[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    valid = [p for p in paths if os.path.exists(p)]
    if not valid:
        return result
    with ThreadPoolExecutor(max_workers=min(len(valid), 4)) as pool:
        futures = {pool.submit(compute_sha256, p): p for p in valid}
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                result[os.path.basename(p)] = fut.result()
            except Exception:
                pass
    return result


# ── Part-size distribution ────────────────────────────────────────────────────

def distribute_sizes(
    total_size: int,
    drives: List[str],
    cached: Optional[List[int]] = None,
) -> List[int]:
    """
    Proportional, capacity-aware distribution.
    Raises ValueError if drives don't have enough total free space.
    """
    free = [shutil.disk_usage(d).free for d in drives]
    total_free = sum(free)

    if total_free < total_size:
        raise ValueError(
            f"Not enough space: need {total_size / 1e9:.2f} GB, "
            f"have {total_free / 1e9:.2f} GB across {len(drives)} drive(s)."
        )

    # Reuse cached if still valid
    if cached and len(cached) == len(drives):
        if all(cached[i] <= free[i] for i in range(len(drives))):
            return list(cached)

    sizes = [int(total_size * (f / total_free)) for f in free]

    # Fix rounding; distribute overflow capacity-first
    diff = total_size - sum(sizes)
    for i in range(len(sizes)):
        if diff == 0:
            break
        headroom = free[i] - sizes[i]
        add = min(diff, max(0, headroom))
        sizes[i] += add
        diff -= add

    if diff != 0:
        raise ValueError("Could not distribute bytes — drives too full after rebalancing.")

    return sizes


# ── Splitter class ────────────────────────────────────────────────────────────

class Splitter:
    """
    Encapsulates all split/merge/compress logic for one upload session.
    Constructed by App after drives are confirmed.
    """

    def __init__(
        self,
        drives:         List[str],
        cfg:            "AppConfig",
        cancel_flag:    threading.Event,
        user_pause:     threading.Event,
        drive_pause:    threading.Event,
        on_drive_error: Optional[Callable[[str], None]] = None,
    ):
        self._drives         = drives
        self._cfg            = cfg
        self._cancel         = cancel_flag
        self._user_pause     = user_pause
        self._drive_pause    = drive_pause
        self._on_drive_error = on_drive_error

    # ── split ─────────────────────────────────────────────────────────────────

    def split(
        self,
        src_path:    str,
        on_progress: ProgressCB,
        cached_part_sizes: Optional[List[int]] = None,
    ) -> Tuple[List[str], Dict[str, str], List[int]]:
        """
        Split src_path across self._drives.
        Returns (final_parts, checksums, part_sizes).
        Raises ValueError (pre-flight) or KeyboardInterrupt (cancelled).
        """
        total_size = os.path.getsize(src_path)

        if total_size == 0:
            p = os.path.join(self._drives[0], f"{os.path.basename(src_path)}.part1")
            open(p, "wb").close()
            return [p], {os.path.basename(p): compute_sha256(p)}, [0]

        part_sizes = distribute_sizes(total_size, self._drives, cached_part_sizes)
        fname      = os.path.basename(src_path)
        temp_paths = [
            os.path.join(d, f"{fname}.part{i + 1}.temp")
            for i, d in enumerate(self._drives)
        ]

        written = [
            min(os.path.getsize(p), part_sizes[i]) if os.path.exists(p) else 0
            for i, p in enumerate(temp_paths)
        ]
        done_bytes  = sum(written)
        chunk_bytes = max(1, self._cfg.chunk_mb * 1_048_576)
        start_time  = time.time()

        handles: List[Any] = []
        try:
            for p in temp_paths:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                handles.append(open(p, "ab"))
        except Exception:
            for h in handles:
                try: h.close()
                except Exception: pass
            raise

        write_lock   = threading.Lock()
        write_errors: Dict[int, Exception] = {}

        def _write(idx: int, data: memoryview) -> None:
            try:
                handles[idx].write(data)
                handles[idx].flush()
            except Exception as e:
                with write_lock:
                    write_errors[idx] = e

        try:
            with open(src_path, "rb") as sf:
                sf.seek(done_bytes)
                cur = 0
                while cur < len(written) and written[cur] >= part_sizes[cur]:
                    cur += 1

                while True:
                    if self._cancel.is_set():
                        raise KeyboardInterrupt("Cancelled")
                    while (self._user_pause.is_set() or self._drive_pause.is_set()) \
                            and not self._cancel.is_set():
                        time.sleep(0.1)
                    if self._cancel.is_set():
                        raise KeyboardInterrupt("Cancelled")

                    buf = sf.read(chunk_bytes)
                    if not buf:
                        break

                    mv  = memoryview(buf)
                    off = 0
                    pending: List[Tuple[int, memoryview]] = []

                    while off < len(mv) and cur < len(handles):
                        rem = part_sizes[cur] - written[cur]
                        if rem <= 0:
                            cur += 1
                            continue
                        n = min(rem, len(mv) - off)
                        if n <= 0:
                            break
                        pending.append((cur, mv[off: off + n]))
                        written[cur] += n
                        done_bytes   += n
                        off          += n
                        if written[cur] >= part_sizes[cur]:
                            cur += 1

                    # Parallel write if multiple drives involved
                    if len(pending) > 1:
                        threads = [
                            threading.Thread(target=_write, args=(idx, data), daemon=True)
                            for idx, data in pending
                        ]
                        for t in threads: t.start()
                        for t in threads: t.join()
                    else:
                        for idx, data in pending:
                            _write(idx, data)

                    if write_errors:
                        errs = dict(write_errors)
                        write_errors.clear()
                        err_idx  = next(iter(errs))
                        err_msg  = str(errs[err_idx])
                        if self._on_drive_error:
                            self._on_drive_error(err_msg)
                        self._drive_pause.set()
                        while self._drive_pause.is_set() and not self._cancel.is_set():
                            time.sleep(0.3)
                        if self._cancel.is_set():
                            raise KeyboardInterrupt("Cancelled")

                    elapsed = max(1e-6, time.time() - start_time)
                    speed   = (done_bytes / 1_000_000) / elapsed
                    eta     = (total_size - done_bytes) / (speed * 1_000_000) if speed > 0 else 0.0
                    try:
                        on_progress(done_bytes, total_size, eta, speed)
                    except Exception:
                        pass

        except KeyboardInterrupt:
            for h in handles:
                try: h.close()
                except Exception: pass
            for p in temp_paths:
                try: os.remove(p)
                except Exception: pass
            raise
        finally:
            for h in handles:
                try: h.close()
                except Exception: pass

        # Rename .temp → final and hide from Explorer
        final_parts: List[str] = []
        for p, w in zip(temp_paths, written):
            if w > 0:
                final = p.replace(".temp", "")
                try:
                    if os.path.exists(final):
                        os.remove(final)
                    os.replace(p, final)
                    _hide_file(final)
                    final_parts.append(final)
                except Exception:
                    final_parts.append(p)
            else:
                try: os.remove(p)
                except Exception: pass

        checksums = compute_checksums_parallel(final_parts)
        return final_parts, checksums, part_sizes

    # ── merge ─────────────────────────────────────────────────────────────────

    def merge(
        self,
        parts:       List[str],
        out_path:    str,
        on_progress: Optional[ProgressCB] = None,
    ) -> bool:
        """Concatenate parts into out_path. Returns True on success."""
        total   = sum(os.path.getsize(p) for p in parts if os.path.exists(p))
        written = 0
        try:
            with open(out_path, "wb") as out_f:
                for p in parts:
                    if not os.path.exists(p):
                        continue
                    with open(p, "rb") as pf:
                        for buf in iter(lambda: pf.read(1_048_576), b""):
                            out_f.write(buf)
                            written += len(buf)
                            if on_progress and total:
                                try:
                                    on_progress(written, total, 0.0, 0.0)
                                except Exception:
                                    pass
            return True
        except Exception:
            return False

    # ── compress / decompress ─────────────────────────────────────────────────

    @staticmethod
    def compress(src_path: str, mode: str) -> str:
        """Compress src_path to a temp zip. Returns temp path (caller deletes)."""
        comp = zipfile.ZIP_DEFLATED if mode == "zip" else getattr(zipfile, "ZIP_LZMA", zipfile.ZIP_DEFLATED)
        tmp  = tempfile.mktemp(prefix="vdrive_", suffix=".zip")
        with zipfile.ZipFile(tmp, "w", compression=comp) as zf:
            zf.write(src_path, arcname=os.path.basename(src_path))
        return tmp

    @staticmethod
    def decompress(zip_path: str, dest_dir: str) -> bool:
        """Extract zip_path into dest_dir. Returns True on success."""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(dest_dir)
            return True
        except Exception:
            return False