"""
core/splitter.py — Splitter class, sha256 alias, compress/decompress helpers.

Pipeline per upload:
    raw file → [LZMA/zip compress] → [AES-256-CBC encrypt] → split across drives

Pipeline per download:
    merge parts → [AES-256-CBC decrypt] → [decompress] → output file

Exports:
    sha256(path)   -> str
    Splitter       -> main class
"""

from __future__ import annotations

import hashlib
import lzma
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
    """
    Mark a file as hidden on Windows using ctypes only — no subprocess,
    no shell notification. Safe no-op on non-Windows.
    FILE_ATTRIBUTE_HIDDEN = 0x2
    """
    try:
        import ctypes
        FILE_ATTRIBUTE_HIDDEN = 0x2
        current = ctypes.windll.kernel32.GetFileAttributesW(path)
        if current == -1:  # INVALID_FILE_ATTRIBUTES
            return
        ctypes.windll.kernel32.SetFileAttributesW(path, current | FILE_ATTRIBUTE_HIDDEN)
    except Exception:
        pass


# ── Checksum ──────────────────────────────────────────────────────────────────

def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1_048_576), b""):
            h.update(chunk)
    return h.hexdigest()


sha256 = compute_sha256   # alias used by ui/app.py


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


# ── Part-size distribution ─────────────────────────────────────────────────────

def distribute_sizes(
    total_size: int,
    drives: List[str],
    cached: Optional[List[int]] = None,
) -> List[int]:
    free       = [shutil.disk_usage(d).free for d in drives]
    total_free = sum(free)

    if total_free < total_size:
        raise ValueError(
            f"Not enough space: need {total_size / 1e9:.2f} GB, "
            f"have {total_free / 1e9:.2f} GB across {len(drives)} drive(s)."
        )

    if cached and len(cached) == len(drives):
        if all(cached[i] <= free[i] for i in range(len(drives))):
            return list(cached)

    sizes = [int(total_size * (f / total_free)) for f in free]
    diff  = total_size - sum(sizes)
    for i in range(len(sizes)):
        if diff == 0:
            break
        add = min(diff, max(0, free[i] - sizes[i]))
        sizes[i] += add
        diff     -= add

    if diff != 0:
        raise ValueError("Could not distribute bytes — drives too full after rebalancing.")
    return sizes


# ── Compression ────────────────────────────────────────────────────────────────

def _compress_zip(src: str, dst: str) -> None:
    """Deflate compress src → dst."""
    with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=6) as zf:
        zf.write(src, arcname=os.path.basename(src))


def _compress_lzma(src: str, dst: str) -> None:
    """
    LZMA compress src → dst using Python's native lzma module.
    Streams in 1 MB chunks so large files don't need to fit in RAM.
    """
    with open(src, "rb") as fin, lzma.open(dst, "wb", preset=6) as fout:
        for chunk in iter(lambda: fin.read(1_048_576), b""):
            fout.write(chunk)


def _decompress_zip(src: str, dest_dir: str) -> None:
    with zipfile.ZipFile(src, "r") as zf:
        zf.extractall(dest_dir)


def _decompress_lzma(src: str, dest_dir: str, orig_name: str) -> None:
    out_path = os.path.join(dest_dir, orig_name)
    with lzma.open(src, "rb") as fin, open(out_path, "wb") as fout:
        for chunk in iter(lambda: fin.read(1_048_576), b""):
            fout.write(chunk)


# ── Splitter class ─────────────────────────────────────────────────────────────

class Splitter:
    """
    Encapsulates split / merge / compress / encrypt logic for one session.
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

    # ── Public: compress ──────────────────────────────────────────────────────

    def _tmp_dir(self) -> str:
        """
        Returns a temp work directory inside the first selected drive.
        Already excluded from Explorer watchers and the sync observer.
        Created on first call if it doesn't exist.
        """
        d = os.path.join(self._drives[0], ".vdrive_meta", "tmp")
        os.makedirs(d, exist_ok=True)
        _hide_file(d)
        return d

    def compress(self, src_path: str, mode: str) -> str:
        """
        Compress src_path into a temp file inside the first drive's
        .vdrive_tmp folder (already excluded from Explorer and sync watcher).
        mode: 'zip' (deflate) | 'lzma'
        Returns the temp file path — caller is responsible for deleting it.
        """
        suffix  = ".lzma" if mode == "lzma" else ".zip"
        tmp_dir = self._tmp_dir()
        tmp     = os.path.join(tmp_dir, f"vdrive_compress_{os.getpid()}{suffix}")
        if mode == "lzma":
            _compress_lzma(src_path, tmp)
        else:
            _compress_zip(src_path, tmp)
        return tmp

    @staticmethod
    def decompress(archive_path: str, dest_dir: str, orig_name: str = "") -> bool:
        """
        Decompress archive_path into dest_dir.
        orig_name is required for .lzma archives (used as the output filename).
        Returns True on success.
        """
        try:
            if archive_path.endswith(".lzma"):
                name = orig_name or os.path.basename(archive_path).replace(".lzma", "")
                _decompress_lzma(archive_path, dest_dir, name)
            else:
                _decompress_zip(archive_path, dest_dir)
            return True
        except Exception as e:
            import logging
            logging.error(f"vdrive decompress failed: {e}")
            return False

    # ── Public: encrypt / decrypt ─────────────────────────────────────────────

    @staticmethod
    def encrypt_file(src: str, dst: str, key: bytes, iv: bytes) -> None:
        """Encrypt src → dst (AES-256-CBC). Delegates to core.crypto."""
        from core.crypto import encrypt_file as _enc
        _enc(src, dst, key, iv)

    @staticmethod
    def decrypt_file(src: str, dst: str, key: bytes) -> None:
        """Decrypt src → dst. Delegates to core.crypto."""
        from core.crypto import decrypt_file as _dec
        _dec(src, dst, key)

    # ── Public: split ─────────────────────────────────────────────────────────

    def split(
        self,
        src_path:          str,
        on_progress:       ProgressCB,
        cached_part_sizes: Optional[List[int]] = None,
    ) -> Tuple[List[str], Dict[str, str], List[int]]:
        """
        Split src_path across drives.
        Returns (final_parts, {basename: sha256}, part_sizes).
        """
        total_size = os.path.getsize(src_path)

        if total_size == 0:
            p = os.path.join(self._drives[0], f"{os.path.basename(src_path)}.part1")
            open(p, "wb").close()
            _hide_file(p)
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

                    mv, off = memoryview(buf), 0
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
                        msg = str(next(iter(errs.values())))
                        if self._on_drive_error:
                            self._on_drive_error(msg)
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

        # Rename .temp → final and hide
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

    # ── Public: merge ─────────────────────────────────────────────────────────

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