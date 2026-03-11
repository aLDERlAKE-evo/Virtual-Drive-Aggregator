"""
tests/test_splitter.py
Run with: python -m pytest tests/ -v
"""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.splitter import compute_sha256, compute_checksums_parallel, distribute_sizes, sha256, Splitter
from config import AppConfig


class TestSha256Alias(unittest.TestCase):
    def test_alias_matches(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"vdrive test")
            name = f.name
        try:
            self.assertEqual(sha256(name), compute_sha256(name))
        finally:
            os.remove(name)


class TestComputeSha256(unittest.TestCase):
    def test_known_hash(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello vdrive")
            name = f.name
        try:
            expected = hashlib.sha256(b"hello vdrive").hexdigest()
            self.assertEqual(compute_sha256(name), expected)
        finally:
            os.remove(name)

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            name = f.name
        try:
            self.assertEqual(compute_sha256(name), hashlib.sha256(b"").hexdigest())
        finally:
            os.remove(name)


class TestDistributeSizes(unittest.TestCase):
    def _patch(self, free_list):
        import shutil
        orig = shutil.disk_usage
        drives = [f"/fake/d{i}" for i in range(len(free_list))]
        class FU:
            def __init__(self, f): self.free = f; self.total = f; self.used = 0
        shutil.disk_usage = lambda p: FU(free_list[drives.index(p)])
        return drives, orig, shutil

    def test_sum_equals_total(self):
        import shutil
        for total in [1000, 999_999, 4_000_000_000]:
            drives, orig, sh = self._patch([total, total * 2])
            try:
                sizes = distribute_sizes(total, drives)
                self.assertEqual(sum(sizes), total)
            finally:
                sh.disk_usage = orig

    def test_insufficient_raises(self):
        import shutil
        drives, orig, sh = self._patch([100, 100])
        try:
            with self.assertRaises(ValueError):
                distribute_sizes(1000, drives)
        finally:
            sh.disk_usage = orig

    def test_cached_reused_when_valid(self):
        import shutil
        drives, orig, sh = self._patch([500, 500])
        try:
            cached = [300, 400]
            result = distribute_sizes(700, drives, cached)
            self.assertEqual(result, cached)
        finally:
            sh.disk_usage = orig


class TestSplitterRoundtrip(unittest.TestCase):
    def _noop(self, *_): pass

    def test_split_and_merge(self):
        with tempfile.TemporaryDirectory() as d0, \
             tempfile.TemporaryDirectory() as d1:
            content = os.urandom(128 * 1024)
            with tempfile.NamedTemporaryFile(delete=False) as sf:
                sf.write(content)
                src = sf.name
            try:
                cfg     = AppConfig(chunk_mb=1)
                cancel  = threading.Event()
                pause   = threading.Event()
                sp      = Splitter(
                    drives=[d0 + "/", d1 + "/"],
                    cfg=cfg,
                    cancel_flag=cancel,
                    user_pause=pause,
                    drive_pause=pause,
                )
                parts, checksums, sizes = sp.split(src, self._noop)
                self.assertEqual(sum(sizes), len(content))
                self.assertTrue(len(parts) > 0)

                with tempfile.NamedTemporaryFile(delete=False) as of:
                    out = of.name
                try:
                    ok = sp.merge(parts, out)
                    self.assertTrue(ok)
                    with open(out, "rb") as f:
                        self.assertEqual(f.read(), content)
                finally:
                    os.remove(out)
            finally:
                os.remove(src)

    def test_cancel_cleans_temp_files(self):
        with tempfile.TemporaryDirectory() as d0, \
             tempfile.TemporaryDirectory() as d1:
            with tempfile.NamedTemporaryFile(delete=False) as sf:
                sf.write(os.urandom(1024))
                src = sf.name
            try:
                cfg    = AppConfig()
                cancel = threading.Event()
                cancel.set()
                pause  = threading.Event()
                sp     = Splitter([d0+"/", d1+"/"], cfg, cancel, pause, pause)
                with self.assertRaises(KeyboardInterrupt):
                    sp.split(src, self._noop)
                for d in [d0, d1]:
                    self.assertEqual([f for f in os.listdir(d) if f.endswith(".temp")], [])
            finally:
                os.remove(src)


if __name__ == "__main__":
    unittest.main()
