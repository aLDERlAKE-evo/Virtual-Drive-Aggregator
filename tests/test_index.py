"""
tests/test_index.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import Status
from core.index import IndexManager, build_entry, entry_parts


class TestBuildEntry(unittest.TestCase):
    def test_complete_entry_fields(self):
        e = build_entry(
            ["/d/f.part1"], False, "store", "f.txt", 1024,
            Status.COMPLETE, {"f.part1": "abc"}, True, [1024]
        )
        self.assertEqual(e["status"], "complete")
        self.assertEqual(e["checksums"], {"f.part1": "abc"})
        self.assertEqual(e["part_sizes"], [1024])
        self.assertTrue(e["verified"])

    def test_entry_parts_list(self):
        self.assertEqual(entry_parts(["/a", "/b"]), ["/a", "/b"])

    def test_entry_parts_dict(self):
        self.assertEqual(entry_parts({"parts": ["/x"]}), ["/x"])

    def test_entry_parts_empty(self):
        self.assertEqual(entry_parts({}), [])


class TestIndexManager(unittest.TestCase):
    def setUp(self):
        self.tmp  = tempfile.mkdtemp()
        self.im   = IndexManager([self.tmp + "/"])

    def test_save_load_roundtrip(self):
        e = build_entry([], False, "store", "f.txt", 0, Status.COMPLETE, {}, True, [])
        self.im.set("f.txt", e)
        self.im.save()
        im2 = IndexManager([self.tmp + "/"])
        im2.load()
        self.assertIn("f.txt", im2.snapshot())

    def test_get_returns_none_for_missing(self):
        self.assertIsNone(self.im.get("nonexistent"))

    def test_delete_removes_key(self):
        self.im.set("del.txt", {})
        self.im.delete("del.txt")
        self.assertIsNone(self.im.get("del.txt"))

    def test_update_status(self):
        self.im.set("up.txt", build_entry([], False, "store", "up.txt", 0, Status.UPLOADING))
        self.im.update_status("up.txt", Status.COMPLETE)
        self.assertEqual(self.im.get("up.txt")["status"], "complete")

    def test_snapshot_is_copy(self):
        self.im.set("a.txt", {})
        snap = self.im.snapshot()
        snap["b.txt"] = {}
        self.assertNotIn("b.txt", self.im.snapshot())

    def test_history_created_on_save(self):
        self.im.set("h.txt", {})
        self.im.save()
        self.im.save()  # second save creates history
        hist = self.im.list_history()
        self.assertTrue(len(hist) >= 1)

    def test_restore_snapshot(self):
        self.im.set("orig.txt", {})
        self.im.save()
        self.im.save()  # create a history entry
        hist = self.im.list_history()
        if hist:
            self.im.set("new.txt", {})
            self.im.restore_snapshot(hist[0])
            # after restore, new.txt should be gone
            self.assertNotIn("new.txt", self.im.snapshot())

    def test_replicate_to_all_drives(self):
        tmp2 = tempfile.mkdtemp()
        im   = IndexManager([self.tmp + "/", tmp2 + "/"])
        im.set("rep.txt", {})
        im.save()
        import json
        for d in [self.tmp, tmp2]:
            p = os.path.join(d, ".vdrive_meta", "index.json")
            self.assertTrue(os.path.exists(p), f"Missing on {d}")
        import shutil
        shutil.rmtree(tmp2)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
