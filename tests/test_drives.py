"""
tests/test_drives.py — Unit tests for drive detection logic.
"""

import unittest
import unittest.mock as mock

from config import AppConfig


class TestDriveDetection(unittest.TestCase):

    def _make_partition(self, mountpoint: str, opts: str = "rw,removable"):
        """Helper to create a mock disk_partitions entry."""
        p = mock.MagicMock()
        p.mountpoint = mountpoint
        p.opts       = opts
        return p

    def test_removable_drive_included(self):
        from core.drives import _detect_psutil
        partitions = [self._make_partition("E:\\", "rw,removable")]
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = _detect_psutil(set())
        self.assertIn("E:/", result)

    def test_fixed_drive_excluded(self):
        from core.drives import _detect_psutil
        partitions = [self._make_partition("C:\\", "rw")]  # no 'removable'
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = _detect_psutil(set())
        self.assertNotIn("C:/", result)

    def test_blocked_letter_excluded(self):
        from core.drives import _detect_psutil
        partitions = [self._make_partition("C:\\", "rw,removable")]
        blocked = {"C"}
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = _detect_psutil(blocked)
        self.assertNotIn("C:/", result)

    def test_d_drive_blocked_by_default(self):
        from core.drives import list_removable_drives
        cfg = AppConfig()  # default blocked: C, D
        partitions = [
            self._make_partition("D:\\", "rw,removable"),
            self._make_partition("E:\\", "rw,removable"),
        ]
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = list_removable_drives(cfg)
        self.assertNotIn("D:/", result)
        self.assertIn("E:/", result)

    def test_multiple_removable_drives(self):
        from core.drives import _detect_psutil
        partitions = [
            self._make_partition("E:\\", "rw,removable"),
            self._make_partition("F:\\", "rw,removable"),
            self._make_partition("G:\\", "rw,removable"),
        ]
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = _detect_psutil(set())
        self.assertEqual(sorted(result), ["E:/", "F:/", "G:/"])

    def test_no_drives_returns_empty(self):
        from core.drives import _detect_psutil
        with mock.patch("psutil.disk_partitions", return_value=[]):
            result = _detect_psutil(set())
        self.assertEqual(result, [])

    def test_normalise(self):
        from core.drives import _normalise
        self.assertEqual(_normalise("E:\\"), "E:/")
        self.assertEqual(_normalise("/media/usb"), "/media/usb/")
        self.assertEqual(_normalise("E:/"), "E:/")

    def test_drive_letter_extraction(self):
        from core.drives import _drive_letter
        self.assertEqual(_drive_letter("C:\\Users"), "C")
        self.assertEqual(_drive_letter("E:/"), "E")
        self.assertIsNone(_drive_letter("/media/usb"))

    def test_config_blocked_letters_respected(self):
        from core.drives import list_removable_drives
        cfg = AppConfig()
        cfg.blocked_letters = ["C", "D", "E"]   # block E too
        partitions = [
            self._make_partition("E:\\", "rw,removable"),
            self._make_partition("F:\\", "rw,removable"),
        ]
        with mock.patch("psutil.disk_partitions", return_value=partitions):
            result = list_removable_drives(cfg)
        self.assertNotIn("E:/", result)
        self.assertIn("F:/", result)


if __name__ == "__main__":
    unittest.main()
