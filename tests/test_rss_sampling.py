"""Windows sampling must not retain a new ctypes type or process handle per call."""
import ctypes
import gc
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import export_soak


@unittest.skipUnless(os.name == 'nt', 'Windows process-memory API regression')
class RssSamplingTests(unittest.TestCase):
    def test_own_process_has_valid_rss(self):
        self.assertGreater(export_soak.rss_bytes(os.getpid()), 0)

    def test_repeated_sampling_does_not_retain_types_or_handles(self):
        from ctypes import wintypes
        get_count = export_soak._rss_kernel.GetProcessHandleCount
        get_count.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        get_count.restype = wintypes.BOOL

        def handles():
            count = wintypes.DWORD()
            self.assertTrue(get_count(wintypes.HANDLE(-1), ctypes.byref(count)))
            return count.value

        export_soak.rss_bytes(os.getpid())
        gc.collect()
        before_types = len(ctypes._pointer_type_cache)
        before_handles = handles()
        for _ in range(5000):
            self.assertGreater(export_soak.rss_bytes(os.getpid()), 0)
        gc.collect()
        self.assertEqual(before_types, len(ctypes._pointer_type_cache))
        self.assertEqual(before_handles, handles())

    def test_invalid_process_returns_none(self):
        for pid in (None, 0, -1, 'invalid-pid'):
            with self.subTest(pid=pid):
                self.assertIsNone(export_soak.rss_bytes(pid))

    def test_opened_handle_is_closed_if_memory_read_fails(self):
        original_close = export_soak._rss_close_handle
        for error in (False, OSError('simulated memory read failure')):
            with self.subTest(error=type(error).__name__):
                with patch.object(export_soak, '_rss_get_memory',
                        side_effect=error if isinstance(error, OSError) else None,
                        return_value=error), patch.object(export_soak, '_rss_close_handle',
                        wraps=original_close) as close:
                    self.assertIsNone(export_soak.rss_bytes(os.getpid()))
                    close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
