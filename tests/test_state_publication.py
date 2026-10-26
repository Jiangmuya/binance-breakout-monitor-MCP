"""Regression coverage for atomic state publication under Windows file sharing."""
from concurrent.futures import ThreadPoolExecutor
import ctypes
import errno
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from monitor import maintenance


class StatePublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.path = self.root / 'health.json'
        self.old = {'running': True, 'generation': 'previous', 'text': 'old' * 1024}
        self.path.write_text(json.dumps(self.old), encoding='utf-8')

    def tearDown(self):
        self.temporary.cleanup()

    def assert_no_temporary_files(self):
        self.assertEqual([self.path], list(self.root.iterdir()))

    def deny_rename(self):
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                      ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                                      wintypes.HANDLE]
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        # A real reader permits readers/writers, but deliberately omits
        # FILE_SHARE_DELETE so replacing the destination must fail while held.
        handle = kernel.CreateFileW(str(self.path), 0x80000000, 1 | 2, None, 3, 0, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        return kernel, handle

    @unittest.skipUnless(os.name == 'nt', 'requires Windows file-sharing semantics')
    def test_temporary_real_reader_lock_preserves_old_file_then_recovers(self):
        kernel, handle = self.deny_rename()
        replacement = {'running': True, 'generation': 'next', 'text': '\u6d4b\u8bd5' * 2048}
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(maintenance._write_json, self.path, replacement)
            try:
                deadline = time.monotonic() + 2
                while not list(self.root.glob('health.json.pending*')) and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(list(self.root.glob('health.json.pending*')))
                time.sleep(.12)
                self.assertFalse(future.done(), 'the real reader must still prevent replacement')
                self.assertEqual(self.old, json.loads(self.path.read_text(encoding='utf-8')))
            finally:
                kernel.CloseHandle(handle)
            future.result(timeout=5)
        self.assertEqual(replacement, json.loads(self.path.read_text(encoding='utf-8')))
        self.assert_no_temporary_files()

    @unittest.skipUnless(os.name == 'nt', 'requires Windows file-sharing semantics')
    def test_persistent_real_reader_lock_fails_bounded_and_keeps_previous_json(self):
        kernel, handle = self.deny_rename()
        started = time.monotonic()
        try:
            with self.assertRaises(OSError) as raised:
                maintenance._write_json(self.path, {'generation': 'must-not-publish'})
            self.assertIn(raised.exception.winerror, (5, 32, 33))
            self.assertLess(time.monotonic() - started, 8, 'lock retries must be bounded')
            self.assertEqual(self.old, json.loads(self.path.read_text(encoding='utf-8')))
            self.assert_no_temporary_files()
        finally:
            kernel.CloseHandle(handle)

    def test_nontransient_disk_failure_is_not_retried_and_keeps_previous_json(self):
        failure = OSError(errno.ENOSPC, 'disk full')
        failure.winerror = 112
        with patch.object(maintenance.os, 'replace', side_effect=failure) as replace, \
                patch.object(maintenance.time, 'sleep') as sleep:
            with self.assertRaises(OSError) as raised:
                maintenance._write_json(self.path, {'generation': 'must-not-publish'})
        self.assertIs(failure, raised.exception)
        replace.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(self.old, json.loads(self.path.read_text(encoding='utf-8')))
        self.assert_no_temporary_files()

    def test_concurrent_publishers_keep_complete_independent_documents(self):
        values = [{'writer': i, 'text': chr(65 + i) * 65536} for i in range(4)]
        original_replace = os.replace
        ready = threading.Barrier(len(values))
        # Synchronize only the first publication attempt from each writer;
        # the filesystem still performs every real atomic replacement.
        local = threading.local()
        def concurrent_replace(source, destination):
            if not getattr(local, 'ready', False):
                local.ready = True
                ready.wait(timeout=5)
            return original_replace(source, destination)
        with patch.object(maintenance.os, 'replace', side_effect=concurrent_replace):
            with ThreadPoolExecutor(max_workers=len(values)) as pool:
                futures = [pool.submit(maintenance._write_json, self.path, value) for value in values]
                for future in futures:
                    future.result(timeout=8)
        self.assertIn(json.loads(self.path.read_text(encoding='utf-8')), values)
        self.assert_no_temporary_files()


if __name__ == '__main__':
    unittest.main()
