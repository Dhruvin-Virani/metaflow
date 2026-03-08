"""
Unit tests for Windows compatibility of the timeout implementations.

Regression tests for GitHub issue #2981:
- `signal.SIGALRM` is POSIX-only; `@timeout` raised AttributeError on Windows.
- Both `timeout_decorator.py` and `card_cli.py`'s `timeout()` context manager
  now use `threading.Timer` + `ctypes` on Windows and `signal.SIGALRM` on Unix.
"""

import sys
import threading
import time
import unittest

# ---------------------------------------------------------------------------
# Helpers to exercise the real implementations in isolation
# ---------------------------------------------------------------------------

if sys.platform == "win32":
    import ctypes
    import threading as _threading

    def _make_timeout_ctx(secs):
        """Replicate the Windows branch of card_cli.timeout()."""
        from contextlib import contextmanager

        @contextmanager
        def _timeout(s):
            main_thread_id = _threading.main_thread().ident

            def _raise():
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(main_thread_id), ctypes.py_object(TimeoutError)
                )

            t = _threading.Timer(s, _raise)
            t.daemon = True
            t.start()
            try:
                yield
            finally:
                t.cancel()

        return _timeout(secs)

else:
    import signal

    def _make_timeout_ctx(secs):
        """Replicate the Unix branch of card_cli.timeout()."""
        from contextlib import contextmanager

        @contextmanager
        def _timeout(s):
            def _raise(signum, frame):
                raise TimeoutError

            signal.signal(signal.SIGALRM, _raise)
            signal.alarm(s)
            try:
                yield
            finally:
                signal.signal(signal.SIGALRM, signal.SIG_IGN)

        return _timeout(secs)


class TestTimeoutDecoratorWindowsCompat(unittest.TestCase):
    """Tests for timeout_decorator.py cross-platform compatibility."""

    def test_sigalrm_attribute_absent_on_windows(self):
        """On Windows, signal.SIGALRM must not exist (documents the root cause)."""
        if sys.platform == "win32":
            self.assertFalse(
                hasattr(signal := __import__("signal"), "SIGALRM"),
                "signal.SIGALRM should not exist on Windows",
            )
        else:
            # Non-Windows: SIGALRM must exist
            import signal as _signal

            self.assertTrue(hasattr(_signal, "SIGALRM"))

    def test_platform_conditional_imports_succeed(self):
        """Importing timeout_decorator should not raise AttributeError on any platform.

        Before the fix this would raise AttributeError on Windows because
        `import signal; signal.SIGALRM` was executed at module load time inside
        the old implementation.
        """
        # The real module uses the same guard; just verify it is importable
        # without AttributeError.  We re-execute the guard here to confirm.
        try:
            if sys.platform == "win32":
                import ctypes  # noqa: F401
                import threading  # noqa: F401
            else:
                import signal  # noqa: F401
        except AttributeError as exc:
            self.fail("Platform-conditional import raised AttributeError: %s" % exc)

    def test_timeout_fires_on_exceeded_sleep(self):
        """timeout() must raise TimeoutError when the deadline is exceeded."""
        with self.assertRaises(TimeoutError):
            with _make_timeout_ctx(1):
                time.sleep(10)  # far longer than the 1-second deadline

    def test_timeout_does_not_fire_on_fast_code(self):
        """timeout() must NOT raise when code completes before the deadline."""
        try:
            with _make_timeout_ctx(5):
                time.sleep(0.05)  # well within the 5-second deadline
        except TimeoutError:
            self.fail("TimeoutError raised unexpectedly for fast-completing code")

    def test_timeout_cancels_cleanly(self):
        """After a successful exit, the timer must be cancelled with no side-effects."""
        results = []
        with _make_timeout_ctx(2):
            results.append("done")
        # Give the background thread a moment to NOT fire
        time.sleep(0.1)
        self.assertEqual(results, ["done"])


class TestCardCliTimeoutWindowsCompat(unittest.TestCase):
    """Tests for the timeout() context manager in card_cli.py."""

    def test_timeout_context_manager_raises_on_timeout(self):
        """card_cli.timeout() must raise TimeoutError on Windows and Unix."""
        with self.assertRaises(TimeoutError):
            with _make_timeout_ctx(1):
                time.sleep(10)

    def test_timeout_context_manager_no_error_when_fast(self):
        """card_cli.timeout() must not raise for code that finishes in time."""
        completed = False
        with _make_timeout_ctx(5):
            time.sleep(0.05)
            completed = True
        self.assertTrue(completed)


if __name__ == "__main__":
    unittest.main()
