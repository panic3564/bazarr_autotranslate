import os
import subprocess
import sys

import main


def test_startup_banner_is_logged_before_the_connection_check(tmp_path):
    """
    Boot the script as a subprocess with a valid configuration and an
    unreachable Bazarr URL: the startup banner must appear on stdout
    (which is what `docker logs` shows) even though the process then
    exits because the connection check fails.
    """
    env = {
        **os.environ,
        # Port 9 (discard) is not in use: the connection is refused
        # immediately, so the test does not wait for a timeout
        "BAZARR_BASE_URL": "http://127.0.0.1:9",
        "BAZARR_API_KEY": "test-key",
        "BASE_LANGUAGES": "en",
        "TO_LANGUAGES": "nl",
        "LOG_DIRECTORY": str(tmp_path),
        "LOG_LEVEL": "INFO",
    }
    result = subprocess.run(
        [sys.executable, "main.py"],
        cwd=os.path.dirname(main.__file__),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    # The connection check fails by design: the banner must have been
    # printed before that failure
    assert result.returncode == 1
    assert f"Starting bazarr_autotranslate {main.__version__}" in result.stdout
    assert "Could not connect to Bazarr" in result.stdout
