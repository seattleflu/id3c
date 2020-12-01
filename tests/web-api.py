import pytest
from pathlib import Path
from signal import SIGINT
from subprocess import Popen, TimeoutExpired
from time import sleep

topdir = Path(__file__).resolve().parent.parent

def test_web_api():
    with Popen(["python3", "-m", "id3c.api"], cwd = topdir) as api_server:
        # Give it a couple seconds to start up and settle.  Timeout is expected
        # if the server successfully started up, as it won't exit until we ask
        # it to.
        try:
            api_server.wait(timeout = 2)
        except TimeoutExpired:
            pass

        # Check that it's still running.
        assert api_server.poll() is None, "not running"

        # Send the same signal as Ctrl-C, which the dev server interprets as a
        # request to exit cleanly.
        api_server.send_signal(SIGINT)
        api_server.wait()

        assert api_server.returncode == 0, "exited with error"
