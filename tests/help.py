from pathlib import Path
from subprocess import run

topdir = Path(__file__).resolve().parent.parent

def test_help():
    # Check the exit status ourselves for nicer test output on failure
    result = run(["./dev/show-help", "./bin/id3c"], cwd = topdir)
    assert result.returncode == 0, "exited with errors"
