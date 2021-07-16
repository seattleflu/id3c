import pytest
import re
from os import environ
from pathlib import Path

GITHUB_EVENT_NAME = environ.get("GITHUB_EVENT_NAME", "")
GITHUB_REF = environ.get("GITHUB_REF", "")

testing_master = (
    # Push to master
    (GITHUB_EVENT_NAME == "push" and GITHUB_REF == "refs/heads/master")

    # Trial merge of PR branch + master
    or GITHUB_EVENT_NAME == "pull_request"
)

# Only test on master, since we only care that master always has a sqitch tag
# as the final sqitch plan element.  This avoids spurious failures on branches
# which add the tag as their last commit.
if not testing_master:
    pytest.skip("skipping master-only test", allow_module_level = True)


topdir = Path(__file__).resolve().parent.parent
plan = topdir / "schema/sqitch.plan"


def test_sqitch_plan():
    change_or_tag_lines = [
        line.strip()
          for line in plan.read_text(encoding = "utf-8").splitlines()
           if not param_comment_or_blank(line)
    ]

    assert change_or_tag_lines[-1].startswith("@"), f"last line of {plan.relative_to(topdir)} is not a tag"


def param_comment_or_blank(line):
    """
    In a Sqitch plan file, ``%`` starts a parameter line, ``#`` starts a
    comment line, and blank lines and leading whitespace are allowed.
    """
    return re.search(r'^\s*([%#]|$)', line)

