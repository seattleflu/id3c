import pytest
import re
from pathlib import Path
from sqlparse import split

topdir = Path(__file__).resolve().parent.parent

sql_files = list(sorted(topdir.glob("schema/**/*.sql")))

# Yes, this is matching each SQL statement with a regex.  Yes, it should use
# the tokens instead, but that's more complex than it may initially seem and
# this is good enough for now.
#   -trs, 18 Oct 2019
#
bad_search_path = re.compile(
    r"""
    # SET command not followed by LOCAL modifier
    set \s+ (?!local\s+)

    # …followed, potentially after other parameters first, by "search_path"
    [^;]*? search_path

    # …which is not followed by a comment telling us to ignore it
    (?! [^\n]*? -- \s* tests/search-path: \s* ignore)
    """,
    re.DOTALL | re.IGNORECASE | re.VERBOSE
)

@pytest.mark.parametrize("path", sql_files, ids = lambda path: str(path))
def test_sql_script(path):
    sql = path.read_text(encoding = "utf-8")

    for statement in split(sql):
        match = bad_search_path.search(statement)
        assert not match, f"{path.relative_to(topdir)} contains an unlocalized `set search_path`:\n{match[0]}"
