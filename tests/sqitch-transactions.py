import pytest
from pathlib import Path
from sqlparse import split, parse
from sqlparse.tokens import Keyword

topdir = Path(__file__).resolve().parent.parent

sql_files = list(sorted(topdir.glob("schema/**/*.sql")))


@pytest.mark.parametrize("path", sql_files, ids = lambda path: str(path))
def test_sql_script(path):
    sql = path.read_text(encoding = "utf-8")

    statements = [parse(s)[0] for s in split(sql)]

    if verify_script(path):
        final_type = "rollback"
    else:
        final_type = "commit"

    is_final_type = lambda statement: statement.get_type() == final_type.upper()

    has_begin = statements[0].token_first(skip_cm = True).match(Keyword, ["begin"])
    has_final = is_final_type(statements[-1])
    has_premature = any(map(is_final_type, statements[:-1]))

    assert has_begin, f"{path.relative_to(topdir)}: first statement is not begin"
    assert has_final, f"{path.relative_to(topdir)}: final statement is not a {final_type}"
    assert not has_premature, f"{path.relative_to(topdir)}: premature {final_type}(s) found"


def verify_script(path) -> bool:
    return path.relative_to(topdir).parts[:2] == ("schema", "verify")
