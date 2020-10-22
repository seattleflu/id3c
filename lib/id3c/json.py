"""
Standardized JSON conventions.
"""
import json
from datetime import datetime
from typing import Iterable


def as_json(value):
    """
    Converts *value* to a JSON string using our custom :class:`JsonEncoder`.
    """
    return json.dumps(value, allow_nan = False, cls = JsonEncoder)


def dump_ndjson(iterable: Iterable) -> None:
    """
    :func:`print` *iterable* as a set of newline-delimited JSON records.
    """
    for item in iterable:
        print(as_json(item))


def load_ndjson(file: Iterable[str]) -> Iterable:
    """
    Load newline-delimited JSON records from *file*.
    """
    yield from (json.loads(line) for line in file)


class JsonEncoder(json.JSONEncoder):
    """
    Encodes Python values into JSON for non-standard objects.
    """

    def default(self, value):
        """
        Returns *value* as JSON or raises a TypeError.

        Serializes:

        * :class:`~datetime.datetime` using :meth:`~datetime.datetime.isoformat()`
        """
        if isinstance(value, datetime):
            return value.isoformat()
        else:
            # Let the base class raise the TypeError
            return super().default(value)
