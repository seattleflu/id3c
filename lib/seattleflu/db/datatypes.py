"""
Data type routines for going between Python and Postgres and back.
"""
import json
import psycopg2.extras
from datetime import datetime


def as_json(value):
    """
    Converts *value* to a JSON string using our custom :class:`JsonEncoder`.
    """
    return json.dumps(value, allow_nan = False, cls = JsonEncoder)


class Json(psycopg2.extras.Json):
    """
    psycopg2 adapter for converting Python values into JSON strings for
    Postgres.
    """

    def dumps(self, value):
        """
        Converts *value* to a JSON string for Postgres.

        Floating point values NaN, Infinity, and -Infinity are allowed in JSON
        by JavaScript and Python, but not Postgres.
        """
        return as_json(value)


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
