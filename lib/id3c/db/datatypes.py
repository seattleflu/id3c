"""
Data type routines for going between Python and Postgres and back.
"""
import psycopg2.extras
from ..json import as_json


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
