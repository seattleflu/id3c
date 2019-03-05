"""
Data type routines for going between Python and Postgres and back.
"""
import json
import warnings

# Ignore noisy warning
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message = "The psycopg2 wheel package will be renamed from release 2\.8",
        module  = "psycopg2")

    import psycopg2.extras


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
        return json.dumps(value, allow_nan = False)
