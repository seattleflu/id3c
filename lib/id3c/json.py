"""
Standardized JSON conventions.
"""
import json
from datetime import datetime


def as_json(value):
    """
    Converts *value* to a JSON string using our custom :class:`JsonEncoder`.
    """
    return json.dumps(value, allow_nan = False, cls = JsonEncoder)


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
