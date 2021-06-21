"""
Standardized JSON conventions.
"""
import json
from datetime import datetime
from typing import Iterable
from .utils import contextualize_char, shorten_left


def as_json(value):
    """
    Converts *value* to a JSON string using our custom :class:`JsonEncoder`.
    """
    return json.dumps(value, allow_nan = False, cls = JsonEncoder)


def load_json(value):
    """
    Converts *value* from a JSON string with better error messages.

    Raises an :exc:`id3c.json.JSONDecodeError` which provides improved error
    messaging, compared to :exc:`json.JSONDecodeError`, when stringified.
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise JSONDecodeError(e) from e


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
    yield from (load_json(line) for line in file)


class JsonEncoder(json.JSONEncoder):
    """
    Encodes Python values into JSON for non-standard objects.
    """

    def __init__(self, *args, **kwargs):
        """
        Disallows the floating point values NaN, Infinity, and -Infinity.

        Python's :class:`json` allows them by default because they work with
        JSON-as-JavaScript, but they don't work with spec-compliant JSON
        parsers.
        """
        kwargs["allow_nan"] = False
        super().__init__(*args, **kwargs)

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


class JSONDecodeError(json.JSONDecodeError):
    """
    Subclass of :class:`json.JSONDecodeError` which contextualizes the
    stringified error message by including a snippet of the JSON source input.

    Typically you won't need to ever reference this class directly.  It will be
    raised by :func:`load_json` and be caught by except blocks which catch the
    standard :class:`json.JSONDecodeError`.

    >>> load_json('{foo: "bar"}')
    Traceback (most recent call last):
        ...
    id3c.json.JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1): '{▸▸▸f◂◂◂oo: "bar"}'

    >>> load_json('not json')
    Traceback (most recent call last):
        ...
    id3c.json.JSONDecodeError: Expecting value: line 1 column 1 (char 0): 'not json'

    >>> load_json("[0, 1, 2, 3, 4, 5")
    Traceback (most recent call last):
        ...
    id3c.json.JSONDecodeError: Expecting ',' delimiter: line 1 column 18 (char 17): unexpected end of document: '…, 3, 4, 5'

    >>> load_json("[\\n")
    Traceback (most recent call last):
        ...
    id3c.json.JSONDecodeError: Expecting value: line 2 column 1 (char 2): unexpected end of document: '[\\n'

    >>> load_json('')
    Traceback (most recent call last):
        ...
    id3c.json.JSONDecodeError: Expecting value: line 1 column 1 (char 0): (empty source document)
    """
    CONTEXT_LENGTH = 10

    def __init__(self, exc: json.JSONDecodeError):
        super().__init__(exc.msg, exc.doc, exc.pos)

    def __str__(self):
        error = super().__str__()

        if self.doc:
            if self.pos == 0 and self.msg == "Expecting value":
                # Most likely not a JSON document at all, so show the whole thing.
                context = repr(self.doc)
            elif self.pos > 0 and self.pos == len(self.doc):
                context = "unexpected end of document: " + repr(shorten_left(self.doc, self.CONTEXT_LENGTH, "…"))
            else:
                context = repr(contextualize_char(self.doc, self.pos, self.CONTEXT_LENGTH))
        else:
            context = "(empty source document)"

        return f"{error}: {context}"
