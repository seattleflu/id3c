"""
Utilities.
"""
import ctypes
import threading
from typing import Any, Sequence, Union


def format_doc(**kwargs):
    """
    Decorator which calls :method:`str.format_map` with *kwargs* to interpolate
    variables into the docstring of the decorated function.

    This should be used sparingly, but it can be useful, for example, to
    incorporate shared constants into help text, particularly for commands.
    """
    def wrap(function):
        function.__doc__ = function.__doc__.format_map(kwargs)
        return function
    return wrap


def getattrpath(value: Any, attrpath: Union[str, Sequence[str]]) -> Any:
    """
    Get a nested named attribute, described by *attrpath*, from the
    object *value*, or return None if one of the attributes in the path
    doesn't exist.

    *attrpath* may be a dotted-string like ``a.b.c`` or a sequence
    (list or tuple) like ``("a", "b", "c")``.

    >>> class Namespace:
    ...     def __init__(self):
    ...         self.__dict__ = {}
    ...     def __repr__(self):
    ...         return repr(self.__dict__)

    >>> obj = Namespace()
    >>> obj.a = Namespace()
    >>> obj.a.b = Namespace()
    >>> obj.a.b.c = 42
    >>> getattrpath(obj, "a")
    {'b': {'c': 42}}
    >>> getattrpath(obj, "a.b.c")
    42
    >>> getattrpath(obj, "a.b.x")
    >>> getattrpath(obj, "a.x")
    >>> getattrpath(obj, "x")
    >>> getattrpath(obj, ("a", "b", "c"))
    42
    """
    if isinstance(attrpath, str):
        attrpath = attrpath.split(".")

    assert len(attrpath) != 0

    for attr in attrpath:
        value = getattr(value, attr, None)

        if value is None:
            return None

    return value


def shorten(text, length, placeholder):
    """
    Truncate *text* to a maximum *length* (if necessary), indicating truncation
    with the given *placeholder*.

    The maximum *length* must be longer than the length of the *placeholder*.

    The right-hand end of *text* is stripped of whitespace.

    Behaviour is slightly different than :py:func:`textwrap.shorten` which is
    intended for shortening sentences and works at the word, not character,
    level.

    >>> shorten("foobar", 6, "...")
    'foobar'
    >>> shorten("foobar  ", 6, "...")
    'foobar'
    >>> shorten("foobarbaz", 6, "...")
    'foo...'
    >>> shorten("foobar", 3, "...")
    Traceback (most recent call last):
        ...
    ValueError: maximum length (3) must be greater than length of placeholder (3)
    """
    text = text.rstrip()

    if length <= len(placeholder):
        raise ValueError(f"maximum length ({length}) must be greater than length of placeholder ({len(placeholder)})")

    if len(text) > length:
        return text[0:length - len(placeholder)] + placeholder
    else:
        return text


LIBCAP = None

def set_thread_name(thread: threading.Thread):
    global LIBCAP

    if LIBCAP is None:
        try:
            LIBCAP = ctypes.CDLL("libcap.so.2")
        except:
            LIBCAP = False

    if not LIBCAP:
        return

    # From the prctl(2) manpage, PR_SET_NAME is 15.
    LIBCAP.prctl(15, thread.name.encode())
