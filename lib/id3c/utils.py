"""
Utilities.
"""
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

    Behaviour is slightly different than :py:func:`textwrap.shorten` which is
    intended for shortening sentences and works at the word, not character,
    level.

    >>> shorten("foobar", 6, "...")
    'foobar'
    >>> shorten("foobarbaz", 6, "...")
    'foo...'
    >>> shorten("foobar", 3, "...")
    Traceback (most recent call last):
        ...
    ValueError: maximum length (3) must be greater than length of placeholder (3)
    """
    if length <= len(placeholder):
        raise ValueError(f"maximum length ({length}) must be greater than length of placeholder ({len(placeholder)})")

    if len(text) > length:
        return text[0:length - len(placeholder)] + placeholder
    else:
        return text


def shorten_left(text, length, placeholder):
    """
    A variant of :py:func:`shorten` which shortens from the left end of *text*
    instead of the right.

    >>> shorten_left("foobar", 6, "...")
    'foobar'
    >>> shorten_left("foobarbaz", 6, "...")
    '...baz'
    >>> shorten_left("foobar", 3, "...")
    Traceback (most recent call last):
        ...
    ValueError: maximum length (3) must be greater than length of placeholder (3)
    """
    if length <= len(placeholder):
        raise ValueError(f"maximum length ({length}) must be greater than length of placeholder ({len(placeholder)})")

    if len(text) > length:
        return placeholder + text[-(length - len(placeholder)):]
    else:
        return text


def contextualize_char(text, idx, context = 10):
    """
    Marks the *idx* char in *text* and snips out a surrounding amount of
    *context*.

    Avoids making a copy of *text* before snipping, in case *text* is very
    large.

    >>> contextualize_char('hello world', 0, context = 4)
    '▸▸▸h◂◂◂ello…'
    >>> contextualize_char('hello world', 5, context = 3)
    '…llo▸▸▸ ◂◂◂wor…'
    >>> contextualize_char('hello world', 5, context = 100)
    'hello▸▸▸ ◂◂◂world'
    >>> contextualize_char('hello world', 10)
    'hello worl▸▸▸d◂◂◂'
    >>> contextualize_char('hello world', 2, context = 0)
    '…▸▸▸l◂◂◂…'

    >>> contextualize_char('hello world', 11)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range
    """
    if context < 0:
        raise ValueError("context must be positive")

    start = max(0, idx - context)
    end   = min(len(text), idx + context + 1)
    idx   = min(idx, context)

    start_placeholder = "…" if start > 0         else ""
    end_placeholder   = "…" if end   < len(text) else ""

    return start_placeholder + mark_char(text[start:end], idx) + end_placeholder


def mark_char(text, idx):
    """
    Prominently marks the *idx* char in *text*.

    >>> mark_char('hello world', 0)
    '▸▸▸h◂◂◂ello world'
    >>> mark_char('hello world', 2)
    'he▸▸▸l◂◂◂lo world'
    >>> mark_char('hello world', 10)
    'hello worl▸▸▸d◂◂◂'

    >>> mark_char('hello world', 11)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range

    >>> mark_char('', 0)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range
    """
    return text[0:idx] + '▸▸▸' + text[idx] + '◂◂◂' + text[idx+1:]
