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
