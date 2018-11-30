"""
Utility functions.
"""
import sys
from typing import Iterable


def prose_list(iterable: Iterable[str], conjunction: str = "or") -> str:
    """
    Construct a nice natural language list of items from the *iterable*.  The
    default *conjunction* is "or".
    """
    values = list(iterable)

    if len(values) > 2:
        return ", ".join([*values[:-1], f"{conjunction} " + values[-1]])
    else:
        return f" {conjunction} ".join(values)


def export(function):
    """
    Decorator to mark a function as "exported" by adding it to the module's
    :data:`__all__`.

    This is useful to avoid exposing module internals to importers.
    """
    module = sys.modules[function.__module__]

    if hasattr(module, '__all__'):
        module.__all__.append(function.__name__)
    else:
        module.__all__ = [function.__name__]

    return function
