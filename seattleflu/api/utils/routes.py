"""
Utility decorators for routes.
"""
from flask import request
from functools import wraps
from typing import Any, Callable, Iterable, Tuple, Union
from werkzeug.exceptions import RequestEntityTooLarge, UnsupportedMediaType
from ..utils import prose_list, export


@export
def content_types_accepted(allowed: Iterable[str]):
    """
    Decorate a route with set of allowed Content-Types for the request body.

    Raises an :class:`werkzeug.exceptions.UnsupportedMediaType` exception if
    the request's Content-Type is not allowed.
    """
    def decorator(route):
        @wraps(route)
        def wrapped_route(*args, **kwargs):
            if request.mimetype not in allowed:
                raise UnsupportedMediaType(f"Body data Content-Type must be {prose_list(allowed)}")

            return route(*args, **kwargs)

        return wrapped_route

    return decorator


@export
def check_content_length(route):
    """
    Decorator for routes to check Content-Length against the app-configured
    maximum.

    Raises an :class:`werkzeug.exceptions.RequestEntityTooLarge` exception if
    the request sends too much data.
    """
    @wraps(route)
    def wrapped_route(*args, **kwargs):
        if request.content_length > request.max_content_length:
            raise RequestEntityTooLarge(f"Content-Length exceeded {request.max_content_length} bytes")

        return route(*args, **kwargs)

    return wrapped_route
