"""
Utility decorators for routes.
"""
from flask import request
from functools import wraps
from typing import Any, Callable, Iterable, Tuple, Union
from werkzeug.exceptions import RequestEntityTooLarge, UnsupportedMediaType
from ..exceptions import AuthenticationRequired
from ..utils import prose_list, export
from .. import datastore


@export
def authenticated_datastore_session_required(route):
    """
    Requires requests have an ``Authorization`` header and uses it to login to
    the :class:`~seattleflu.api.datastore`.

    The logged in datastore *session* is provided as a keyword-argument to the
    original route.

    Raises a :class:`seattleflu.api.exceptions.AuthenticationRequired`
    exception if the request doesn't provide an ``Authorization`` header.
    """
    @wraps(route)
    def wrapped_route(*args, **kwargs):
        auth = request.authorization

        if not (auth and auth.username and auth.password):
            raise AuthenticationRequired()

        session = datastore.login(
            username = auth.username,
            password = auth.password)

        return route(*args, **kwargs, session = session)

    return wrapped_route


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
