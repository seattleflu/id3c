"""
Utility decorators for routes.
"""
import json
from flask import request, Response
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
    the :class:`~id3c.api.datastore`.

    The logged in datastore *session* is provided as a keyword-argument to the
    original route.

    Raises a :class:`id3c.api.exceptions.AuthenticationRequired`
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
    Decorate a route with set of *allowed* Content-Types for the request body.

    Include the value ``None`` in *allowed* to allow Content-Type to be omitted
    for requests without a body.

    Raises an :class:`werkzeug.exceptions.UnsupportedMediaType` exception if
    the request's Content-Type is not allowed.
    """
    allowed = set(allowed)

    try:
        allowed.remove(None)
        missing_ok = True
    except KeyError:
        missing_ok = False

    def decorator(route):
        @wraps(route)
        def wrapped_route(*args, **kwargs):
            if missing_ok and request.content_length is None and not request.mimetype:
                pass
            elif request.mimetype not in allowed:
                raise UnsupportedMediaType(f"Body data Content-Type must be {prose_list(allowed)}{' when a body is sent' if missing_ok else ''}")

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
        if (request.content_length is not None
        and request.content_length > request.max_content_length): # type: ignore
            raise RequestEntityTooLarge(f"Content-Length exceeded {request.max_content_length} bytes")

        return route(*args, **kwargs)

    return wrapped_route
