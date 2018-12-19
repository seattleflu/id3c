"""
Exceptions for the API.
"""
import inspect
import logging
import werkzeug.exceptions
from flask import jsonify
from typing import Any
from werkzeug.exceptions import Unauthorized
from werkzeug.datastructures import WWWAuthenticate
from .utils import export


LOG = logging.getLogger(__name__)

WWW_AUTHENTICATE = WWWAuthenticate("basic", {"realm": "api"})


@export
class AuthenticationRequired(Unauthorized):
    """
    A specialized :class:`werkzeug.exceptions.Unauthorized` with a preset
    ``WWW-Authenticate`` header.
    """
    def get_headers(self, *args, **kwargs):
        """
        Unconditionally adds a statically defined ``WWW-Authenticate`` header
        using :const:`WWW_AUTHENTICATE`.

        This will be rendered unnecessary when the latest dev version of
        werkzeug is released, as it has another mechanism for passing through
        the header value.
        """
        headers = super().get_headers(*args, **kwargs)
        return (*headers, ("WWW-Authenticate", str(WWW_AUTHENTICATE)))


@export
class BadRequest(werkzeug.exceptions.BadRequest):
    """
    Subclass of :class:`werkzeug.exceptions.BadRequest` which forms a JSON
    response instead of an HTML response.
    """
    def __init__(self, error: Any, extra: dict = None) -> None:
        super().__init__()

        # Log this error using the _calling_ module's logger, so messages come
        # from the right place.  Falls back to our own logger if the stack
        # inspection goes wrong.
        try:
            log_as = inspect.stack()[1].frame.f_globals["__name__"]
            log = logging.getLogger(log_as)
        except Exception as exception:
            LOG.warning(f"An exception occurred trying to use the caller's logger: {exception}")
            log = LOG

        # Log the message with the concrete class name, which may be a
        # subclass (e.g. BadRequestDatabaseError).
        log.error(f"{self.__class__.__name__}: {error} {extra}")

        self.response = jsonify({
            "error": error,
            **(extra or {})
        })

        self.response.status_code = self.code
