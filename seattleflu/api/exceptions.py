"""
Exceptions for the API.
"""
from werkzeug.exceptions import Unauthorized
from werkzeug.datastructures import WWWAuthenticate
from .utils import export


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
