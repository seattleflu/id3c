"""
Logging handlers for ID3C.
"""
import logging
import logging.handlers
import os.path


class SysLogUnixSocketHandler(logging.Handler):
    """
    Outputs log messages to syslog if a well-known local Unix domain socket is
    available.

    The first socket in the following list that exists will be used:

    * ``/dev/log`` (typically available on Linux)
    * ``/var/run/syslog`` (typically available on macOS)

    If none of those sockets are available, this handler is equivalent to
    :py:class`logging.NullHandler`.

    Any keyword-arguments provided (other than ``address``) are passed through
    to :py:class:`logging.handlers.SysLogHandler`.
    """
    def __init__(self, **kwargs):
        potential_sockets = [
            "/dev/log",
            "/var/run/syslog",
        ]

        socket = next(filter(os.path.exists, potential_sockets), None)

        if socket:
            self.__class__ = type("SysLogUnixSocketPresentHandler", (logging.handlers.SysLogHandler,), {})
            logging.handlers.SysLogHandler.__init__(self, address = socket, **kwargs) # type: ignore
        else:
            self.__class__ = type("SysLogUnixSocketAbsentHandler", (logging.NullHandler,), {})
            logging.NullHandler.__init__(self)
