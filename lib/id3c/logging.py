"""
Logging configuration and handlers for ID3C.
"""
import logging
import logging.config
import logging.handlers
import os
import os.path
import sys
import yaml
from pkg_resources import resource_stream


LOG_CONFIG = os.environ.get("LOG_CONFIG")
LOG_LEVEL = os.environ.get("LOG_LEVEL")
IS_DEBUG = LOG_LEVEL and LOG_LEVEL.upper() == "DEBUG"


def configure():
    """
    Configures logging for ID3C.

    A stock configuration is loaded from the ``id3c/data/logging-*.yaml``
    files, chosen based on if we're running with ``LOG_LEVEL=debug`` or not.

    Python library warnings are captured and logged at the ``WARNING`` level.
    Uncaught exceptions are logged at the ``CRITICAL`` level before they cause
    the process to exit.

    Finally, if a custom configuration file is specified with ``LOG_CONFIG``,
    it is loaded as YAML and applied with :py:meth:`logging.config.dictConfig`.
    The file itself has control over whether it replaces or overlays the
    existing stock config using the ``incremental`` and
    ``disable_existing_loggers``.  Two custom YAML tags, ``!LOG_LEVEL`` and
    ``!coalesce``, are supported to aid with configs; see
    :py:class:`LogConfigLoader` for more details.
    """
    stock_config = load_stock_config("debug" if IS_DEBUG else "default")
    logging.config.dictConfig(stock_config)

    # Log library API warnings.
    logging.captureWarnings(True)

    # Log any uncaught exceptions which are about to cause process exit.
    sys.excepthook = (lambda *args:
        logging.getLogger().critical("Uncaught exception:", exc_info = args)) # type: ignore

    if LOG_CONFIG:
        with open(LOG_CONFIG, "rb") as file:
            logging.config.dictConfig(load_config(file))


def load_stock_config(name = "default"):
    """
    Loads a built-in stock logging configuration based on *name*.
    """
    with resource_stream(__package__, f"data/logging-{name}.yaml") as file:
        return load_config(file)


def load_config(config):
    """
    Loads a given logging *config* written in YAML.

    *config* may be a string or open file object, both of which are accepted as
    the first argument to :py:func:`yaml.load`.
    """
    return yaml.load(config, Loader = LogConfigLoader)


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


class LogConfigLoader(yaml.SafeLoader):
    """
    A :py:class:`yaml.SafeLoader` subclass which implements some custom `!` tags.

    Local, custom tags supported:

    * ``!LOG_LEVEL``
    * ``!coalesce``

    >>> os.environ["LOG_LEVEL"] = "info"
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': 'INFO'}

    >>> os.environ["LOG_LEVEL"] = ""
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': None}
    >>> yaml.load('''
    ... level: !coalesce
    ...   - !LOG_LEVEL
    ...   - WARNING
    ... ''', Loader = LogConfigLoader)
    {'level': 'WARNING'}

    >>> del os.environ["LOG_LEVEL"]
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': None}
    >>> yaml.load('''
    ... level: !coalesce
    ...   - !LOG_LEVEL
    ...   - WARNING
    ... ''', Loader = LogConfigLoader)
    {'level': 'WARNING'}
    """
    pass

def log_level_constructor(loader, node):
    """
    Implements a custom YAML tag ``!LOG_LEVEL``.

    Produces the uppercased value of the ``LOG_LEVEL`` environment variable,
    if the variable has a value.  Otherwise, returns ``None``.
    """
    level = os.environ.get("LOG_LEVEL")
    return level.upper() if level else None

def coalesce_constructor(loader, node):
    """
    Implements a custom YAML tag ``!coalesce``.

    When applied to a YAML sequence (e.g. list or array), the produced value is
    the first value which is not ``None``.  Akin to SQL's coalesce() function.
    """
    values = loader.construct_sequence(node)
    return first(lambda x: x is not None, values)

def first(predicate, iterable):
    """
    Return the first item in *iterable* for which *predicate* returns ``True``.

    If *iterable* is empty or contains no items passing *predicate*, returns
    ``None``.
    """
    return next(filter(predicate, iterable), None)

LogConfigLoader.add_constructor("!LOG_LEVEL", log_level_constructor)
LogConfigLoader.add_constructor("!coalesce", coalesce_constructor)
