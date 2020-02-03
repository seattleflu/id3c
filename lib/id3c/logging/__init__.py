"""
Logging for ID3C.
"""
import logging
import logging.config
import os
import sys
from .config import load_stock_config, load_config


LOG_CONFIG = os.environ.get("LOG_CONFIG")
LOG_LEVEL = os.environ.get("LOG_LEVEL")
IS_DEBUG = LOG_LEVEL and LOG_LEVEL.upper() == "DEBUG"


def configure():
    """
    Configures logging for ID3C.

    A stock configuration is loaded from the ``id3c/logging/data/*.yaml``
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
