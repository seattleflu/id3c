"""
CLI setup
"""
import logging
import os


# Setup logging for the whole CLI
log_handler = logging.StreamHandler()

root_logger = logging.getLogger()
root_logger.addHandler(log_handler)

LOG = logging.getLogger(__name__)


# Set logging level from environment if requested
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info")

if LOG_LEVEL:
    root_logger.setLevel(LOG_LEVEL.upper())


# Default to timestamped messages, but include lots of useful debugging/tracing
# info for lower priority levels than warning.
if LOG.getEffectiveLevel() >= logging.INFO:
    log_format = "[{asctime}] {message}"
else:
    log_format = "[{asctime}] [pid {process}] {name} {levelname}: {message}"

log_handler.setFormatter(
    logging.Formatter(
        fmt     = log_format,
        datefmt = "%Y-%m-%d %H:%M:%S",
        style   = "{",
    )
)
