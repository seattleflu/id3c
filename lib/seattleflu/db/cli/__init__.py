"""
CLI setup
"""
import logging
import os
import os.path
from logging import StreamHandler
from logging.handlers import SysLogHandler


# Setup root logger for this process.
#
# Filtering of messages by level is done at the handler level by using NOTSET
# on the root logger to emit everything.  This lets us keep console output
# readable while emitting verbose output to alternate handlers.
root_logger = logging.getLogger()
root_logger.setLevel(logging.NOTSET)


# Configure console logging, setting level to INFO or from the environment
# variable LOG_LEVEL.
console = StreamHandler()
console.setLevel(logging.INFO)

LOG_LEVEL = os.environ.get("LOG_LEVEL")

if LOG_LEVEL:
    console.setLevel(LOG_LEVEL.upper())

# Default to timestamped messages, but include lots of useful debugging/tracing
# info for lower priority levels than warning.
if console.level >= logging.INFO:
    console_format = "[{asctime}] {message}"
else:
    console_format = "[{asctime}] [pid {process}] {name} {levelname}: {message}"

console.setFormatter(
    logging.Formatter(
        fmt     = console_format,
        datefmt = "%Y-%m-%d %H:%M:%S",
        style   = "{",
    )
)

root_logger.addHandler(console)


# Configure syslog handler if a well-known Unix socket exists.  Linux generally
# uses /dev/log; macOS /var/run/syslog.  This handler emits all log levels;
# filtering is more usefully done in syslog config.
syslog_socket = next(filter(os.path.exists, ["/dev/log", "/var/run/syslog"]), None)

if syslog_socket:
    syslog = SysLogHandler(address = str(syslog_socket))
    syslog.setLevel(logging.NOTSET)
    syslog.setFormatter(
        logging.Formatter(
            fmt   = "%s[{process}] {name} {levelname}: {message}" % __name__,
            style = "{",
        )
    )

    root_logger.addHandler(syslog)
