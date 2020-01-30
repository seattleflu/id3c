"""
Global process setup for this package.
"""
import id3c.logging


if not __debug__:
    raise Exception("Asserts are used for validation and integrity.  Please turn off optimization!")


id3c.logging.configure()
