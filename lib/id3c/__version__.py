"""
Defines the ID3C version as ``__version__``.

ID3C uses calendar versioning (http://calver.org) because it encompasses a
broad collection of things—including a CLI, web API, and database schema—which
are ~impossible to semantically version together.

The scheme is YYYY.N where YYYY is the full four-digit year and N is the
release number within that year.

This scheme is short, somewhat informative, avoids issues with multiple
releases happening on the same day, and is unlikely to be misinterpreted as a
full date.
"""
__version__ = '2020.2'
