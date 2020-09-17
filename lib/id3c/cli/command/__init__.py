"""
Commands for the database CLI.
"""
import click
import pickle
import logging
from functools import wraps
from typing import IO, Any
from cachetools import TTLCache
from contextlib import contextmanager
from fcntl import flock, LOCK_EX, LOCK_UN, LOCK_NB
from id3c.db.session import DatabaseSession


__all__ = [
    "etl",
    "identifier",
    "location",
    "manifest",
    "user",
    "sequence_read_set",
    "consensus_genome",
    "redcap_det",
    "receiving",
    "geocode",
    "de_identify",
    "refresh_materialized_view",
]


LOG = logging.getLogger(__name__)

CACHE_TTL = 60 * 60 * 24 * 365  # 1 year
CACHE_SIZE = float("inf")       # Unlimited


def with_database_session(command):
    """
    Decorator to provide database session and error handling for a command.

    The decorated function is called with a ``db`` keyword argument to provide
    a :class:`~id3c.db.session.DatabaseSession` object.  The call happens
    within an exception handler that commits or rollsback the database
    transaction, possibly interactively.  Three new options are added to the
    command (``--dry-run``, ``--prompt``, and ``--commit``) to control this
    behaviour.
    """
    @click.option("--dry-run", "action",
        help        = "Only go through the motions of changing the database (default)",
        flag_value  = "rollback",
        default     = True)

    @click.option("--prompt", "action",
        help        = "Ask if changes to the database should be saved",
        flag_value  = "prompt")

    @click.option("--commit", "action",
        help        = "Save changes to the database",
        flag_value  = "commit")

    @wraps(command)
    def decorated(*args, action, **kwargs):
        db = DatabaseSession()

        processed_without_error = None

        try:
            command(*args, **kwargs, db = db)

        except Exception as error:
            processed_without_error = False

            LOG.error(f"Aborting with error: {error}")
            raise error from None

        else:
            processed_without_error = True

        finally:
            if action == "prompt":
                ask_to_commit = \
                    "Commit all changes?" if processed_without_error else \
                    "Commit successfully processed records up to this point?"

                commit = click.confirm(ask_to_commit)
            else:
                commit = action == "commit"

            if commit:
                LOG.info(
                    "Committing all changes" if processed_without_error else \
                    "Committing successfully processed records up to this point")
                db.commit()

            else:
                LOG.info("Rolling back all changes; the database will not be modified")
                db.rollback()

    return decorated


@contextmanager
def pickled_cache(filename: str = None) -> TTLCache:
    """
    Context manager for reading/writing a :class:`TTLCache` from/to the given
    *filename*.

    If *filename* exists, it is unpickled and the :class:`TTLCache` object is
    returned.  If *filename* does not exist, an empty cache will be returned.
    In either case, the cache object will be written back to the given
    *filename* upon exiting the ``with`` block.

    If no *filename* is provided, a transient, in-memory cache is returned
    instead.

    >>> with pickled_cache("/tmp/id3c-geocoding.cache") as cache:
    ...     cache["key1"] = "value1"

    >>> with pickled_cache("/tmp/id3c-geocoding.cache") as cache:
    ...     print(cache["key1"])
    value1
    """
    empty_cache = TTLCache(maxsize = CACHE_SIZE, ttl = CACHE_TTL)

    if filename:
        LOG.info(f"Loading cache from «{filename}»")
        try:
            with open(filename, "rb") as file:
                lock_file(file)
                cache = pickle.load(file)

        except FileNotFoundError:
            LOG.warning(f"Cache file «{filename}» does not exist; starting with empty cache.")
            cache = empty_cache
        else:
            assert isinstance(cache, TTLCache), \
                f"Cache file contains a {cache!r}, not a TTLCache"
    else:
        LOG.warning("No cache file provided; using transient, in-memory cache.")
        cache = empty_cache

    try:
        yield cache
    finally:
        if filename:
            with open(filename, "wb") as file:
                pickle.dump(cache, file)


@contextmanager
def lock_file(file_object: IO[Any]):
    """
    Context manager for locking/unlocking a given :class:`IOBase` *file_object*.
    """
    # Apply POSIX-style, exclusive lock
    flock(file_object, LOCK_EX)

    yield

    # Remove the lock
    flock(file_object, LOCK_UN)
