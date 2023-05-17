"""
Commands for the database CLI.
"""
import enum
import logging
import os
import pickle
from contextlib import contextmanager
from functools import wraps
from sys import maxsize
from typing import Iterator, Optional

import click
from cachetools import TTLCache

from id3c.cli.redcap import Project
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
    "redcap_sync",
]


LOG = logging.getLogger(__name__)

CACHE_TTL = 60 * 60 * 24 * 365  # 1 year
CACHE_SIZE = maxsize


@enum.unique
class DatabaseSessionAction(enum.Enum):
    """
    Enum representing the database session transaction action selected for a
    command decorated by :py:func:`.with_database_session`.

    You will not need to use this class unless you provide ``pass_action =
    True`` to :py:func:`.with_database_session`.
    """

    DRY_RUN = "rollback"
    PROMPT = "prompt"
    COMMIT = "commit"


def with_database_session(command=None, *, pass_action: bool = False):
    """
    Decorator to provide database session and error handling for a *command*.

    The *command* callable must be a :py:class:`click.Command` instance.

    The decorated *command* is called with a ``db`` keyword argument to provide
    a :class:`~id3c.db.session.DatabaseSession` object.  The call happens
    within an exception handler that commits or rollsback the database
    transaction, possibly interactively.  Three new options are added to the
    *command* (``--dry-run``, ``--prompt``, and ``--commit``) to control this
    behaviour.

    >>> @click.command
    ... @with_database_session
    ... def cmd(db: DatabaseSession):
    ...     pass

    If the optional, keyword-only argument *pass_action* is ``True``, then the
    :py:class:`.DatabaseSessionAction` selected by the CLI options above is
    passed as an additional ``action`` argument to the decorated *command*.

    >>> @click.command
    ... @with_database_session(pass_action = True)
    ... def cmd(db: DatabaseSession, action: DatabaseSessionAction):
    ...     pass

    One example where this is useful is when the *command* accesses
    non-database resources and wants to extend dry run mode to them as well.
    """

    def decorator(command):
        @click.option(
            "--dry-run",
            "action",
            help="Only go through the motions of changing the database (default)",
            flag_value=DatabaseSessionAction("rollback"),
            default=True,
        )
        @click.option(
            "--prompt",
            "action",
            help="Ask if changes to the database should be saved",
            flag_value=DatabaseSessionAction("prompt"),
        )
        @click.option(
            "--commit",
            "action",
            help="Save changes to the database",
            flag_value=DatabaseSessionAction("commit"),
        )
        @wraps(command)
        def decorated(*args, action, **kwargs):
            db = DatabaseSession()

            kwargs["db"] = db

            if pass_action:
                kwargs["action"] = action

            processed_without_error = None

            try:
                command(*args, **kwargs)

            except Exception as error:
                processed_without_error = False

                LOG.error(f"Aborting with error: {error}")
                raise error from None

            else:
                processed_without_error = True

            finally:
                if action is DatabaseSessionAction.PROMPT:
                    ask_to_commit = (
                        "Commit all changes?"
                        if processed_without_error
                        else "Commit successfully processed records up to this point?"
                    )

                    commit = click.confirm(ask_to_commit)
                else:
                    commit = action is DatabaseSessionAction.COMMIT

                if commit:
                    LOG.info(
                        "Committing all changes"
                        if processed_without_error
                        else "Committing successfully processed records up to this point"
                    )
                    db.commit()

                else:
                    LOG.info(
                        "Rolling back all changes; the database will not be modified"
                    )
                    db.rollback()

        return decorated

    return decorator(command) if command else decorator


def with_redcap_project(command=None):
    """
    Decorator to provide a redcap project to a *command*.

    The *command* callable must be a :py:class:`click.Command` instance.

    The decorated *command* is called with a ``project`` keyword argument to provide
    a :class:`~id3c.cli.redcap.Project` object. Three new options are added to the
    *command* (``--api-url``, ``--project-id``, and ``--token``), which are the
    minimum data required to construct a REDCap project.

    >>> @click.command
    ... @with_redcap_project
    ... def cmd():
    ...     pass
    """

    def decorator(command):
        @click.option(
            "--api-url",
            metavar="<url>",
            help="The API endpoint of the REDCap instance.",
            required=True,
            envvar="REDCAP_API_URL",
            show_envvar=True,
        )
        @click.option(
            "--project-id",
            metavar="<id>",
            type=int,
            help="The project id from which to fetch records.  "
            "Must match the project associated with the provided API token.",
            required=True,
            envvar="REDCAP_PROJECT_ID",
            show_envvar=True,
        )
        @click.option(
            "--token",
            metavar="<token-name>",
            help="The name of the environment variable that holds the API token.  "
            "Defaults to a name based on the --api-url and --project-id values: "
            "REDCAP_API_TOKEN_{api_url_origin}_{project_id}.",
        )
        @wraps(command)
        def decorated(*args, api_url, project_id, token, **kwargs):
            api_token = os.environ[token] if token else None
            project = Project(api_url, project_id, token=api_token)
            kwargs["project"] = project

            command(*args, **kwargs)

        return decorated

    return decorator(command) if command else decorator


@contextmanager
def pickled_cache(
    filename: str = None, create_if_missing: bool = False
) -> Iterator[TTLCache]:
    """
    Context manager for reading/writing a :class:`TTLCache` from/to the given
    *filename*.

    If *filename* exists, it is unpickled and the :class:`TTLCache` object is
    returned.  If *filename* does not exist, an empty cache will be returned.
    In either case, the cache object will be written back to the given
    *filename* upon exiting the ``with`` block.

    If no *filename* is provided, a transient, in-memory cache is returned
    instead.

    If a *filename* is provided that does not currently exist, and create_if_missing
    is `True`, a new cache file will be created. If the provided *filename* does not
    exist and create_if_missing is `False`, an error will be raised.

    >>> with pickled_cache("/tmp/id3c-geocoding.cache", True) as cache:
    ...     cache["key1"] = "value1"

    >>> with pickled_cache("/tmp/id3c-geocoding.cache", True) as cache:
    ...     print(cache["key1"])
    value1
    """
    empty_cache: TTLCache = TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL)

    if filename:
        LOG.info(f"Loading cache from «{filename}»")
        try:
            with open(filename, "rb") as file:
                cache = pickle.load(file)
        except FileNotFoundError as error:
            if create_if_missing:
                LOG.warning(
                    f"Cache file «{filename}» does not exist; starting with empty cache."
                )
                cache = empty_cache
            else:
                LOG.error(
                    f"Cache file «{filename}» does not exist, please provide a valid cache."
                )
                raise error from None
        else:
            assert isinstance(
                cache, TTLCache
            ), f"Cache file contains a {cache!r}, not a TTLCache"
    else:
        LOG.warning("No cache file provided; using transient, in-memory cache.")
        cache = empty_cache

    try:
        yield cache
    finally:
        if filename:
            with open(filename, "wb") as file:
                pickle.dump(cache, file)
