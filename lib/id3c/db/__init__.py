"""
Database interfaces
"""
import logging
import secrets
import statistics
from datetime import datetime
from psycopg2 import IntegrityError
from psycopg2.errors import ExclusionViolation
from psycopg2.sql import SQL, Identifier
from statistics import median, StatisticsError
from typing import Any, Dict, Iterable, List, NamedTuple, Optional
from .types import IdentifierRecord
from .session import DatabaseSession


LOG = logging.getLogger(__name__)


class IdentifierMintingError(Exception):
    pass


class IdentifierSetNotFoundError(IdentifierMintingError):
    """
    Raised when a named identifier set does not exist.
    """
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"No identifier set named «{self.name}»"


class TooManyFailuresError(IdentifierMintingError):
    """
    Raised when :func:``mint_identifiers`` is unable to generate an allowable
    barcode.
    """
    def __init__(self, count):
        self.count = count

    def __str__(self):
        return f"Too many consecutive failures ({self.count}); trying again may succeed"


def mint_identifiers(session: DatabaseSession, name: str, n: int) -> Any:
    """
    Generate *n* new identifiers in the set *name*.

    Raises a :class:`~werkzeug.exceptions.NotFound` exception if the set *name*
    doesn't exist and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.  If too many consecutive minting failures
    happen, then a :class:`~werkzeug.exceptions.ServiceUnavailable` exception
    is raised.
    """
    minted: List[Any] = []
    failures: Dict[int, int] = {}

    # This is a guess at a threshold that indicates an "unreasonable" level of
    # effort to mint identifiers, but I don't know the chance of hitting it
    # stochastically.
    max_consecutive_failures = 15

    # Lookup identifier set by name
    identifier_set = session.fetch_row("""
        select identifier_set_id as id
          from warehouse.identifier_set
         where name = %s
        """, (name,))

    if not identifier_set:
        LOG.error(f"Identifier set «{name}» does not exist")
        raise IdentifierSetNotFoundError(name)

    started = datetime.now()

    while len(minted) < n:
        m = len(minted) + 1

        LOG.debug(f"Minting identifier {m}/{n}")

        try:
            with session.savepoint(f"identifier {m}"):
                new_identifier = session.fetch_row("""
                    insert into warehouse.identifier (identifier_set_id, generated)
                        values (%s, now())
                        returning uuid, barcode, generated
                    """,
                    (identifier_set.id,))

                minted.append(new_identifier)

        except ExclusionViolation:
            LOG.debug("Barcode excluded")

            failures.setdefault(m, 0)
            failures[m] += 1

            if failures[m] > max_consecutive_failures:
                LOG.error("Too many consecutive failures, aborting")
                raise TooManyFailuresError(failures[m])
            else:
                LOG.debug("Retrying")

    duration = datetime.now() - started
    per_second = n / duration.total_seconds()
    per_identifier = duration.total_seconds() / n

    failure_counts = list(failures.values())

    LOG.info(f"Minted {n} identifiers in {n + sum(failure_counts)} tries ({sum(failure_counts)} retries) over {duration} ({per_identifier:.2f} s/identifier = {per_second:.2f} identifiers/s)")

    if failure_counts:
        LOG.info(f"Failure distribution: max={max(failure_counts)} mode={mode(failure_counts)} median={median(failure_counts)}")

    return minted


def find_identifier(db: DatabaseSession, barcode: str) -> Optional[IdentifierRecord]:
    """
    Lookup a known identifier by *barcode*.
    """
    LOG.debug(f"Looking up barcode {barcode}")

    identifier: IdentifierRecord = db.fetch_row("""
        select uuid::text,
               barcode,
               generated,
               identifier_set.name as set_name
          from warehouse.identifier
          join warehouse.identifier_set using (identifier_set_id)
         where barcode = %s
        """, (barcode,))

    if identifier:
        LOG.info(f"Found {identifier.set_name} identifier {identifier.uuid}")
        return identifier
    else:
        LOG.warning(f"No identifier found for barcode «{barcode}»")
        return None


def create_user(session: DatabaseSession, name, comment: str = None) -> None:
    """
    Create the user *name*, described by an optional *comment*.
    """
    with session.cursor() as cursor:
        LOG.info(f"Creating user «{name}»")

        cursor.execute(
            sqlf("create user {}", Identifier(name)))

        if comment is not None:
            cursor.execute(
                sqlf("comment on role {} is %s", Identifier(name)),
                (comment,))


def grant_roles(session: DatabaseSession, username, roles: Iterable = []) -> None:
    """
    Grants the given set of *roles* to *username*.
    """
    if not roles:
        LOG.warning("No roles provided; none will be granted.")
        return

    with session.cursor() as cursor:
        for role in roles:
            LOG.info(f"Granting «{role}» to «{username}»")

            cursor.execute(
                sqlf("grant {} to {}",
                     Identifier(role),
                     Identifier(username)))


def reset_password(session: DatabaseSession, username) -> str:
    """
    Sets the password for *username* to a generated random string.

    The new password is returned.
    """
    new_password = secrets.token_urlsafe()

    with session.cursor() as cursor:
        LOG.info(f"Setting password of user «{username}»")

        cursor.execute(
            sqlf("alter user {} password %s", Identifier(username)),
            (new_password,))

    return new_password


def sqlf(sql, *args, **kwargs):
    """
    Format the given *sql* statement using the given arguments.

    This should only be used for SQL statements which need to be dynamic, or
    need to include quoted identifier values.  Literal values are best
    supported by using placeholders in the call to ``execute()``.
    """
    return SQL(sql).format(*args, **kwargs)


def mode(values):
    """
    Wraps :py:func:`statistics.mode` or :py:func:`statistics.multimode`, if
    available, to do the right thing regardless of the Python version.

    Returns ``None`` if the underlying functions raise a
    :py:exc:`~statistics.StatisticsError`.
    """
    mode_ = getattr(statistics, "multimode", statistics.mode)

    try:
        return mode_(values)
    except StatisticsError:
        return None
