"""
Seattle Flu Study database libraries
"""
import logging
from psycopg2 import IntegrityError
from psycopg2.errors import ExclusionViolation
from typing import Any
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
    minted = []
    failures = {}

    # This is a guess at a threshold that indicates an "unreasonable" level of
    # effort to mint identifiers, but I don't know the chance of hitting it
    # stochastically.
    max_consecutive_failures = 3

    with session:
        # Lookup identifier set by name
        identifier_set = session.fetch_row("""
            select identifier_set_id as id
              from warehouse.identifier_set
             where name = %s
            """, (name,))

        if not identifier_set:
            LOG.error(f"Identifier set «{name}» does not exist")
            raise IdentifierSetNotFoundError(name)

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

    LOG.info(f"Minted {n} identifiers in {n + sum(failures.values())} tries")

    return minted
