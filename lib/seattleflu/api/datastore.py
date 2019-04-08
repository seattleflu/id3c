"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from functools import wraps
from psycopg2 import DataError, DatabaseError, IntegrityError, ProgrammingError
from psycopg2.errors import InsufficientPrivilege
from typing import Any
from werkzeug.exceptions import Forbidden
from ..db.session import DatabaseSession
from .exceptions import AuthenticationRequired, BadRequest
from .utils import export


LOG = logging.getLogger(__name__)


def catch_permission_denied(function):
    """
    Decorator to catch :class:`psycopg2.ProgrammingError` exceptions with the
    ``INSUFFICIENT_PRIVILEGE`` error code and rethrow them as
    :class:`~werkzeug.exceptions.Forbidden` exceptions instead.
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        try:
            return function(*args, **kwargs)

        except InsufficientPrivilege as error:
            LOG.error("Forbidden: %s", error)
            raise Forbidden()

    return decorated


@export
def login(username: str, password: str) -> DatabaseSession:
    """
    Creates a new database session authenticated as the given user.

    Returns an opaque session object which other functions in this module
    require.
    """
    LOG.debug(f"Logging into PostgreSQL database as '{username}'")

    try:
        return DatabaseSession(username = username, password = password)

    except DatabaseError as error:
        raise AuthenticationRequired() from None


@export
@catch_permission_denied
def store_enrollment(session: DatabaseSession, document: str) -> None:
    """
    Store the given enrollment JSON *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "INSERT INTO receiving.enrollment (document) VALUES (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_scan(session: DatabaseSession, scan: dict) -> None:
    """
    Store the given *scan* (a **dictionary**) in the backing database using
    *session*.

    Raises a :class:`~werkzeug.exceptions.BadRequest` exception if the given
    *scan* isn't valid and a :class:`~werkzeug.exceptions.Forbidden` exception
    if the database reports a ``permission denied`` error.
    """
    try:
        collection = scan["collection"]
        sample     = scan["sample"]
        aliquots   = scan["aliquots"]
    except KeyError as error:
        raise BadRequest(f"Required field {error} is missing from the scan document") from None

    with session, session.cursor() as cursor:
        try:
            if collection:
                cursor.execute(
                    "insert into receiving.collection (collection_barcode) values (%s)",
                        (collection,))

            cursor.execute("""
                with new_scan as (
                    insert into receiving.scan_set default values
                        returning scan_set_id
                )
                insert into receiving.sample (sample_barcode, collection_barcode, scan_set_id)
                    values (%s, %s, (select scan_set_id from new_scan))
                """,
                (sample, collection or None))

            for aliquot in aliquots:
                cursor.execute(
                    "insert into receiving.aliquot (aliquot_barcode, sample_barcode) values (%s, %s)",
                        (aliquot, sample))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_presence_absence(session: DatabaseSession, document: str) -> None:
    """
    Store the given presence/absence *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "insert into receiving.presence_absence (document) VALUES (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
class BadRequestDatabaseError(BadRequest):
    """
    Subclass of :class:`seattleflu.api.exceptions.BadRequest` which takes a
    :class:`psycopg2.DatabaseError` and forms a JSON response detailing the
    error.

    This intentionally does not expose the query context itself, only the
    context related to the data handling.
    """
    def __init__(self, error: DatabaseError) -> None:
        super().__init__(
            error = error.diag.message_primary,
            extra = {
                "detail": error.diag.message_detail,
                "context": error.diag.context,
            }
        )
