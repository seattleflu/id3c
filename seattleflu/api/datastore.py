"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from functools import wraps
from psycopg2 import DataError, DatabaseError, IntegrityError, ProgrammingError
from typing import Any
from werkzeug.exceptions import Forbidden
from .exceptions import AuthenticationRequired, BadRequest
from .utils import export


LOG = logging.getLogger(__name__)

# Really psycopg2.extensions.connection, but avoiding annotating that so it
# isn't relied upon.
Session = Any


def catch_permission_denied(function):
    """
    Decorator to catch :class:`psycopg2.ProgrammingError` exceptions starting
    with ``permission denied`` and rethrow them as
    :class:`~werkzeug.exceptions.Forbidden` exceptions instead.
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        try:
            return function(*args, **kwargs)

        except ProgrammingError as error:
            if error.diag.message_primary.startswith("permission denied"):
                LOG.error("Forbidden: %s", error)
                raise Forbidden()
            else:
                raise error from None

    return decorated


@export
def login(username: str, password: str) -> Session:
    """
    Creates a new database session authenticated as the given user.

    Returns an opaque session object which other functions in this module
    require.
    """
    # Connection details like host and database are controlled entirely by
    # standard libpq environment variables:
    #
    #    https://www.postgresql.org/docs/current/libpq-envars.html
    #
    LOG.debug(f"Authenticating to PostgreSQL database as '{username}'")

    try:
        session = psycopg2.connect(user = username, password = password)
    except DatabaseError as error:
        LOG.error(f"Authentication failed: {error}")
        raise AuthenticationRequired() from None

    LOG.debug(f"Session created for {session_info(session)}")

    return session


def session_info(session) -> str:
    """
    Takes a *session* object and returns a concise string describing it.
    """
    info = [
        "user",
        "dbname",
        "host",
        "port",
        "sslmode",
    ]

    params = session.get_dsn_parameters()

    return " ".join(
        f"{param}={params.get(param)}"
            for param in info
             if params.get(param))


@export
@catch_permission_denied
def store_enrollment(session: Session, document: str) -> None:
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
                "INSERT INTO staging.enrollment (document) VALUES (%s)",
                    (document,))

        except DataError as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_scan(session: Session, scan: dict) -> None:
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
                    "insert into staging.collection (collection_barcode) values (%s)",
                        (collection,))

            cursor.execute("""
                with new_scan as (
                    insert into staging.scan_set default values
                        returning scan_set_id
                )
                insert into staging.sample (sample_barcode, collection_barcode, scan_set_id)
                    values (%s, %s, (select scan_set_id from new_scan))
                """,
                (sample, collection or None))

            for aliquot in aliquots:
                cursor.execute(
                    "insert into staging.aliquot (aliquot_barcode, sample_barcode) values (%s, %s)",
                        (aliquot, sample))

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
