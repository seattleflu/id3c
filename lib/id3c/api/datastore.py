"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from functools import wraps
from psycopg2 import DataError, DatabaseError, IntegrityError, ProgrammingError
from psycopg2.errors import InsufficientPrivilege
from typing import Any
from uuid import UUID
from werkzeug.exceptions import Forbidden, NotFound
from .. import db
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
@catch_permission_denied
def store_sequence_read_set(session: DatabaseSession, document: str) -> None:
    """
    Store the given sequence read set *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "insert into receiving.sequence_read_set (document) values (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_consensus_genome(session: DatabaseSession, document: str) -> None:
    """
    Store the given consensus genome *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "insert into receiving.consensus_genome (document) values (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_redcap_det(session: DatabaseSession, document: str) -> None:
    """
    Store the given REDCap DET *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "insert into receiving.redcap_det (document) values (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def store_fhir(session: DatabaseSession, document: str) -> None:
    """
    Store the given FHIR *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDatabaseError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "insert into receiving.fhir (document) values (%s)",
                    (document,))

        except (DataError, IntegrityError) as error:
            raise BadRequestDatabaseError(error) from None


@export
@catch_permission_denied
def fetch_identifier(session: DatabaseSession, id: str) -> Any:
    """
    Fetch the identifier *id* from the backing database using *session*.

    *id* may be a full UUID or shortened barcode.

    Returns a named tuple with ``uuid``, ``barcode``, ``generated``, and
    ``set`` attributes.  If the identifier doesn't exist, raises a
    :class:`~werkzeug.exceptions.NotFound` exception.
    """
    try:
        uuid = UUID(id)
        id_field = "uuid"
    except ValueError:
        id_field = "barcode"

    with session:
        identifier = session.fetch_row(f"""
            select uuid, barcode, generated, identifier_set.name as set
              from warehouse.identifier
              join warehouse.identifier_set using (identifier_set_id)
             where {id_field} = %s
            """, (id,))

    if not identifier:
        LOG.error(f"Identifier {id_field} «{id}» not found")
        raise NotFound(f"Identifier {id_field} «{id}» not found")

    return identifier


@export
@catch_permission_denied
def fetch_identifier_sets(session: DatabaseSession) -> Any:
    """
    Fetch all identifier sets from the backing database using *session*.

    Returns a list of named tuples with ``name`` and ``description``
    attributes.
    """
    with session, session.cursor() as cursor:
        cursor.execute("""
            select name, description
              from warehouse.identifier_set
            """)

        return list(cursor)


@export
@catch_permission_denied
def fetch_identifier_set(session: DatabaseSession, name: str) -> Any:
    """
    Fetch the identifier set *name* from the backing database using *session*.

    Returns a named tuple with ``name`` and ``description`` attributes.  If the
    set doesn't exist, raises a :class:`~werkzeug.exceptions.NotFound`
    exception.
    """
    with session:
        set = session.fetch_row("""
            select name, description
              from warehouse.identifier_set
             where name = %s
            """, (name,))

    if not set:
        LOG.error(f"Identifier set «{name}» not found")
        raise NotFound(f"Identifier set «{name}» not found")

    return set


@export
@catch_permission_denied
def make_identifier_set(session: DatabaseSession, name: str, **fields) -> bool:
    """
    Create a new identifier set *name* in the backing database using *session*
    if it doesn't already exist.

    If *description* is provided as a keyword argument, its value is
    set/updated in the database.

    Returns ``True`` if the set was created or updated and ``False`` if it
    already existed.
    """
    with session, session.cursor() as cursor:
        # If I expected to have additional columns in the future, I'd build up
        # the SQL dynamically, but I don't, so a simple conditional is enough.
        #   -trs, 1 July 2021
        if "description" in fields:
            cursor.execute("""
                insert into warehouse.identifier_set (name, description)
                    values (%s, nullif(%s, ''))
                    on conflict (name) do update
                        set description = excluded.description
                """, (name, fields["description"]))

        else:
            cursor.execute("""
                insert into warehouse.identifier_set (name)
                    values (%s)
                    on conflict (name) do nothing
                """, (name,))

        return cursor.rowcount == 1


@export
@catch_permission_denied
def mint_identifiers(session: DatabaseSession, name: str, n: int) -> None:
    """
    Generate *n* new identifiers in the set *name*.

    Raises a :class:`~werkzeug.exceptions.NotFound` exception if the set *name*
    doesn't exist and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session:
        try:
            return db.mint_identifiers(session, name, n)

        except db.IdentifierSetNotFoundError as error:
            raise NotFound(str(error)) from None


@export
class BadRequestDatabaseError(BadRequest):
    """
    Subclass of :class:`id3c.api.exceptions.BadRequest` which takes a
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
