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

    Returns a named tuple with ``uuid``, ``barcode``, ``generated``, ``set``,
    and ``use`` attributes.  If the identifier doesn't exist, raises a
    :class:`~werkzeug.exceptions.NotFound` exception.
    """
    try:
        uuid = UUID(id)
        id_field = "uuid"
    except ValueError:
        id_field = "barcode"

    with session:
        identifier = session.fetch_row(f"""
            select uuid, barcode, generated, identifier_set.name as set, identifier_set.use
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

    Returns a list of named tuples with ``name``, ``description``, and ``use``
    attributes.
    """
    with session, session.cursor() as cursor:
        cursor.execute("""
            select name, description, use
              from warehouse.identifier_set
            """)

        return list(cursor)


@export
@catch_permission_denied
def fetch_identifier_set(session: DatabaseSession, name: str) -> Any:
    """
    Fetch the identifier set *name* from the backing database using *session*.

    Returns a named tuple with ``name``, ``description``, and ``use`` attributes.
    If the set doesn't exist, raises a :class:`~werkzeug.exceptions.NotFound`
    exception.
    """
    with session:
        set = session.fetch_row("""
            select name, description, use
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
    if it doesn't already exist, or update if it does exist.

    If *use* and/or *description* are provided as keyword arguments, their values
    are set in the database. Becuase *use* is a required field in the target table,
    if it is not provided as a keyword argument the query will attempt to retrieve
    its value from an existing record.

    Returns ``True`` if the set was created or updated and ``False`` if it
    already existed and was not updated.

    Raises a :class:`BadRequestDatabaseError` exception if the database reports a
    `constraint` error and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        if "use" in fields and "description" in fields:
            try:
                cursor.execute("""
                    insert into warehouse.identifier_set (name, use, description)
                        values (%s, %s, nullif(%s, ''))
                        on conflict (name) do update
                            set use = excluded.use,
                                description = excluded.description
                            where identifier_set.use <> excluded.use
                                or coalesce(identifier_set.description,'') <> coalesce(excluded.description,'')
                    """, (name, fields["use"], fields["description"]))
            except (DataError, IntegrityError) as error:
                raise BadRequestDatabaseError(error) from None
        elif "use" in fields:
            try:
                cursor.execute("""
                    insert into warehouse.identifier_set (name, use)
                        values (%s, %s)
                        on conflict (name) do update
                            set use = excluded.use
                            where identifier_set.use <> excluded.use
                    """, (name, fields["use"]))
            except (DataError, IntegrityError) as error:
                raise BadRequestDatabaseError(error) from None
        elif "description" in fields:
            try:
                cursor.execute("""
                    insert into warehouse.identifier_set (name, use, description)
                        select s.name, t.use, s.description 
                        from (values(%s, nullif(%s,''))) s(name, description)
                        left join (
                            select name, use 
                            FROM warehouse.identifier_set WHERE name = %s
                        ) t using (name)
                        on conflict (name) do update
                            set use = excluded.use, description = excluded.description
                            where identifier_set.use <> excluded.use
                            or coalesce(identifier_set.description,'') <> coalesce(excluded.description,'')
                    """, (name, fields["description"], name))
            except (DataError, IntegrityError) as error:
                raise BadRequestDatabaseError(error) from None
        else:
            try:
                cursor.execute("""
                    insert into warehouse.identifier_set (name, use)
                        select s.name, t.use
                        from (values(%s)) s(name)
                        left join (
                            select name, use 
                            FROM warehouse.identifier_set WHERE name = %s
                        ) t using (name)
                        on conflict (name) do update
                            set use = excluded.use
                            where identifier_set.use <> excluded.use
                    """, (name, name))
            except (DataError, IntegrityError) as error:
                raise BadRequestDatabaseError(error) from None

        return cursor.rowcount == 1

@export
@catch_permission_denied
def fetch_identifier_set_uses(session: DatabaseSession) -> Any:
    """
    Fetch all identifier set uses from the backing database using *session*.

    Returns a list of named tuples with ``use`` and ``description`` attributes.
    """
    with session, session.cursor() as cursor:
        cursor.execute("""
            select use, description
              from warehouse.identifier_set_use
            """)

        return list(cursor)

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
