"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from functools import wraps
from psycopg2 import DataError, DatabaseError, IntegrityError, ProgrammingError, sql
from psycopg2.errors import InsufficientPrivilege
from typing import Any
from uuid import UUID
from werkzeug.exceptions import Forbidden, NotFound, Conflict
from .. import db
from ..db import find_identifier, upsert_sample
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
def verify_barcode_use_list(session: DatabaseSession, barcode_use_list: list) -> Any:
    """
    Check the given *barcode_use_list* containing objects with ``barcode`` and ``use``
    keys and values to verify that each barcode exists in the backing database and that the
    given use matches the stored use.

    Returns a list of objects in the same order as the input, with each object including the
    ``barcode`` (string) and ``use`` (string) being verified, ``barcode_found`` (boolean)
    indicating whether the given barcode exists, and ``use_match`` (boolean) indicating whether
    the given use matches the stored use. The ``use_match`` value will be `null` if the barcode
    does not exist.
    """
    barcode_use_tuples = [(bu["barcode"],bu["use"]) for bu in barcode_use_list]
    args_str = ','.join(['%s'] * len(barcode_use_tuples))
    sql = "select q.barcode, q.use, \
            case \
                when identifier.barcode is not null then true else false \
            end as barcode_found, \
            case \
                when identifier_set.use IS NULL then null \
                when q.use::citext=identifier_set.use then true \
                else false \
            end as use_match \
            from (values {}) as q (barcode, use) \
            left join warehouse.identifier on q.barcode::citext = identifier.barcode \
            left join warehouse.identifier_set using (identifier_set_id)".format(args_str)

    result = session.fetch_all(sql, tuple(barcode_use_tuples))
    return result
    

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
@catch_permission_denied
def get_sample(session: DatabaseSession, barcode: str) -> Any:
    """
    Fetch the sample with identifier or collection identifier matching *barcode* from the backing
    database using *session*.

    Returns a named tuple with ``identifier``, ``collection_identifier``, ``encounter_id``,
    ``details``, ``created``, ``modified``, and ``collected`` attributes.
    
    If the identifier barcode or sample doesn't exist, raises a :class:`~werkzeug.exceptions.NotFound`
    exception. If the identifier barcode exists but is not from a sample or collection identifier set,
    raises a :class:`~werkzeug.exceptions.Conflict` exception.
    """

    with session:
        sample_identifier = find_identifier(session, barcode) or None

        if not sample_identifier:
            LOG.error(f"Identifier barcode «{barcode}» not found")
            raise NotFound(f"Identifier barcode «{barcode}» not found")
        elif sample_identifier.set_use=='sample':
            identifier_field = sql.Identifier('identifier')
        elif sample_identifier.set_use=='collection':
            identifier_field = sql.Identifier('collection_identifier')
        else:
            error_msg = f"Identifier barcode «{barcode}» has use type «{sample_identifier.set_use}» instead of expected use type «sample» or «collection»"
            LOG.error(error_msg)
            raise Conflict(error_msg)
            
        query = sql.SQL("""
            select identifier, collection_identifier, encounter_id, 
            details, created::text, modified::text, collected::text
            from warehouse.sample
            where {field} = %s
            """).format(field=identifier_field)

        sample = session.fetch_row(query, (sample_identifier.uuid,))
    
    if not sample:
        raise NotFound(f"Sample record with {sample_identifier.set_use} identifier barcode «{barcode}» not found")
    else:
        return sample

@export
@catch_permission_denied
def store_sample(session: DatabaseSession, sample: dict) -> Any:
    """"
    Validate the given *sample* and insert or update in the backing database.

    Returns a list of in the same order as the input, with each object including
    the ``sample_id`` (string), ``status`` (string) to indicate if inserted,
    updated, or validation failed, and ``details`` to indicate reason for
    failed validation.
    """
    with session:
        sample_barcode = sample.pop("sample_id", None)
        sample_identifier = find_identifier(session, sample_barcode) if sample_barcode else None
        collection_barcode = sample.pop("collection_id", None) 
        collection_identifier = find_identifier(session, collection_barcode) if collection_barcode else None
        
        result = {
            "sample_barcode": sample_barcode,
            "collection_barcode": collection_barcode
        }

        # validate barcodes
        if sample_barcode and not sample_identifier:
            result["status"] = "validation_failed"
            result["details"] = f"sample barcode «{sample_barcode}» not found"
        elif sample_identifier and sample_identifier.set_use != 'sample':
            result["status"] = "validation_failed"
            result["details"] = f"barcode «{sample_barcode}» has use type «{sample_identifier.set_use}» instead of expected use type «sample»"
        elif collection_barcode and not collection_identifier:
            result["status"] = "validation_failed"
            result["details"] = f"collection barcode «{collection_barcode}» not found"
        elif collection_identifier and collection_identifier.set_use != 'collection':
            result["status"] = "validation_failed"
            result["details"] = f"barcode «{collection_barcode}» has use type «{collection_identifier.set_use}» instead of expected use type «collection»"

        if result.get("status", None) == "validation_failed":
            LOG.debug(f"Validation failed for {sample} with details: {result.get('details')}")
            return result

        collected_date = sample.pop("collection_date", None)
        
        # Add date to sample so that it gets written to the 'details' column in warehouse.sample
        if collected_date:
            sample["date"] = collected_date
        
        # Rename specific properties to include in 'details' column in warehouse.sample
        if "clia_id" in sample:
            sample["clia_barcode"] = sample.pop("clia_id")
        if "aliquoted_date" in sample:
            sample["aliquot_date"] = sample.pop("aliquoted_date")
        if "received_date" in sample:
            sample["arrival_date"] = sample.pop("received_date")

        # When updating an existing row, update the identifiers only if the record has both
        # the 'sample_barcode' and 'collection_barcode' keys
        should_update_identifiers = True if (sample_identifier and collection_identifier) else False

        try:
            sample, status = upsert_sample(session,
                    update_identifiers          = should_update_identifiers,
                    overwrite_collection_date   = True,
                    identifier                  = sample_identifier.uuid if sample_identifier else None,
                    collection_identifier       = collection_identifier.uuid if collection_identifier else None,
                    collection_date             = collected_date,
                    encounter_id                = None,
                    additional_details          = sample)

            result["sample"] = sample
            result["status"] = status
        except Exception as e:
            result["status"] = "upsert_error"
            result["details"] = f"error upserting sample record: {str(e)}"
            LOG.debug(f"Error on upsert_sample: {str(e)}")

        return result

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
