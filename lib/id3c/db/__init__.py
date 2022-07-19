"""
Database interfaces
"""
import logging
import secrets
import statistics
from datetime import datetime
from psycopg2 import IntegrityError
from psycopg2.errors import ExclusionViolation
from psycopg2.sql import SQL, Identifier, Literal
from statistics import median, StatisticsError
from typing import Any, Dict, Iterable, List, Tuple, NamedTuple, Optional
from .types import IdentifierRecord
from .session import DatabaseSession
from .datatypes import Json

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


def mint_identifiers(session: DatabaseSession, name: str, n: int) -> Any:
    """
    Generate *n* new identifiers in the set *name*.

    Raises a :class:`~werkzeug.exceptions.NotFound` exception if the set *name*
    doesn't exist and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    minted: List[Any] = []

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

    minted = session.fetch_all("""
        select uuid, barcode, identifier_set_id, generated
        from mint_identifiers(%s, %s)
        """, (identifier_set.id, n))

    LOG.debug(f"finished minting ")

    failure_count = 0

    for notice in session.connection.notices:
        if "failure_count:" in notice:
            try:
                failure_count = int(notice.split(':')[-1])
            except:
                pass
        LOG.debug(f"{notice}")


    duration = datetime.now() - started
    per_second = n / duration.total_seconds()
    per_identifier = duration.total_seconds() / n

    LOG.info(f"Minted {n} identifiers in {n + failure_count} tries ({failure_count} retries) over {duration} ({per_identifier:.2f} s/identifier = {per_second:.2f} identifiers/s)")

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
               identifier_set.name as set_name,
               identifier_set.use as set_use
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

def upsert_sample(db: DatabaseSession,
                  update_identifiers: bool,
                  overwrite_collection_date: bool,
                  identifier: Optional[str],
                  collection_identifier: Optional[str],
                  collection_date: Optional[str],
                  encounter_id: Optional[int],
                  additional_details: dict) -> Tuple[Any, str]:
    """
    Upsert sample by its *identifier* and/or *collection_identifier*.
    An existing sample has its *identifier*, *collection_identifier*,
    *collection_date* updated, and the provided *additional_details* are
    merged (at the top-level only) into the existing sample details, if any.
    Raises an exception if there is more than one matching sample.
    """
    data = {
        "identifier": identifier,
        "collection_identifier": collection_identifier,
        "collection_date": collection_date,
        "encounter_id": encounter_id,
        "additional_details": Json(additional_details) if additional_details else None,
    }

    # Look for existing sample(s)
    with db.cursor() as cursor:
        cursor.execute("""
            select
                sample_id as id, identifier, collection_identifier, encounter_id, details,
                row (
                    identifier,
                    collection_identifier
                )::text !=
                row (
                    %(identifier)s,
                    %(collection_identifier)s
                )::text as identifiers_changed,
                row(
                    collected::timestamp,
                    encounter_id,
                    details
                )::text !=
                row(
                    coalesce(%(collection_date)s, collected)::timestamp,
                    coalesce(%(encounter_id)s::integer, encounter_id),
                    coalesce(details, '{}'::jsonb) || coalesce(%(additional_details)s, '{}')::jsonb
                )::text as metadata_changed
            from warehouse.sample
            where identifier = %(identifier)s
                or collection_identifier = %(collection_identifier)s
               for update
            """, data)

        samples = list(cursor)

    # Nothing found → create
    if not samples:
        LOG.info("Creating new sample")
        status = 'created'

        sample = db.fetch_row("""
            insert into warehouse.sample (identifier, collection_identifier, collected, encounter_id, details)
                values (%(identifier)s,
                        %(collection_identifier)s,
                        date_or_null(%(collection_date)s),
                        %(encounter_id)s,
                        %(additional_details)s)
            returning sample_id as id, identifier, collection_identifier, encounter_id
            """, data)

    # One found → update
    elif len(samples) == 1:
        status = 'updated'
        sample = samples[0]

        LOG.info(f"Updating existing sample {sample.id}")
        LOG.info(f"Sample.identifiers_changed is «{sample.identifiers_changed}» ")
        LOG.info(f"Sample.metadata_changed is «{sample.metadata_changed}» ")

        # can safely skip upsert if metadata is unchanged and not updating identifiers or if all data is unchanged
        if sample.metadata_changed == False and (not update_identifiers or sample.identifiers_changed == False):
            LOG.info(f"Skipping upsert for sample {sample.id} «{sample.identifier}» (no change).")
            return sample, status

        # Log when critical fields are changed to a different value, for manual followup as needed
        if encounter_id and sample.encounter_id and encounter_id != sample.encounter_id:
            LOG.debug(f"upsert_sample: encounter_id is changing on sample {sample.id} from {sample.encounter_id} to {encounter_id}")

        if identifier and sample.identifier and identifier != sample.identifier:
            LOG.warning(f"upsert_sample: identifier is changing on sample {sample.id} from {sample.identifier} to {identifier}")

        if collection_identifier and sample.collection_identifier and collection_identifier != sample.collection_identifier:
            LOG.warning(f"upsert_sample: collection_identifier is changing on sample {sample.id} from {sample.collection_identifier} to {collection_identifier}")

        if sample.identifiers_changed == False and update_identifiers:
            LOG.warning(f"upsert_sample: updating identifiers on sample {sample.id} with only one provided. Incoming identifiers are collection_identifier: {collection_identifier}, sample_identifier: {identifier}")

        # Update identifier and collection_identifier if update_identifiers is True
        identifiers_update_composable = SQL("""
             identifier = %(identifier)s,
                collection_identifier = %(collection_identifier)s, """)  \
                    if update_identifiers else SQL("")

        collected_update_composable = SQL("""
             collected = coalesce(date_or_null(%(collection_date)s), collected), """) \
                 if overwrite_collection_date else SQL("""
                    collected = coalesce(collected, date_or_null(%(collection_date)s)), """)

        sample = db.fetch_row(SQL("""
            update warehouse.sample
                set {}
                    {}
                    encounter_id = coalesce(%(encounter_id)s, encounter_id),
                    details = coalesce(details, {}) || %(additional_details)s
             where sample_id = %(sample_id)s
            returning sample_id as id, identifier, collection_identifier, encounter_id
            """).format(identifiers_update_composable,
                collected_update_composable,
                Literal(Json({}))),
                { **data, "sample_id": sample.id })

        assert sample.id, "Update affected no rows!"

    # More than one found → error
    else:
        raise Exception(f"More than one sample matching sample and/or collection barcodes: {samples}")

    if sample:
        if identifier:
            LOG.info(f"Upserted sample {sample.id} with identifier «{sample.identifier}»")
        elif collection_identifier:
            LOG.info(f"Upserted sample {sample.id} with collection identifier «{sample.collection_identifier}»")

    return sample, status
