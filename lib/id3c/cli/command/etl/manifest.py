"""
Process sample manifest records into the relational warehouse.

Manifest records are JSON documents in the ``receiving.manifest`` table which
have a sample barcode, an optional collection barcode, and other metadata.
These records are parsed out of Excel workbooks into JSON documents using
``id3c manifest parse`` and received via ``id3c manifest upload``.  This ETL
command is the final step in creating/updating the samples in our data
warehouse.

Three separate, loosely-coupled commands let us build a pipeline to handle
different kinds of manifests with different data update strategies.  For
example, we can track the output of preparing the manifest and only receive new
or updated records.
"""
import click
import logging
import re
from datetime import datetime, timezone
from typing import Any, Tuple, Optional
from id3c.cli.command import with_database_session
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import etl


LOG = logging.getLogger(__name__)


# The revision number and etl name are stored in the processing_log of each
# manifest record when the manifest record is successfully processed
# or skipped by this ETL routine. The routine finds new-to-it records
# to process by looking for those lacking this etl revision number and etl name
# in their log.  If a change to the ETL routine necessitates
# re-processing all manifest records, this revision number should be
# incremented.
# The etl name has been added to allow multiple etls to process the same
# receiving table.
REVISION = 1
ETL_NAME = "manifest"


@etl.command("manifest", help = __doc__)
@with_database_session

def etl_manifest(*, db: DatabaseSession):
    LOG.debug(f"Starting the manifest ETL routine, revision {REVISION}")

    # XXX TODO: Stop hardcoding valid identifier sets.  Instead, accept them as
    # an option or config (and validate option choices against what's actually
    # in the database).  We won't want to validate using click.option(),
    # because that would necessitate a database connection simply to run
    # bin/id3c at all.
    #   -trs, 13 May 2019
    expected_identifier_sets = {
        "samples": {"samples"},
        "collections": {
            "collections-environmental",
            "collections-fluathome.org",
            "collections-household-intervention",
            "collections-household-observation",
            "collections-kiosks",
            "collections-seattleflu.org",
            "collections-swab&send",
        },
        "rdt": {"collections-fluathome.org"}
    }

    # Fetch and iterate over samples that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same samples.
    LOG.debug("Fetching unprocessed manifest records")

    manifest = db.cursor("manifest")
    manifest.execute("""
        select manifest_id as id, document
          from receiving.manifest
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "etl": ETL_NAME, "revision": REVISION }]),))

    for manifest_record in manifest:
        with db.savepoint(f"manifest record {manifest_record.id}"):
            LOG.info(f"Processing record {manifest_record.id}")

            # Convert sample barcode to full identifier, ensuring it's
            # known and from the correct identifier set.
            sample_barcode = manifest_record.document.pop("sample")
            sample_identifier = find_identifier(db, sample_barcode)

            if not sample_identifier:
                LOG.warning(f"Skipping sample with unknown sample barcode «{sample_barcode}»")
                mark_skipped(db, manifest_record.id)
                continue

            if (manifest_record.document.get("sample_type") and
                manifest_record.document["sample_type"] == "rdt"):
                assert sample_identifier.set_name in expected_identifier_sets["rdt"], \
                    (f"Sample identifier found in set «{sample_identifier.set_name}», " +
                    f"not {expected_identifier_sets['rdt']}")
            else:
                assert sample_identifier.set_name in expected_identifier_sets["samples"], \
                    (f"Sample identifier found in set «{sample_identifier.set_name}», " +
                    f"not {expected_identifier_sets['samples']}")

            # Optionally, convert the collection barcode to full
            # identifier, ensuring it's known and from the correct
            # identifier set.
            collection_barcode = manifest_record.document.pop("collection", None)
            collection_identifier = find_identifier(db, collection_barcode) if collection_barcode else None

            sample_origin = manifest_record.document["sample_origin"]
            # Only UW retrospectives do not have collection barcodes, prevent
            # ingestion of other samples that do not have collection barcode yet
            if not collection_barcode and not re.match("(uwmc|nwh|hmc)_retro", sample_origin):
                LOG.info(f"Skipping sample that is not a UW retrospective sample that does not have a collection barcode.")
                mark_skipped(db, manifest_record.id)
                continue

            if collection_barcode and not collection_identifier:
                LOG.warning(f"Skipping sample with unknown collection barcode «{collection_barcode}»")
                mark_skipped(db, manifest_record.id)
                continue

            assert not collection_identifier \
                or collection_identifier.set_name in expected_identifier_sets["collections"], \
                    f"Collection identifier found in set «{collection_identifier.set_name}», not {expected_identifier_sets['collections']}" # type: ignore

            # Upsert sample cooperatively with enrollments ETL routine
            #
            # The details document was intentionally modified by two pop()s
            # earlier to remove barcodes that were looked up.  The
            # rationale is that we want just one clear place in the
            # warehouse for each piece of information.
            sample, status = upsert_sample(db,
                identifier            = sample_identifier.uuid,
                collection_identifier = collection_identifier.uuid if collection_identifier else None,
                additional_details    = manifest_record.document)

            mark_loaded(db, manifest_record.id,
                status = status,
                sample_id = sample.id)

            LOG.info(f"Finished processing manifest record {manifest_record.id}")


def upsert_sample(db: DatabaseSession,
                  identifier: str,
                  collection_identifier: Optional[str],
                  additional_details: dict) -> Tuple[Any, str]:
    """
    Upsert sample by its *identifier* and/or *collection_identifier*.

    An existing sample has its *identifier* and *collection_identifier* updated,
    and the provided *additional_details* are merged (at the top-level only)
    into the existing sample details, if any.

    Raises an exception if there is more than one matching sample.
    """
    data = {
        "identifier": identifier,
        "collection_identifier": collection_identifier,
        "additional_details": Json(additional_details),
    }

    # Look for existing sample(s)
    with db.cursor() as cursor:
        cursor.execute("""
            select sample_id as id, identifier, collection_identifier, encounter_id
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
            insert into warehouse.sample (identifier, collection_identifier, details)
                values (%(identifier)s,
                        %(collection_identifier)s,
                        %(additional_details)s)
            returning sample_id as id, identifier, collection_identifier, encounter_id
            """, data)

    # One found → update
    elif len(samples) == 1:
        status = 'updated'
        sample = samples[0]

        LOG.info(f"Updating existing sample {sample.id}")
        sample = db.fetch_row("""
            update warehouse.sample
               set identifier = %(identifier)s,
                   collection_identifier = %(collection_identifier)s,
                   details = coalesce(details, '{}') || %(additional_details)s

             where sample_id = %(sample_id)s

            returning sample_id as id, identifier, collection_identifier, encounter_id
            """,
            { **data, "sample_id": sample.id })

        assert sample.id, "Update affected no rows!"

    # More than one found → error
    else:
        raise Exception(f"More than one sample matching sample and/or collection barcodes: {samples}")

    return sample, status


def mark_loaded(db, manifest_id: int, status: str, sample_id: int) -> None:
    LOG.debug(f"Marking sample manifest record {manifest_id} as loaded")
    mark_processed(db, manifest_id, { "status": status, "sample_id": sample_id })


def mark_skipped(db, manifest_id: int) -> None:
    LOG.debug(f"Marking sample manifest record {manifest_id} as skipped")
    mark_processed(db, manifest_id, { "status": "skipped" })


def mark_processed(db, manifest_id: int, entry = {}) -> None:
    LOG.debug(f"Appending to processing log of sample manifest record {manifest_id}")

    data = {
        "manifest_id": manifest_id,
        "log_entry": Json({
            **entry,
            "etl": ETL_NAME,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.manifest
               set processing_log = processing_log || %(log_entry)s
             where manifest_id = %(manifest_id)s
            """, data)
