"""
Process sample manifest records into the relational warehouse.

Manifest records are JSON documents in the ``receiving.manifest`` table which
have a sample barcode, an optional collection barcode, and other metadata.
These records are parsed out of spreadsheets into JSON documents using
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
from datetime import datetime, timezone
from psycopg2 import sql
from typing import Any, Tuple, Optional
from id3c.cli.command import with_database_session
from id3c.db import find_identifier, upsert_sample
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
            "collections-household-intervention-asymptomatic",
            "collections-household-observation",
            "collections-household-observation-asymptomatic",
            "collections-kiosks",
            "collections-kiosks-asymptomatic",
            "collections-seattleflu.org",
            "collections-swab&send",
            "collections-swab&send-asymptomatic",
            "collections-self-test",
            "collections-scan",
            "collections-scan-kiosks",
            "collections-haarvi",
            "samples-haarvi",
            "collections-validation",
            "collections-uw-home",
            "collections-uw-observed",
            "collections-uw-tiny-swabs",
            "collections-uw-tiny-swabs-home",
            "collections-uw-tiny-swabs-observed",
            "collections-household-general",
            "collections-childcare",
            "collections-school-testing-home",
            "collections-school-testing-observed",
            "collections-apple-respiratory",
            "collections-apple-respiratory-serial",
            "collections-adult-family-home-outbreak",
            "collections-workplace-outbreak",
            "collections-radxup-yakima-schools-home",
            "collections-radxup-yakima-schools-observed",
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

            # When updating an existing row, update the identifiers
            # only if the record has both the 'sample' and
            # 'collection' keys.
            should_update_identifiers = "sample" in manifest_record.document \
                and "collection" in manifest_record.document

             # Sample collection date
             # Don't pop this entry off the document. For backwards
             # compatibility reasons, keep it in the document so that 'date'
             # also gets written to the 'details' column in warehouse.sample.
            collected_date = manifest_record.document.get("date", None)

            # Attempt to find barcodes and their related identifiers
            sample_barcode = manifest_record.document.pop("sample", None)
            sample_identifier = find_identifier(db, sample_barcode) if sample_barcode else None
            collection_barcode = manifest_record.document.pop("collection", None)
            collection_identifier = find_identifier(db, collection_barcode) if collection_barcode else None

            # Skip a record if it has no associated barcodes
            if not sample_barcode and not collection_barcode:
                LOG.warning(f"Skipping record «{manifest_record.id}» because it has neither a sample "
                    "barcode nor a collection barcode")
                mark_skipped(db, manifest_record.id)
                continue

            # Skip a record if it has a sample barcode but the barcode doesn't match an identifier
            if sample_barcode and not sample_identifier:
                LOG.warning(f"Skipping record «{manifest_record.id}» with unknown sample barcode «{sample_barcode}»")
                mark_skipped(db, manifest_record.id)
                continue

            # Skip a record if it has a collection barcode but the barcode doesn't match an identifier
            if collection_barcode and not collection_identifier:
                LOG.warning(f"Skipping record «{manifest_record.id}» with unknown collection barcode «{collection_barcode}»")
                mark_skipped(db, manifest_record.id)
                continue

             # Skip a record if the collection identifier is from an unexpected set
            if collection_identifier and collection_identifier.set_name not in expected_identifier_sets["collections"]:
                LOG.warning(f"Skipping record «{manifest_record.id}» because collection identifier found in set «{collection_identifier.set_name}», not \
                    {expected_identifier_sets['collections']}")
                mark_skipped(db, manifest_record.id)
                continue

            # Validate the sample identifer and assert if a record fails
            if sample_identifier:
                if (manifest_record.document.get("sample_type") and
                    manifest_record.document["sample_type"] == "rdt"):
                    assert sample_identifier.set_name in expected_identifier_sets["rdt"], \
                        (f"Sample identifier found in set «{sample_identifier.set_name}»," +
                        f"not {expected_identifier_sets['rdt']}")
                else:
                    assert sample_identifier.set_name in expected_identifier_sets["samples"], \
                        (f"Sample identifier found in set «{sample_identifier.set_name}», " +
                        f"not {expected_identifier_sets['samples']}")


            # Upsert sample cooperatively with enrollments ETL routine
            #
            # The details document was intentionally modified by two pop()s
            # earlier to remove barcodes that were looked up.
            # The rationale is that we want just one clear place in the
            # warehouse for each piece of information.
            sample, status = upsert_sample(db,
                update_identifiers          = should_update_identifiers,
                identifier                  = sample_identifier.uuid if sample_identifier else None,
                collection_identifier       = collection_identifier.uuid if collection_identifier else None,
                collection_date             = collected_date,
                encounter_id                = None,
                additional_details          = manifest_record.document)

            mark_loaded(db, manifest_record.id,
                status = status,
                sample_id = sample.id)

            LOG.info(f"Finished processing manifest record {manifest_record.id}")

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
