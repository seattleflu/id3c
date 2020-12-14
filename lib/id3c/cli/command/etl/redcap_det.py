"""
Process REDCap DET notifications.

This command group supports custom ETL routines specific to a project in
REDCap.
"""
import os
import click
import logging
import psycopg2.sql as sql
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from more_itertools import chunked
from textwrap import dedent
from typing import Callable, Iterable, Optional, Tuple, Dict, List, Any, DefaultDict
from urllib.parse import urljoin
from id3c.cli.command import with_database_session
from id3c.cli.redcap import is_complete, Project
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json, Json
from id3c.cli.command import pickled_cache
from . import etl


LOG = logging.getLogger(__name__)


@etl.group("redcap-det", help = __doc__)
def redcap_det():
    pass


def command_for_project(name: str,
                        redcap_url: str,
                        project_id: int,
                        revision: int,
                        include_incomplete: bool = False,
                        raw_coded_values: bool = False,
                        **kwargs) -> Callable[[Callable], click.Command]:
    """
    Decorator to create REDCap DET ETL subcommands.

    The decorated function should be an ETL routine for an individual DET and
    REDCap record pair.  It must accept a dictionary *det* and a list
    *redcap_record_instances* as arguments.  The function must return another dictionary
    which represents a FHIR document to insert into ``receiving.fhir``.  If no
    FHIR document is appropriate, the function should return ``None``.

    *name* is the name of the ETL command, which will be invokable as ``id3c
    etl redcap-det <name>``.  *name* is also used in the processing log for
    each DET.

    *redcap_url* and *project_id* are used to select DETs for processing from
    ``receiving.redcap_det``.  They will also be used to make requests to the
    appropriate REDCap web API.

    *include_incomplete* is a boolean specifying if the ETL should pull records
    for DETs where the instrument is marked incomplete. When false (default),
    the ETL skips DETs where the instrument is marked incomplete.

    *revision* is an integer specifying the version of the routine.  If it
    increments, previously processed DETs will be re-processed by the new
    version of the routine.

    *raw_coded_values* is a boolean specifying if raw coded values are returned
    for multiple choice answers. When false (default), the entire string labels
    are returned.
    """
    etl_id = {
        "etl": f"redcap-det {name}",
        "revision": revision,
    }

    det_contains = {
        "redcap_url": redcap_url,
        "project_id": str(project_id), # REDCap DETs send project_id as a string
    }

    def decorator(routine: Callable[..., Optional[dict]]) -> click.Command:
        @click.option("--det-limit",
            metavar = "<number>",
            help    = "Maximum number of received DETs to process in this invocation. "
                      "If more unprocessed DETs exist, they will be processed on the next invocation of this ETL. "
                      "Not limited by default.",
            type    = int)

        @click.option("--redcap-api-batch-size",
            metavar = "<number>",
            help    = "Maximum number of records to fetch in one batch from REDCap. "
                      "Multiple batches will be fetched if there are more than this number of records to fetch. "
                      "The default (5,000) is somewhat arbitrary and what will or won't strain the REDCap API depends on both the number of repeating instrument/event instances and the REDCap server itself.",
            default = 5000)

        @click.option("--log-output/--no-output",
            help        = "Write the output FHIR documents to stdout. You will likely want to redirect this to a file",
            default     = False)

        # XXX FIXME: I don't think @command_for_project should even be
        # providing the "cache" parameter.  What is cached and where it is stored is
        # something specific to each REDCap DET routine, not a global invariant.
        #   -trs, 19 Dec 2019
        @click.option("--geocoding-cache",
            metavar = "<cache.pickle>",
            envvar = "GEOCODING_CACHE",
            help = "Local file for caching the results of address geocoding. Geocoding lookups will not be cached otherwise.",
            required = False,
            type = click.Path(dir_okay=False, writable=True))

        @redcap_det.command(name, **kwargs)
        @with_database_session
        @wraps(routine)

        def decorated(*args, db: DatabaseSession, log_output: bool, det_limit: int = None, redcap_api_batch_size: int, geocoding_cache: str = None, **kwargs):
            LOG.debug(f"Starting the REDCap DET ETL routine {name}, revision {revision}")

            project = Project(redcap_url, project_id)

            if det_limit:
                LOG.debug(f"Processing up to {det_limit:,} pending DETs")
                limit = sql.Literal(det_limit)
            else:
                LOG.debug(f"Processing all pending DETs")
                limit = sql.SQL("all")

            redcap_det = db.cursor(f"redcap-det {name}")
            redcap_det.execute(sql.SQL("""
                select redcap_det_id as id, document
                  from receiving.redcap_det
                 where not processing_log @> %s
                   and document::jsonb @> %s
                 order by id
                 limit {}
                   for update
                """).format(limit), (Json([etl_id]), Json(det_contains)))

            # First loop of the DETs to determine how to process each one.
            # Uses `first_complete_dets` to keep track of which DET to
            # use to process a unique REDCap record.
            # Uses `all_dets` to keep track of the status for each DET record
            # so that they can be processed in order of `redcap_det_id` later.
            #   --Jover, 21 May 2020
            first_complete_dets: Dict[str, Any] = {}
            all_dets: List[Dict[str, str]] = []
            for det in redcap_det:
                instrument = det.document['instrument']
                record_id = det.document['record']
                # Assume we are loading all DETs
                # Status will be updated to "skip" if DET does not need to be processed
                det_record = { "id": det.id, "status": "load" }

                # Only pull REDCap record if
                # `include_incomplete` flag was not included and
                # the current instrument is complete
                if not include_incomplete and not is_complete(instrument, det.document):
                   det_record.update({
                       "status": "skip",
                       "reason": "incomplete/unverified DET"
                   })

                # Check if this is record has an older DET
                # Skip latest DET in favor of the first DET
                # This is done to continue our first-in-first-out
                # semantics of our receiving tables
                elif first_complete_dets.get(record_id):
                    det_record.update({
                        "status": "skip",
                        "reason": "repeat REDCap record"
                    })

                else:
                    first_complete_dets[record_id] = det
                    det_record["record_id"] = record_id

                all_dets.append(det_record)

            if not first_complete_dets:
                LOG.info("No new complete DETs found.")
            else:
                # Batch request records from REDCap
                LOG.info(f"Fetching REDCap project {project_id}")
                record_ids = list(first_complete_dets.keys())

                LOG.info(f"Fetching {len(record_ids):,} REDCap records from project {project.id}")

                # Convert list of REDCap records to a dict so that
                # records can be looked up by record id.
                # Records with repeating instruments or longitudinal
                # events will have multiple entries in the list.
                redcap_records: DefaultDict[str, List[dict]] = defaultdict(list)

                batches = list(chunked(record_ids, redcap_api_batch_size))

                for i, batch in enumerate(batches, 1):
                    LOG.info(f"Fetching REDCap record batch {i:,}/{len(batches):,} of size {len(batch):,}")

                    for record in project.records(ids = batch, raw = raw_coded_values):
                        redcap_records[record.id].append(record)

            # Process all DETs in order of redcap_det_id
            with pickled_cache(geocoding_cache) as cache:
                for det in all_dets:
                    with db.savepoint(f"redcap_det {det['id']}"):
                        LOG.info(f"Processing REDCap DET {det['id']}")

                        if det["status"] == "skip":
                            LOG.debug(f"Skipping REDCap DET {det['id']} due to {det['reason']}")
                            mark_skipped(db, det["id"], etl_id, det["reason"])
                            continue

                        received_det = first_complete_dets.pop(det["record_id"])
                        redcap_record_instances = redcap_records.get(received_det.document["record"])

                        if not redcap_record_instances:
                            LOG.debug(f"REDCap record is missing or invalid.  Skipping REDCap DET {received_det.id}")
                            mark_skipped(db, received_det.id, etl_id, "invalid REDCap record")
                            continue

                        bundle = routine(db = db, cache = cache, det = received_det, redcap_record_instances = redcap_record_instances)

                        if not bundle:
                            LOG.debug(f"Skipping REDCap DET {received_det.id} due to insufficient data in REDCap record.")
                            mark_skipped(db, received_det.id, etl_id, "insufficient data in record")
                            continue

                        if log_output:
                            print(as_json(bundle))

                        insert_fhir_bundle(db, bundle)
                        mark_loaded(db, received_det.id, etl_id, bundle['id'])

        return decorated
    return decorator


def insert_fhir_bundle(db: DatabaseSession, bundle: dict) -> None:
    """
    Insert FHIR bundles into the receiving area of the database.
    """
    LOG.debug(f"Inserting FHIR bundle «{bundle['id']}»")

    fhir = db.fetch_row("""
        insert into receiving.fhir(document)
            values (%s)

        returning fhir_id as id
        """, (Json(bundle),))

    assert fhir.id, "Insert affected no rows!"

    LOG.info(f"Inserted FHIR document {fhir.id} «{bundle['id']}»")


def mark_loaded(db: DatabaseSession, det_id: int, etl_id: dict, bundle_uuid: str) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as loaded")
    mark_processed(db, det_id, {**etl_id, "status": "loaded", "fhir_bundle_id": bundle_uuid})


def mark_skipped(db: DatabaseSession, det_id: int, etl_id: dict, reason: str) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as skipped")
    mark_processed(db, det_id, {**etl_id, "status": "skipped", "skip_reason": reason})


def mark_processed(db: DatabaseSession, det_id: int, entry = {}) -> None:
    LOG.debug(f"Appending to processing log of REDCap DET record {det_id}")

    data = {
        "det_id": det_id,
        "log_entry": Json({
            **entry,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.redcap_det
               set processing_log = processing_log || %(log_entry)s
             where redcap_det_id = %(det_id)s
            """, data)
