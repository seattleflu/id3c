"""
Process REDCap DET notifications.

This command group supports custom ETL routines specific to a project in
REDCap.
"""
import os
import click
import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Iterable, Optional, Tuple, Dict, List, Any
from urllib.parse import urljoin
from id3c.cli.command import with_database_session
from id3c.cli.redcap import is_complete, Project
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json, Json
from id3c.cli.command.geocode import pickled_cache
from . import etl


LOG = logging.getLogger(__name__)


# XXX FIXME: I don't think we should hardcode a cache name like this,
# particularly with a name that doesn't give any hint as to what uses it or
# what it contains. The `id3c geocode` command, for instance, explicitly
# parameterizes the cache file as an option.
#
# Going a step further, I don't think @command_for_project should even be
# providing the "cache" parameter.  What is cached and where it is stored is
# something specific to each REDCap DET routine, not a global invariant.
#   -trs, 19 Dec 2019
CACHE_FILE = 'cache.pickle'


@etl.group("redcap-det", help = __doc__)
def redcap_det():
    pass


def command_for_project(name: str,
                        redcap_url: str,
                        project_id: int,
                        revision: int,
                        required_instruments: Iterable[str] = [],
                        include_incomplete: bool = False,
                        raw_coded_values: bool = False,
                        **kwargs) -> Callable[[Callable], click.Command]:
    """
    Decorator to create REDCap DET ETL subcommands.

    The decorated function should be an ETL routine for an individual DET and
    REDCap record pair.  It must take take two dictionaries, *det* and
    *redcap_record*, as arguments.  The function must return another dictionary
    which represents a FHIR document to insert into ``receiving.fhir``.  If no
    FHIR document is appropriate, the function should return ``None``.

    *name* is the name of the ETL command, which will be invokable as ``id3c
    etl redcap-det <name>``.  *name* is also used in the processing log for
    each DET.

    *redcap_url* and *project_id* are used to select DETs for processing from
    ``receiving.redcap_det``.  They will also be used to make requests to the
    appropriate REDCap web API.

    *required_instruments* is an optional list of REDCap instrument names which
    are required for the decorated routine to run.

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
        @click.option("--log-output/--no-output",
            help        = "Write the output FHIR documents to stdout. You will likely want to redirect this to a file",
            default     = False)

        @redcap_det.command(name, **kwargs)
        @with_database_session
        @wraps(routine)

        def decorated(*args, db: DatabaseSession, log_output: bool, **kwargs):
            LOG.debug(f"Starting the REDCap DET ETL routine {name}, revision {revision}")

            # If the correct environment variables aren't defined, this will
            # throw an exception.  Check early to fail fast before we do
            # anything else.
            api_url = urljoin(redcap_url, "api/")
            api_token = get_redcap_api_token(api_url)

            redcap_det = db.cursor(f"redcap-det {name}")
            redcap_det.execute("""
                select redcap_det_id as id, document
                  from receiving.redcap_det
                 where not processing_log @> %s
                   and document::jsonb @> %s
                 order by id
                   for update
                """, (Json([etl_id]), Json(det_contains)))

            with pickled_cache(CACHE_FILE) as cache:
                latest_complete_dets: Dict[str, Any] = {}
                for det in redcap_det:
                    with db.savepoint(f"redcap_det {det.id}"):
                        instrument = det.document['instrument']
                        record_id = det.document['record']

                        # Only pull REDCap record if
                        # `include_incomplete` flag was not included and
                        # the current instrument is complete
                        if not include_incomplete and not is_complete(instrument, det.document):
                            LOG.debug(f"Skipping incomplete or unverified REDCap DET {det.id}")
                            mark_skipped(db, det.id, etl_id)
                            continue

                        # Check if this is record has an older DET
                        # Skip older DET in favor of the latest DET
                        elif latest_complete_dets.get(record_id):
                            old_det = latest_complete_dets[record_id]
                            LOG.debug(f"Skipping older REDCap DET {old_det.id}")
                            mark_skipped(db, old_det.id, etl_id)

                        latest_complete_dets[record_id] = det

                if not latest_complete_dets:
                    LOG.info("No new complete DETs found.")
                    return

                # Batch request records from REDCap
                LOG.info(f"Fetching REDCap project {project_id}")
                project = Project(api_url, api_token, project_id)
                record_ids = list(latest_complete_dets.keys())

                LOG.info(f"Fetching {len(record_ids)} REDCap records from project {project.id}")
                redcap_records = project.records(ids = record_ids, raw = raw_coded_values)

                # If no valid records are returned, mark all DETs as skipped.
                if not redcap_records:
                    skip_missing_records(db, latest_complete_dets.values(), etl_id)
                    return

                for redcap_record in redcap_records:
                    record_id = redcap_record[project.record_id_field]
                    # XXX TODO: Handle records with repeating instruments or longitudinal
                    # events.
                    try:
                        det = latest_complete_dets.pop(record_id)
                    except KeyError:
                        LOG.warning(f"Found duplicate record id «{record_id}» in project {project.id}")
                        continue

                    with db.savepoint(f"redcap_det {det.id}"):
                        LOG.info(f"Processing REDCap DET {det.id}")

                        # Only process REDCap record if all required instruments are complete
                        incomplete_instruments = {
                            instrument
                                for instrument
                                in required_instruments
                                if not is_complete(instrument, redcap_record)
                        }

                        if incomplete_instruments:
                            LOG.debug(f"The following required instruments «{incomplete_instruments}» are not yet marked complete. " + \
                                      f"Skipping REDCap DET {det.id}")
                            mark_skipped(db, det.id, etl_id)
                            continue

                        bundle = routine(db = db, cache = cache, det = det, redcap_record = redcap_record)

                        if not bundle:
                            mark_skipped(db, det.id, etl_id)
                            continue

                        if log_output:
                            print(as_json(bundle))

                        insert_fhir_bundle(db, bundle)
                        mark_loaded(db, det.id, etl_id, bundle['id'])

                # After all of REDCap records are processed, mark missing or
                # invalid DETs as skipped.
                skip_missing_records(db, latest_complete_dets.values(), etl_id)


        return decorated
    return decorator


def get_redcap_api_token(api_url: str) -> str:
    """
    Returns the authentication token configured for use with the REDCap web API
    endpoint *api_url*.

    Requires the environmental variables ``REDCAP_API_URL`` and
    ``REDCAP_API_TOKEN``.  ``REDCAP_API_URL`` must match the provided *api_url*
    as a safety check.
    """
    url = os.environ.get("REDCAP_API_URL")
    token = os.environ.get("REDCAP_API_TOKEN")

    if not url and not token:
        raise Exception(f"The environment variables REDCAP_API_URL and REDCAP_API_TOKEN are required.")
    elif not url:
        raise Exception(f"The environment variable REDCAP_API_URL is required.")
    elif not token:
        raise Exception(f"The environment variable REDCAP_API_TOKEN is required.")

    # This comparison may need URL canonicalization in the future.
    if url != api_url:
        raise Exception(f"The environment variable REDCAP_API_URL does not match the requested API endpoint «{api_url}»")

    return token


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


def skip_missing_records(db: DatabaseSession, dets: Iterable[Any], etl_id: dict) -> None:
    for det in dets:
        with db.savepoint(f"redcap_det {det.id}"):
            LOG.debug(f"REDCap record is missing or invalid.  Skipping REDCap DET {det.id}")
            mark_skipped(db, det.id, etl_id)


def mark_loaded(db: DatabaseSession, det_id: int, etl_id: dict, bundle_uuid: str) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as loaded")
    mark_processed(db, det_id, {**etl_id, "status": "loaded", "fhir_bundle_id": bundle_uuid})


def mark_skipped(db: DatabaseSession, det_id: int, etl_id: dict) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as skipped")
    mark_processed(db, det_id, {**etl_id, "status": "skipped"})


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
