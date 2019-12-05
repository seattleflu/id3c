"""
Process REDCap DET notifications.

This command group supports custom ETL routines specific to a project in
REDCap.
"""
import os
import json
import click
import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Iterable, Optional, Tuple
from urllib.parse import urljoin
from id3c.cli.command import with_database_session
from id3c.cli.redcap import CachedProject, is_complete
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.cli.command.geocode import pickled_cache
from . import etl


LOG = logging.getLogger(__name__)


CACHE_FILE = 'cache.pickle'


@etl.group("redcap-det", help = __doc__)
def redcap_det():
    pass


def command_for_project(name: str,
                        redcap_url: str,
                        project_id: int,
                        revision: int,
                        required_instruments: Iterable[str] = [],
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
        @click.option("--indent",
            help        = "The number of spaces used to indent the output file."
                          "If the value is 0 (the default value), prints one FHIR document per line.",
            default     = 0)

        @redcap_det.command(name, **kwargs)
        @with_database_session
        @wraps(routine)

        def decorated(*args, db: DatabaseSession, log_output: bool, indent: int, **kwargs):
            LOG.debug(f"Starting the REDCap DET ETL routine {name}, revision {revision}")

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
                for det in redcap_det:
                    with db.savepoint(f"redcap_det {det.id}"):
                        LOG.info(f"Processing REDCap DET {det.id}")

                        instrument = det.document['instrument']

                        # Only pull REDCap record if the current instrument is complete
                        if not is_complete(instrument, det.document):
                            LOG.debug(f"Skipping incomplete or unverified REDCap DET {det.id}")
                            mark_skipped(db, det.id, etl_id)
                            continue

                        redcap_record = get_redcap_record_from_det(det.document)

                        if not redcap_record:
                            LOG.debug(f"REDCap record is missing or invalid.  Skipping REDCap DET {det.id}")
                            mark_skipped(db, det.id, etl_id)
                            continue

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

                        if log_output:
                            if indent:
                                print(json.dumps(bundle, indent=indent))
                            else:
                                print(json.dumps(bundle))

                        if bundle:
                            insert_fhir_bundle(db, bundle)
                            mark_loaded(db, det.id, etl_id)
                        else:
                            mark_skipped(db, det.id, etl_id)

        return decorated
    return decorator


def get_redcap_record_from_det(det: dict) -> Optional[dict]:
    """
    Fetch the REDCap record for the given *det* notification.

    The DET's ``redcap_url``, ``project_id``, and ``record`` fields are used to
    make the API call.

    All instruments will be fetched.
    """
    api_url = urljoin(det["redcap_url"], "api/")
    api_token = get_redcap_api_token(api_url)

    project_id = int(det["project_id"])

    try:
        record_id = int(det["record"])
    except ValueError:
        return None

    LOG.info(f"Fetching REDCap record {record_id}")

    project = CachedProject(api_url, api_token, project_id)
    record = project.record(record_id)

    # XXX TODO: Handle records with repeating instruments or longitudinal
    # events.
    return record[0] if record else None


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


def mark_loaded(db: DatabaseSession, det_id: int, etl_id: dict) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as loaded")
    mark_processed(db, det_id, {**etl_id, "status": "loaded"})


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
