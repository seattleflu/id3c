"""
Process REDCap DET notifications.

This command group supports custom ETL routines specific to a project in
REDCap.
"""
import os
import click
import logging
import requests
from datetime import datetime, timezone
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import etl


LOG = logging.getLogger(__name__)

COMPLETE_AND_VERIFIED = 'Complete'  # A status code from REDCap API

HEADERS = {
    'Content-type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json'
}

# This revision number is stored in the processing_log of each REDCap DET
# record when the REDCap DET record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# REDCap DET records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all REDCap DET records,
# this revision number should be incremented.
REVISION = 1


@etl.group("redcap-det", help = __doc__)
def redcap_det():
    pass


def get_redcap_record(project_id: str, record_id: str, token_name: str) -> dict:
    """
    Gets one REDCap record containing all instruments via web API based on the
    provided *project_id* and *record_id*.
    Requires an environmental variable described by *token_name* for REDCap to
    access the records from the given *project_id*.
    """
    LOG.debug(f"Getting REDCap record «{record_id}» for all instruments within project «{project_id}»")

    data = {
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'label',
        'exportCheckboxLabel': 'true',
        'token': os.environ[token_name],
        'records': record_id,
    }

    r = requests.post(os.environ['REDCAP_API_URL'], data=data, headers=HEADERS)
    if r.status_code != 200:
        raise Exception(f"REDCap returned response status code {r.status_code}")

    return r.json()[0]


def insert_fhir_bundle(db, bundle: dict) -> None:
    """
    Insert FHIR bundles into the receiving area of the database.
    """
    LOG.debug(f"Upserting FHIR bundle «{bundle['id']}»")

    fhir = db.fetch_row("""
        insert into receiving.fhir(document)
            values (%s)

        returning fhir_id as id
        """, (Json(bundle),))

    assert fhir.id, "Insert affected no rows!"

    LOG.info(f"Inserted FHIR document {fhir.id} «{bundle['id']}»")


def mark_skipped(db, det_id: int) -> None:
    LOG.debug(f"Marking REDCap DET record {det_id} as skipped")
    mark_processed(db, det_id, { "status": "skipped" })


def mark_processed(db, det_id: int, entry = {}) -> None:
    LOG.debug(f"Appending to processing log of REDCap DET record {det_id}")

    data = {
        "det_id": det_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.redcap_det
               set processing_log = processing_log || %(log_entry)s
             where redcap_det_id = %(det_id)s
            """, data)
