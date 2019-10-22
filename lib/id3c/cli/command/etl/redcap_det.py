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
from typing import Tuple
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import etl


LOG = logging.getLogger(__name__)


@etl.group("redcap-det", help = __doc__)
def redcap_det():
    pass


def get_redcap_record(record_id: str) -> dict:
    """
    Gets one REDCap record containing all instruments via web API based on the
    provided *record_id*.
    """
    LOG.debug(f"Getting REDCap record «{record_id}» for all instruments")

    url, token = get_redcap_api_credentials()

    data = {
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'label',
        'exportCheckboxLabel': 'true',
        'token': token,
        'records': record_id,
    }

    headers = {
        'Content-type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()

    return response.json()[0]


def get_redcap_api_credentials() -> Tuple[str, str]:
    """
    Returns a tuple of ``(url, token)`` for use with REDCap's web API.

    Requires the environmental variables ``REDCAP_API_URL`` and
    ``REDCAP_API_TOKEN``.
    """
    url = os.environ.get("REDCAP_API_URL")
    token = os.environ.get("REDCAP_API_TOKEN")

    if not url and not token:
        raise Exception(f"The environment variables REDCAP_API_URL and REDCAP_API_TOKEN are required.")
    elif not url:
        raise Exception(f"The environment variable REDCAP_API_URL is required.")
    elif not token:
        raise Exception(f"The environment variable REDCAP_API_TOKEN is required.")

    LOG.debug(f"REDCap endpoint is {url}")

    return url, token


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
