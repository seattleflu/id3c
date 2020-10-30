"""
Utilities for interacting with Google services.
"""
from enum import Enum
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse
import re


# See a full list of Google Drive MIME types at
# https://developers.google.com/drive/api/v3/ref-export-formats

class GoogleDriveExportFormat(Enum):
    CSV = "CSV"
    EXCEL = "Excel"
    HTML = "HTML"
    HTML_ZIPPED = "HTML zipped"
    PDF = "PDF"
    PLAIN_TEXT = "Plain text"
    RICH_TEXT = "Rich text"
    WORD = "Word"

GOOGLE_FORMAT_MIME_MAP = {
    GoogleDriveExportFormat.CSV: "text/csv",
    GoogleDriveExportFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    GoogleDriveExportFormat.HTML: "text/html",
    GoogleDriveExportFormat.HTML_ZIPPED: "application/zip",
    GoogleDriveExportFormat.PDF: "application/pdf",
    GoogleDriveExportFormat.PLAIN_TEXT: "text/plain",
    GoogleDriveExportFormat.RICH_TEXT: "application/rtf",
    GoogleDriveExportFormat.WORD: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
}

def export_file_from_google_drive(document_id: str, destination_format: GoogleDriveExportFormat) -> BytesIO:
    """
    From Google Drive export the specified document in the specified MIME format.
    Requires that the GOOGLE_APPLICATION_CREDENTIALS environment variable be set
    pointing to the service account credentials file and that the service account have
    read access to the document being exported.

    Not every type of document can be exported as every GoogleDriveExportFormat type.

    If you export a Google Sheets spreadsheet containing multiple sheets as a CSV,
    it's the first sheet that will be exported.
    """
    mime_type = GOOGLE_FORMAT_MIME_MAP[destination_format]

    drive_service = discovery.build("drive", "v2")
    request = drive_service.files().export_media(fileId=document_id, mimeType=mime_type)
    buffer = BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    # Move the buffer to the beginning so the consumer can call read()
    buffer.seek(0)
    return buffer

def extract_document_id_from_google_url(url_str: str) -> Optional[str]:
    """
    Determines whether the input url is to a Google Documents resource at
    docs.google.com and if so extracts the document ID. Document types include
    Google Docs, Google Sheets, and Google Slides.

    >>> extract_document_id_from_google_url('https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0')
    '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'

    >>> extract_document_id_from_google_url('s3://my-s3-bucket/filepath.xlsx')
    """

    url = urlparse(url_str)

    if url.hostname != "docs.google.com":
        return None

    google_docs_pattern = re.compile("^/[^/]+/d/(?P<document_id>[a-zA-Z0-9-_]+)", flags=re.IGNORECASE)
    google_docs_matches = re.match(google_docs_pattern, url.path)

    return google_docs_matches["document_id"] if google_docs_matches else None

def get_document_details(document_id: str) -> dict:
    """
    Returns a set of metadata details about a Google Drive document.
    """
    drive_service = discovery.build("drive", "v2")
    metadata = drive_service.files().get(fileId=document_id).execute()

    details = {'version': metadata.get('version'), 'etag': metadata.get('etag'), 'modified_date': metadata.get('modifiedDate'),
        'modifiying_user': metadata.get('lastModifyingUserName')}

    return details
