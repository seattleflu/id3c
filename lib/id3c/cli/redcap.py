"""
Minimal library for interacting with REDCap's web API.
"""
import logging
import re
import requests
from operator import itemgetter
from typing import Any, Dict, List


LOG = logging.getLogger(__name__)


class Project:
    """
    Interact with a REDCap project via the REDCap web API.

    The constructor requires an *api_url* and *api_token* which must point to
    REDCap's web API endpoint.  The third required parameter *project_id* must
    match the project id returned by the API.  This is a sanity check that the
    API token is for the intended project, since tokens are project-specific.
    """
    api_url: str
    api_token: str
    base_url: str
    _details: dict
    _instruments: List[str] = None

    def __init__(self, api_url: str, api_token: str, project_id: int) -> None:
        self.api_url = api_url
        self.api_token = api_token

        # Assuming that the base url for a REDCap instance is just removing the
        # trailing 'api' from the API URL
        self.base_url = re.sub(r'api/?$', '', api_url)

        # Sanity check project details
        self._details = self._fetch("project")

        assert self.id == project_id, \
            f"REDCap API token provided for project {project_id} is actually for project {self.id} ({self.title!r})!"


    @property
    def id(self) -> int:
        """Numeric ID of this project."""
        return self._details["project_id"]


    @property
    def title(self) -> str:
        """Name of this project."""
        return self._details["project_title"]


    @property
    def instruments(self) -> List[str]:
        """
        Names of all instruments in this REDCap project.
        """
        if not self._instruments:
            nameof = itemgetter("instrument_name")
            self._instruments = list(map(nameof, self._fetch("instrument")))

        return self._instruments


    def records(self, since_date: str = None, raw: bool = False) -> List[dict]:
        """
        Fetch records for this REDCap project.

        Values are returned as string labels not numeric ("raw") codes.

        The optional *since_date* parameter can be used to limit records to
        those created/modified after the given timestamp, which must be
        formatted as ``YYYY-MM-DD HH:MM:SS`` in the REDCap server's configured
        timezone.

        The optional *raw* parameter controls if numeric values are returned
        for multiple choice fields.  When false (the default), string labels
        are returned.
        """
        parameters = {
            'type': 'flat',
            'rawOrLabel': 'raw' if raw else 'label',
            'exportCheckboxLabel': 'true',
        }

        if since_date:
            parameters['dateRangeBegin'] = since_date

        return self._fetch("record", parameters)


    def _fetch(self, content: str, parameters: Dict[str, str] = {}) -> Any:
        """
        Fetch REDCap *content* with a POST request to the REDCap API.

        Consult REDCap API documentation for required and optional parameters
        to include in API request.
        """
        LOG.debug(f"Fetching content={content} from REDCap with params {parameters}")

        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }

        data = {
            **parameters,
            'content': content,
            'token': self.api_token,
            'format': 'json',
        }

        response = requests.post(self.api_url, data=data, headers=headers)
        response.raise_for_status()

        return response.json()
