"""
Minimal library for interacting with REDCap's web API.
"""
import logging
import re
import requests
from enum import Enum
from functools import lru_cache
from operator import itemgetter
from typing import Any, Dict, List, Optional


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
    _events: List[str] = None
    _fields: List[dict] = None
    _redcap_version: str = None

    def __init__(self, api_url: str, api_token: str, project_id: int) -> None:
        self.api_url = api_url
        self.api_token = api_token

        # Assuming that the base url for a REDCap instance is just removing the
        # trailing 'api' from the API URL
        self.base_url = re.sub(r'api/?$', '', api_url)

        # Check if project details match our expectations
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


    @property
    def events(self) -> List[str]:
        """
        Names of all events in this REDCap project.
        """
        if self._events is None:
            nameof = itemgetter("unique_event_name")

            if self._details["is_longitudinal"]:
                self._events = list(map(nameof, self._fetch("event")))
            else:
                self._events = []

        return self._events


    @property
    def fields(self) -> List[dict]:
        """
        Metadata about all fields in this REDCap project.
        """
        if not self._fields:
            self._fields = self._fetch("metadata")

        return self._fields


    @property
    def record_id_field(self) -> str:
        """
        Name of the field containing the unique id for each record.

        For auto-numbered projects, this is typically ``record_id``, but for
        other data entry projects, it can be any arbitrary name.  It is always
        the first field in a project.
        """
        return self.fields[0]["field_name"]


    @property
    def redcap_version(self) -> str:
        """
        Version string of the REDCap instance.
        """
        if not self._redcap_version:
            self._redcap_version = self._fetch("version", format = "text")

        return self._redcap_version


    def record(self, record_id: str, *, raw: bool = False) -> List['Record']:
        """
        Fetch the REDCap record *record_id* with all its instruments.

        The optional *raw* parameter controls if numeric/coded values are
        returned for multiple choice fields.  When false (the default),
        string labels are returned.

        Note that in longitudinal projects with events or classic projects with
        repeating instruments, this may return more than one result.  The
        results will be share the same record id but be differentiated by the
        fields ``redcap_event_name``, ``redcap_repeat_instrument``, and
        ``redcap_repeat_instance``.
        """
        return self.records(ids = [record_id], raw = raw)


    def records(self, *,
                since_date: str = None,
                until_date: str = None,
                ids: List[str] = None,
                fields: List[str] = None,
                events: List[str] = None,
                filter: str = None,
                raw: bool = False) -> List['Record']:
        """
        Fetch records for this REDCap project.

        Values are returned as string labels not numeric ("raw") codes.

        The optional *since_date* parameter can be used to limit records to
        those created/modified after the given timestamp.

        The optional *until_date* parameter can be used to limit records to
        those created/modified before the given timestamp.

        Both *since_date* and *until_date* must be formatted as
        ``YYYY-MM-DD HH:MM:SS`` in the REDCap server's configured timezone.

        The optional *ids* parameter can be used to limit results to the given
        record ids.

        The optional *fields* parameter can be used to limit the fields
        returned for each record.

        The optional *events* parameter can be used to limit the events/arms
        returned for each record.

        The optional *filter* parameter can be used provide a REDCap
        conditional logic string determining which records are returned.
        Records for which *filter* evaluates to true are returned.

        The optional *raw* parameter controls if numeric/coded values are
        returned for multiple choice fields.  When false (the default), string
        labels are returned.
        """
        parameters = {
            'type': 'flat',
            'rawOrLabel': 'raw' if raw else 'label',
            'exportCheckboxLabel': 'true', # ignored by API if rawOrLabel == raw
        }

        assert not ((since_date or until_date) and ids), \
            "The REDCap API does not support fetching records filtered by id *and* date."

        if since_date:
            parameters['dateRangeBegin'] = since_date

        if until_date:
            parameters['dateRangeEnd'] = until_date

        if ids is not None:
            parameters['records'] = ",".join(map(str, ids))

        if fields is not None:
            parameters['fields'] = ",".join(map(str, fields))

        if events is not None:
            parameters['events'] = ",".join(map(str, events))

        if filter is not None:
            parameters['filterLogic'] = str(filter)

        return [Record(self, r) for r in self._fetch("record", parameters)]


    def _fetch(self, content: str, parameters: Dict[str, str] = {}, *, format: str = "json") -> Any:
        """
        Fetch REDCap *content* with a POST request to the REDCap API.

        Consult REDCap API documentation for required and optional parameters
        to include in API request.
        """
        LOG.debug(f"Fetching content={content} from REDCap with params {parameters}")

        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json' if format == "json" else 'text/*'
        }

        data = {
            **parameters,
            'content': content,
            'token': self.api_token,
            'format': format,
        }

        response = requests.post(self.api_url, data=data, headers=headers)
        response.raise_for_status()

        return response.json() if format == "json" else response.text


@lru_cache()
def CachedProject(api_url: str, api_token: str, project_id: int) -> Project:
    """
    Memoized constructor for a :class:`Project`.

    Useful when loading projects dynamically, e.g. from REDCap DET
    notifications, to avoid the initial fetch of project details every time.
    """
    return Project(api_url, api_token, project_id)


class Record(dict):
    """
    A single REDCap record ``dict``.

    All key/value pairs returned by the REDCap API request are present as
    dictionary items.

    Must be constructed with a REDCap :py:class:``Project``, which is stored as
    the ``project`` attribute and used to set the ``id`` attribute storing the
    record's primary id.

    Note that the ``event_name`` and ``repeat_instance`` attributes might not
    be populated because their corresponding fields (``redcap_event_name`` and
    ``redcap_repeat_instance``) won't be returned by the API if the request
    includes the ``fields`` parameter but not the primary record id field.
    """
    project: Project
    id: str
    event_name: Optional[str]
    repeat_instance: Optional[int]

    def __init__(self, project: Project, data: Any = {}) -> None:
        super().__init__(data)
        self.project = project
        self.id = self[self.project.record_id_field]

        # These field names are not variable across REDCap projects
        self.event_name = self.get("redcap_event_name")

        if self.get("redcap_repeat_instance"):
            self.repeat_instance = int(self["redcap_repeat_instance"])
        else:
            self.repeat_instance = None


class InstrumentStatus(Enum):
    """
    Numeric and string codes used by REDCap for instrument status.
    """
    Incomplete = 0
    Unverified = 1
    Complete = 2


def is_complete(instrument: str, data: dict) -> bool:
    """
    Test if the named *instrument* is marked complete in the given *data*.

    The *data* may be a DET notification or a record.

    Will return `None` if the *instrument*_complete key does not exist in
    given *data*.

    >>> is_complete("test", {"test_complete": "Complete"})
    True
    >>> is_complete("test", {"test_complete": 2})
    True
    >>> is_complete("test", {"test_complete": "2"})
    True
    >>> is_complete("test", {"test_complete": "Incomplete"})
    False
    >>> is_complete("test", {}) is None
    True
    """
    instrument_complete_field = data.get(f"{instrument}_complete")

    if instrument_complete_field is None:
        return None

    return instrument_complete_field in {
        InstrumentStatus.Complete.name,
        InstrumentStatus.Complete.value,
        str(InstrumentStatus.Complete.value)
    }
