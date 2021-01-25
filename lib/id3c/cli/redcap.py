"""
Minimal library for interacting with REDCap's web API.
"""
import json
import logging
import os
import re
import requests
from enum import Enum
from functools import lru_cache
from operator import itemgetter
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from .utils import running_command_name
from ..url import Url


LOG = logging.getLogger(__name__)


class Project:
    """
    Interact with a REDCap project via the REDCap web API.

    The constructor requires a *url*, which must point to REDCap's web API
    endpoint or base URL, and *project_id*.  These will be used to find an API
    token in the environment via :py:func:`.api_token`.  Alternatively, provide
    the keyword-only argument, *token*, to explicitly specify an API token to
    use.

    During initialization, project metadata is fetched via the API.  The given
    *project_id* must match the project id returned in the metadata.  This is a
    safety check that the API token is for the intended project, since tokens
    determine the project accessed.

    If the *dry_run* keyword-only argument is set to ``True``, then methods
    which could modify data in REDCap will pretend to succeed but not actually
    make API requests.  Read-only methods are unaffected and will return real
    data.  Defaults to ``False``.
    """
    api_url: str
    api_token: str
    base_url: str
    dry_run: bool
    _details: dict
    _instruments: List[str] = None
    _events: List[str] = None
    _fields: List[dict] = None
    _redcap_version: str = None

    def __init__(self, url: str, project_id: int, arg3 = None, *, token: str = None, dry_run: bool = False) -> None:
        # XXX TODO: Remove this and the associated "arg3" once we update all
        # existing callers to use the signature that takes only 2 positional
        # args + token as a keyword-only arg.
        #   -trs, 28 Oct 2020
        if arg3:
            token, project_id = project_id, arg3 # type: ignore

        self.api_url, self.base_url = url_endpoints(url)

        self.api_token = token or api_token(url, project_id)
        self.dry_run = bool(dry_run)

        # Check if project details match our expectations
        self._details = self._fetch("project")

        assert int(self.id) == int(project_id), \
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
        # This is always a List[Record] because we don't pass page_size, but
        # mypy (0.790) can't figure that out.
        return self.records(ids = [record_id], raw = raw) # type: ignore


    def records(self, *,
                since_date: str = None,
                until_date: str = None,
                ids: List[str] = None,
                instruments: List[str] = None,
                fields: List[str] = None,
                events: List[str] = None,
                filter: str = None,
                raw: bool = False,
                page_size: int = None) -> Union[List['Record'], Iterator['Record']]:
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

        The optional *instruments* parameter can be used to limit the
        instruments ("forms") returned for each record.

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

        The optional *page_size* parameter, when set, enables paged fetching of
        records by their primary record id (i.e. :attr:`.record_id_field`).
        *page_size* can only be used with REDCap projects that use record
        auto-numbering.  Each fetch will include a maximum of *page_size*
        records, although may contain many more result rows for records with
        repeating instruments/events).  The last fetch may contain results
        for more than *page_size* records because it uses an unrestricted upper
        bound in order to catch anything created since the start of the
        pagination process.  When *page_size* is provided, an iterator, as
        opposed to a list, is returned.
        """
        parameters = {
            'type': 'flat',
            'rawOrLabel': 'raw' if raw else 'label',
            'exportCheckboxLabel': 'true', # ignored by API if rawOrLabel == raw
            'exportSurveyFields': 'true', # pulls the _identifier and _timestamp fields from surveys
        }

        assert not ((since_date or until_date) and ids), \
            "The REDCap API does not support fetching records filtered by id *and* date."

        if since_date:
            parameters['dateRangeBegin'] = since_date

        if until_date:
            parameters['dateRangeEnd'] = until_date

        if ids is not None:
            parameters['records'] = ",".join(map(str, ids))

        if instruments is not None:
            parameters['forms'] = ",".join(map(str, instruments))

        if fields is not None:
            parameters['fields'] = ",".join(map(str, fields))

        if events is not None:
            parameters['events'] = ",".join(map(str, events))

        if filter is not None:
            parameters['filterLogic'] = str(filter)

        if page_size is not None:
            return self._fetch_records_paged(parameters, page_size)
        else:
            return list(self._fetch_records(parameters))


    def _fetch_records_paged(self, parameters: dict, page_size: int) -> Iterator['Record']:
        assert bool(self._details["record_autonumbering_enabled"]), \
            "Record auto-numbering must be enabled to use page_size parameter"

        # Query the current maximum record id + 1 for the project.  Due to the
        # absence of transactions, this is a hideously unsafe API design choice
        # on REDCap's part, but it's fine for our purposes.
        next_record_id = self._fetch("generateNextRecordName")

        pages = [
            (lower, lower + page_size if lower + page_size < next_record_id else None)
                for lower
                in range(1, next_record_id, page_size)]

        LOG.debug(f"Computed pages for record fetch: {pages!r}")

        for lower_bound, upper_bound in pages:
            page_filter = f"[{self.record_id_field}] >= {lower_bound}"

            if upper_bound is not None:
                page_filter += f" and [{self.record_id_field}] < {upper_bound}"

            page_parameters = parameters.copy()
            existing_filter = page_parameters.get("filterLogic")

            if existing_filter:
                page_parameters["filterLogic"] = f"({page_filter}) and ({existing_filter})"
            else:
                page_parameters["filterLogic"] = page_filter

            yield from self._fetch_records(page_parameters)


    def _fetch_records(self, parameters: dict) -> Iterator['Record']:
        return (Record(self, r) for r in self._fetch("record", parameters))


    def update_records(self, records: List[Dict[str, str]], date_format: str = "YMD") -> int:
        """
        Update existing *records* in this REDCap project.

        *records* must be an iterable of :py:class:``dict``s mapping REDCap
        field names to values.  The primary record id field, at a minimum, must
        be included.  Other pseudo-fields like ``redcap_event_name`` or
        ``redcap_repeat_instance`` may be necessary.  All keys and values must
        be strings.

        Dates must be formatted as ``YYYY-MM-DD`` with the default
        *date_format* of ``YMD``, as ``D/M/YYYY`` with ``DMY``, and as
        ``M/D/YYYY`` with ``MDY``. Times are always assumed to be in the REDCap
        server's timezone.

        Any value provided for a field, including the empty string, will
        overwrite any existing value.

        This method is not suitable for creating new records in projects that
        use auto-numbered record ids.

        Returns a count of the number of records updated, as reported by
        REDCap.
        """
        assert date_format in {'YMD', 'DMY', 'MDY'}

        parameters = {
            'data': json.dumps(records),
            'type': 'flat',
            'overwriteBehavior': 'overwrite',
            'forceAutoNumber': 'false',
            'dateFormat': date_format,
            'returnContent': 'count',
        }

        expected_count = len(records)

        if not self.dry_run:
            LOG.debug(f"Updating {expected_count:,} REDCap records for {self}")
            result = self._fetch("record", parameters)

            updated_count = int(result["count"])
        else:
            LOG.debug(f"Pretending to update {expected_count:,} REDCap records for {self} (dry run)")
            updated_count = expected_count

        assert expected_count == updated_count, \
            "Expected vs. actual records updated do not match: {expected_count:,} != {updated_count:,}"

        LOG.debug(f"Updated {updated_count:,} REDCap records for {self}")

        return updated_count


    def _fetch(self, content: str, parameters: Dict[str, str] = {}, *, format: str = "json") -> Any:
        """
        Fetch REDCap *content* with a POST request to the REDCap API.

        Consult REDCap API documentation for required and optional parameters
        to include in API request.
        """
        LOG.debug(f"Requesting content={content} from REDCap with params {parameters}")

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


    def __repr__(self) -> str:
        return f"<{self.__module__}.{type(self).__name__} object: api_url={self.api_url!r} project_id={self.id!r}>"


@lru_cache()
def CachedProject(api_url: str, project_id: int, *, token: str = None) -> Project:
    """
    Memoized constructor for a :class:`Project`.

    Useful when loading projects dynamically, e.g. from REDCap DET
    notifications, to avoid the initial fetch of project details every time.
    """
    return Project(api_url, project_id, token = token)


class Record(dict):
    """
    A single REDCap record ``dict``.

    All key/value pairs returned by the REDCap API request are present as
    dictionary items.

    Must be constructed with a REDCap :py:class:``Project``, which is stored as
    the ``project`` attribute and used to set the ``id`` attribute storing the
    record's primary id.

    Note that the ``event_name``, ``repeat_instance``, and ``repeat_instrument``
    attributes might not be populated because their corresponding fields
    (``redcap_event_name``, ``redcap_repeat_instance``, and
    ``redcap_repeat_instrument``) won't be returned by the API if the request
    includes the ``fields`` parameter but not the primary record id field.
    """
    project: Project
    event_name: Optional[str]
    repeat_instance: Optional[int]
    repeat_instrument: Optional[str]

    def __init__(self, project: Project, data: Any = {}) -> None:
        super().__init__(data)
        self.project = project

        # These field names are not variable across REDCap projects
        self.event_name = self.get("redcap_event_name")
        self.repeat_instrument = self.get("redcap_repeat_instrument")

        if self.get("redcap_repeat_instance"):
            self.repeat_instance = int(self["redcap_repeat_instance"])
        else:
            self.repeat_instance = None


    @property
    def id(self) -> str:
        """
        Returns the record's primary id.

        Raises a :py:class:`RuntimeError` if the primary id field is not available,
        usually because it was not requested from the API.
        """
        try:
            return self[self.project.record_id_field]
        except KeyError as e:
            raise RuntimeError(f"Primary record id field «{self.project.record_id_field}» not available on fetched record") from e


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
    instrument_complete_field = data.get(completion_status_field(instrument))

    if instrument_complete_field is None:
        return None

    return instrument_complete_field in {
        InstrumentStatus.Complete.name,
        InstrumentStatus.Complete.value,
        str(InstrumentStatus.Complete.value)
    }


def completion_status_field(instrument: str) -> str:
    """
    Returns the REDCap automatic field name for the completion status of
    *instrument*.

    If want to know the completion status itself, use :func:`is_complete`
    instead.
    """
    # XXX TODO: It would be good to normalize *instrument* here, including:
    #
    #   - Lowercasing
    #   - Replacing runs of whitespace and/or non-alphanumerics (?) with a
    #     single underscore.
    #   - Maybe: removing leading numbers?
    #
    # The full set of transformations REDCap applies aren't entirely clear to
    # me at the moment, so I'm punting for now.  The caller must provide the
    # internal names.
    #   -trs, 8 Jan 2020
    return f"{instrument}_complete"


def api_token(url: str, project_id: int) -> str:
    """
    Obtain an API token from the environment for the given REDCap *url* and
    *project_id*.

    The environment variable name is constructed using the *url* and
    *project_id*; see the examples below.

    Raises a :py:exc:`ValueError` if the environment variable is missing or has
    no value.

    >>> os.environ.update({
    ...     "REDCAP_API_TOKEN_redcap.iths.org_12345": "secret 1",
    ...     "REDCAP_API_TOKEN_example.com-redcap_67890": "secret 2",
    ...     "REDCAP_API_TOKEN_example.com:8080-redcap_67890": "secret 3",
    ...     "REDCAP_API_TOKEN_example.com_67890": "",
    ... })

    >>> api_token("https://redcap.iths.org/api/", 12345)
    'secret 1'
    >>> api_token("https://redcap.iths.org", 12345)
    'secret 1'

    >>> api_token("https://example.com/redcap/", 67890)
    'secret 2'

    >>> api_token("https://example.com:8080/redcap/", 67890)
    'secret 3'

    >>> api_token("https://example.com", 12345)
    Traceback (most recent call last):
        ...
    ValueError: No REDCap API token available for https://example.com project 12345: environment variable «REDCAP_API_TOKEN_example.com_12345» is missing

    >>> api_token("https://example.com", 67890)
    Traceback (most recent call last):
        ...
    ValueError: No REDCap API token available for https://example.com project 67890: environment variable «REDCAP_API_TOKEN_example.com_67890» has no value

    There is a slight risk of collision where two distinct URLs will map to the
    same token name, e.g.::

        a-b.com/c/d → a-b.com-c-d
        a/b.com/c/d → a-b.com-c-d

    But the risk of this manifesting in practice seems very low.  It could be
    mitigated in the future with a more aggressive (but less human-readable)
    encoding scheme like URI escaping or even Base64.
    """
    base_url = Url(url_endpoints(url)[1])

    if base_url.port:
        origin = f"{base_url.hostname}:{base_url.port}"
    else:
        origin = base_url.hostname

    hyphenated_path = base_url.path.rstrip("/").replace("/", "-")

    token_name = f"REDCAP_API_TOKEN_{origin}{hyphenated_path}_{project_id}"

    try:
        token = os.environ[token_name]
    except KeyError as e:
        raise ValueError(f"No REDCap API token available for {base_url} project {project_id}: environment variable «{token_name}» is missing") from e

    if not token:
        raise ValueError(f"No REDCap API token available for {base_url} project {project_id}: environment variable «{token_name}» has no value")

    return token


def url_endpoints(url) -> Tuple[str, str]:
    """
    Returns a tuple of ``(api_url, base_url)`` based on the given REDCap
    instance *url*.

    *url* may be either the API endpoint or the base path of the REDCap
    instance.

    >>> url_endpoints("https://redcap.iths.org/")
    ('https://redcap.iths.org/api/', 'https://redcap.iths.org/')

    >>> url_endpoints("https://redcap.iths.org/api/")
    ('https://redcap.iths.org/api/', 'https://redcap.iths.org/')

    >>> url_endpoints("https://example.org/redcap/")
    ('https://example.org/redcap/api/', 'https://example.org/redcap/')

    >>> url_endpoints(url_endpoints("https://redcap.iths.org/api/")[1])
    ('https://redcap.iths.org/api/', 'https://redcap.iths.org/')

    Assumes that the API endpoint is always at ``…/api/`` under the REDCap
    instance's base URL and vice versa.  This appears to be true for all the
    instances inspected (redcap.iths.org, redcap.fhcrc.org,
    redcapdemo.vanderbilt.edu).
    """
    url = Url(url)

    if re.search(r'(?<=/)api/?$', url.path):
        api_url  = str(url)
        base_url = str(url.parent)
    else:
        api_url  = str(url / "api/")
        base_url = str(url)

    return api_url, base_url


def det(project: Project, record: dict, instrument: str, generated_by: str = None) -> dict:
    """
    Create a "fake" DET notification that mimics the format of REDCap DETs:

    \b
    {
        'redcap_url',
        'project_id',
        'record',
        'instrument',
        '<instrument>_complete',
        'redcap_event_name',      // for longitudinal projects only
        'redcap_repeat_instance',
        'redcap_repeat_instrument',
    }
    """
    instrument_complete = completion_status_field(instrument)

    if not generated_by:
        generated_by = running_command_name()

    # Intentionally access only dictionary keys on *record*, not Record
    # attributes, so that *record* is not required to be a Record.  For
    # example, this why the code below still uses
    #
    #     record[project.record_id_field]
    #
    # instead of
    #
    #     record.id
    #
    # -trs, 5 Nov 2020

    det_record = {
       'redcap_url': project.base_url,
       'project_id': str(project.id),                   # REDCap DETs send project_id as a string
       'record': str(record[project.record_id_field]),  # ...and record as well.
       'instrument': instrument,
       instrument_complete: record[instrument_complete],
       'redcap_repeat_instance': record.get('redcap_repeat_instance'),
       'redcap_repeat_instrument': record.get('redcap_repeat_instrument'),
       '__generated_by__': generated_by,
    }

    if 'redcap_event_name' in record:
        det_record['redcap_event_name'] = record['redcap_event_name']

    return det_record
