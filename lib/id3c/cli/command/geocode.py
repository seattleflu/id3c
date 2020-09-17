"""
Geocode addresses into longitude/latitude.

Input addresses must be in a tabular data format (CSV, TSV, or Excel) with only
one address per row.

Geocoding is performed by submitting addresses (with no other information) to
an external service called SmartyStreets.  A SmartyStreets account is required.
Credentials must be provided by the SMARTYSTREETS_AUTH_ID and
SMARTYSTREETS_AUTH_TOKEN environment variables.

Each address geocoded incurs a cost against the SmartyStreets account in use,
so lookups are cached locally and re-used for up to a year when an address is
encountered more than once.  For example, running a dataset through this
command twice will only incur costs the first time.  The cache file should be
protected as identifiable data since it contains the addresses themselves and
the geocoding result.
"""
import click
import logging
import pandas as pd
import sys
import json
import yaml
from os import environ, chdir
from os.path import dirname
from textwrap import dedent
from cachetools import TTLCache
from typing import Tuple, Dict, Any
from smartystreets_python_sdk import StaticCredentials, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup
from smartystreets_python_sdk.us_extract import Lookup as ExtractLookup
from id3c.cli import cli
from id3c.cli.io.pandas import (
    load_file_as_dataframe
)
from id3c.cli.command import pickled_cache


LOG = logging.getLogger(__name__)


# Shared SmartyStreets client, initialized when first needed.
STREET_CLIENT = None
EXTRACT_CLIENT = None


@cli.group("geocode", help = __doc__)
def geocode():
    pass

@geocode.command("using-options")
@click.argument("filename",
    metavar = "<filename.{csv,tsv,xlsx,xls}>",
    required = True,
    type = click.Path(exists=True))

@click.option("--street-column",
    metavar = "<column>",
    help = "Column name for address street",
    required = True)

@click.option("--secondary-column",
    metavar = "<column>",
    help = "Column name for address apartment, suite, or office number",
    required = False)

@click.option("--city-column",
    metavar = "<column>",
    help = "Column name for address city",
    required = True)

@click.option("--state-column",
    metavar = "<column>",
    help = "Column name for address state",
    required = True)

@click.option("--zipcode-column",
    metavar = "<column>",
    help = "Column name for address zipcode",
    required = True)

@click.option("--cache-file",
    metavar = "<cache.pickle>",
    help = "Local cache file for storing address lookups. Must be specified to cache lookups",
    required = False,
    type = click.Path())

def geocode_using_options(**kwargs):
    """
    Geocode addresses listed in <filename.{csv,tsv,xlsx,xls}>.

    Options specify column names to extract address parts for geocoding.
    Of these, --street, --city, --state, and --zipcode are required.

    Requires two environment variables: SMARTYSTREETS_AUTH_ID and
    SMARTYSTREETS_AUTH_TOKEN.

    Geocoded addresses are output to stdout as comma-separated values, with
    extra values for addresses latitude, longitude, and canonicalized address.
    If an address cannot be geocoded, then these extra columns are left blank.

    See `id3c geocode --help` for more information.
    """
    geocoded_addresses = get_geocoded_addresses(**kwargs)
    geocoded_addresses.to_csv(sys.stdout, index=False)


@geocode.command("using-config")
@click.argument("filename",
    metavar = "<filename.{csv,tsv,xlsx,xls}>",
    required = True,
    type = click.Path(exists=True, resolve_path=True))

@click.argument("config_file",
    metavar = "<config.yaml>",
    required = True,
    type = click.File("r"))

def geocode_using_config(filename, config_file):
    """
    Geocode addresses listed in <filename.{csv,tsv,xlsx,xls}>
    with column names and cache file specified by a <config.yaml>.
    If cache is not specified in <config.yaml>, then lookups will not be
    cached locally and will not be re-used for future lookups.

    <config.yaml> must be a file with one YAML document in it.  The document
    corresponds closely to the command-line options taken by the
    "using-options" command (a sibling to this command).
    For example:

    \b
        ---
        cache: cache.pickle
        columns:
          street: "Street"
          secondary: "Street2"
          city: "City"
          state: "State"
          zipcode: "ZipCode"

    Relative paths in <config.yaml> are treated relative to the containing
    directory of the configuration file itself.

    Requires two environment variables: SMARTYSTREETS_AUTH_ID and
    SMARTYSTREETS_AUTH_TOKEN.

    Geocoded addresses are output to stdout as comma-separated values, with
    extra values for addresses latitude, longitude, and canonicalized address.
    If an address cannot be geocoded, then these extra columns are left blank.

    See `id3c geocode --help` for more information.
    """
    config = yaml.safe_load(config_file)

    # The input data filename is fully-resolved by Click, so even if the
    # filename is relative, it's safe to change directories before reading it.

    if config_file.name != "<stdin>":
        config_dir = dirname(config_file.name)

        # dirname is the empty string if we're in the same directory as the
        # config file.
        if config_dir:
            chdir(config_dir)

    try:
        kwargs = {
            "filename":          filename,
            "street_column":     config["columns"]["street"],
            "city_column":       config["columns"]["city"],
            "state_column":      config["columns"]["state"],
            "zipcode_column":    config["columns"]["zipcode"],
            "secondary_column":  config["columns"].get("secondary"),
            "cache_file":        config.get("cache")
        }
    except KeyError as key:
        LOG.error(f"Required key «{key}» missing from config {config}")
        raise key from None

    geocoded_addresses = get_geocoded_addresses(**kwargs)
    geocoded_addresses.to_csv(sys.stdout, index=False)


def get_geocoded_addresses(*,
                           filename,
                           street_column,
                           city_column,
                           state_column,
                           zipcode_column,
                           secondary_column = None,
                           cache_file = None):
    """
    Internal function powering :func:`geocode_using_options` and
    :func:`geocode_using_config`.
    """
    LOG.debug(f"Reading addresses file {filename}")

    with pickled_cache(cache_file) as cache:
        address_column_map = {
            'street': street_column,
            'secondary': secondary_column,
            'city': city_column,
            'state': state_column,
            'zipcode': zipcode_column
        }

        addresses_df = load_file_as_dataframe(filename)
        addresses_df['std_address'] = addresses_df.apply(
            lambda row: standardize_address(row, address_column_map),
            axis='columns')

        (addresses_df['lat'],
         addresses_df['lng'],
         addresses_df['canonicalized_address']) = zip(
             *addresses_df['std_address'].apply(
                 lambda row: get_response_from_cache_or_geocoding(row, cache)))

    return addresses_df


def standardize_address(address_series: pd.Series,
                        address_column_map: dict) -> dict:
    """
    Create standardized address dictionary from *address_series* with the
    expected address keys for the SmartyStreets API provided in
    *address_column_map*.

    All address values are converted to uppercase and stripped of leading and
    trailing whitespaces.
    """
    address_columns = list(filter(None, address_column_map.values()))

    if not address_columns:
        raise NoAddressColumnsFoundError(address_column_map)

    address = address_series.to_dict()
    std_address: Dict[str, str] = {}

    for standardized_column, provided_column in address_column_map.items():
        std_address[standardized_column] = None
        if provided_column:
            std_address[standardized_column] = address[provided_column].upper().strip()

    return std_address



def get_response_from_cache_or_geocoding(address: dict,
                                         cache: TTLCache) -> Tuple[Any, Any, Any]:
    """
    Provided an *address* dict in a format that SmartyStreets US Address API
    expects, get a response from the cache or by geocoding with API.
    """
    key = json.dumps(address, sort_keys=True)

    try:
        response = cache[key]
        LOG.debug('Response found in cache.')
    except KeyError:
        response = cache[key] = geocode_address(address)
        LOG.debug('Adding new response to cache.')

    if not response:
        return None, None, None

    else:
        return response.get('lat'), response.get('lng'), response.get('canonicalized_address')


def geocode_address(address: dict) -> dict:
    """
    Given an *address* matching format expected for the SmartyStreets API,
    returns a dict containing a canonicalized address and lat/long coordinates
    from SmartyStreet's US Street geocoding API.
    """
    LOG.debug("Making SmartyStreets geocoding API request")

    global STREET_CLIENT
    if not STREET_CLIENT:
        STREET_CLIENT = smartystreets_client_builder().build_us_street_api_client()

    lookup = us_street_lookup(address)
    if not lookup.street:
        LOG.warning(f"Missing street address; can't geocode")
        return None

    STREET_CLIENT.send_lookup(lookup)
    result = lookup.result

    if not result:
        LOG.info("Previous lookup failed. Looking up address as free text")
        result = extract_address(address)

    if not result:
        LOG.info(f"Invalid address: no response from SmartyStreets.")

    return parse_first_smartystreets_result(result)


def smartystreets_client_builder():
    """
    Returns a new :class:`smartystreets_python_sdk.ClientBuilder` using
    credentials from the required environment variables
    ``SMARTYSTREETS_AUTH_ID`` and ``SMARTYSTREETS_AUTH_TOKEN``.
    """
    auth_id = environ.get('SMARTYSTREETS_AUTH_ID')
    auth_token = environ.get('SMARTYSTREETS_AUTH_TOKEN')

    if not auth_id and not auth_token:
        raise Exception("The environment variables SMARTYSTREETS_AUTH_ID and SMARTYSTREETS_AUTH_TOKEN are required.")
    elif not auth_id:
        raise Exception("The environment variable SMARTYSTREETS_AUTH_ID is required.")
    elif not auth_token:
        raise Exception("The environment variable SMARTYSTREETS_AUTH_TOKEN is required.")

    return ClientBuilder(StaticCredentials(auth_id, auth_token))


def us_street_lookup(address: dict) -> Lookup:
    """
    Creates and returns a SmartyStreets US Street API Lookup object for a given
    *address*.

    Raises a :class:`InvalidAddressMappingError` if a Lookup property from the
    SmartyStreets geocoding API is not present in the given *address* data.
    """
    lookup = Lookup()

    try:
        lookup.street = address['street']
        lookup.secondary = address['secondary']
        lookup.city = address['city']
        lookup.state = address['state']
        lookup.zipcode = address['zipcode']
    except KeyError as e:
        raise InvalidAddressMappingError(e)

    lookup.candidates = 1
    lookup.match = "Invalid"  # Most permissive
    return lookup


def parse_first_smartystreets_result(result: list) -> dict:
    """
    Given an address *result* from SmartyStreets geocoding API, parse the
    canonicalized address and lat/lng information of the first result
    to return as a dict.

    If address is invalid and response is empty, then returns a dict with
    empty string values
    """
    response = {
        'canonicalized_address': '',
        'lat': '',
        'lng': ''
    }

    if result:
        first_candidate = result[0]
        address = ' '.join([
            first_candidate.delivery_line_1,
            first_candidate.last_line
        ])
        response['canonicalized_address'] = address
        response['lat'] = first_candidate.metadata.latitude
        response['lng'] = first_candidate.metadata.longitude

    return response


def extract_address(address: dict) -> dict:
    """
    Given an *address*, converts it to text and returns a result from
    the SmartyStreets US Extract API containing information about an address
    connected to the text.

    Note that this API is not consistent with the US Street API, and the lookup
    and responses must be handled differently.
    """
    LOG.debug("Making SmartyStreets extract API request")

    global EXTRACT_CLIENT
    if not EXTRACT_CLIENT:
        EXTRACT_CLIENT = smartystreets_client_builder().build_us_extract_api_client()

    address_text = ', '.join([str(val) for val in list(address.values()) if val])

    lookup = ExtractLookup()
    lookup.text = address_text

    result = EXTRACT_CLIENT.send(lookup)
    addresses = result.addresses

    for add in addresses:
        if len(add.candidates) > 0:
            return add.candidates

    return None


class InvalidAddressMappingError(KeyError):
    """
    Raised when a an *address_key* used in the SmartyStreets geocoding
    API Lookup object is not present in the data to be transformed.
    """
    def __init__(self, address_key):
        self.address_key = address_key

    def __str__(self):
        return dedent(f"""
        {self.address_key} not found in the address mapping.
        Is there an error in your column map?
        """)


class NoAddressColumnsFoundError(InvalidAddressMappingError):
    """
    Raised by :func:`create_address_df` when a *address_columns_map*
    has no values.
    """
    def __init__(self, address_columns_map):
        self.address_columns_map = address_columns_map

    def __str__(self):
        return dedent(f"""
        No address columns have been provided.
        The expected columns are:
            {self.address_columns_map.keys()}
        """)
