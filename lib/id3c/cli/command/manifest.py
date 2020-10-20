"""
Parse, diff, and upload sample manifests.

Manifests are listings of known samples with associated barcodes and other,
minimal metadata.  They are usually produced by the lab processing collection
tubes and contain the link between encounter survey data and molecular biology
results.

The workflow for processing new or updated manifests is generally:

    parse → diff (usually) → upload → etl

The first three correspond to subcommands below; the last to the "manifest"
subcommand of the "etl" command.
"""
import click
import fnmatch
import json
import logging
import pandas
import re
import yaml
from functools import reduce
from deepdiff import DeepHash
from hashlib import sha1
from os import chdir
from os.path import dirname
from typing import Iterable, List, Optional, Set, Tuple, Union
from id3c.cli import cli
from id3c.cli.io import LocalOrRemoteFile, urlopen
from id3c.cli.io.google import *
from id3c.cli.io.pandas import read_excel
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json, Json
from id3c.utils import format_doc


LOG = logging.getLogger(__name__)

PROVENANCE_KEY = "_provenance"

RESERVED_COLUMNS = {"sample", "collection", "date"}


@cli.group("manifest", help = __doc__)
def manifest():
    pass


@manifest.command("parse")
@click.argument("workbook", metavar = "<filepath>")

@click.option("--sheet",
    metavar = "<name>",
    help = "Name of the workbook sheet to read",
    required = True)

@click.option("--sample-column",
    metavar = "<column>",
    help = "Name of the single column containing sample barcodes.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--collection-column",
    metavar = "<column>",
    help = "Name of the single column containing collection barcodes.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--date-column",
    metavar = "<column>",
    help = "Name of the single column containing the sample collected date.",
    required = False)

@click.option("--sample-type",
    metavar = "<type>",
    help = "The type of sample within this manifest. "
           "Only applicable to samples from self-test kits.",
    type=click.Choice(["utm", "rdt"]),
    required = False)

@click.option("--extra-column", "extra_columns",
    metavar = "<field>:<column>|<field>:{…}",
    help = "Name of an additional <column> to extract into manifest record <field>.  "
           "Must match exactly; shell-style glob patterns are supported.  "
           "May be specified multiple times.  "
           "Option value is parsed as a YAML fragment, so additional options supported by the sibling command \"parse-with-config\" may be inlined for testing, but you're likely better off using a config file at that point.",
    multiple = True)

@click.option("--row-filter",
    metavar = "<query>",
    help = "The pandas query to filter rows (using the python engine) in the manifest.  "
           "Column names refer to columns in the manifest itself.  "
           "Example: `corrective action`.notnull() and `corrective action`.str.lower().str.startswith(\"discard\") ",
    required = False)

def parse(**kwargs):
    """
    Parse a single manifest workbook sheet.

    <filepath> must be a path or URL to an Excel workbook or Google Sheets
    spreadsheet with at least one sheet in it, identified by name using the required option --sheet.
    Supported URL schemes include http[s]:// and s3://, as well as others.

    The --sample-column option specifies the name of the column
    containing the sample barcode. The --collection-column option specifies
    the name of the column containing the collection barcode. You must supply one
    or both of those options.

    The --date-column specifies the name of the column containing the sample collected date.

    Other columns may be extracted into the manifest records as desired using the
    --extra-column option.

    The row-filter entry specifies a pandas query to filter
    (using the python engine) rows in the manifest. Column names refer to columns
    in the manifest itself.
    Example: `corrective action`.notnull() and `corrective action`.str.lower().str.startswith("discard")

    Manifest records are output to stdout as newline-delimited JSON records.
    You will likely want to redirect stdout to a file.
    """
    kwargs["extra_columns"] = [
        (dst, yaml.safe_load(src))
            for dst, src
            in [arg.split(":", 1) for arg in kwargs["extra_columns"]]

       ]

    manifest = _parse(**kwargs)
    dump_ndjson(manifest)


@manifest.command("parse-using-config")
@click.argument("config_file",
    metavar = "<config.yaml>",
    type = click.File("r"))

def parse_using_config(config_file):
    """
    Parse multiple manifest sheets specified by a config file.

    <config.yaml> must be a file with at least one YAML document in it.  Each
    document corresponds closely to the command-line options taken by the
    "parse" command (a sibling to this command).  For example, the following
    configuration contains two documents:

    \b
        ---
        workbook: OneDrive/SFS Prospective Samples 2018-2019.xlsx
        sheet: HMC
        sample_column: "Barcode ID*"
        date_column: "Coll_date"
        extra_columns:
          collection:
            name: "Collection ID*"
            barcode: true
          aliquots:
            name: "Aliquot [ABC]"
            multiple: true
          date: "Collection date*"
          aliquot_date: "Date aliquoted"
          racks:
            name: "Rack [ABC]*"
            multiple: true
          notes: "Notes"
    \b
        ---
        workbook: OneDrive/SFS Retrospective Samples 2018-2019.xlsx
        sheet: HMC
        sample_column: "Barcode ID*"
        extra_columns:
          aliquots:
            name: "Aliquot [ABC]"
            multiple: true
          date: "Collection date*"
          aliquot_date: "Date aliquoted"
          racks:
            name: "Rack [ABC]*"
            multiple: true
          test_results: "Test ResulTS"
        ...

    The sample_column entry specifies the name of the column
    containing the sample barcode. The collection_column entry specifies
    the name of the column containing the collection barcode. You must supply one
    or both of those entries.

    The date_column specifies the name of the column containing the sample collected date.

    The row_filter entry specifies a pandas query to filter
    (using the python engine) rows in the manifest. Column names refer to columns
    in the manifest itself.
    Example: `corrective action`.notnull() and `corrective action`.str.lower().str.startswith("discard")

    The key: value pairs in "extra_columns" name destination record fields (as
    the key) and source columns (as the value).  For most source columns, a
    simple string name (or shell-glob pattern) is enough.  Other behaviour is
    available by using a dictionary value.

    To collect values from multiple source columns into one record field,
    specify a dictionary like:

    \b
        field:
          name: column_[abc]
          multiple: true

    To mark a field as containing unique barcodes, similar to the built-in
    "sample_column" option, specify a dictionary like:

    \b
        field:
          name: column
          barcode: true

    Barcode fields are checked for duplicates and any records containing a
    duplicated value are dropped with a warning.

    Relative paths in <config.yaml> are treated relative to the containing
    directory of the configuration file itself.

    All manifest records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    configs = list(yaml.safe_load_all(config_file))

    if config_file.name != "<stdin>":
        config_dir = dirname(config_file.name)

        # dirname is the empty string if we're in the same directory as the
        # config file.
        if config_dir:
            chdir(config_dir)

    for config in configs:
        try:
            kwargs = {
                "workbook": config["workbook"],
                "sheet": config["sheet"],
                "sample_column": config.get("sample_column"),
                "collection_column": config.get("collection_column"),
                "date_column": config.get("date_column"),
                "extra_columns": list(config.get("extra_columns", {}).items()),
                "sample_type": config.get("sample_type"),
                "row_filter" : config.get("row_filter")
            }
        except KeyError as key:
            LOG.error(f"Required key «{key}» missing from config {config}")
            raise key from None

        dump_ndjson(_parse(**kwargs))


def _parse(*,
           workbook,
           sheet,
           sample_column = None,
           collection_column = None,
           date_column = None,
           extra_columns: List[Tuple[str, Union[str, dict]]] = [],
           sample_type = None,
           row_filter: Optional[str] = None):
    """
    Internal function powering :func:`parse` and :func:`parse_using_config`.
    """
    if not sample_column and not collection_column:
        raise ValueError("You must specify the sample_column, the collection_column, or both.")

    disallowed_extra_columns = {dst for dst, src in extra_columns} & RESERVED_COLUMNS
    assert len(disallowed_extra_columns) == 0, \
        f"A reserved column name has been configured in extra_columns: {disallowed_extra_columns}"

    # Used to capture internal provenance metadata for data tracing
    digest = None

    # Determine if the workbook URL is for a Google Document and if so
    # retrieve the Google Sheets file as an Excel spreadsheet. Otherwise,
    # retrieve it using urlopen.
    google_docs_document_id = extract_document_id_from_google_url(workbook)

    if google_docs_document_id:
        LOG.debug(f"Reading Google Sheets document «{workbook}»")
        with export_file_from_google_drive(google_docs_document_id, GoogleDriveExportFormat.EXCEL) as file:
            workbook_bytes = file.read()
        document_details = get_document_details(google_docs_document_id)
        digest = sha1(document_details['etag'].encode()).hexdigest()

    else:
        LOG.debug(f"Reading Excel workbook «{workbook}»")
        with urlopen(workbook, "rb") as file:
            workbook_bytes = file.read()
        digest = sha1(workbook_bytes).hexdigest()

    LOG.debug(f"Parsing sheet «{sheet}» in workbook «{workbook}»")

    # Read all columns as strings using our pandas wrapper
    manifest = read_excel(workbook_bytes, sheet_name = sheet)
    LOG.debug(f"Columns in manifest: {list(manifest.columns)}")

    # Strip leading/trailing spaces from values and replace missing values and
    # empty strings (possibly from stripping) with None so they are converted
    # to null in JSON.
    #
    # Note that the two .replace() calls can't be combined because the first
    # instance of NA → None will change the column dtype from string → object
    # and render subsequent comparisons to NA invalid.
    manifest = manifest.apply(
        lambda column: (
            column
                .str.strip()
                .replace({pandas.NA: ""})
                .replace({"": None, "na": None})))

    # If a filter query was provided filter the manifest rows
    # using the python engine.
    if row_filter:
        manifest = manifest.query(row_filter, engine="python")

    # Construct parsed manifest by copying columns from source to destination.
    # This approach is used to allow the same source column to end up as
    # multiple destination columns.
    parsed_manifest = pandas.DataFrame()

    column_map: List[Tuple[str, dict]] = []

    if sample_column:
        column_map += [("sample", {"name": sample_column, "barcode": True})]

    if collection_column:
        column_map += [("collection", {"name": collection_column, "barcode": True})]

    if date_column:
        column_map += [("date", {"name": date_column})]

    column_map += [
        (dst, src) if isinstance(src, dict) else (dst, {"name":src})
            for dst, src
            in extra_columns
            if src]

    for dst, src in column_map:
        if src.get("multiple"):
            parsed_manifest[dst] = select_columns(manifest, src["name"]).apply(list, axis="columns")
        else:
            parsed_manifest[dst] = select_column(manifest, src["name"])

    # Set of columns names for barcodes
    barcode_columns = {dst for dst, src in column_map if src.get("barcode")}

    parsed_manifest = perform_qc(sample_column, collection_column, barcode_columns, parsed_manifest)

    # Add sample type for kit related samples
    if sample_type:
        parsed_manifest["sample_type"] = sample_type

    parsed_manifest[PROVENANCE_KEY] = list(
        map(lambda index: {
                "workbook": workbook,
                "sha1sum": digest,
                "sheet": sheet,
                # Account for header row and convert from 0-based to 1-based indexing
                "row": index + 2,
            }, parsed_manifest.index))

    # Return a standard list of dicts instead of a DataFrame
    return parsed_manifest.to_dict(orient = "records")


@manifest.command("diff")

@click.argument("manifest_a",
    metavar = "<manifest-a.ndjson>",
    type = LocalOrRemoteFile("r"))

@click.argument("manifest_b",
    metavar = "<manifest-b.ndjson>",
    type = LocalOrRemoteFile("r"))

@format_doc(PROVENANCE_KEY = PROVENANCE_KEY)

def diff(manifest_a, manifest_b):
    """
    Compare two manifests and output new or changed records.

    <manifest-a.ndjson> and <manifest-b.ndjson> must be newline-delimited JSON
    files produced by the "parse" or "parse-using-config" commands which are
    siblings to this command.

    Records in <manifest-b.ndjson> which do not appear in <manifest-a.ndjson>
    will be output to stdout.  The internal provenance-tracking field,
    "{PROVENANCE_KEY}", is ignored for the purposes of comparison.
    """
    manifest_a_hashes = {
        deephash(record)
            for record in load_ndjson(manifest_a) }

    new_or_changed = (
        record for record in load_ndjson(manifest_b)
            if deephash(record) not in manifest_a_hashes )

    dump_ndjson(new_or_changed)


@manifest.command("upload")
@click.argument("manifest_file",
    metavar = "<manifest.ndjson>",
    type = LocalOrRemoteFile("r"))

def upload(manifest_file):
    """
    Upload manifest records into the database receiving area.

    <manifest.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling commands.

    Once records are uploaded, the manifest ETL routine will reconcile the
    manifest records with known identifiers and existing samples.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying sample manifest records from {manifest_file.path}")

        row_count = db.copy_from_ndjson(("receiving", "manifest", "document"), manifest_file)

        LOG.info(f"Received {row_count:,} manifest records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise


def select_column(table: pandas.DataFrame, name: str) -> pandas.Series:
    """
    Select the single column matching *name* in *table*.

    *table* must be a :class:`pandas.DataFrame`.

    *name* must be a string, which may contain shell-style wildcards and
    pattern matching.

    Matching is performed case-insensitively.  An `AssertionError` is raised if
    no columns are found or if more than one column is found.

    Returns a :class:`pandas.Series` column from *table*.
    """
    matching = select_columns(table, name)

    assert len(matching.columns) == 1, f"More than one column name matching «{name}»: {matching.columns}"
    return matching[matching.columns[0]]


def select_columns(table: pandas.DataFrame, name: str) -> pandas.DataFrame:
    """
    Select one or more columns matching *name* in *table*.

    *table* must be a :class:`pandas.DataFrame`.

    *name* must be a string, which may contain shell-style wildcards and
    pattern matching.

    Matching is performed case-insensitively.  An `AssertionError` is raised if
    no columns are found.

    Returns a :class:`pandas.DataFrame` containing a subset of columns in
    *table*.
    """
    pattern = re.compile(fnmatch.translate(name), re.IGNORECASE)
    matches = list(filter(pattern.match, table.columns.astype(str)))

    assert matches, f"No column name matching «{name}» found; column names are: {list(table.columns)}"
    return table[matches]


def perform_qc(sample_column: str, collection_column: str, barcode_columns: Set[str],
    parsed_manifest: pandas.DataFrame) -> pandas.DataFrame:
    """
    Perform quality control on the manifest data, dropping rows which violate
    our standards for complete and accurate data.
    """
    parsed_manifest = drop_missing_barcodes(sample_column, collection_column, parsed_manifest)

    # Drop any rows that have duplicated barcodes
    parsed_manifest = deduplicate_barcodes(parsed_manifest, barcode_columns)
    return parsed_manifest


def drop_missing_barcodes(sample_column: str, collection_column: str,
    parsed_manifest: pandas.DataFrame) -> pandas.DataFrame:
    """
    Drop rows that have no data for the *sample_column* and/or the *collection_column*, depending
    on which columns are configured. If both *sample_column* and *collection_column* are configured,
    drop rows if both columns don't have data.

    >>> drop_missing_barcodes(sample_column='sample', collection_column='collection', \
        parsed_manifest=pandas.DataFrame([['aa', 'bb', 'foo'], [None, 'dd', 'bar'], \
            ['ee', None, 'baz'], [None, None, 'fizz']], \
        columns=['sample', 'collection', 'other']))
      sample collection other
    0     aa         bb   foo
    1   None         dd   bar
    2     ee       None   baz

    >>> drop_missing_barcodes(sample_column='sample', collection_column=None, \
        parsed_manifest=pandas.DataFrame([['aa', 'bb', 'foo'], [None, 'dd', 'bar'], \
            ['ee', None, 'baz'], [None, None, 'fizz']], \
        columns=['sample', 'collection', 'other']))
      sample collection other
    0     aa         bb   foo
    2     ee       None   baz

    >>> drop_missing_barcodes(sample_column=None, collection_column='collection', \
        parsed_manifest=pandas.DataFrame([['aa', 'bb', 'foo'], [None, 'dd', 'bar'], \
            ['ee', None, 'baz'], [None, None, 'fizz']], \
        columns=['sample', 'collection', 'other']))
      sample collection other
    0     aa         bb   foo
    1   None         dd   bar
    """
    if sample_column and collection_column:
        parsed_manifest = parsed_manifest.dropna(subset = ["sample", "collection"], how='all')
    elif sample_column:
        parsed_manifest = parsed_manifest.dropna(subset = ["sample"])
    elif collection_column:
        parsed_manifest = parsed_manifest.dropna(subset = ["collection"])

    return parsed_manifest


def deduplicate_barcodes(df: pandas.DataFrame, columns: Iterable) -> pandas.DataFrame:
    """
    Check all barcode columns for duplicates and drops records that have
    duplicated barcodes.

    >>> deduplicate_barcodes(pandas.DataFrame([['aa', 'bb', 'foo'], ['aa', 'cc', 'bar']], \
        columns=['sample', 'collection', 'other']), columns=['sample', 'collection'])
    Empty DataFrame
    Columns: [sample, collection, other]
    Index: []

    >>> deduplicate_barcodes(pandas.DataFrame([['aa', 'bb', 'foo'], ['aa', 'cc', 'bar']], \
        columns=['sample', 'collection', 'other']), columns=['collection'])
      sample collection other
    0     aa         bb   foo
    1     aa         cc   bar

    >>> deduplicate_barcodes(pandas.DataFrame([['aa', 'bb', 'foo'], ['aa', 'cc', 'bar'], \
        ['bb', 'aa', 'baz']], columns=['sample', 'collection', 'other']), \
        columns=['sample', 'collection'])
      sample collection other
    2     bb         aa   baz
    """
    deduplicated = df

    for column in columns:
        # Drop null values so they don't get counted as duplicates
        col = df[column].dropna()

        # Find duplicates within column
        duplicates = col[col.duplicated(keep=False)]

        # If duplicates are found, drop rows with duplicate barcodes
        if len(duplicates) > 0:
            LOG.warning(f"Found duplicate barcodes in column «{column}»")
            dup_barcodes = list(duplicates.unique())
            LOG.warning(f"Duplicated barcodes: {dup_barcodes}")
            LOG.warning(f"Dropping records with duplicate barcodes")
            deduplicated_df = df[(~df[column].duplicated(keep=False)) \
                                | (df[column].isnull())][column].to_frame()
            common_idx = deduplicated.index.intersection(deduplicated_df.index)
            deduplicated = deduplicated.loc[common_idx]

    return deduplicated


def deephash(record):
    """
    Return a :class:`DeepHash` of the given manifest *record*, ignoring
    the provenance information.
    """
    return DeepHash(record, exclude_paths = {f"root['{PROVENANCE_KEY}']"})[record]


def dump_ndjson(iterable: Iterable) -> None:
    """
    :func:`print` *iterable* as a set of newline-delimited JSON records.
    """
    for item in iterable:
        print(as_json(item))


def load_ndjson(file: Iterable[str]) -> Iterable:
    """
    Load newline-delimited JSON records from *file*.
    """
    yield from (json.loads(line) for line in file)
