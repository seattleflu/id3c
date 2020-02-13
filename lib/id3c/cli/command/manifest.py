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
from typing import Iterable, List, Tuple, Union
from id3c.cli import cli
from id3c.cli.io import LocalOrRemoteFile, urlopen
from id3c.cli.io.pandas import read_excel
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json, Json
from id3c.utils import format_doc


LOG = logging.getLogger(__name__)

PROVENANCE_KEY = "_provenance"


@cli.group("manifest", help = __doc__)
def manifest():
    pass


@manifest.command("parse")
@click.argument("workbook", metavar = "<manifest.xlsx>")

@click.option("--sheet",
    metavar = "<name>",
    help = "Name of the workbook sheet to read",
    required = True)

@click.option("--sample-column",
    metavar = "<column>",
    help = "Name of the single column containing sample barcodes.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = True)

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

def parse(**kwargs):
    """
    Parse a single manifest workbook sheet.

    <manifest.xlsx> must be a path or URL to an Excel workbook with at least
    one sheet in it, identified by name using the required option --sheet.
    Supported URL schemes include http[s]:// and s3://, as well as others.

    The required --sample-column option specifies the name of the column
    containing the sample barcode.  Other columns may be extracted into the
    manifest records as desired using the --extra-column option.

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
                "sample_column": config["sample_column"],
                "extra_columns": list(config.get("extra_columns", {}).items()),
                "sample_type": config.get("sample_type")
            }
        except KeyError as key:
            LOG.error(f"Required key «{key}» missing from config {config}")
            raise key from None

        dump_ndjson(_parse(**kwargs))


def _parse(*,
           workbook,
           sheet,
           sample_column,
           extra_columns: List[Tuple[str, Union[str, dict]]] = [],
           sample_type = None):
    """
    Internal function powering :func:`parse` and :func:`parse_using_config`.
    """
    LOG.debug(f"Reading Excel workbook «{workbook}»")

    with urlopen(workbook, "rb") as file:
        workbook_bytes = file.read()

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
                .replace({"": None})))

    # Construct parsed manifest by copying columns from source to destination.
    # This approach is used to allow the same source column to end up as
    # multiple destination columns.
    parsed_manifest = pandas.DataFrame()

    column_map: List[Tuple[str, dict]] = [
        ("sample", {"name": sample_column, "barcode": True})]

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

    # Drop rows with null sample values, which may be introduced by space
    # stripping.
    parsed_manifest = parsed_manifest.dropna(subset = ["sample"])

    # Set of columns names for barcodes
    barcode_columns = {dst for dst, src in column_map if src.get("barcode")}

    # Drop any rows that have duplicated barcodes
    parsed_manifest = qc_barcodes(parsed_manifest, barcode_columns)

    # Add sample type for kit related samples
    if sample_type:
        parsed_manifest["sample_type"] = sample_type

    # Add internal provenance metadata for data tracing
    digest = sha1(workbook_bytes).hexdigest()

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
    matches = list(filter(pattern.match, table.columns))

    assert matches, f"No column name matching «{name}» found; column names are: {list(table.columns)}"
    return table[matches]


def qc_barcodes(df: pandas.DataFrame, columns: Iterable) -> pandas.DataFrame:
    """
    Check all barcode columns for duplicates and drops records that have
    duplicated barcodes.
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
            dup_barcodes = duplicates.unique().tolist()
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
