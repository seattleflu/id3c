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
from typing import Iterable, List
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json, Json
from seattleflu.utils import format_doc
from seattleflu.db.cli.command import qc_barcodes


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

@click.option("--aliquot-columns",
    metavar = "<column>",
    help = "Name of the, possibly multiple, columns containing aliquot barcodes.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--collection-column",
    metavar = "<column>",
    help = "Name of the single column containing collection barcodes.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--date-column",
    metavar = "<column>",
    help = "Name of the single column containing a collection date.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--aliquot-date-column",
    metavar = "<column>",
    help = "Name of the single column containing an aliquot date.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--rack-columns",
    metavar = "<column>",
    help = "Name of the, possibly multiple, columns containing rack identifiers.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--test-results-column",
    metavar = "<column>",
    help = "Name of the single column containing test results.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--pcr-result-column",
    metavar = "<column>",
    help = "Name of the single column containing rapid PCR results.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--notes-column",
    metavar = "<column>",
    help = "Name of the single column containing additional information.  "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--kit-column",
    metavar = "<column>",
    help = "Name of the single column containing additional information. "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--test-strip-column",
    metavar = "<column>",
    help = "Name of the single column containing test strip barcodes. "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--test-origin-column",
    metavar = "<column>",
    help = "Name of the single column containing test origin. "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--arrival-date-column",
    metavar = "<column>",
    help = "Name of the single column containing the sample arrival date. "
           "Must match exactly; shell-style glob patterns are supported.",
    required = False)

@click.option("--sample-type",
    metavar = "<type>",
    help = "The type of sample within this manifest. "
           "Only applicable to samples from self-test kits.",
    type=click.Choice(["utm", "rdt"]),
    required = False)

def parse(**kwargs):
    """
    Parse a single manifest workbook sheet.

    <manifest.xlsx> must be an Excel workbook with at least one sheet in it,
    identified by name using the required option --sheet.

    Other options specify columns to extract into the manifest records.  Of
    these, only --sample-column is required.

    Manifest records are output to stdout as newline-delimited JSON records.
    You will likely want to redirect stdout to a file.
    """
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
        columns:
          collection: "Collection ID*"
          sample: "Barcode ID*"
          aliquots: "Aliquot [ABC]"
          date: "Collection date*"
          aliquot_date: "Date aliquoted"
          racks: "Rack [ABC]*"
          notes: "Notes"
    \b
        ---
        workbook: OneDrive/SFS Retrospective Samples 2018-2019.xlsx
        sheet: HMC
        columns:
          sample: "Barcode ID"
          aliquots: "Aliquot [ABC]"
          date: "Collection date*"
          aliquot_date: "Date aliquoted"
          racks: "Rack [ABC]"
          test_results: "Test ResulTS"
        ...

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
                "workbook":             config["workbook"],
                "sheet":                config["sheet"],
                "sample_column":        config["columns"]["sample"],
                "collection_column":    config["columns"].get("collection"),
                "aliquot_columns":      config["columns"].get("aliquots"),
                "date_column":          config["columns"].get("date"),
                "aliquot_date_column":  config["columns"].get("aliquot_date"),
                "rack_columns":         config["columns"].get("racks"),
                "test_results_column":  config["columns"].get("test_results"),
                "pcr_result_column":    config["columns"].get("pcr_result"),
                "notes_column":         config["columns"].get("notes"),
                "kit_column":           config["columns"].get("kit"),
                "test_strip_column":    config["columns"].get("test_strip"),
                "test_origin_column":   config["columns"].get("test_origin"),
                "arrival_date_column":  config["columns"].get("arrival_date"),
                "sample_type":          config.get("sample_type")
            }
        except KeyError as key:
            LOG.error(f"Required key «{key}» missing from config {config}")
            raise key from None

        dump_ndjson(_parse(**kwargs))


def _parse(*,
           workbook,
           sheet,
           sample_column,
           aliquot_columns = None,
           collection_column = None,
           date_column = None,
           aliquot_date_column = None,
           rack_columns = None,
           test_results_column = None,
           pcr_result_column = None,
           notes_column = None,
           kit_column = None,
           test_strip_column = None,
           test_origin_column = None,
           arrival_date_column = None,
           sample_type = None):
    """
    Internal function powering :func:`parse` and :func:`parse_using_config`.
    """
    LOG.debug(f"Reading Excel workbook «{workbook}», sheet «{sheet}»")

    # All columns are read as strings so that we can type values at load time.
    manifest = pandas.read_excel(workbook, sheet_name = sheet, dtype = str)
    LOG.debug(f"Columns in manifest: {list(manifest.columns)}")

    # Strip leading/trailing spaces from values and replace missing values
    # (numpy's NaN) and empty strings (possibly from stripping) with None so
    # they are converted to null in JSON.
    manifest = manifest \
        .apply(lambda column: column.str.strip()) \
        .replace({ pandas.np.nan: None, "": None })

    # Construct parsed manifest by copying columns from source to destination.
    # This approach is used to allow the same source column to end up as
    # multiple destination columns.
    parsed_manifest = pandas.DataFrame()

    parsed_manifest["sample"] = select_column(manifest, sample_column)

    single_columns = {
        "collection": collection_column,
        "kit": kit_column,
        "date": date_column,
        "aliquot_date": aliquot_date_column,
        "test_results": test_results_column,
        "pcr_result": pcr_result_column,
        "notes": notes_column,
        "test_strip": test_strip_column,
        "test_origin": test_origin_column,
        "arrival_date": arrival_date_column }

    for dst, src in single_columns.items():
        if src:
            parsed_manifest[dst] = select_column(manifest, src)

    group_columns = {
        "aliquots": aliquot_columns,
        "racks": rack_columns }

    for dst, src in group_columns.items():
        if src:
            parsed_manifest[dst] = select_columns(manifest, src).apply(list, axis="columns")

    # Drop rows with null sample values, which may be introduced by space
    # stripping.
    parsed_manifest = parsed_manifest.dropna(subset = ["sample"])

    # Set of columns names for barcodes
    barcode_columns = {"sample", "collection", "kit", "test_strip"}

    # Drop any rows that have duplicated barcodes
    parsed_manifest = qc_barcodes(parsed_manifest, barcode_columns)

    # Add sample type for kit related samples
    if sample_type:
        parsed_manifest["sample_type"] = sample_type

    # Add internal provenance metadata for data tracing
    digest = sha1sum(workbook)

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
    type = click.File("r"))

@click.argument("manifest_b",
    metavar = "<manifest-b.ndjson>",
    type = click.File("r"))

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
    type = click.File("r"))

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
        LOG.info(f"Copying sample manifest records from {manifest_file.name}")

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


def select_columns(table: pandas.DataFrame, name: str) -> List[str]:
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


def sha1sum(path: str) -> str:
    """
    Compute the SHA-1 digest (as a hexadecimal string) of the file at *path*.
    """
    digest = sha1()

    # Rather arbitrary!
    chunk_size = 4096

    with open(path, "rb") as data:
        for chunk in iter(lambda: data.read(chunk_size), b""):
            digest.update(chunk)

    return digest.hexdigest()


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
