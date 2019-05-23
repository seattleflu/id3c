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
from deepdiff import DeepHash
from hashlib import sha1
from os import chdir
from os.path import dirname
from typing import Iterable, List
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json, Json
from seattleflu.utils import format_doc


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
        ---
        workbook: OneDrive/SFS Retrospective Samples 2018-2019.xlsx
        sheet: HMC
        columns:
          sample: "Barcode ID"
          aliquots: "Aliquot [ABC]"
          date: "Collection date*"
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
                "workbook":          config["workbook"],
                "sheet":             config["sheet"],
                "sample_column":     config["columns"]["sample"],
                "collection_column": config["columns"].get("collection"),
                "aliquot_columns":   config["columns"].get("aliquots"),
                "date_column":       config["columns"].get("date"),
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
           date_column = None):
    """
    Internal function powering :func:`parse` and :func:`parse_using_config`.
    """
    LOG.debug(f"Reading Excel workbook «{workbook}», sheet «{sheet}»")

    # All columns are read as strings so that we can type values at load time.
    manifest = pandas.read_excel(workbook, sheet_name = sheet, dtype = str)

    LOG.debug(f"Columns in manifest: {list(manifest.columns)}")

    column_map = {
        find_one_column(manifest, sample_column): "sample",
    }

    if collection_column:
        column_map.update({
            find_one_column(manifest, collection_column): "collection" })

    if aliquot_columns:
        column_map.update({
            aliquot_column: "aliquot"
                for aliquot_column
                 in find_columns(manifest, aliquot_columns) })

    if date_column:
        column_map.update({
            find_one_column(manifest, date_column): "date" })

    LOG.debug(f"Column map: {column_map}")

    # Select just our columns of interest (renamed to our mapped output names),
    # strip leading/trailing spaces from values, replace missing values
    # (numpy's NaN) and empty strings (possibly from stripping) with None so
    # they are converted to null in JSON, and drop rows with null sample values
    # (which may be introduced by stripping).
    manifest = manifest \
        .filter(items = column_map.keys()) \
        .rename(columns = column_map) \
        .apply(lambda column: column.str.strip()) \
        .replace({ pandas.np.nan: None, "": None }) \
        .dropna(subset = ["sample"])

    # Combine individual aliquot columns into one list-valued column
    if aliquot_columns:
        manifest["aliquots"] = manifest.aliquot.apply(list, axis = "columns")
        manifest.drop(columns = manifest.aliquot, inplace = True)

    # Add internal provenance metadata for data tracing
    digest = sha1sum(workbook)

    manifest[PROVENANCE_KEY] = list(
        map(lambda index: {
                "workbook": workbook,
                "sha1sum": digest,
                "sheet": sheet,
                # Account for header row and convert from 0-based to 1-based indexing
                "row": index + 2,
            }, manifest.index))

    # Return a standard list of dicts instead of a DataFrame
    return manifest.to_dict(orient = "records")


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
        for document in (line.strip() for line in manifest_file):
            LOG.debug(f"Processing sample manifest record: {document}")

            manifest = db.fetch_row("""
               insert into receiving.manifest (document) values (%s)
                   returning manifest_id as id
               """, (document,))

            LOG.info(f"Created received manifest record {manifest.id}")

        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise


def find_one_column(table: pandas.DataFrame, name: str) -> str:
    """
    Find the single column matching *name* in *table*.

    *table* must be a :class:`pandas.DataFrame`.

    *name* must be a string, which may contain shell-style wildcards and
    pattern matching.

    Matching is performed case-insensitively.  An `AssertionError` is raised if
    no columns are found or if more than one column is found.

    Returns a column name from *table*.
    """
    matches = find_columns(table, name)

    assert len(matches) == 1, f"More than one column name matching «{name}»: {matches}"
    return matches[0]


def find_columns(table: pandas.DataFrame, name: str) -> List[str]:
    """
    Find one or more columns matching *name* in *table*.

    *table* must be a :class:`pandas.DataFrame`.

    *name* must be a string, which may contain shell-style wildcards and
    pattern matching.

    Matching is performed case-insensitively.  An `AssertionError` is raised if
    no columns are found.

    Returns a list of column names from *table*.
    """
    pattern = re.compile(fnmatch.translate(name), re.IGNORECASE)
    matches = list(filter(pattern.match, table.columns))

    assert matches, f"No column name matching «{name}» found; column names are: {list(table.columns)}"
    return matches


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
