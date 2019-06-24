"""
Parse and upload self-test kit results
"""
import re
import click
import logging
import yaml
import pandas as pd
import seattleflu.db as db
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json
from seattleflu.db.cli.command import dump_ndjson, qc_barcodes


LOG = logging.getLogger(__name__)


@cli.group("kit-result", help = __doc__)
def kit_result():
    pass

@kit_result.command("parse-using-config")
@click.argument("config_file",
    metavar = "<config.yaml>",
    type = click.File("r"))


def parse_using_config(config_file):
    """
    Parse self-test kit results from manifest using columns listed in <config.yaml>

    <config.yaml> must be a file with one YAML document in it.
    Example document:

    \b
        ---
        workbook: OneDrive/Self-Test Samples 2018-2019.xlsx
        sheet: Sheet1
        columns:
          kit_barcode: "Box Barcode ID"
          test_date: "Cepheid Test date"
          strip_barcode: "Strip Barcode"
          strip_flu_a: "Strip Flu A"
          strip_flu_b: "Strip Flu B"
          rdt_barcode: "RDT Barcode"
          rdt_flu_a: "RDT Flu A"
          rdt_flu_b: "RDT Flu B"
          rdt_rsv: "RDT RSV"
          utm_barcode: "UTM Sample Barcode "
          utm_flu_a: "UTM Flu A"
          utm_flu_b: "UTM Flu B"
          utm_rsv: "UTM RSV"

    Kit result records are output to stdout as newline-delimited JSON records.
    You will likely want to redirect stdout to a file.
    """
    config = yaml.safe_load(config_file)

    try:
        kit_result = {
            "workbook": config["workbook"],
            "sheet": config["sheet"],
            "kit_barcode": config["columns"]["kit_barcode"],
            "test_date": config["columns"]["test_date"],
            "strip_barcode": config["columns"]["strip_barcode"],
            "strip_flu_a": config["columns"]["strip_flu_a"],
            "strip_flu_b": config["columns"]["strip_flu_b"],
            "rdt_barcode": config["columns"]["rdt_barcode"],
            "rdt_flu_a": config["columns"]["rdt_flu_a"],
            "rdt_flu_b": config["columns"]["rdt_flu_b"],
            "rdt_rsv": config["columns"]["rdt_rsv"],
            "utm_barcode": config["columns"]["utm_barcode"],
            "utm_flu_a": config["columns"]["utm_flu_a"],
            "utm_flu_b": config["columns"]["utm_flu_b"],
            "utm_rsv": config["columns"]["utm_rsv"]
        }
    except KeyError as key:
        LOG.error(f"Required key «{key}» missing from config")
        raise key from None

    barcode_columns = [v for k,v in kit_result.items() if "barcode" in k]

    manifest = load_manifest_data(
        workbook = kit_result["workbook"],
        sheet = kit_result["sheet"],
        barcode_columns = barcode_columns)

    strip = subset_manifest(manifest, "strip", kit_result)

    rdt = subset_manifest(manifest, "rdt", kit_result)

    utm = subset_manifest(manifest, "utm", kit_result)

    for i in manifest.index:
        print(as_json(strip.loc[i].to_dict()))
        print(as_json(rdt.loc[i].to_dict()))
        print(as_json(utm.loc[i].to_dict()))


def load_manifest_data(workbook: str, sheet:str, barcode_columns: list) -> tuple:
    """
    Load the manifest given the *workbook* and *sheet*.
    Will drop any records that have NA or duplicated values for the given *barcode_columns*.
    """
    replacements = {
        pd.np.nan: None,
        "": None,
        "NA": None,
        "N/A": None,
        "?": None
    }

    manifest = pd.read_excel(workbook, sheet_name = sheet, dtype=str)
    manifest = manifest \
        .apply(lambda column: column.str.strip()) \
        .replace(replacements)

    LOG.debug(f"Columns in manifest: {list(manifest.columns)}")

    manifest = qc_barcodes(manifest, barcode_columns)\
        .dropna(subset=barcode_columns)

    return manifest


def subset_manifest(manifest: pd.DataFrame,
                    subset_type: str,
                    config: dict) -> pd.DataFrame:
    """
    Return a subset dataframe from *manifest* that has the *subset_type*
    in the column names provided in the *config*.

    The returned subset should include barcode and test results.
    """
    included_columns = [v for k,v in config.items() if k.startswith(subset_type)]
    included_columns.append(config["kit_barcode"])
    included_columns.append(config["test_date"])

    subset = manifest[included_columns]
    subset = standardize_columns(subset)
    subset["type"] = subset_type

    return subset


def standardize_columns(subset: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the column names in *subset*.

    This is done on the *subset* instead of the manifest because
    the test result columns will all have the same name.
    """
    columns = {
        r".* Flu\s?A":         "Flu_A",
        r".* Flu\s?B":         "Flu_B",
        r".* RSV*":            "RSV",
        r"Box Barcode ID":     "kit_barcode",
        r"Cepheid Test date":  "test_date",
        r"RDT Barcode":        "barcode",
        r"UTM Sample Barcode":    "barcode",
        r"Strip Barcode":      "barcode",
    }

    column_map = {}

    for column in columns.keys():
        pattern = re.compile(column, re.IGNORECASE)
        match = list(filter(pattern.match, subset.columns))
        if match:
            assert len(match) == 1, \
                f"More than one column name matching «{column}»: {match}"
            column_map[match[0]] = columns[column]

    return subset.rename(columns = column_map)

@kit_result.command("upload")
@click.argument("kit_result_file",
    metavar = "<kit_result.ndjson>",
    type = click.File("r"))

def upload(kit_result_file):
    """
    Upload kit_result records into the database receiving area.

    <kit_result.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling command.

    Once records are uploaded, the kit_result ETL routine will reconcile the
    kit_result records with known identifiers and existing kits/samples.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying kit result records from {kit_result_file.name}")

        row_count = db.copy_from_ndjson(("receiving", "kit_result", "document"), kit_result_file)

        LOG.info(f"Received {row_count:,} kit result records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise
