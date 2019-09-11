"""
Parse and upload clinical data.

Clinical data will contain PII (personally identifiable information) and
unnecessary information that does not need to be stored. This process will only
pull out specific columns of interest that will then be stored in the receiving
schema of ID3C.
"""
import click
import hashlib
import logging
import os
import pandas as pd
import seattleflu.db as db
from math import ceil
from seattleflu.db.session import DatabaseSession
from seattleflu.db.cli import cli
from . import add_metadata, barcode_quality_control, dump_ndjson, trim_whitespace


LOG = logging.getLogger(__name__)


@cli.group("clinical", help = __doc__)
def clinical():
    pass

# UW Clinical subcommand
@clinical.command("parse-uw")
@click.argument("uw_filename", metavar = "<UW Clinical Data filename>")
@click.argument("uw_nwh_file", metavar="<UW/NWH filename>")
@click.argument("hmc_sch_file", metavar="<HMC/SCH filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")


def parse_uw(uw_filename, uw_nwh_file, hmc_sch_file, output):
    """
    Process and insert clinical data from UW.

    Given a <UW Clinical Data filename> of an Excel document, selects specific
    columns of interest and inserts the queried data into the
    receiving.clinical table as a JSON document.

    Uses <UW/NWH filename> and <HMC/SCH filename> to join clinical data to fill
    in SFS barcodes.

    <UW/NWH filename> is the filepath to the data containing
    manifests of the barcodes from UWMC and NWH Samples.

    <HMC/SCH filename> is the filepath to the data containing manifests of the
    barcodes from HMC and SCH Retrospective Samples.

    <output filename> is the desired filepath of the output CSV of problematic
    barcodes encountered while parsing. If not provided, the problematic
    barcodes print to the log.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    clinical_records, uw_manifest, nwh_manifest, hmc_manifest = load_data(uw_filename,
        uw_nwh_file, hmc_sch_file)
    clinical_records = create_unique_identifier(clinical_records)
    clinical_records = standardize_identifiers(clinical_records)

    #Add barcode using manifests
    master_ref = hmc_manifest.append([uw_manifest, nwh_manifest], ignore_index=True)
    master_ref = standardize_identifiers(master_ref)
    master_ref['Barcode ID'] = master_ref['Barcode ID'].str.lower()

    #Join on MRN, Accession and Collection date
    clinical_records = clinical_records.merge(master_ref, how='left',
                  on=['MRN', 'Accession', 'Collection date'])

    # Standardize names of columns that will be added to the database
    column_map = {
        'Age': 'age',
        'Barcode ID': 'barcode',
        'EthnicGroup': 'HispanicLatino',
        'Fac': 'site',
        'FinClass': 'MedicalInsurance',
        'LabDtTm': 'encountered',
        'PersonID': 'individual',
        'Race': 'Race',
        'Sex': 'AssignedSex',
        'census_tract': 'census_tract',
        'fluvaccine': 'FluShot',
        'identifier': 'identifier',
    }

    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])
    # Age must be converted to Int64 dtype because pandas does not support NaNs
    # with normal type 'int'
    clinical_records["age"] = clinical_records["age"].astype(pd.Int64Dtype())

    # Subset df to drop missing barcodes
    clinical_records = clinical_records.loc[clinical_records['barcode'].notnull()]

    # Drop columns we're not tracking
    clinical_records = clinical_records[column_map.values()]

    # Remove PII
    clinical_records['age'] = clinical_records['age'].apply(age_ceiling)
    clinical_records['individual'] = clinical_records['individual'].apply(generate_hash)
    clinical_records['identifier'] = clinical_records['identifier'].apply(generate_hash)

    dump_ndjson(clinical_records)


def load_data(uw_filename: str, uw_nwh_file: str, hmc_sch_file: str):
    """
    Returns a pandas DataFrame containing clinical records from UW given the
    *uw_filename*.

    Returns a pandas DataFrame containing barcode manifest data from UWMC & NWH
    and SCH given the two filepaths *uw_nwh_file* and *hmc_sch_file*,
    respectively.
    """
    clinical_records = load_uw_metadata(uw_filename, date='Collection.Date')

    uw_manifest = load_uw_manifest_data(uw_nwh_file, 'UWMC', 'Collection Date')
    nwh_manifest = load_uw_manifest_data(uw_nwh_file, 'NWH',
                                      'Collection Date (per tube)')
    hmc_manifest = load_uw_manifest_data(hmc_sch_file, 'HMC', 'Collection date')

    return clinical_records, uw_manifest, nwh_manifest, hmc_manifest

def load_uw_metadata(uw_filename: str, date: str) -> pd.DataFrame:
    """
    Given a filename *uw_filename*, returns a pandas DataFrame containing
    clinical metadata.

    Standardizes the collection date column with some hard-coded logic that may
    need to be updated in the future.
    Removes leading and trailing whitespace from str-type columns.
    """
    dtypes = {'census_tract': 'str'}
    dates = ['Collection.Date']
    na_values = ['Unknown']

    if uw_filename.endswith('.csv'):
        df = pd.read_csv(uw_filename, na_values=na_values, parse_dates=dates,
                         dtype=dtypes)
    else:
        df = pd.read_excel(uw_filename, na_values=na_values, parse_dates=dates,
                           dtype=dtypes)

    df = df.rename(columns={date: 'Collection date'})
    df = trim_whitespace(df)
    df = add_metadata(df, uw_filename)

    return df


def load_uw_manifest_data(filename: str, sheet_name: str, date: str) -> pd.DataFrame:
    """
    Given a *filename* and *sheet_name*, returns a pandas DataFrame containing
    barcode manifest data.

    Renames collection *date* and barcode columns with some hard-coded logic
    that may need to be updated in the future.
    Removes leading and trailing whitespace from str-type columns.
    """
    barcode = 'Barcode ID (Sample ID)'
    dtypes = {barcode: 'str'}

    df = pd.read_excel(filename, sheet_name=sheet_name, keep_default_na=False,
        na_values=['NA', '', 'Unknown', 'NULL'], dtype=dtypes)

    rename_map = {
        barcode: 'Barcode ID',
        date: 'Collection date',
    }

    df = df.rename(columns=rename_map)
    df = trim_whitespace(df)

    return df[['Barcode ID', 'MRN', 'Collection date', 'Accession']]


def create_unique_identifier(df: pd.DataFrame):
    """Generate a unique identifier for each encounter and drop duplicates"""
    df['identifier'] = (df['labMRN'] + df['LabAccNum'] + \
                        df['Collection date'].astype(str)
                        ).str.lower()
    return df.drop_duplicates(subset="identifier")


def standardize_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all to lower case for matching purposes"""
    df['MRN'] = df['MRN'].str.lower()
    df['Accession'] = df['Accession'].str.lower()
    return df


def age_ceiling(age: float, max_age=90) -> float:
    """
    Given an *age*, returns the same *age* unless it exceeds the *max_age*, in
    which case the *max_age* is returned.
    """
    return min(age, max_age)

def generate_hash(identifier: str):
    """
    Generate hash for *identifier* that is linked to identifiable records.
    Must provide a "PARTICIPANT_DEIDENTIFIER_SECRET" as an OS environment
    variable.
    """
    secret = os.environ["PARTICIPANT_DEIDENTIFIER_SECRET"]
    new_hash = hashlib.sha256()
    new_hash.update(identifier.encode("utf-8"))
    new_hash.update(secret.encode("utf-8"))
    return new_hash.hexdigest()


@clinical.command("parse-sch")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse_sch(sch_filename, output):
    """
    Process and insert clinical data from SCH.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    dtypes = {'census_tract': 'str'}
    clinical_records = pd.read_csv(sch_filename, dtype=dtypes)
    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_metadata(clinical_records, sch_filename)

    # Standardize column names
    column_map = {
        "pat_id2": "individual",
        "study_id": "barcode",
        "drawndate": "encountered",
        "age": "age",
        "sex": "AssignedSex",
        "census_tract": "census_tract",
    }
    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Drop unnecessary columns
    clinical_records = clinical_records[column_map.values()]

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

    # Insert static value columns
    clinical_records["site"] = "SCH"

    #Create encounter identifier(individual+encountered)
    clinical_records["identifier"] = (clinical_records["individual"] + \
                        clinical_records["encountered"].astype(str)).str.lower()

    # Remove PII
    clinical_records['age'] = clinical_records['age'].apply(age_ceiling)
    clinical_records["individual"] = clinical_records["individual"].apply(generate_hash)
    clinical_records["identifier"] = clinical_records["identifier"].apply(generate_hash)

    # Placeholder columns for future data
    clinical_records["FluShot"] = None
    clinical_records["Race"] = None
    clinical_records["HispanicLatino"] = None
    clinical_records["MedicalInsurace"] = None

    dump_ndjson(clinical_records)


@clinical.command("upload")
@click.argument("clinical_file",
    metavar = "<clinical.ndjson>",
    type = click.File("r"))

def upload(clinical_file):
    """
    Upload clinical records into the database receiving area.

    <clinical.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling commands.

    Once records are uploaded, the clinical ETL routine will reconcile the
    clinical records with known sites, individuals, encounters and samples.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying clinical records from {clinical_file.name}")

        row_count = db.copy_from_ndjson(("receiving", "clinical", "document"), clinical_file)

        LOG.info(f"Received {row_count:,} clinical records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise
