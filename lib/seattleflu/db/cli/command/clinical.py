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
import re
import pandas as pd
import seattleflu.db as db
from math import ceil
from seattleflu.db.session import DatabaseSession
from seattleflu.db.cli import cli
from . import (
    add_metadata,
    barcode_quality_control,
    dump_ndjson,
    group_true_values_into_list,
    trim_whitespace,
)


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



@clinical.command("parse-kp")
@click.argument("kp_filename", metavar = "<KP Clinical Data filename>")
@click.argument("kp_specimen_manifest_filename", metavar = "<KP Specimen Manifest filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse_kp(kp_filename, kp_specimen_manifest_filename, output):
    """
    Process and insert clinical data from KP.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    clinical_records = pd.read_csv(kp_filename)
    clinical_records.columns = clinical_records.columns.str.lower()

    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_metadata(clinical_records, kp_filename)
    clinical_records = add_kp_manifest_data(clinical_records, kp_specimen_manifest_filename)

    clinical_records = convert_numeric_columns_to_binary(clinical_records)
    clinical_records = rename_symptoms_columns(clinical_records)
    clinical_records = collapse_columns(clinical_records, 'symptom')
    clinical_records = collapse_columns(clinical_records, 'race')

    clinical_records['FluShot'] = clinical_records['fluvaxdt'].notna()

    column_map = {  # Missing census_tract
        "enrollid": "individual",
        "enrolldate": "encountered",
        "barcode": "barcode",
        "age": "age",
        "sex": "AssignedSex",
        "race": "Race",
        "hispanic": "HispanicLatino",
        "symptom": "Symptoms",
        "FluShot": "FluShot",
    }
    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Drop unnecessary columns
    clinical_records = clinical_records[column_map.values()]

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

    # Insert static value columns
    clinical_records["site"] = "KP"

    #Create encounter identifier (individual + encountered)
    clinical_records["identifier"] = (clinical_records["individual"] + \
        clinical_records["encountered"].astype(str)).str.lower()

    # Remove PII
    clinical_records['age'] = clinical_records['age'].apply(age_ceiling)
    clinical_records["individual"] = clinical_records["individual"].apply(generate_hash)
    clinical_records["identifier"] = clinical_records["identifier"].apply(generate_hash)

    # Placeholder columns for future data.
    # See https://seattle-flu-study.slack.com/archives/CCAA9RBFS/p1568156642033700?thread_ts=1568145908.029300&cid=CCAA9RBFS
    clinical_records["MedicalInsurace"] = None

    dump_ndjson(clinical_records)


def add_kp_manifest_data(df: pd.DataFrame, manifest_filename: str) -> pd.DataFrame:
    """
    Join the specimen manifest data from the given *manifest_filename* with the
    given clinical records DataFrame *df*
    """
    barcode = 'Barcode ID (Sample ID)'
    dtypes = {barcode: str}

    manifest_data = pd.read_excel(manifest_filename, sheet_name='KP', dtype=dtypes)

    regex = re.compile(r"^KP-([0-9]{6,})-[0-9]$", re.IGNORECASE)
    manifest_data.kp_id = manifest_data.kp_id.apply(lambda x: regex.sub('WA\\1', x))

    rename_map = {
        barcode: 'barcode',
        'kp_id': 'enrollid',
    }

    manifest_data = manifest_data.rename(columns=rename_map)
    manifest_data = trim_whitespace(manifest_data)

    return df.merge(manifest_data[['barcode', 'enrollid']], how='left')


def convert_numeric_columns_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    """
    In a given DataFrame *df* of clinical records, convert a hard-coded list of
    columns from numeric coding to binary.

    See Kaiser Permanente data dictionary for details
    """
    numeric_columns = [
        'runnynose',
        'hispanic',
        'racewhite',
        'raceblack',
        'raceasian',
        'raceamerind',
        'racenativehi',
    ]
    for col in numeric_columns:
        df.loc[df[col] > 1, col] = None

    return df


def rename_symptoms_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Renames the hard-coded symptoms columns in a given DataFrame *df* """
    symptoms_columns = [
        'fever',
        'sorethroat',
        'runnynose',
        'cough'
    ]

    symptoms_map = {}
    for symptom in symptoms_columns:
        symptoms_map[symptom] = 'symptom' + symptom

    return df.rename(columns=symptoms_map)


def collapse_columns(df: pd.DataFrame, stub: str, pid='enrollid') -> pd.DataFrame:
    """
    Given a pandas DataFrame *df* of clinical records, collapses the 0/1
    encoding of multiple race options into a single array in a resulting
    column called "Race". Removes the original "Race*" option columns. Returns
    the new DataFrame.
    """
    stub_data = df.filter(regex=f'{pid}|{stub}*', axis=1)
    stub_columns = list(stub_data)
    stub_columns.remove(pid)

    df = df.drop(columns=stub_columns)

    stub_data_long = pd.wide_to_long(stub_data, stub, i=pid, j=f"new_{stub}",
                        suffix='\\w+').reset_index()

    stub_data_new = group_true_values_into_list(stub_data_long, stub, [pid])

    return df.merge(stub_data_new, how='left')


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
