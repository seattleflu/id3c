"""
Pre-process clinical data.

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


LOG = logging.getLogger(__name__)


@cli.group("preprocess", help = __doc__)
def preprocess():
    pass

# UW Clinical subcommand
@preprocess.command("uw-clinical")
@click.argument("uw_filename", metavar = "<UW Clinical Data filename>")
@click.argument("uw_nwh_file", metavar="<UW/NWH filename>")
@click.argument("hmc_sch_file", metavar="<HMC/SCH filename>")
@click.option("-o", "--output", metavar="<output filename>", 
    help="The filename for the output of missing barcodes")


def uw_clinical(uw_filename, uw_nwh_file, hmc_sch_file, output):
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

    # Perform quality control
    missing_barcodes = missing_barcode(clinical_records)
    duplicated_barcodes = duplicated_barcode(clinical_records)

    print_problem_barcodes(pd.concat([missing_barcodes, duplicated_barcodes],
                                 ignore_index=True), output)

    assert len(duplicated_barcodes) == 0, "You have duplicated barcodes!"

    # Subset df to drop missing barcodes
    clinical_records = clinical_records.loc[clinical_records['barcode'].notnull()]

    # Drop columns we're not tracking
    clinical_records = clinical_records[column_map.values()]

    # Hash PersonID and encounter identifier (MRN+Accession+Collection date)
    clinical_records['individual'] = clinical_records['individual'].apply(generate_hash)
    clinical_records['identifier'] = clinical_records['identifier'].apply(generate_hash)
    
    session = DatabaseSession()
    with session, session.cursor() as cursor:
        clinical_records.apply(lambda x: insert_clinical(x, cursor), axis=1)


def load_data(uw_filename: str, uw_nwh_file: str, hmc_sch_file: str): 
    """
    Returns a pandas DataFrame containing clinical records from UW given the 
    *uw_filename*.

    Returns a pandas DataFrame containing barcode manifest data from UWMC & NWH
    and SCH given the two filepaths *uw_nwh_file* and *hmc_sch_file*, 
    respectively.
    """
    clinical_records = load_uw_metadata(uw_filename)
    clinical_records = clinical_records \
                        .rename(columns={'Collection.Date': 'Collection date'})
    clinical_records['Collection date'] = pd.to_datetime(clinical_records['Collection date'])
    # Convert datatype in 'age' column because
    # Pandas does not support NaNs with normal type 'int'
    clinical_records['Age'] = clinical_records['Age'].astype(pd.Int64Dtype())

    uw_manifest = load_manifest_data(uw_nwh_file, 'UWMC')
    uw_manifest = uw_manifest.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID',
                                  'Collection Date': 'Collection date'})

    nwh_manifest = load_manifest_data(uw_nwh_file, 'NWH')
    nwh_manifest = nwh_manifest.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID',
                                    'Collection Date (per tube)': 'Collection date'})

    hmc_manifest = load_manifest_data(hmc_sch_file, 'HMC')

    return clinical_records, uw_manifest, nwh_manifest, hmc_manifest

def load_uw_metadata(uw_filename: str) -> pd.DataFrame:
    """ 
    Given a filename *uw_filename*, returns a pandas DataFrame containing
    clinical metadata.
    """
    if uw_filename.endswith('.csv'):
        df = pd.read_csv(uw_filename)
    else:
        df = pd.read_excel(uw_filename)
    return df

def load_manifest_data(filename: str, sheet_name: str) -> pd.DataFrame:
    """
    Given a *filename* and *sheet_name*, returns a pandas DataFrame containing
    barcode manifest data
    """
    df = pd.read_excel(filename, sheet_name=sheet_name, keep_default_na=False,
        na_values=['NA', '', 'Unknown', 'NULL'])
    return df.filter(regex=("Barcode ID|MRN|Collection [Dd]ate|Accession"))


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


def missing_barcode(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Given a pandas DataFrame *df*, returns a DataFrame with missing barcodes and
    a description of the problem.
    """
    missing_barcodes = df.loc[df['barcode'].isnull()].copy()
    missing_barcodes['problem'] = 'Missing barcode'

    return missing_barcodes[['MRN', 'Accession', 'barcode', 'problem']]


def duplicated_barcode(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Given a pandas DataFrame *df*, returns a DataFrame with duplicated barcodes
    and a description of the problem.
    """ 
    duplicates = pd.DataFrame(df.barcode.value_counts())
    duplicates = duplicates[duplicates['barcode'] > 1]
    duplicates = pd.Series(duplicates.index)

    duplicated_barcodes = df[df['barcode'].isin(duplicates)].copy()
    duplicated_barcodes['problem'] = 'Barcode is not unique'

    return duplicated_barcodes[['MRN', 'Accession', 'barcode', 'problem']]


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


def insert_clinical(df: pd.DataFrame, cursor):
    """
    Given a pandas DataFrame, inserts it as a JSON document into the 
    receiving.clinical table 
    """
    
    LOG.debug(f"Inserting clinical data for barcode: «{df['barcode']}» ")

    document = df.to_json(date_format='iso')

    cursor.execute(
        "insert into receiving.clinical (document) values (%s)",
        (document,)
    )

def print_problem_barcodes(problem_barcodes: pd.DataFrame, output: str):
    """
    Given a pandas DataFrame of *problem_barcodes*, writes the data to
    stdout unless a filename *output* is given.
    """ 
    if output:
        problem_barcodes.to_csv(output, index=False)
    else:
        print(problem_barcodes.to_csv(index=False))


@preprocess.command("sch-clinical")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")

def sch_clinical(sch_filename):
    """
    Process and insert clinical data from SCH.
    """
    df = pd.read_csv(sch_filename)
    
    # Drop unnecessary columns
    columns_to_keep = ["pat_id", "study_id", "sample_date", 
                       "pat_age", "pat_sex"]
    df = df[columns_to_keep]
    
    # Standardize column names
    df = df.rename(columns={"pat_id": "individual",
                            "study_id": "barcode",
                            "sample_date": "encountered",
                            "pat_age": "age",
                            "pat_sex": "AssignedSex"})

    # Convert to date time format
    df["encountered"] = pd.to_datetime(df["encountered"])

    # Insert static value columns
    df["site"] = "SCH"

    #Create encounter identifier(individual+encountered)
    df["identifier"] = (df["individual"] + \
                        df["encountered"].astype(str)).str.lower()

    #Hash individual and encounter identifiers
    df["individual"] = df["individual"].apply(generate_hash)
    df["identifier"] = df["identifier"].apply(generate_hash)


    # Placeholder columns for future data
    df["FluShot"] = None
    df["Race"] = None
    df["HispanicLatino"] = None
    df["MedicalInsurace"] = None
    df["census_tract"] = None
   
    session = DatabaseSession()
    with session, session.cursor() as cursor:
        df.apply(lambda x: insert_clinical(x, cursor), axis=1)
