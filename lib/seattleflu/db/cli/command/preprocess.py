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

    Given a *uw_filename* to an Excel document, selects specific columns of 
    interest and inserts the queried data into the receiving.clinical table as 
    a JSON document.

    Specifically looks for a sheet named 'pts' and assumes that each row
    represents one encounter.

    Uses *uw_nwh_file* and *hmc_sch_file* to join clinical data to fill in
    SFS barcodes. *uw_nwh_file* is the filepath to the data containing 
    manifests of the barcodes from UWMC and NWH Samples. *hmc_sch_file* is the
    filepath to the data containing manifests of the barcodes from SFS 
    Retrospective Samples.

    """
    df, uw_df, nwh_df, hmc_df = load_data(uw_filename, uw_nwh_file, hmc_sch_file)

    df = create_unique_identifier(df)

    #Add barcode using manifests
    master_ref = hmc_df.append([uw_df, nwh_df], ignore_index=True)

    master_ref = standardize_identifiers(master_ref)
    df = standardize_identifiers(df)
    master_ref['Barcode ID'] = master_ref['Barcode ID'].str.lower()

    # TODO: how to deal with duplicate MRN/Accession values that are not null?
    # temp = master_ref[['MRN', 'Accession']]
    # duplicated_refs = temp.duplicated(subset=['MRN', 'Accession'])

    #Join on MRN, Accession and Collection date
    df = df.merge(master_ref, how='left', 
                  on=['MRN', 'Accession', 'Collection date'])

    # Standardize column names and drop all other columns
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

    df = df.rename(columns=column_map)
    df = drop_columns(column_map.values(), df)

    # Subset df to drop missing barcodes
    # Handle these missing barcodes separately 
    missing_barcodes = missing_barcode(df)
    df = df.loc[df['barcode'].notnull()]
    
    problem_barcodes = pd.concat([missing_barcodes, duplicated_barcode(df)], 
                                 ignore_index=True)
    print_problem_barcodes(problem_barcodes, output)

    # Subset to drop duplicate barcodes, keeping first instance
    # df = df.drop_duplicates(subset='barcode')
    assert len(df) == len(df.barcode.unique()), "You have duplicated barcodes"

    # Hash PersonID and encounter identifier(MRN+Accession)
    df['individual'] = df['individual'].apply(generate_hash)
    df['identifier'] = df['identifier'].apply(generate_hash)
    
    session = DatabaseSession()
    with session, session.cursor() as cursor:
        df.apply(lambda x: insert_clinical(x, cursor), axis=1)


def load_data(uw_filename, uw_nwh_file, hmc_sch_file):
    """ """
    df = load_uw_metadata(uw_filename)
    df = df.rename(columns={'Collection.Date': 'Collection date'})
    df['Collection date'] = pd.to_datetime(df['Collection date'])
    # Convert datatype in 'age' column because
    # Pandas does not support NaNs with normal type 'int'
    df['Age'] = df['Age'].astype(pd.Int64Dtype())

    uw_df = load_uw_manifest_data(uw_nwh_file)
    uw_df = uw_df.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID',
                                  'Collection Date': 'Collection date'})

    nwh_df = load_nwh_manifest_data(uw_nwh_file)
    nwh_df = nwh_df.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID',
                                    'Collection Date (per tube)': 'Collection date'})

    hmc_df = load_hmc_manifest_data(hmc_sch_file)

    return df, uw_df, nwh_df, hmc_df

def load_uw_metadata(uw_filename: str) -> pd.DataFrame:
    if uw_filename.endswith('.csv'):
        df = pd.read_csv(uw_filename)
    else:
        df = pd.read_excel(uw_filename)
    return df

def load_uw_manifest_data(uw_nwh_file: str) -> pd.DataFrame:
    return pd.read_excel(uw_nwh_file, sheet_name='UWMC', keep_default_na=False,
                         usecols=['Barcode ID (Sample ID)', 'MRN', 
                                  'Collection Date', 'Accession'],
                         na_values=['NA', ''])

def load_nwh_manifest_data(uw_nwh_file: str) -> pd.DataFrame:
    return pd.read_excel(uw_nwh_file,sheet_name='NWH',keep_default_na=False,
                         usecols=['Barcode ID (Sample ID)', 'MRN', 
                                  'Collection Date (per tube)', 'Accession'],
                         na_values=['NA', ''])

def load_hmc_manifest_data(hmc_sch_file: str) -> pd.DataFrame:
    return pd.read_excel(hmc_sch_file,sheet_name='HMC',keep_default_na=False,
                         usecols=['Barcode ID', 'MRN', 'Accession', 
                                  'Collection date'], 
                         na_values=['NA', ''])

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


def drop_columns(columns_to_keep: list, df: pd.DataFrame):
    """
    Given a list of column names and a pandas DataFrame,
    drop all columns in Dataframe that are not in list. 
    """
    columns = list(df.columns)
    columns_to_drop = [col for col in columns if col not in columns_to_keep]
    return df.drop(columns=columns_to_drop, axis=1)


def missing_barcode(df: pd.DataFrame): 
    """ TODO """
    missing_barcodes = df.loc[df['barcode'].isnull()].copy()
    missing_barcodes['problem'] = 'Missing barcode'
    return generate_problem_barcode_columns(missing_barcodes)


def generate_problem_barcode_columns(df: pd.DataFrame):
    """ TODO Should we just carry over MRN, Accession and Date instead of 
        doing it this way? 
    """ 
    df.loc[:, 'MRN'] = df['identifier'].str[:8]
    df.loc[:, 'Accession'] = df['identifier'].str[8:]
    return df[['MRN', 'Accession', 'barcode', 'problem']]


def duplicated_barcode(df: pd.DataFrame):
    """ TODO """ 
    duplicates = pd.DataFrame(df.barcode.value_counts())
    duplicates = duplicates[duplicates['barcode'] > 1]
    duplicates = pd.Series(duplicates.index)

    duplicated_barcodes = df[df['barcode'].isin(duplicates)].copy()
    duplicated_barcodes['problem'] = 'Barcode is not unique'

    return generate_problem_barcode_columns(duplicated_barcodes)


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
    """ TODO """ 
    if output:
        problem_barcodes.to_csv(output, index=False)
    else:
        print(problem_barcodes.to_csv(index=False))


@preprocess.command("sch-clinical")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")

def sch_clinical(sch_filename):
    """
    Process and insert clinical data from SCH.

    TODO add hashing of SCH identifier.
    """
    df = pd.read_csv(sch_filename)
    
    # Drop unnecessary columns
    columns_to_keep = ["pat_id", "study_id", "sample_date", 
                       "pat_age", "pat_sex"]
    df = drop_columns(columns_to_keep, df)
    
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
