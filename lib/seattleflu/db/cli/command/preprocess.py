"""
Pre-process clinical data.

Clinical data will contain PII(personally identifiable information) and 
unnecessary information that do not need to be stored. This process will only
pull out specific columns of interest that will then be stored in the
receiving schema of ID3C.
"""
import click
import logging
import os
from math import ceil
import pandas as pd
import hashlib
import seattleflu.db as db
from seattleflu import labelmaker
from seattleflu.db.session import DatabaseSession
from seattleflu.db.cli.__main__ import cli


LOG = logging.getLogger(__name__)


@cli.group("preprocess", help = __doc__)
def preprocess():
    pass

# UW Clinical subcommand
@preprocess.command("uw-clinical")
@click.argument("uw_filename", metavar = "<UW Clinical Data filename>")
@click.argument("uw_nwh_file", metavar="<UW/NWH filename>")
@click.argument("hmc_sch_file", metavar="<HMC/SCH filename>")


def uw_clinical(uw_filename, uw_nwh_file, hmc_sch_file):
    """
    Process and insert clinical data from UW.

    Given a *uw_filename* to an Excel document, selects specific columns of 
    interest and inserts the queried data into the receiving.clinical table as 
    a JSON document.

    Specifically looks for a sheet named 'pts' and assumes that each row
    represents one encounter.

    Uses *uw_nwh_file* and *hmc_sch_file* to join clinical data to fill in
    SFS barcodes. *uw_nwh_file* is the filepath to the data containing 
    manifests of the barcodes from UWNC and NWH Samples. *hmc_sch_file* is the
    filepath to the data containing manifests of the barcodes from SFS 
    Retrospective Samples.

    Declare your database connection when running this command be prefixing
    with `PGDATABASE=seatteflu`.
    """
    if uw_filename.endswith('.csv'):
        df = pd.read_csv(uw_filename)
    else:
        df = pd.read_excel(uw_filename, sheet_name='pts')

    uw_df = pd.read_excel(uw_nwh_file,sheet_name='UWMC',keep_default_na=False,
                          usecols=['Barcode ID (Sample ID)', 'MRN', 'Accession'])
    uw_df = uw_df.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID'})
    nwh_df = pd.read_excel(uw_nwh_file,sheet_name='NWH',keep_default_na=False,
                           usecols=['Barcode ID (Sample ID)', 'MRN', 'Accession'])
    nwh_df = nwh_df.rename(columns={'Barcode ID (Sample ID)': 'Barcode ID'})
    hmc_df = pd.read_excel(hmc_sch_file,sheet_name='HMC',keep_default_na=False,
                           usecols=['Barcode ID', 'MRN', 'Accession'])
    
    #Generate a unique identifier for each encounter and drop duplicates
    df['identifier'] = (df['labMRN'] + df['LabAccNum']).str.lower()
    df = df.drop_duplicates(subset="identifier")

    #Add barcode using manifests
    master_ref = hmc_df.append([uw_df, nwh_df], ignore_index=True)

    # Convert all to lower case for matching purposes
    master_ref['MRN'] = master_ref['MRN'].str.lower()
    master_ref['Accession'] = master_ref['Accession'].str.lower()
    master_ref['Barcode ID'] = master_ref['Barcode ID'].str.lower()
    df['MRN'] = df['MRN'].str.lower()
    df['Accession'] = df['Accession'].str.lower()
    #Join on MRN and Accession
    df = df.merge(master_ref, how='left', on=['MRN','Accession'])
    
    #Drop unnecessary columns
    columns_to_keep = ["PersonID", "Age", "LabDtTm", "identifier", "Sex", 
                       "Race", "Fac", "EthnicGroup", "fluvaccine",
                       "FinClass", "Barcode ID", "census_tract"]
    df = drop_columns(columns_to_keep, df)

    #Standardize column names
    df = df.rename(columns={'PersonID': 'individual', 
                            'Age': 'age', 
                            'LabDtTm': 'encountered',
                            'EthnicGroup': 'HispanicLatino',
                            'Sex': 'AssignedSex',
                            'Fac': 'site', 
                            'fluvaccine': 'FluShot',
                            'FinClass': 'MedicalInsurance',
                            'Barcode ID': 'barcode'})

    # Convert datatype in 'age' column because
    # Pandas does not support NaNs with normal type 'int'
    df['age'] = df['age'].astype(pd.Int64Dtype())

    #Hash PersonID and encounter identifier(MRN+Accession)
    df['individual'] = df['individual'].apply(generate_hash)
    df['identifier'] = df['identifier'].apply(generate_hash)

    session = DatabaseSession()
    with session, session.cursor() as cursor:
        df.apply(lambda x: insert_clinical(x, cursor), axis=1)


def drop_columns(columns_to_keep: list, df: pd.DataFrame):
    """
    Given a list of column names and a pandas DataFrame,
    drop all columns in Dataframe that are not in list. 
    """
    columns = df.columns.tolist()
    columns_to_drop = [col for col in columns if col not in columns_to_keep]
    return df.drop(columns=columns_to_drop, axis=1)


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

@preprocess.command("sch-clinical")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")

def sch_clinical(sch_filename):
    """
    Process and insert clinical data from SCH.
    """
    df = pd.read_csv(sch_filename)
    
    # Drop unnecessary columns
    columns_to_keep = ["study_id", "sample_date", "age", "sex"]
    df = drop_columns(columns_to_keep, df)
    
    # Standardize column names
    df = df.rename(columns={"study_id": "barcode",
                            "sample_date": "encountered",
                            "sex": "AssignedSex"})

    # Convert to date time format
    df["encountered"] = pd.to_datetime(df["encountered"])
    #Convert age(float) to int
    df["age"] = df["age"].apply(lambda x: ceil(x))

    # Insert static value columns
    df["site"] = "SCH"

    # Placeholder columns for future data
    df["FluShot"] = None
    df["ZipCode"] = None
    df["Race"] = None
    df["HispanicLatino"] = None
    df["MedicalInsurace"] = None
    df["identifier"] = None
    df["individual"] = None
   
    session = DatabaseSession()
    with session, session.cursor() as cursor:
        df.apply(lambda x: insert_clinical(x, cursor), axis=1)
