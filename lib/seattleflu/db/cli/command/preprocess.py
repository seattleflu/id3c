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

# Clinical subcommand
@preprocess.command("uw_clinical")
@click.argument("filename", metavar = "<Clinical Data filename>")
@click.argument("uw_nwh_file", metavar="<UW/NWH filename>")
@click.argument("hmc_sch_file", metavar="<HMC/SCH filename>")


def uw_clinical(filename, uw_nwh_file, hmc_sch_file):
    """
    Process and insert clinical data from UW.

    Given a *filename* to an Excel document, selects specific columns of 
    interest and inserts the queried data into the receiving.clinical table as 
    a JSON document.

    Specifically looks for a sheet named 'pts' and assumes that each row
    represents one encounter.

    Uses *uw_nwh_file* and *hmc_sch_file* to join clinical data to fill in
    SFS barcodes. 

    Hint: ~/Documents/Inpatient\ Clinical\ Data\ Pulls/Datasets/4.9.19.xlsx
        ~/Documents/Inpatient Clinical Data Pulls/Datasets/4.9.19.xlsx
    """
    df = pd.read_excel(filename, sheet_name='pts')
    uw_df = pd.read_excel(uw_nwh_file,sheet_name='UWMC',keep_default_na=False,
                          usecols=['Barcode ID', 'MRN', 'Accession'])
    nwh_df = pd.read_excel(uw_nwh_file,sheet_name='NWH',keep_default_na=False,
                           usecols=['Barcode ID', 'MRN', 'Accession'])
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
                       "FinClass", "Barcode ID", "ZipCode"]# To be replaced w/ census tract  
    columns = df.columns.tolist()
    columns_to_drop = [col for col in columns if col not in columns_to_keep]
    df = df.drop(columns=columns_to_drop, axis=1)

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
    
    LOG.debug(f"Inserting clinical data for «{df['identifier']}» ")

    document = df.to_json(date_format='iso')

    cursor.execute(
        "insert into receiving.clinical (document) values (%s)",
        (document,)
    )
