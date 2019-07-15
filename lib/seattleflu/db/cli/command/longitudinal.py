"""
Parse and upload longitudinal data.

Longitudinal childcare data will contain PII (personally identifiable
information) and unnecessary information that does not need to be stored. This
process will only pull out specific columns of interest that will then be stored
in the receiving schema of ID3C.
"""
import re
import click
import logging
import numpy as np
import pandas as pd
import seattleflu.db as db
from typing import List
from itertools import combinations
from seattleflu.db.session import DatabaseSession
from seattleflu.db.cli import cli
from . import add_metadata, barcode_quality_control, dump_ndjson, trim_whitespace


LOG = logging.getLogger(__name__)


@cli.group("longitudinal", help = __doc__)
def longitudinal():
    pass

@longitudinal.command("parse")
@click.argument("baseline_filename",
    metavar = "<Longitudinal Childcare Data baseline (enrollments) filename>")
@click.argument("weekly_filename",
    metavar = "<Longitudinal Childcare Data weekly assessments filename>")
@click.argument("survey_filename",
    metavar = "<Longitudinal Childcare Data weekly surveys filename>")
@click.option("-o", "--output",
    metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse(baseline_filename, weekly_filename, survey_filename, output):
    """
    Process and insert longitudinal childcare data from SCH.

    Drops records with no encounter date that result after reshaping from wide
    to long format.

    All childcare records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    lnginal_records = format_and_merge_data(baseline_filename, weekly_filename,
                                            survey_filename)

    lnginal_records = weekly_assessments_wide_to_long(lnginal_records)
    lnginal_records = select_all_that_apply_wide_to_long(lnginal_records)

    lnginal_records = duplicate_audere_keys(lnginal_records)
    lnginal_records = insert_static_values(lnginal_records)
    lnginal_records = create_identifiers(lnginal_records)

    lnginal_records = lnginal_records[lnginal_records.encountered.notnull()]

    barcode_quality_control(lnginal_records, output)

    dump_ndjson(lnginal_records)


def format_and_merge_data(baseline_filename: str, weekly_filename: str,
                          survey_filename: str) -> pd.DataFrame:
    """
    For each given filename, loads the related data, performs minimal processing
    and renaming, and merges each dataset into one DataFrame that is returned.
    """
    baseline_records = load_data(baseline_filename)
    weekly_records = load_data(weekly_filename)
    survey_records = load_data(survey_filename)

    baseline_records = rename_baseline_columns(baseline_records)

    survey_records = rename_stub(survey_records, regex='^household_(?!ill)',
                                 current_stub='household',
                                 desired_stub='household_sx')

    return merge_data(baseline_records, weekly_records, survey_records)


def load_data(filename: str) -> pd.DataFrame:
    """
    Loads longitudinal childcare data from SCH from a given *filename*.

    Removes leading and trailing whitespace from string columns in the
    underlying data.
    """
    df = pd.read_csv(filename)
    df = trim_whitespace(df)
    df = add_metadata(df, filename)

    return df


def merge_data(baseline_records: pd.DataFrame, weekly_records: pd.DataFrame,
               survey_records: pd.DataFrame) -> pd.DataFrame:
    """
    Given three DataFrames (*baseline_records*, *weekly_records*, and
    *survey_records*) of longitudinal childcare data, merges them on the unique
    person identifier (pid).

    Raises a :class:`AssertionError` if the pid columns are missing in any
    DataFrame or if there are duplicated columns between any two DataFrames
    other than the pid and ``_metadata`` columns (added by a previous process).

    The ``_metadata`` columns in *weekly_records* and *survey_records* receive
    suffixes of ``_followup`` and ``_survey`` after merging, respectively.
    """
    pid = ['study_id']  # This harcoding may need to be updated in the future
    merge_method = 'left'

    for combo in combinations([baseline_records, weekly_records, survey_records], 2):
        duplicated_columns = set(list(combo[0])).intersection(list(combo[1]))

        expected_duplicates = set(pid + ['_metadata'])
        assert duplicated_columns == expected_duplicates, \
            f"""You are either missing {expected_duplicates} columns in your
            dataset or have duplicated columns other than {expected_duplicates}
            in your data."""

    merged_data = baseline_records.merge(weekly_records, how=merge_method,
                                         on=pid, suffixes=['', '_followup'])
    merged_data = merged_data.merge(survey_records, how=merge_method,
                                    on=pid, suffixes=['', '_survey'])

    assert len(baseline_records) == len(merged_data), \
        "Something went wrong when merging your files together."

    return merged_data


def rename_baseline_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Two columns in the longitudinal data are reported at baseline but do not
    follow the stubnaming-convention of the follow-up columns. Rename these
    columns in *df* to follow the weekly data naming conventions.

    Note that baseline columns are named inconsistently. Some end with '_bl'.
    Other contain '_bl_' in the middle of the column name. Handle both of these
    cases.

    Contains some hard-coded column names that may need to be updated in the
    future.
    """
    df['swab_date_0'] = df['enroll_date']

    rename_map = {
        'enroll_date': 'assess_date_0',
        'baseline_id': 'sample_id_0',
    }

    baseline_columns = list(df.filter(regex='_bl$|_bl_', axis=1))

    for col in baseline_columns:
        rename_map[col] = re.sub("_bl", "", col) + "_0"

    return df.rename(columns=rename_map)


def weekly_assessments_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts a given DataFrame *df* from wide to long on longitudinal data.

    Contains some hard-coded column names that may need to be updated in the
    future.

    Raises a :class:`Exception` if after reshaping from wide-to-long, the
    resulting time column (e.g. 'week') contains non-integer dtypes.
    """
    pid = ['study_id']
    time = 'week'

    df = rename_stub(df, regex='^swab_[0-9]+$', current_stub='swab',
                     desired_stub='swab_collected')

    stubnames = longitudinal_stubnames(df)

    reshaped = pd.wide_to_long(df, stubnames, i=pid, j=time, sep="_",
                              ).reset_index()

    try:
        reshaped[time] = reshaped[time].astype('int')
    except ValueError:
        raise Exception(f"""Expected the «{time}» column to contain only
            integers after casting longitudinal data from wide-to-long.
            Please check your stubnames and try again.""")

    return reshaped


def rename_stub(df: pd.DataFrame, regex: str, current_stub: str,
                desired_stub: str) -> pd.DataFrame:
    """
    Renames *current_stub* columns in a given DataFrame *df* to a
    *desired_stub*, using the given *regex* to capture the desired columns
    to rename.

    Motivation: useful for avoiding future DataFrame reshaping conflicts with
    the original stubname.

    For example, the stub "swab" in the original data is not sufficient for
    capturing longitudinal data only related to "swab_x" columns; other distinct
    "swab_*" stubs like "swab_date_x" would be artifically captured by the
    "stub" swabname if the original stub were not converted to a longer or more
    unique stubname like "swab_collected_x".
    """
    rename_map = {}

    stub_columns = list(df.filter(regex=regex, axis=1))

    for col in stub_columns:
        rename_map[col] = re.sub(current_stub, desired_stub, col)

    return df.rename(columns=rename_map)


def longitudinal_stubnames(df: pd.DataFrame) -> [str]:
    """
    Returns a list of the stubnames of longitudinal columns that have suffixes
    indicating the week number of the study (e.g. 'swab_date_11').
    """
    stubnames = []
    for column in list(df):
        match = re.match(".*(?=_[0-9]+$)", column)
        if match:
            stubnames.append(match[0])

    return list(set(stubnames))


def select_all_that_apply_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a wide DataFrame *df*, collapses "Select All That Apply"-type
    categorical survey responses with pseudo-one-hot-encoding format and
    converts the responses into human-readable lists of equivalent logical
    value.

    Throws a :class:`AssertionError` if the number of rows in the resulting
    DataFrame differs from the given *df*.

    Contains hard-coded column names that may need to be updated in the future.
    """

    stubnames = [
        "race", "feeding", "meds", "rn_sx", "sx_specific", "missed_impact",
        "household_sx", "travel_type",
    ]
    pid = ['study_id'] + ['week']

    stubbed_columns = list(df.filter(regex='|'.join(stubnames)))
    reshaped_data = df.drop(stubbed_columns, axis=1)

    for stub in stubnames:
        true_values = collapse_wide_stubbed_columns(df, stub, pid)
        if true_values is None:
            continue

        reshaped_data = reshaped_data.merge(true_values, how='left', on=pid)

    assert len(df) == len(reshaped_data), f"You do not have a 1:1 merge on {pid}"

    return reshaped_data


def collapse_wide_stubbed_columns(df: pd.DataFrame, stub: str,
                                  pid: List[str]) -> pd.DataFrame:
    """
    Takes a wide DataFrame *df* with columns partially matching the given
    *stub*. For each row, collapses the values from the matching column-group
    with the same *pid* into a list containing structurally different but
    logically equivalent values as the original DataFrame.
    """
    stubset = df.filter(regex=f"{'|'.join(pid)}|{stub}_")
    if set(list(stubset)) == set(pid):
        LOG.warning(f"Stub {stub} not found in data")
        return None

    long_stubset = pd.wide_to_long(stubset, stub, i=pid, j=f"new_{stub}",
                                   sep="_", suffix='\\w+'
                                  ).reset_index()

    return group_true_values_into_list(long_stubset, stub, pid)


def group_true_values_into_list(long_subset: pd.DataFrame, stub: str,
                                pid: [str]) -> pd.DataFrame:
    """
    Given a long DataFrame *long_subset*, collapses rows with the same *pid*
    such that every *pid* is represented once in the resulting DataFrame. True
    values for each category in the given *stub* are collapsed into a
    human-readable list.
    """
    long_subset.dropna(inplace=True)
    long_subset[stub] = long_subset[stub].astype('bool')
    true_subset = long_subset[long_subset[stub]]

    return true_subset.groupby(pid + [stub]) \
                      .agg(lambda x: x.tolist()) \
                      .reset_index() \
                      .drop(stub, axis=1) \
                      .rename(columns={f'new_{stub}': stub})


def duplicate_audere_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given DataFrame *df* of longitudinal data, duplicate every column
    whose meaning is a 1:1 match with Audere data. The duplicated column will
    match the Audere naming conventions for easier queries down the road.

    Contains hard-coded column names that may need to be updated in the future.
    """
    sch_to_audere_map = {
        "study_id": "individual",
        "age": "age",
        "sex": "AssignedSex",
        "race": "Race",
        "ethnicity": "HispanicLatino",
        "insurance": "MedicalInsurance",
        "home_type": "WhereLive",
        "bedrooms": "Bedrooms",
        "house_members": "PeopleInHousehold",
        "smoking": "HouseholdSmoke",
        "enroll_site": "site",
        "flu_shot": "FluShot",
        "sx_specific": "Symptoms",
        "illness_impact": "DailyInterference",
        "travel_type": "ChildrensRecentTravel",
        "sample_id": "barcode",
        "assess_date": "encountered",  # Choose weekly assess date over survey date
    }

    for key in sch_to_audere_map:
        try:
            df[sch_to_audere_map[key]] = df[key]
        except:
            LOG.warning(f"Column {key} not found in the given data.")

    return df


def insert_static_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Inserts static values into a given DataFrame *df*.

    Contains hard-coded column names that may need to be updated in the future.
    """
    HUTCH_KIDS = 1  # Site ID from the SCH childcare data dictionary

    df["ChildrenNearChildren"] = "yes"
    df["SchoolType"] = "childcareCenter"
    df["ChildrenHutchKids"] = np.where(df['enroll_site'] == HUTCH_KIDS, 'yes', 'no')
    df["type"] = "childcare"

    return df


def create_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a long, unique identifier for each individual and encounter in a
    given DataFrame *df*.
    Contains some hard-coded values that may need to be updated in the future.
    """
    df['individual'] = 'sch/year-1/childcare/' + df['study_id'].astype('str')
    df['identifier'] = df['individual'] + '/' + df['week'].astype('str')

    return df


@longitudinal.command("upload")
@click.argument("longitudinal_file",
    metavar = "<longitudinal.ndjson>",
    type = click.File("r"))

def upload(longitudinal_file):
    """
    Upload longitudinal records into the database receiving area.

    <longitudinal.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling commands.

    Once records are uploaded, the longitudinal ETL routine will reconcile the
    longitudinal records with known sites, individuals, encounters and samples.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying longitudinal records from {longitudinal_file.name}")

        row_count = db.copy_from_ndjson(("receiving", "longitudinal", "document"),
                                        longitudinal_file)

        LOG.info(f"Received {row_count:,} longitudinal records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise
