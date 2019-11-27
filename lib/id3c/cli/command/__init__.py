"""
Commands for the database CLI.
"""
import click
import logging
import pandas as pd
from functools import wraps
from typing import List
from id3c.db.session import DatabaseSession


__all__ = [
    "etl",
    "identifier",
    "location",
    "manifest",
    "clinical",
    "user",
    "longitudinal",
    "sequence_read_set",
    "consensus_genome",
    "redcap_det",
    "receiving",
]


LOG = logging.getLogger(__name__)


def with_database_session(command):
    """
    Decorator to provide database session and error handling for a command.

    The decorated function is called with a ``db`` keyword argument to provide
    a :class:`~id3c.db.session.DatabaseSession` object.  The call happens
    within an exception handler that commits or rollsback the database
    transaction, possibly interactively.  Three new options are added to the
    command (``--dry-run``, ``--prompt``, and ``--commit``) to control this
    behaviour.
    """
    @click.option("--dry-run", "action",
        help        = "Only go through the motions of changing the database (default)",
        flag_value  = "rollback",
        default     = True)

    @click.option("--prompt", "action",
        help        = "Ask if changes to the database should be saved",
        flag_value  = "prompt")

    @click.option("--commit", "action",
        help        = "Save changes to the database",
        flag_value  = "commit")

    @wraps(command)
    def decorated(*args, action, **kwargs):
        db = DatabaseSession()

        processed_without_error = None

        try:
            command(*args, **kwargs, db = db)

        except Exception as error:
            processed_without_error = False

            LOG.error(f"Aborting with error: {error}")
            raise error from None

        else:
            processed_without_error = True

        finally:
            if action == "prompt":
                ask_to_commit = \
                    "Commit all changes?" if processed_without_error else \
                    "Commit successfully processed records up to this point?"

                commit = click.confirm(ask_to_commit)
            else:
                commit = action == "commit"

            if commit:
                LOG.info(
                    "Committing all changes" if processed_without_error else \
                    "Committing successfully processed records up to this point")
                db.commit()

            else:
                LOG.info("Rolling back all changes; the database will not be modified")
                db.rollback()

    return decorated


def add_metadata(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """ Adds a metadata column to a given DataFrame *df* for reporting """
    df['_metadata'] = list(map(lambda index: {
        'filename': filename,
        'row': index + 2}, df.index))
    return df


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """ Trims leading and trailing whitespace from strings in *df* """
    str_columns = df.columns[every_value_is_str_or_na(df)]

    # Guard against AttributeErrors from entirely empty non-object dtype columns
    str_columns = list(df[str_columns].select_dtypes(include='object'))

    df[str_columns] = df[str_columns].apply(lambda column: column.str.strip())

    return df


def every_value_is_str_or_na(df):
    """
    Evaluates whether every value in the columns of a given DataFrame *df* is
    either a string or NA.
    """
    return df.applymap(lambda col: isinstance(col, str) or pd.isna(col)).all()


def barcode_quality_control(clinical_records: pd.DataFrame, output: str) -> None:
    """ Perform quality control on barcodes """
    missing_barcodes = missing_barcode(clinical_records)
    duplicated_barcodes = duplicated_barcode(clinical_records)

    print_problem_barcodes(pd.concat([missing_barcodes, duplicated_barcodes],
                                 ignore_index=True), output)

    assert len(duplicated_barcodes) == 0, "You have duplicated barcodes!"


def missing_barcode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a pandas DataFrame *df*, returns a DataFrame with missing barcodes and
    a description of the problem.
    """
    missing_barcodes = df.loc[df['barcode'].isnull()].copy()
    missing_barcodes['problem'] = 'Missing barcode'

    return missing_barcodes


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

    return duplicated_barcodes


def print_problem_barcodes(problem_barcodes: pd.DataFrame, output: str):
    """
    Given a pandas DataFrame of *problem_barcodes*, writes the data to
    the log unless a filename *output* is given.
    """
    if output:
        problem_barcodes.to_csv(output, index=False)
    else:
        problem_barcodes.apply(lambda x: LOG.warning(
            f"{x['problem']} in row {x['_metadata']['row']} of file "
            f"{x['_metadata']['filename']}, barcode {x['barcode']}"
        ), axis=1)


def dump_ndjson(df):
    """
    Prints a :class:`pandas.DataFrame` as NDJSON.

    Dates are formatted according to ISO 8601.
    """
    print(df.to_json(orient = "records", lines = True, date_format = "iso"))


def group_true_values_into_list(long_subset: pd.DataFrame, stub: str,
                                pid: List[str]) -> pd.DataFrame:
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
