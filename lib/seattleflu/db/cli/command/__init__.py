"""
Commands for the database CLI.
"""
import logging
import pandas as pd


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
    "reportable_conditions",
]


LOG = logging.getLogger(__name__)


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
