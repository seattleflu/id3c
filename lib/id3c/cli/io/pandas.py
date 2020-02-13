"""
Pandas utilities for I/O.
"""
import click
import logging
import pandas as pd
from textwrap import dedent


LOG = logging.getLogger(__name__)


def dump_ndjson(df: pd.DataFrame):
    """
    Prints a :class:`pandas.DataFrame` as NDJSON.

    Dates are formatted according to ISO 8601.
    """
    print(df.to_json(orient = "records", lines = True, date_format = "iso"))


def load_file_as_dataframe(filename: str) -> pd.DataFrame:
    """
    Given a *filename*, loads its data as a pandas DataFrame.
    Supported extensions are csv, tsv, xls, and xlsx.

    Raises a :class: `UnsupportedFileExtensionError` if the given *filename*
    ends with an unsupported extension.
    """
    supported_extensions = ('.csv', '.tsv', '.xls', '.xlsx')

    if not filename.endswith(supported_extensions):
        raise UnsupportedFileExtensionError(dedent(f"""
            Unsupported file extension for file «{filename}».
            Please choose from one of the following file extensions:
                {supported_extensions}
            """
        ))

    if filename.endswith(('.csv', '.tsv')):
        separator = '\t' if filename.endswith('.tsv') else ','
        df = pd.read_csv(filename, sep=separator, dtype="string", na_filter=False)
    else:
        df = pd.read_excel(filename, dtype="string", na_filter=False)

    return df


def load_input_from_file_or_stdin(filename: click.File) -> pd.DataFrame:
    """
    Load input data from *filename*, which can be a file or stdin.
    """
    LOG.debug(f"Loading input from {filename.name}")

    if filename.name == "<stdin>":
        input_df = pd.read_csv(filename, dtype="string", na_filter=False)
    else:
        input_df = load_file_as_dataframe(filename.name)

    return input_df


class UnsupportedFileExtensionError(ValueError):
    """
    Raised when the given *filename* ends with an unsupported extension.
    """
    pass
