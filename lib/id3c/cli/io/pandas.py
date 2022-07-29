"""
Pandas utilities for I/O.
"""
import click
import logging
import pandas as pd
import sys
import warnings
from sys import stdout
from textwrap import dedent
from typing import List


LOG = logging.getLogger(__name__)

# Shut up openpyxl
if not sys.warnoptions:
    warnings.filterwarnings("ignore", category=UserWarning,
                            message="Data Validation extension is not "
                                    "supported and will be removed")

def mask_values(df: pd.DataFrame, columns_to_mask: List[str]) -> None:
    for col in columns_to_mask:
        if col in df.columns:
            df[col] = "*****"
        else:
            raise Exception(f"Error: column «{col}» not found in dataframe.")


def dump_ndjson(df: pd.DataFrame, file = None, columns_to_mask: List[str] = None):
    """
    Prints a :class:`pandas.DataFrame` as NDJSON.

    Dates are formatted according to ISO 8601.

    Outputs to the stream *file*, if given, otherwise the current value of
    :attr:`sys.stdout`.  (Just like :func:`print`.)
    """
    if file is None:
        file = stdout

    if columns_to_mask:
        mask_values(df, columns_to_mask)

    print(df.to_json(orient = "records", lines = True, date_format = "iso").rstrip(), file = file)


def load_file_as_dataframe(filename: str) -> pd.DataFrame:
    """
    Given a *filename*, loads its data as a pandas DataFrame.
    Supported extensions are csv, tsv, xls, and xlsx.

    If given an Excel workbook, defaults to loading only the first sheet.

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
        df = read_excel(filename, na_filter=False)

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


def read_excel(io, sheet_name = 0, na_filter: bool = True):
    """
    Reads an Excel file while ensuring all values are treated as strings.

    This bends over backwards to make ``pandas.read_excel(..., dtype =
    "string")`` behave like you might hope (all cells are read as or cast to
    string values), instead of how it actually does (cells are read as native
    types and an exception is thrown if there are non-string values).

    Due to the gymnastics performed, most optional arguments to
    :py:func:`pandas.read_excel` are not initially supported.  However, support
    for these missing options could be added in the future with additional
    logic.
    """
    # Read header row and zero data rows for the sheet(s)
    sheet_headers = pd.read_excel(io, sheet_name = sheet_name, nrows = 0)

    # Read each sheet, specifying str() as a converter function for each
    # column.  As documented by Pandas, converters takes precedence over dtype
    # and acts earlier.  Once we have an object-dtype DataFrame (containing
    # strings), we can convert that to a string-dtype DataFrame without risk of
    # errors being thrown if an Excel cell is typed as a number.
    def read_sheet(sheet: str, columns: List[str]) -> pd.DataFrame:
        converters = {
            column: str
                for column in columns }

        return (
            pd
            .read_excel(
                io,
                sheet_name = sheet,
                na_filter = na_filter,
                converters = converters)
            .astype("string"))

    # Multiple sheets at a time are supported.
    if isinstance(sheet_headers, dict):
        return {
            sheet: read_sheet(sheet, header.columns)
                for sheet, header in sheet_headers.items() }
    else:
        return read_sheet(sheet_name, sheet_headers.columns)


class UnsupportedFileExtensionError(ValueError):
    """
    Raised when the given *filename* ends with an unsupported extension.
    """
    pass
