"""
Commands for the database CLI.
"""
import click
import logging
import pandas as pd
from functools import wraps
from sys import stdin
from textwrap import dedent
from typing import Tuple
from id3c.db.session import DatabaseSession


__all__ = [
    "etl",
    "identifier",
    "location",
    "manifest",
    "user",
    "sequence_read_set",
    "consensus_genome",
    "redcap_det",
    "receiving",
    "geocode",
    "de_identify",
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
        df = pd.read_csv(filename, sep=separator, dtype=str, na_filter=False)
    else:
        df = pd.read_excel(filename, dtype=str, na_filter=False)

    return df


def load_input_from_file_or_stdin(filename: click.File) -> pd.DataFrame:
    """
    Load input data from *filename*, which can be a file or stdin.
    """
    LOG.debug(f"Loading input from {filename.name}")

    if filename.name == "<stdin>":
        input_df = pd.read_csv(filename, dtype=str, na_filter=False)
    else:
        input_df = load_file_as_dataframe(filename.name)

    return input_df


def drop_columns_from_output(input_df: pd.DataFrame,
                             output_df: pd.DataFrame,
                             drop_columns: Tuple[str, ...]) -> pd.DataFrame:
    """
    Check all *drop_columns* exist within *input_df* and drop them from
    *output_df*.

    Raises a :class:`ColumnDoesNotExistError` if a column within *drop_columns*
    does not exist in *input_df*.
    """
    input_columns = list(input_df.columns)

    for column in drop_columns:
        if column not in input_columns:
            raise ColumnDoesNotExistError(dedent(f"""
                Provided column to drop «{column}» does not exist.
                Check input columns: {input_columns}
            """))

    return output_df.drop(columns=list(drop_columns))


class UnsupportedFileExtensionError(ValueError):
    """
    Raised when the given *filename* ends with an unsupported extension.
    """
    pass


class ColumnDoesNotExistError(ValueError):
    """
    Raised by :func:`drop_columns_from_output` if column provided does not
    exist in *input_df*.
    """
    pass
