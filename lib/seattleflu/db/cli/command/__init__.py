"""
Commands for the database CLI.
"""
import pandas as pd


__all__ = [
    "etl",
    "identifier",
    "manifest",
    "clinical",
    "user",
]


def add_metadata(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """ Adds a metadata column to a given DataFrame *df* for reporting """
    df['_metadata'] = list(map(lambda index: {
        'filename': filename,
        'row': index + 2}, df.index))
    return df
