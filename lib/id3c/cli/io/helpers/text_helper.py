"""
Utilities for dealing with user-entered text.
"""

import json
from os import environ

# A dict of default character replacements if no JSON is specified.
# If empty, it will just replace any unencodeable characters with
# spaces.
LATIN1_REPLACEMENTS = {
}

def load_latin1_replacements(path):
    """
    Load a list of characters to replace from the JSON file at `path`.
    """
    global LATIN1_REPLACEMENTS
    try:
        with open(path, 'rb') as fp:
            LATIN1_REPLACEMENTS = json.load(fp)
    except FileNotFoundError:
        print(f"Latin-1 replacements file {path} not found, using default")


def coerce_to_latin1(text):
    """
    Check if `text` can be entirely represented in the Latin-1 encoding.

    If not, check every character in the string to find those that
    cannot be so encoded. Replace any character that fails this check
    with a Latin-1-encodable replacement if we have one, or a space if not.
    """
    try:
        text.encode('latin1')
    except UnicodeEncodeError:
        for char in text:
            try:
                char.encode('latin1')
            except UnicodeEncodeError:
                text = text.replace(char, LATIN1_REPLACEMENTS.get(char, ' '))
    return text 


load_latin1_replacements(environ.get('LATIN1_REPLACEMENTS','/etc/latin1_replacements.json'))
