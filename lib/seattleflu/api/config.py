"""
Flask app config values.
"""
from os import environ

variables = [
    ("MAX_CONTENT_LENGTH", int, 20 * 1024**2),  # 20MB
]

def from_environ() -> dict:
    """
    Get config values from the environment, or fall back to defaults.
    """
    return dict(
        (key, typecast(environ.get(f"FLASK_{key.upper()}", default)))
            for key, typecast, default
             in variables
    )
