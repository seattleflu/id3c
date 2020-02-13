"""
Unified file/url IO helpers for the CLI.
"""
import click
import fsspec
from functools import partial


class LocalOrRemoteFile(click.File):
    """
    Extended version of :py:class:`click.File` which supports remote URLs too
    using :py:func:`urlopen`.

    Intended for use as the ``type`` of a :py:class:`click.Option`.
    """
    def convert(self, value, param, ctx):
        original_path = str(value)

        if isinstance(value, str) and "://" in value:
            remote_file = urlopen(
                value,
                mode = self.mode,           # type: ignore
                encoding = self.encoding,   # type: ignore
                errors = self.errors)       # type: ignore

            value = remote_file.open()

            ctx.call_on_close(remote_file.close)

        fh = super().convert(value, param, ctx)

        # Most normal file-like objects have a .name attribute, including those
        # returned by click.File.  The objects returned by fsspec often do not.
        # While they do have a .path attribute, it's not the complete URL.
        #
        # Due to internal implementation details of things like core Python's
        # io.TextIOWrapper, we can't set .name, so instead standardize on .path
        # so our callers can use something consistent.
        setattr(fh, "path", original_path)

        return fh


def urlopen(path, mode = "rb", encoding = None, errors = None):
    """
    Open a local file path or URL with :py:func:`fsspec.open`.

    Notable supported URL schemes include ``http[s]://`` and ``s3://``, but
    other schemes are supported as well.

    The returned object is an :py:class:`~fsspec.core.OpenFile`, ready to be
    used as a context manager.
    """
    return fsspec.open(path, mode = mode, encoding = encoding, errors = errors)
