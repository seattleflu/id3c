"""
URL handling.
"""
from copy import copy
from typing import List, Tuple, Union
from urllib.parse import urlsplit, SplitResult, urljoin


class Url(SplitResult):
    """
    Parse and manipulate a URL string.

    :py:class:`.Url` subclasses :py:class:`urllib.parse.SplitResult` to provide
    additional attributes and methods that make manipulation of URL paths
    easier.

    It may be constructed directly from a URL string or from any 5-tuple of
    ``(scheme, netloc, path, query, fragment)``, just like the kind returned by
    :py:func:`urllib.parse.urlsplit`.

    >>> a = Url("https://example.com/a/")
    >>> a
    Url(scheme='https', netloc='example.com', path='/a/', query='', fragment='')

    >>> str(a)
    'https://example.com/a/'

    The primary manipulations provided are:

    * The :py:attr:`.parent` attribute to remove the last path component.

      >>> a.parent
      Url(scheme='https', netloc='example.com', path='/', query='', fragment='')

    * The `/` operator to append a child path (similar to
      :py:class:`pathlib.Path`'s operator).  See :py:meth:`.__truediv__` for
      more information.

      >>> a / "b"
      Url(scheme='https', netloc='example.com', path='/a/b', query='', fragment='')

    * The :py:meth:`.with_path` method to replace the path, leaving all other
      attributes the same.

      >>> a.with_path("/c")
      Url(scheme='https', netloc='example.com', path='/c', query='', fragment='')
    """

    def __new__(cls, *args):
        """
        Create a new instance directly from a string, in addition to the
        5-tuple supported by the base class.
        """
        if len(args) == 1:
            args = tuple(urlsplit(str(args[0])))

        return super().__new__(cls, *args)


    def __str__(self) -> str:
        """
        Returns :py:class:`.Url` as a string.

        Equivalent to calling :py:meth:`.geturl`.
        """
        return self.geturl()


    # Special path handling
    @property
    def path_parts(self) -> Tuple[str, ...]:
        """
        A :py:class:`tuple` of :py:attr:`.path` components.
        """
        return tuple(self.path.split("/"))


    @property
    def parent(self) -> 'Url':
        """
        The :py:class:`.Url` of the parent, or containing, path at the same
        origin (scheme + netloc).

        >>> str(Url("https://example.com/a/b/c").parent)
        'https://example.com/a/b/'

        >>> str(Url("https://example.com/a/b/").parent)
        'https://example.com/a/'

        >>> str(Url("https://example.com/").parent)
        'https://example.com/'

        The parent's :py:attr:`.path` will always end in a slash (``/``).

        All other attributes of the existing :py:class:`.Url` are preserved.
        """
        # /a/b/c/ → /a/b/ is ('', 'a', 'b', 'c', '') → ('', 'a', 'b', '')
        # /a/b/c  → /a/b/ is ('', 'a', 'b', 'c')     → ('', 'a', 'b', '')
        if self.path.endswith("/"):
            last_part = -2
        else:
            last_part = -1

        new_path = "/".join((*self.path_parts[:last_part], ""))

        return self.with_path(new_path or "/")


    def __truediv__(self, child: str) -> 'Url':
        """
        Returns a new :py:class:`.Url` with the *child*, or sub-, path
        appended.

        >>> str(Url("https://example.com/") / "a/")
        'https://example.com/a/'

        >>> str(Url("https://example.com/") / "a/b")
        'https://example.com/a/b'

        >>> str(Url("https://example.com/a/b/") / "c/")
        'https://example.com/a/b/c/'

        The new :py:class:`.Url`'s path will never have a duplicate slash
        (``/``) between the original path and the path addition, e.g.::

        >>> str(Url("https://example.com/") / "/a/b")
        'https://example.com/a/b'

        All other attributes of the existing :py:class:`.Url` are preserved.
        """
        has_slash = (self.path.endswith("/"), child.startswith("/"))

        if all(has_slash):
            new_path = self.path + child[1:]
        elif any(has_slash):
            new_path = self.path + child
        else:
            new_path = self.path + "/" + child

        return self.with_path(new_path)


    def with_path(self, path: str) -> 'Url':
        """
        Returns a new :py:class:`.Url` with the given *path* at the same origin
        (scheme + netloc).

        All other attributes of the existing :py:class:`.Url` are preserved.
        """
        return type(self)(
            self.scheme,
            self.netloc,
            path,
            self.query,
            self.fragment)
