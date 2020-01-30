"""
Logging filters for ID3C.
"""
import operator
import re
from functools import partial


def suppress_records_matching(**attrs):
    """
    Generate a :py:class`logging` filter function which suppresses a
    :py:class:`~logging.LogRecord` if it matches all *attrs*.

    Attributes are compared using :py:func:`attribute_matcher`.  See its
    documentation for value handling.

    The :py:class:`~logging.LogRecord` documentation includes `the list of
    available attributes <https://docs.python.org/3/library/logging.html#logrecord-attributes>`.

    The return value of the generated filter function determines if a record is
    emitted or not, so, somewhat counterintuitively, it returns ``False`` if
    the record matches and ``True`` if it does not.

    >>> import logging, re
    >>> record = logging.LogRecord(
    ...     name = 'milky_way.earth.north_america.pacific_northwest',
    ...     level = logging.INFO,
    ...     pathname = 'a/path.py',
    ...     lineno = 42,
    ...     msg = 'Hello, Universe!',
    ...     args = None,
    ...     exc_info = None,
    ...     func = 'shout_out')

    >>> record.levelname
    'INFO'
    >>> suppress_records_matching(levelname = 'INFO')(record)
    False
    >>> suppress_records_matching(levelname = 'WARNING')(record)
    True

    >>> record.msg
    'Hello, Universe!'
    >>> suppress_records_matching(levelname = 'INFO', msg = {'pattern': '^Hello'})(record)
    False
    >>> suppress_records_matching(levelname = 'WARNING', msg = {'pattern': '^Hello'})(record)
    True

    >>> record.name
    'milky_way.earth.north_america.pacific_northwest'
    >>> suppress_records_matching(name = {'pattern': '^milky_way[.]earth[.]north_america[.].+'})(record)
    False
    >>> suppress_records_matching(name = re.compile('^milky_way[.]earth[.]north_america[.].+'))(record)
    False

    >>> record.funcName
    'shout_out'
    >>> suppress_records_matching(funcName = 'shout_out')(record)
    False
    """
    # Generate all attribute matchers just once at the time of logging
    # configuration so the filter function can close over the list.
    attribute_matchers = [
        attribute_matcher(k, v)
            for k, v in attrs.items() ]

    def record_filter(record):
        # Match all configured attributes against the given record.  If not all
        # match, return True to emit the log record.  Otherwise, all match, so
        # return False to suppress the log record.
        return not all(match(record) for match in attribute_matchers)

    return record_filter


def attribute_matcher(name, value):
    """
    Generate a match function to compare a :py:class:`~logging.LogRecord`'s
    attribute *name* to *value*.

    The default comparator is ``==``.

    If *value* is a regular expression object of the type returned by
    :py:func:``re.compile``, then the comparator is *value*'s ``search``
    method.  That is, the regular expression will be matched against the
    record's value for the attribute.

    If *value* is a dictionary with a single key named ``pattern``, the value
    of ``pattern`` is converted to a compiled regular expression object and
    will be handled as above.
    """
    if isinstance(value, dict) and {*value.keys()} == {"pattern"}:
        value = re.compile(value["pattern"])

    if isinstance(value, type(re.compile(""))):
        value_matches = value.search
    else:
        value_matches = partial(operator.eq, value)

    def attribute_matches(record):
        try:
            return bool(value_matches(getattr(record, name)))
        except AttributeError:
            # record is missing the attribute, so can't match
            return False

    return attribute_matches
