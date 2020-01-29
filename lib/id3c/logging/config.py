"""
Logging config for ID3C.
"""
import os
import yaml
from pkg_resources import resource_stream


def load_stock_config(name = "default"):
    """
    Loads a built-in stock logging configuration based on *name*.
    """
    with resource_stream(__package__, f"data/{name}.yaml") as file:
        return load_config(file)


def load_config(config):
    """
    Loads a given logging *config* written in YAML.

    *config* may be a string or open file object, both of which are accepted as
    the first argument to :py:func:`yaml.load`.
    """
    return yaml.load(config, Loader = LogConfigLoader)


class LogConfigLoader(yaml.SafeLoader):
    """
    A :py:class:`yaml.SafeLoader` subclass which implements some custom `!` tags.

    Local, custom tags supported:

    * ``!LOG_LEVEL``
    * ``!coalesce``

    >>> os.environ["LOG_LEVEL"] = "info"
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': 'INFO'}

    >>> os.environ["LOG_LEVEL"] = ""
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': None}
    >>> yaml.load('''
    ... level: !coalesce
    ...   - !LOG_LEVEL
    ...   - WARNING
    ... ''', Loader = LogConfigLoader)
    {'level': 'WARNING'}

    >>> del os.environ["LOG_LEVEL"]
    >>> yaml.load("level: !LOG_LEVEL", Loader = LogConfigLoader)
    {'level': None}
    >>> yaml.load('''
    ... level: !coalesce
    ...   - !LOG_LEVEL
    ...   - WARNING
    ... ''', Loader = LogConfigLoader)
    {'level': 'WARNING'}
    """
    pass

def log_level_constructor(loader, node):
    """
    Implements a custom YAML tag ``!LOG_LEVEL``.

    Produces the uppercased value of the ``LOG_LEVEL`` environment variable,
    if the variable has a value.  Otherwise, returns ``None``.
    """
    level = os.environ.get("LOG_LEVEL")
    return level.upper() if level else None

def coalesce_constructor(loader, node):
    """
    Implements a custom YAML tag ``!coalesce``.

    When applied to a YAML sequence (e.g. list or array), the produced value is
    the first value which is not ``None``.  Akin to SQL's coalesce() function.
    """
    values = loader.construct_sequence(node)
    return first(lambda x: x is not None, values)

def first(predicate, iterable):
    """
    Return the first item in *iterable* for which *predicate* returns ``True``.

    If *iterable* is empty or contains no items passing *predicate*, returns
    ``None``.
    """
    return next(filter(predicate, iterable), None)

LogConfigLoader.add_constructor("!LOG_LEVEL", log_level_constructor)
LogConfigLoader.add_constructor("!coalesce", coalesce_constructor)
