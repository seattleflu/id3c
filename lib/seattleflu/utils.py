"""
Utilities.
"""

def format_doc(**kwargs):
    """
    Decorator which calls :method:`str.format_map` with *kwargs* to interpolate
    variables into the docstring of the decorated function.

    This should be used sparingly, but it can be useful, for example, to
    incorporate shared constants into help text, particularly for commands.
    """
    def wrap(function):
        function.__doc__ = function.__doc__.format_map(kwargs)
        return function
    return wrap
