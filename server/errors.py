"""
Exceptions services raise, and the HTTP status each maps to (registered once in server.main.create_app):

    LookupError -> 404    something named in the request doesn't exist
    Conflict    -> 409    the request clashes with existing state
    ValueError  -> 400    anything else wrong with the request
"""


class Conflict(ValueError):
    pass
