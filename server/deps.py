from collections.abc import Generator

from sqlalchemy.orm import Session, sessionmaker

from server.db import get_session_factory


def get_db() -> Generator[Session, None, None]:
    db = get_session_factory()()
    try:
        yield db
    finally:
        db.close()


def get_db_session_factory() -> sessionmaker:
    """
    For endpoints that need to open many short-lived sessions (e.g. the data
    stream's poll loop) rather than one per request. A dependency so tests
    can override it alongside get_db.
    """
    return get_session_factory()