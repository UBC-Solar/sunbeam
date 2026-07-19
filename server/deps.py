from collections.abc import Generator

from sqlalchemy.orm import Session

from server.db import get_session_factory


def get_db() -> Generator[Session, None, None]:
    db = get_session_factory()()
    try:
        yield db
    finally:
        db.close()