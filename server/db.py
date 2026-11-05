import functools

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from config.context import Context


# Created lazily, so that importing server modules never requires a resolved database configuration (e.g. in tests,
# which override get_db instead).
@functools.cache
def get_engine() -> Engine:
    return create_engine(Context().sunbeam_db.build_url(), echo=False, pool_pre_ping=True)


@functools.cache
def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
