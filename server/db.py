from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from config.context import Context, ServiceType

# Created lazily so that importing server modules never requires a resolved
# database configuration (e.g. in tests, which override get_db instead).
_engine: Engine | None = None
_session_factory: sessionmaker | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            Context.load(ServiceType.Broker).sunbeam_db.build_url(),
            echo=False,
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory() -> sessionmaker:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
        )
    return _session_factory


def dispose_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None