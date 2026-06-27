import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.sunbeamdb.models import Base


DATABASE_URL = os.environ.get(
    "SUNBEAM_DATABASE_URL",
    "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry",
)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


def create_schema() -> None:
    Base.metadata.create_all(bind=engine)