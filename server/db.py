from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.context import Context, ServiceType

engine = create_engine(
    Context.load(ServiceType.Broker).sunbeam_db.build_url(),
    echo=False,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)