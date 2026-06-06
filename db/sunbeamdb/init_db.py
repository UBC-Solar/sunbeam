from sqlalchemy import create_engine, text
from db.sunbeamdb.models import Base


def create_schema(engine) -> None:
    with engine.begin() as conn:
        # Enable TimescaleDB extension
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))

    # Create normal SQLAlchemy tables first
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        # Convert tables to hypertables.
        # if_not_exists => true avoids errors on repeated runs.
        conn.execute(
            text(
                """
                SELECT create_hypertable(
                    'raw_sample',
                    'ts',
                    chunk_time_interval => INTERVAL '1 day',
                    if_not_exists => TRUE
                );
                """
            )
        )

        conn.execute(
            text(
                """
                SELECT create_hypertable(
                    'aligned_sample',
                    'ts',
                    chunk_time_interval => INTERVAL '1 day',
                    if_not_exists => TRUE
                );
                """
            )
        )

        # Useful indexes for common queries
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS aligned_sample_event_signal_ts_idx
                ON aligned_sample (event_id, signal_id, ts DESC);
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS aligned_sample_event_ts_idx
                ON aligned_sample (event_id, ts DESC);
                """
            )
        )

    print("Database schema initialized.")


if __name__ == "__main__":
    import os

    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry",
    )

    engine = create_engine(DATABASE_URL, echo=True)
    create_schema(engine)
    engine.dispose()