# config_bd.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from contextlib import contextmanager
from dotenv import load_dotenv
import os

load_dotenv()

# Você pode usar DB_DSN diretamente (se preferir montar a string completa),
# ou as variáveis abaixo para construir com service_name.
DB_DSN = os.getenv(
    "DB_DSN"
)  # ex: oracle+cx_oracle://user:pass@host:1521/?service_name=ORCLPDB1

if DB_DSN:
    engine = create_engine(DB_DSN, pool_pre_ping=True)
else:
    oracle_connection_string = "oracle+cx_oracle://{username}:{password}@{hostname}:{port}/?service_name={service_name}"
    engine = create_engine(
        oracle_connection_string.format(
            username=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            hostname=os.getenv("DB_HOSTNAME"),
            port=os.getenv("DB_PORT"),
            service_name=os.getenv("DB_SERVICE_NAME"),
        ),
        pool_pre_ping=True,
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise e
    finally:
        session.close()
