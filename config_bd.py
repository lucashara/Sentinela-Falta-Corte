# config_bd.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from contextlib import contextmanager
from dotenv import load_dotenv
import os

load_dotenv()

oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/?service_name={service_name}'

engine = create_engine(
    oracle_connection_string.format(
        username=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        hostname=os.getenv("DB_HOSTNAME"),
        port=os.getenv("DB_PORT"),
        service_name=os.getenv("DB_SERVICE_NAME")
    ),
    # Adiciona pool_pre_ping para verificar a validade da conexão antes de usar
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise e
    finally:
        session.close()
