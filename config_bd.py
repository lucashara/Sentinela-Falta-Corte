# config_bd.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
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
    )
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
