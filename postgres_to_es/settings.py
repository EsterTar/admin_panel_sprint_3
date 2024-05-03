import os
from dotenv import load_dotenv

from pydantic_settings import BaseSettings

load_dotenv(dotenv_path='.env')


class PostgresSettings(BaseSettings):
    dbname: str = os.environ.get('POSTGRES_DB')
    user: str = os.environ.get('POSTGRES_USER')
    password: str = os.environ.get('POSTGRES_PASSWORD')
    host: str = os.environ.get('ETL_POSTGRES_HOST')
    port: str = os.environ.get('POSTGRES_PORT')


class ElasticsearchSettings(BaseSettings):
    hosts: str = f"{os.environ.get('ETL_ELASTICSEARCH_URL')}"


postgres_settings = PostgresSettings()
elasticsearch_settings = ElasticsearchSettings()
