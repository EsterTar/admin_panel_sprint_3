import os
import logging
from dotenv import load_dotenv
import sqlite3
from contextlib import contextmanager
import contextlib
import psycopg2
from dataclasses import dataclass, astuple, fields
from typing import List

from models import Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork

script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, 'env.example')
load_dotenv(dotenv_path=env_path)

SQLITE_DB_PATH: str = os.path.join(script_dir, os.environ.get('SQLITE_DB_PATH'))

PSQL_DB_NAME: str = os.environ.get('PSQL_DB_NAME')
PSQL_DB_USER: str = os.environ.get('PSQL_DB_USER')
PSQL_DB_PASSWORD: str = os.environ.get('PSQL_DB_PASSWORD')
PSQL_DB_HOST: str = os.environ.get('PSQL_DB_HOST')
PSQL_DB_PORT: str = os.environ.get('PSQL_DB_PORT')
PSQL_DB_OPTIONS: str = os.environ.get('PSQL_DB_OPTIONS')

dsn = {
    'dbname': PSQL_DB_NAME,
    'user': PSQL_DB_USER,
    'password': PSQL_DB_PASSWORD,
    'host': PSQL_DB_HOST,
    'port': PSQL_DB_PORT,
    'options': PSQL_DB_OPTIONS,
}


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def get_data_from_sqlite(conn, table_name: str, batch_size: int, offset: int):
    curs = conn.cursor()
    query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
    curs.execute(query)
    data = curs.fetchall()
    return data


def upload_data_to_psql(conn, table_name: str, data: List[dataclass]):
    fields_mapping = {
        'created_at': 'created',
        'updated_at': 'modified'
    }

    cursor = conn.cursor()

    column_names = [fields_mapping.get(field.name, field.name) for field in fields(data[0])]
    column_names_str = ','.join(column_names)

    col_count = ', '.join(['%s'] * len(column_names))

    bind_values = ','.join(cursor.mogrify(f"({col_count})", astuple(document)).decode('utf-8') for document in data)
    cursor.execute(
        f'INSERT INTO content.{table_name} ({column_names_str}) VALUES {bind_values} '
        f'ON CONFLICT (id) DO NOTHING'
    )
    conn.commit()


def main():
    tables_for_migration = [
        {'name': 'film_work', 'model': Filmwork},
        {'name': 'genre', 'model': Genre},
        {'name': 'person', 'model': Person},
        {'name': 'genre_film_work', 'model': GenreFilmwork},
        {'name': 'person_film_work', 'model': PersonFilmwork}
    ]

    batch_size = 1000

    for table_data in tables_for_migration:
        table_name = table_data['name']
        document_model = table_data['model']
        offset = 0

        while True:
            try:
                data = get_data_from_sqlite(
                    table_name=table_name,
                    conn=sqlite_conn,
                    batch_size=batch_size,
                    offset=offset
                )
                if not data:
                    break
            except Exception as e:
                raise TypeError(f'Failed to get data from sqlite db. {str(e)}')

            document_models = [document_model(**document) for document in data]

            try:
                upload_data_to_psql(conn=psql_conn, table_name=table_name, data=document_models)
                offset += batch_size
            except Exception as e:
                logging.error(f'Failed upload data to psql db. {str(e)}')
                break


if __name__ == "__main__":
    with conn_context(SQLITE_DB_PATH) as sqlite_conn, contextlib.closing(psycopg2.connect(**dsn)) as psql_conn:
        main()
