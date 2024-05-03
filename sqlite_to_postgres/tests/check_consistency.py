import os
from dotenv import load_dotenv
import sqlite3
from contextlib import contextmanager
import contextlib
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime

load_dotenv(dotenv_path='../env.example')

SQLITE_DB_PATH: str = f"../{os.environ.get('SQLITE_DB_PATH')}"
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


def get_data_from_sqlite(conn, table_name: str):
    curs = conn.cursor()
    curs.execute(f"SELECT * FROM {table_name};")
    data = curs.fetchall()
    return data


def compare_documents_count(psql_cursor, sqlite_conn, table_name: str):
    psql_cursor.execute(f"SELECT COUNT(*) FROM content.{table_name};")
    psql_documents_count = psql_cursor.fetchone()[0]

    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    sqlite_documents_count = sqlite_cursor.fetchone()[0]

    assert psql_documents_count == sqlite_documents_count


def compare_documents_data(psql_cursor, sqlite_conn, table_name: str):
    psql_cursor.execute(f"SELECT * FROM content.{table_name};")
    psql_documents_row = psql_cursor.fetchall()
    psql_documents = {document['id']: dict(document) for document in psql_documents_row}

    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT * FROM {table_name};")
    sqlite_documents_row = sqlite_cursor.fetchall()
    sqlite_documents = {document['id']: dict(document) for document in sqlite_documents_row}

    for document_id, sqlite_document in sqlite_documents.items():
        postgres_document = psql_documents.get(document_id)
        if 'updated_at' in sqlite_document:
            sqlite_document['modified'] = datetime.fromisoformat(sqlite_document.pop('updated_at'))
        sqlite_document['created'] = datetime.fromisoformat(sqlite_document.pop('created_at'))
        assert postgres_document == sqlite_document


def main():
    table_names = ['film_work', 'genre', 'person', 'person_film_work', 'genre_film_work']

    with (
        conn_context(SQLITE_DB_PATH) as sqlite_conn,
        contextlib.closing(psycopg2.connect(**dsn)) as psql_conn,
        psql_conn.cursor(cursor_factory=DictCursor) as psql_cursor
    ):
        for table_name in table_names:
            compare_documents_count(table_name=table_name, sqlite_conn=sqlite_conn, psql_cursor=psql_cursor)
            compare_documents_data(table_name=table_name, sqlite_conn=sqlite_conn, psql_cursor=psql_cursor)


if __name__ == "__main__":
    main()
