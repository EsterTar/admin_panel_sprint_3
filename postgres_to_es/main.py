from datetime import datetime
from time import sleep
from dotenv import load_dotenv
from settings import postgres_settings, elasticsearch_settings

import psycopg
from psycopg.conninfo import make_conninfo
from elasticsearch import Elasticsearch, ConnectionError as EsConnectionError, ConnectionTimeout as EsConnectionTimeout
from elasticsearch.helpers import bulk
from psycopg import ServerCursor
from psycopg.errors import (
    ConnectionTimeout as PsConnectionTimeout, ConnectionFailure as PsConnectionFailure,
    OperationalError as PsOperationalError)
from psycopg.rows import dict_row

from logger import logger
from state.json_file_storage import JsonFileStorage
from state.models import State, Movie

from decorators import backoff

load_dotenv(dotenv_path='.env')


def execute_batch(
        cursor: ServerCursor,
        sql: str,
        limit: int,
        last_updated: datetime,
        end_updated: datetime
):
    offset = 0
    while True:
        cursor.execute(sql, (last_updated, end_updated, limit, offset,))
        items_data = cursor.fetchall()
        items_ids = [item['id'] for item in items_data]
        if not items_ids:
            break
        yield items_ids
        offset += limit


def extract_changed_persons(cursor: ServerCursor, last_updated: datetime, end_updated: datetime):
    logger.info(f'Fetching movies changed after %s', last_updated)

    person_sql = '''
            SELECT id, modified
            FROM content.person
            WHERE modified > %s AND modified < %s
            ORDER BY modified
            LIMIT %s OFFSET %s;
        '''

    for persons_batch_ids in execute_batch(cursor, person_sql, 100, last_updated, end_updated):
        # getting filmwork ids with modified persons
        formatted_person_ids = ', '.join(['%s'] * len(persons_batch_ids))
        filmworks_sql = f'''
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.person_id IN ({formatted_person_ids})
            ORDER BY fw.modified;
        '''
        cursor.execute(filmworks_sql, persons_batch_ids)
        filmworks = cursor.fetchall()
        filmwork_ids = [filmwork['id'] for filmwork in filmworks]
        # return filmwork_ids, last_modified
        yield filmwork_ids


def extract_changed_genres(cursor: ServerCursor, last_updated: datetime, end_updated: datetime):
    logger.info(f'Fetching movies changed after %s', last_updated)

    # getting modified genre ids
    genre_sql = '''
        SELECT id, modified
        FROM content.genre
        WHERE modified > %s AND modified < %s
        ORDER BY modified
        LIMIT %s OFFSET %s;
    '''

    for genres_batch_ids in execute_batch(cursor, genre_sql, 100, last_updated, end_updated):
        # getting filmwork ids with modified genres
        formatted_genre_ids = ', '.join(['%s'] * len(genres_batch_ids))
        filmworks_sql = f'''
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.genre_id IN ({formatted_genre_ids})
            ORDER BY fw.modified;
        '''
        cursor.execute(filmworks_sql, genres_batch_ids)
        filmworks = cursor.fetchall()
        filmwork_ids = [filmwork['id'] for filmwork in filmworks]
        # return filmwork_ids, last_modified
        yield filmwork_ids


def extract_changed_filmworks(cursor: ServerCursor, last_updated: datetime, end_updated: datetime):
    logger.info(f'Fetching filmworks changed after {last_updated}')

    # Getting modified filmwork ids
    filmwork_sql = '''
        SELECT id, modified
        FROM content.film_work
        WHERE modified > %s AND modified < %s
        ORDER BY modified
        LIMIT %s OFFSET %s;
    '''
    for filmworks_batch_ids in execute_batch(cursor, filmwork_sql, 100, last_updated, end_updated):
        yield filmworks_batch_ids


def transform_data(cursor: ServerCursor, filmwork_ids: list):
    formatted_filmwork_ids = ', '.join(['%s'] * len(filmwork_ids))
    details_sql = f'''
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating AS imdb_rating,
            ARRAY_AGG(DISTINCT g.name) AS genres,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'director') AS directors,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director') AS directors_names,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors_names,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers_names
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({formatted_filmwork_ids})
        GROUP BY fw.id;
    '''
    cursor.execute(details_sql, tuple(filmwork_ids))
    formatted_data = cursor.fetchall()
    for row in formatted_data:
        for key in ['directors_names', 'actors_names', 'writers_names', 'directors', 'actors', 'writers']:
            if row[key] is None:
                row[key] = []
    return formatted_data


@backoff(exceptions=(EsConnectionError, EsConnectionTimeout,))
def load_to_es(formatted_filmworks_data: list, es_client: Elasticsearch):

    bulk_request = [
        {
            "_index": "movies",
            "_id": filmwork['id'],
            "_source": Movie(**dict(filmwork)).json()
        } for filmwork in formatted_filmworks_data
    ]
    responses = bulk(es_client, bulk_request)
    logger.info(f'Bulk indexing completed, {responses[0]} documents indexed.')


def execute_update_process(
        cursor: ServerCursor,
        es_client: Elasticsearch,
        fetch_function,
        start_datetime: datetime,
        end_datetime: datetime
):
    for filmwork_ids in fetch_function(cursor, start_datetime, end_datetime):
        if filmwork_ids:
            formatted_filmworks_data = transform_data(cursor, filmwork_ids)
            load_to_es(formatted_filmworks_data, es_client)


@backoff(exceptions=(PsConnectionFailure, PsConnectionTimeout, PsOperationalError,))
def main():
    state = State(JsonFileStorage(logger=logger))
    es_client = Elasticsearch(hosts=elasticsearch_settings.hosts)

    dsn = make_conninfo(**postgres_settings.dict())

    with (psycopg.connect(dsn, row_factory=dict_row) as conn,
          ServerCursor(conn, 'fetcher') as cur):
        while True:
            last_update_row = state.get_state('last_update') or datetime.min.strftime('%d-%m-%y %H:%M:%S')
            last_update = datetime.strptime(last_update_row, '%d-%m-%y %H:%M:%S')

            start_update_datetime = datetime.now()
            execute_update_process(cur, es_client, extract_changed_persons, last_update, start_update_datetime)
            execute_update_process(cur, es_client, extract_changed_genres, last_update, start_update_datetime)
            execute_update_process(cur, es_client, extract_changed_filmworks, last_update, start_update_datetime)

            state.set_state('last_update', start_update_datetime.strftime('%d-%m-%y %H:%M:%S'))
            sleep(10)


if __name__ == '__main__':
    main()
