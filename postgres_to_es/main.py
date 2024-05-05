from datetime import datetime
from time import sleep
from typing import List, Union, Type

from dotenv import load_dotenv
from pydantic import BaseModel

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
from state.models import State, Movie, Genre, Person

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


def extract_changed_items(
        cursor: ServerCursor,
        table_name: str,
        last_updated: datetime,
        end_updated: datetime
):
    logger.info(f'Fetching {table_name} changed after %s', last_updated)

    sql_request = f'''
        SELECT id, modified
        FROM content.{table_name}
        WHERE modified > %s AND modified < %s
        ORDER BY modified
        LIMIT %s OFFSET %s;
    '''

    for items_batch_ids in execute_batch(cursor, sql_request, 100, last_updated, end_updated):
        yield items_batch_ids


def extract_related_items(
        cursor: ServerCursor,
        item_ids: list,
        primary_table: str,
        join_table: str,
        join_column: str,
        filter_column: str

):
    """
    Извлекает идентификаторы элементов из основной таблицы, которые связаны с элементами во вторичной таблице.

    Данная функция выполняет SQL-запрос, который соединяет две таблицы: основную и связанную, используя указанный
    столбец для соединения. Фильтрация происходит по столбцу в связанной таблице, включая только те элементы,
    идентификаторы которых перечислены в item_ids. Результатом функции является список уникальных идентификаторов из
    основной таблицы, которые соответствуют условиям фильтрации.

    Параметры:\n
    cursor (ServerCursor): Курсор базы данных для выполнения запросов.\n
    item_ids (list): Список идентификаторов элементов для фильтрации.\n
    primary_table (str): Название основной таблицы в базе данных.\n
    join_table (str): Название связанной таблицы, которая будет присоединена к основной.\n
    join_column (str): Название столбца в связанной таблице, который используется для соединения с основной таблицей.\n
    filter_column (str): Название столбца в связанной таблице, который используется для фильтрации элементов.\n

    Возвращает:
    List[str]: Список идентификаторов элементов из основной таблицы, соответствующих условиям фильтрации.
    """
    formatted_items_ids = ', '.join(['%s'] * len(item_ids))
    sql_request = f'''
        SELECT mt.id, mt.modified
        FROM content.{primary_table} mt
        LEFT JOIN content.{join_table} rt ON rt.{join_column} = mt.id
        WHERE rt.{filter_column} IN ({formatted_items_ids})
        ORDER BY mt.modified;
    '''
    cursor.execute(sql_request, item_ids)
    items = cursor.fetchall()
    filtered_item_ids = [item['id'] for item in items]
    return filtered_item_ids


def get_changed_filmworks(
        cursor: ServerCursor,
        last_updated: datetime,
        end_updated: datetime
):
    changed_persons = extract_changed_items(
        cursor=cursor,
        table_name='person',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_persons_ids in changed_persons:
        changed_filmworks_ids = extract_related_items(
            cursor=cursor,
            item_ids=changed_persons_ids,
            primary_table='film_work',
            join_table='person_film_work',
            join_column='film_work_id',
            filter_column='person_id'
        )
        yield changed_filmworks_ids

    changed_genres = extract_changed_items(
        cursor=cursor,
        table_name='genre',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_genres_ids in changed_genres:
        changed_filmworks_ids = extract_related_items(
            cursor=cursor,
            item_ids=changed_genres_ids,
            primary_table='film_work',
            join_table='genre_film_work',
            join_column='film_work_id',
            filter_column='genre_id'
        )
        yield changed_filmworks_ids

    changed_filmworks = extract_changed_items(
        cursor=cursor,
        table_name='film_work',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_filmworks_ids in changed_filmworks:
        yield changed_filmworks_ids


def get_changed_genres(
        cursor: ServerCursor,
        last_updated: datetime,
        end_updated: datetime
):
    changed_genres = extract_changed_items(
        cursor=cursor,
        table_name='genre',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_genres_ids in changed_genres:
        yield changed_genres_ids

    changed_filmworks = extract_changed_items(
        cursor=cursor,
        table_name='film_work',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_filmworks_ids in changed_filmworks:
        changed_genres_ids = extract_related_items(
            cursor=cursor,
            item_ids=changed_filmworks_ids,
            primary_table='genre',
            join_table='genre_film_work',
            join_column='genre_id',
            filter_column='film_work_id'
        )
        yield changed_genres_ids


def get_changed_persons(
        cursor: ServerCursor,
        last_updated: datetime,
        end_updated: datetime
):
    changed_persons = extract_changed_items(
        cursor=cursor,
        table_name='person',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_persons_ids in changed_persons:
        yield changed_persons_ids

    changed_filmworks = extract_changed_items(
        cursor=cursor,
        table_name='film_work',
        last_updated=last_updated,
        end_updated=end_updated
    )
    for changed_filmworks_ids in changed_filmworks:
        changed_persons_ids = extract_related_items(
            cursor=cursor,
            item_ids=changed_filmworks_ids,
            primary_table='genre',
            join_table='genre_film_work',
            join_column='genre_id',
            filter_column='film_work_id'
        )
        yield changed_persons_ids


def transform_filmworks_data(cursor: ServerCursor, filmwork_ids: list):
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
        WHERE fw.id = ANY(%s)
        GROUP BY fw.id;
    '''
    cursor.execute(details_sql, (filmwork_ids,))
    formatted_data = cursor.fetchall()
    for row in formatted_data:
        for key in ['directors_names', 'actors_names', 'writers_names', 'directors', 'actors', 'writers']:
            if row[key] is None:
                row[key] = []
    return formatted_data


def transform_genres_data(cursor: ServerCursor, genres_ids: list):
    formatted_genre_ids = ', '.join(['%s'] * len(genres_ids))
    details_sql = f'''
        SELECT
            g.id,
            g.name,
            g.description,
            JSON_AGG(DISTINCT jsonb_build_object('id', fw.id, 'title', fw.title)) AS films
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE g.id IN ({formatted_genre_ids})
        GROUP BY g.id;
    '''
    cursor.execute(details_sql, tuple(genres_ids))
    formatted_data = cursor.fetchall()
    for row in formatted_data:
        if row['films'] is None:
            row['films'] = []
    return formatted_data


def transform_persons_data(cursor: ServerCursor, persons_ids: list):
    details_sql = '''
        SELECT
            p.id,
            p.full_name
        FROM
            content.person AS p
        WHERE 
            p.id = ANY(%s)
    '''
    cursor.execute(details_sql, (persons_ids,))
    formatted_data = cursor.fetchall()
    return formatted_data


@backoff(exceptions=(EsConnectionError, EsConnectionTimeout,))
def load_to_es(data: list, index: str, es_client: Elasticsearch, model: Type[BaseModel]):

    bulk_request = [
        {
            "_index": index,
            "_id": item['id'],
            "_source": model(**dict(item)).json()
        } for item in data
    ]
    responses = bulk(es_client, bulk_request)
    logger.info(f'Bulk indexing completed, {responses[0]} documents indexed.')


def update_filmworks(
        cursor: ServerCursor,
        es_client: Elasticsearch,
        last_updated: datetime,
        end_updated: datetime
):
    changed_filmworks = get_changed_filmworks(cursor=cursor, last_updated=last_updated, end_updated=end_updated)
    for changed_filmwork_ids in changed_filmworks:
        formatted_filmworks = transform_filmworks_data(cursor=cursor, filmwork_ids=changed_filmwork_ids)
        load_to_es(data=formatted_filmworks, index='movies', es_client=es_client, model=Movie)


def update_genres(
        cursor: ServerCursor,
        es_client: Elasticsearch,
        last_updated: datetime,
        end_updated: datetime
):
    changed_genres = get_changed_genres(cursor=cursor, last_updated=last_updated, end_updated=end_updated)
    for changed_genres_ids in changed_genres:
        formatted_genres = transform_genres_data(cursor=cursor, genres_ids=changed_genres_ids)
        load_to_es(data=formatted_genres, index='genres', es_client=es_client, model=Genre)


def update_persons(
        cursor: ServerCursor,
        es_client: Elasticsearch,
        last_updated: datetime,
        end_updated: datetime
):
    changed_persons = get_changed_persons(cursor=cursor, last_updated=last_updated, end_updated=end_updated)
    for changed_persons_ids in changed_persons:
        formatted_persons = transform_persons_data(cursor=cursor, persons_ids=changed_persons_ids)
        load_to_es(data=formatted_persons, index='persons', es_client=es_client, model=Person)


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

            update_filmworks(
                cursor=cur,
                es_client=es_client,
                last_updated=last_update,
                end_updated=start_update_datetime
            )
            update_genres(
                cursor=cur,
                es_client=es_client,
                last_updated=last_update,
                end_updated=start_update_datetime
            )
            update_persons(
                cursor=cur,
                es_client=es_client,
                last_updated=last_update,
                end_updated=start_update_datetime
            )

            state.set_state('last_update', start_update_datetime.strftime('%d-%m-%y %H:%M:%S'))
            sleep(10)


if __name__ == '__main__':
    main()
