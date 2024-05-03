from uuid import uuid4
from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class FilmworkType(Enum):
    movie = "movie"
    tv_show = "tv show"


@dataclass
class Filmwork:
    id: uuid4
    title: str
    description: str
    creation_date: str
    file_path: str
    rating: float
    type: FilmworkType
    created_at: datetime
    updated_at: datetime


@dataclass
class Genre:
    id: uuid4
    name: str
    description: str
    created_at: datetime
    updated_at: datetime


@dataclass
class Person:
    id: uuid4
    full_name: str
    created_at: datetime
    updated_at: datetime


@dataclass
class GenreFilmwork:
    id: uuid4
    genre_id: uuid4
    film_work_id: uuid4
    created_at: datetime


@dataclass
class PersonFilmwork:
    id: uuid4
    person_id: uuid4
    film_work_id: uuid4
    role: str
    created_at: datetime



