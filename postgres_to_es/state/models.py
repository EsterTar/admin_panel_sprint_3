import uuid
from typing import Any, Optional, List
from pydantic import BaseModel, Field

from .base_storage import BaseStorage


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        try:
            state = self.storage.retrieve_state()
        except FileNotFoundError:
            state = dict()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key)


class PersonNested(BaseModel):
    id: uuid.UUID
    name: str


class Movie(BaseModel):
    id: uuid.UUID
    imdb_rating: Optional[float] = None
    genres: List[str]
    title: str
    description: Optional[str] = None
    directors_names: List[str] = Field(default_factory=list)
    actors_names: List[str] = Field(default_factory=list)
    writers_names: List[str] = Field(default_factory=list)
    directors: List[PersonNested] = Field(default_factory=list)
    actors: List[PersonNested] = Field(default_factory=list)
    writers: List[PersonNested] = Field(default_factory=list)


class FilmNested(BaseModel):
    id: uuid.UUID
    title: str


class Genre(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    films: List[FilmNested] = Field(default_factory=list)


class Person(BaseModel):
    id: uuid.UUID
    full_name: str
