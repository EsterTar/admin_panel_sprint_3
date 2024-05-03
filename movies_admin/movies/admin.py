from django.contrib import admin
from .models import Genre, Filmwork, Person, GenreFilmwork, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created', 'modified',)

    list_filter = ('created', 'modified',)

    search_fields = ('name', 'description',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    list_display = ('title', 'type', 'rating', 'creation_date', 'created', 'modified',)

    list_filter = ('type', 'creation_date', 'created', 'modified',)

    search_fields = ('title', 'description', 'id',)


@admin.register(Person)
class Person(admin.ModelAdmin):
    list_display = ('full_name', 'created', 'modified',)

    list_filter = ('created', 'modified',)

    search_fields = ('full_name',)

