import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.CharField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('tv show')

    title = models.CharField(_('title'), max_length=255)
    description = models.CharField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'))
    rating = models.FloatField(_('rating'), blank=True, validators=[
        MinValueValidator(0), MaxValueValidator(100)
    ])
    type = models.TextField(_('type'), choices=Type.choices)
    file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')

    filmwork_genres = models.ManyToManyField(
        "Genre", through="GenreFilmwork", related_name="film_works"
    )
    filmwork_persons = models.ManyToManyField(
        "Person", through="PersonFilmwork", related_name="film_works"
    )

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Filmwork')
        verbose_name_plural = _('Filmworks')

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        indexes = [
            models.Index(fields=['film_work', 'genre'], name='film_work_genre_idx'),
        ]
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'genre'], name='unique_film_work_genre')
        ]


class PersonFilmwork(UUIDMixin):
    class Role(models.TextChoices):
        DIRECTOR = 'director', _('Director')
        WRITER = 'writer', _('Writer')
        ACTOR = 'actor', _('Actor')

    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.CharField(
        choices=Role.choices,
        max_length=255,
        verbose_name=_('role')
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        indexes = [
            models.Index(fields=['film_work', 'person'], name='film_work_person_idx'),
        ]
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'person'], name='unique_film_work_person')
        ]
