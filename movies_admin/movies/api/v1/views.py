from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import Filmwork, PersonFilmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        print('get_queryset')
        filmworks = Filmwork.objects.annotate(
            **{f'{role.value}s': ArrayAgg(
                'filmwork_persons__full_name', filter=Q(personfilmwork__role=role), distinct=True
            ) for role in PersonFilmwork.Role},
            genres=ArrayAgg('genrefilmwork__genre__name', distinct=True),
        ).values(
            'id',
            'title',
            'description',
            'creation_date',
            'rating',
            'type',
            'genres',
            'actors',
            'directors',
            'writers'
        ).order_by('id')
        return filmworks

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):

    def get_context_data(self, *, object_list=None, **kwargs):
        filmworks = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(queryset=filmworks, page_size=50)

        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': list(paginator.page(page.number).object_list),
        }

        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, *, object_list=None, **kwargs):
        return self.object
