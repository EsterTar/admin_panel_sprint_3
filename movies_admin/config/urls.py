from django.contrib import admin
from django.urls import include, path


def trigger_error(request):
    division_by_zero = 1 / 0


urlpatterns = [
    path('sentry-debug/', trigger_error),
    path('admin/', admin.site.urls),
    path("__debug__/", include("debug_toolbar.urls")),
    path('api/', include('movies.api.urls')),
]
