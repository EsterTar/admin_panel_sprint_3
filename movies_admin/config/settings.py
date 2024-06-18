import os
from pathlib import Path
from dotenv import load_dotenv

from split_settings.tools import include

load_dotenv(dotenv_path='.env')

AUTH_USER_MODEL = "users.User"

AUTH_API_LOGIN_URL = os.environ.get('AUTH_LOGIN_URL')

AUTHENTICATION_BACKENDS = [
    'users.auth.AuthBackend',
]

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get('SECRET_KEY')

DEBUG = bool(os.environ.get('DEBUG', False))

ALLOWED_HOSTS = ["127.0.0.1", "localhost"]

INTERNAL_IPS = ['127.0.0.1']

LOCALE_PATHS = ['movies/locale']

ROOT_URLCONF = 'config.urls'

WSGI_APPLICATION = 'config.wsgi.application'

include(
    'components/database.py',
    'components/auth_password_validators.py',
    'components/installed_apps.py',
    'components/middleware.py',
    'components/templates.py',
)

LANGUAGE_CODE = 'ru-RU'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

STATIC_URL = '/static/'

STATIC_ROOT = BASE_DIR / 'staticfiles'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'filters': {
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        }
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]',
        },
    },
    'handlers': {
        'debug-console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'filters': ['require_debug_true'],
        },
    },
    'loggers': {
        'django.db.backends': {
            'level': 'DEBUG',
            'handlers': ['debug-console'],
            'propagate': False,
        }
    },
}
