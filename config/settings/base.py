from pathlib import Path
import os
from dotenv import load_dotenv
from environs import Env
from core.djangomodule.network.cloud import DroidDb
from datetime import timedelta
import firebase_admin
import warnings
import numpy as np
from socket import gethostname, gethostbyname


np.seterr(all="ignore")
warnings.filterwarnings("ignore")
warnings.simplefilter(action="ignore", category=FutureWarning)

"""
This is the default Django settings file sans the database settings, caches and celery configurations.
This file will be imported by `local.py`, `development.py`, and `production.py` to be used in their respective environments.
"""

# Build paths inside the project like this: BASE_DIR / 'subdir'
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv()
env = Env()
db = DroidDb()
db_debug = env.bool("DROID_DEBUG")

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "!5fyj@#07=!rc9^)k0tgsl%dp@rmfe$*8t3*+m&mkwk-w^l!_a"

# SECURITY WARNING: don't run with debug turned on in production!
django_debug = env.bool("DEBUG")

DEBUG = django_debug
TESTDEBUG=False # for tests
SQLPRINT=True # for tests



ALLOWED_HOSTS = [
    gethostname(),
    gethostbyname(gethostname()),
    "118.0.1.71",
    "127.0.0.1",
    "services.asklora.ai",
    "0.0.0.0",
    "dev-services.asklora.ai",
]
CORS_ALLOW_ALL_ORIGINS = True


"""
Active applications
"""
DJANGO_DEFAULT_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.postgres",
]
ADDITIONAL_APPS = [
    'django_celery_results',
    'rest_framework',
    'import_export',
    'django_celery_beat',
    'drf_spectacular',
    'corsheaders',
    'rest_framework_simplejwt.token_blacklist',
    'channels',
    'channels_presence',
    'django_redis',
    'simple_history',
    # docs https://github.com/vishalanandl177/DRF-API-Logger
    'drf_api_logger',
]
CORE_APPS = [
    "core.bot",
    "core.services",
    "core.universe",
    "core.user",
    "core.djangomodule",
    "core.master",
    "core.topstock",
    "core.Clients",
    "core.orders",
]

INSTALLED_APPS = DJANGO_DEFAULT_APPS + ADDITIONAL_APPS + CORE_APPS


"""
Django middleware settings
"""
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    # custom midleware
    "config.middleware.HealthCheck.HealthCheckMiddleware",
    # additional
    "config.middleware.ApiLogger.LoggerMiddleware",
]


"""
Templates settings
"""
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": ["templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]


"""
Django configuration
"""
# WSGI_APPLICATION = 'config.wsgi.application'
ASGI_APPLICATION = "config.asgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"
ROOT_URLCONF = "config.urls"
STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "files/staticfiles")
AUTH_USER_MODEL = "user.User"
REST_FRAMEWORK = {
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_PERMISSION_CLASSES": [
        # 'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
    ],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.LimitOffsetPagination",
    "PAGE_SIZE": 10,
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "config.jwt_auth_module.AuthJwt",
    ),
    'DEFAULT_THROTTLE_RATES': {
        'order': '10/min',
        'order_action': '10/min',
    }
}


DB_ENGINE = 'django.db.backends.postgresql_psycopg2'


"""
Password validation (https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators
"""
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

AUTHENTICATION_BACKENDS = ["config.Auth.AuthBackend"]

"""
Django Internatinalization settings (https://docs.djangoproject.com/en/3.2/topics/i18n/)
"""
LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_L10N = True

USE_TZ = False


"""
Settings for Redis and Celery
"""
CELERY_TIMEZONE = "UTC"
CELERY_RESULT_BACKEND = "django-db"
CELERY_ACCEPT_CONTENT = ["application/json"]
CELERY_RESULT_SERIALIZER = "json"
CELERY_TASK_SERIALIZER = "json"
CELERY_IMPORTS = [
    "core.services.ingestiontask",
    "core.services.tasks",
    "channels_presence.tasks",
    "datasource.rkd",
    "core.services.order_services",
    "core.services.exchange_services",
]


CELERYBEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"


APPEND_SLASH = False
email_debug = False
if email_debug:
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
else:
    EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"


"""
Settings for email
"""
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS")
EMAIL_PORT = os.getenv("EMAIL_PORT")
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")

LOGIN_URL = "default-admin-login"


"""
Settings for Spectacular documentation
"""
SPECTACULAR_SETTINGS = {
    # A regex specifying the common denominator for all operation paths. If
    # SCHEMA_PATH_PREFIX is set to None, drf-spectacular will attempt to estimate
    # a common prefix. use '' to disable.
    # Mainly used for tag extraction, where paths like '/api/v1/albums' with
    # a SCHEMA_PATH_PREFIX regex '/api/v[0-9]' would yield the tag 'albums'.
    "SCHEMA_PATH_PREFIX": "/api/",
    # Remove matching SCHEMA_PATH_PREFIX from operation path. Usually used in
    # conjunction with appended prefixes in SERVERS.
    "SCHEMA_PATH_PREFIX_TRIM": False,
    "DEFAULT_GENERATOR_CLASS": "drf_spectacular.generators.SchemaGenerator",
    # Schema generation parameters to influence how components are constructed.
    # Some schema features might not translate well to your target.
    # Demultiplexing/modifying components might help alleviate those issues.
    #
    # Create separate components for PATCH endpoints (without required list)
    "COMPONENT_SPLIT_PATCH": True,
    # Split components into request and response parts where appropriate
    "COMPONENT_SPLIT_REQUEST": False,
    # Aid client generator targets that have trouble with read-only properties.
    "COMPONENT_NO_READ_ONLY_REQUIRED": False,
    # Configuration for serving a schema subset with SpectacularAPIView
    "SERVE_URLCONF": None,
    # complete public schema or a subset based on the requesting user
    "SERVE_PUBLIC": True,
    # include schema enpoint into schema
    "SERVE_INCLUDE_SCHEMA": False,
    # list of authentication/permission classes for spectacular's views.
    "SERVE_PERMISSIONS": ["rest_framework.permissions.AllowAny"],
    # https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.3.md#openapi-object
    "TITLE": "DROID CLIENT API",
    "DESCRIPTION": "CLIENT",
    "VERSION": "1.1.0-beta",
}


"""
Settings for JWT
"""
SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(minutes=90),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=1),
    "ROTATE_REFRESH_TOKENS": False,
    "BLACKLIST_AFTER_ROTATION": True,
    "UPDATE_LAST_LOGIN": True,
    "ALGORITHM": "HS256",
    "SIGNING_KEY": SECRET_KEY,
    "VERIFYING_KEY": None,
    "AUDIENCE": None,
    "ISSUER": None,
    "AUTH_HEADER_TYPES": ("Bearer",),
    "AUTH_HEADER_NAME": "HTTP_AUTHORIZATION",
    "USER_ID_FIELD": "username",
    "USER_ID_CLAIM": "username",
    "USER_AUTHENTICATION_RULE": "rest_framework_simplejwt.authentication.default_user_authentication_rule",
    "AUTH_TOKEN_CLASSES": ("rest_framework_simplejwt.tokens.AccessToken",),
    "TOKEN_TYPE_CLAIM": "token_type",
    "JTI_CLAIM": "jti",
    "SLIDING_TOKEN_REFRESH_EXP_CLAIM": "refresh_exp",
    "SLIDING_TOKEN_LIFETIME": timedelta(minutes=5),
    "SLIDING_TOKEN_REFRESH_LIFETIME": timedelta(days=1),
}


"""
Settings for firebase backend
"""
# FIREBASE CREDENTIALS
FIREBASE_DEV_CREDS = firebase_admin.credentials.Certificate(
    "files/file_json/dev-asklora-firebase.json"
)
FIREBASE_STAGGING_CREDS = firebase_admin.credentials.Certificate(
    "files/file_json/stagging-asklora-firebase.json"
)
FIREBASE_PROD_CREDS = firebase_admin.credentials.Certificate(
    "files/file_json/asklora-firebase.json"
)

# additional logger
DRF_API_LOGGER_DATABASE = True