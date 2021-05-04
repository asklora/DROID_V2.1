
from pathlib import Path
import os
from dotenv import load_dotenv
from environs import Env
from core.djangomodule.network.cloud import DroidDb

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv()
env = Env()
db = DroidDb()
# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '!5fyj@#07=!rc9^)k0tgsl%dp@rmfe$*8t3*+m&mkwk-w^l!_a'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['18.167.118.164', '127.0.0.1']


# Application definition

DJANGO_DEFAULT_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.postgres',

]
ADDITIONAL_APPS = [
    'psqlextra',
    'django_celery_results',
    'rest_framework',
    'import_export',
    'django_celery_beat',
]
CORE_APPS = [
    'core.bot',
    'core.services',
    'core.universe',
    'core.user',
    'core.djangomodule',
    'core.master',
    'core.topstock',
    'core.Clients',
    'core.survey',
    'core.orders',
    'core.ai_value',
]

INSTALLED_APPS = DJANGO_DEFAULT_APPS + ADDITIONAL_APPS + CORE_APPS

# END OF APPS

# =========================

# MIDDLEWARE
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]
# END MIDDLEWARE

# =========================

# TEMPLATE CONF

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': ['templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# =========================

# DJANGO CONFIGURATION

WSGI_APPLICATION = 'config.wsgi.application'
ROOT_URLCONF = 'config.urls'
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'files/staticfiles')
AUTH_USER_MODEL = 'user.User'
REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
        # 'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 10
    # 'DEFAULT_SCHEMA_CLASS': [
    #     'rest_framework.schemas.coreapi.AutoSchema'
    # ]
}

ELASTICSEARCH_DSL = {
    'default': {
        'hosts': 'localhost:9200'
    }
}

# =========================

# Database
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases
db_debug = env.bool("DROID_DEBUG")
if db_debug:
    read_endpoint, write_endpoint, port = db.test_url
else:
    read_endpoint, write_endpoint, port = db.prod_url

# print(f'using read: {read_endpoint}')
# print(f'using write: {write_endpoint}')

DATABASE_ROUTERS = ['config.DbRouter.AuroraRouters']
DB_ENGINE = 'psqlextra.backend'
DATABASES = {
    'default': {
        'ENGINE': DB_ENGINE,
        'NAME': os.getenv('DBNAME'),  # dbname
        'USER': os.getenv('DBUSER'),
        'PASSWORD': os.getenv('DBPASSWORD'),
        'HOST': read_endpoint,
        'PORT': port,
    },
    'aurora_read': {
        'ENGINE': DB_ENGINE,
        'NAME': os.getenv('DBNAME'),  # dbname
        'USER': os.getenv('DBUSER'),
        'PASSWORD': os.getenv('DBPASSWORD'),
        'HOST': read_endpoint,
        'PORT': port,

    },
    'aurora_write': {
        'ENGINE': DB_ENGINE,
        'NAME': os.getenv('DBNAME'),  # dbname
        'USER': os.getenv('DBUSER'),
        'PASSWORD': os.getenv('DBPASSWORD'),
        'HOST': write_endpoint,
        'PORT': port,

    },

}

# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = False


# REDIS AND CELERY
CELERY_TIMEZONE = 'UTC'
CELERY_RESULT_BACKEND = 'django-db'
CELERY_BROKER_URL = 'amqp://rabbitmq:rabbitmq@18.167.118.164:5672'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TASK_SERIALIZER = 'json'
CELERY_IMPORTS = ['core.services.ingestiontask', 'portfolio_hedge']
CELERYBEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'
