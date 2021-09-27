from .base import *
import requests


"""
This Django configuration file in used in the development environment.
You can use this configuration file by running:
`python manage.py runserver --settings=config.settings.development`
"""
try:
    EC2_IP_PUBLIC = requests.get('http://169.254.169.254/latest/meta-data/public-ipv4').text
    EC2_IP_LOCAL =requests.get('http://169.254.169.254/latest/meta-data/local-ipv4').text
    ALLOWED_HOSTS.append(EC2_IP_PUBLIC)
    ALLOWED_HOSTS.append(EC2_IP_LOCAL)
except requests.exceptions.RequestException:
    pass
SQLPRINT=False
CHANNEL_LAYERS = {
    'default': {
        # Method 1: Via redis lab
        # 'BACKEND': 'channels_redis.core.RedisChannelLayer',
        # 'CONFIG': {
        #     "hosts": [
        #       'redis://h:<password>;@<redis Endpoint>:<port>'
        #     ],
        # },

        # Method 2: Via local Redis
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [('redis', 6379)],
            "capacity": 1500,  # default 100
            "expiry": 2,
        },

    },
}
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://redis:6379/1",
}
}



print('using test db changes')
read_endpoint, write_endpoint, port = db.test_url
CELERY_BROKER_URL = 'amqp://rabbitmq:rabbitmq@16.162.110.123:5672'
CELERY_TASK_DEFAULT_QUEUE = 'droid_dev'
HEDGE_WORKER_DEFAULT_QUEUE =CELERY_TASK_DEFAULT_QUEUE
BROADCAST_WORKER_DEFAULT_QUEUE=CELERY_TASK_DEFAULT_QUEUE
PORTFOLIO_WORKER_DEFAULT_QUEUE=CELERY_TASK_DEFAULT_QUEUE
UTILS_WORKER_DEFAULT_QUEUE=CELERY_TASK_DEFAULT_QUEUE
ASKLORA_QUEUE="asklora-dev"

# print(f'using read: {read_endpoint}')
# print(f'using write: {write_endpoint}')
DATABASE_ROUTERS = ['config.DbRouter.AuroraRouters']
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

FIREBASE_COLLECTION={
    'portfolio':'dev_portfolio',
    'universe':'universe'
}
