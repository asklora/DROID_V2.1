from .base import *

"""
This Django configuration file in used in the local development environment for tests.
You can use this configuration file by running:
`python manage.py runserver --settings=config.local`
"""
SQLPRINT=True
CHANNEL_LAYERS = {
    "default": {
        # Method 1: Via redis lab
        # 'BACKEND': 'channels_redis.core.RedisChannelLayer',
        # 'CONFIG': {
        #     "hosts": [
        #       'redis://h:<password>;@<redis Endpoint>:<port>'
        #     ],
        # },
        #
        # Method 2: Via local Redis
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379)],
            "capacity": 1500,  # default 100
            "expiry": 2,
        },
        #
        # Method 3: Via In-memory channel layer
        # Using this method.
        # "BACKEND": "channels.layers.InMemoryChannelLayer"
    },
}
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
    }
}


print("using test db changes local")
read_endpoint, write_endpoint, port = db.test_url
CELERY_BROKER_URL = "amqp://rabbitmq:rabbitmq@16.162.110.123:5672"
CELERY_TASK_DEFAULT_QUEUE ='droid_dev'
HEDGE_WORKER_DEFAULT_QUEUE ='droid_dev'
BROADCAST_WORKER_DEFAULT_QUEUE='droid_dev'
PORTFOLIO_WORKER_DEFAULT_QUEUE='droid_dev'
UTILS_WORKER_DEFAULT_QUEUE='droid_dev'
ASKLORA_QUEUE="asklora-dev"
print(f'using read: {read_endpoint}')
# print(f'using write: {write_endpoint}')
# DATABASE_ROUTERS = ['config.DbRouter.AuroraRouters']
DATABASES = {
    "default": {
        "ENGINE": DB_ENGINE,
        "NAME": os.getenv("DBNAME"),  # dbname
        "USER": os.getenv("DBUSER"),
        "PASSWORD": os.getenv("DBPASSWORD"),
        "HOST": read_endpoint,
        "PORT": port,
    },
    "aurora_read": {
        "ENGINE": DB_ENGINE,
        "NAME": os.getenv("DBNAME"),  # dbname
        "USER": os.getenv("DBUSER"),
        "PASSWORD": os.getenv("DBPASSWORD"),
        "HOST": read_endpoint,
        "PORT": port,
    },
    "aurora_write": {
        "ENGINE": DB_ENGINE,
        "NAME": os.getenv("DBNAME"),  # dbname
        "USER": os.getenv("DBUSER"),
        "PASSWORD": os.getenv("DBPASSWORD"),
        "HOST": write_endpoint,
        "PORT": port,
    },
}


FIREBASE_COLLECTION={
    'portfolio':'portfolio',
    'universe':'universe'
}