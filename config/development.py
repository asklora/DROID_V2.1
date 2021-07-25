from .settings import *


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

        # Method 3: Via In-memory channel layer
        # Using this method.
        # "BACKEND": "channels.layers.InMemoryChannelLayer"
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
CELERY_DEFAULT_QUEUE = 'dev_env'


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
    # 'mongo': {
    #         'ENGINE': 'djongo',
    #         'NAME': 'universe',
    #         'CLIENT': {
    #             'host': 'mongodb+srv://postgres:postgres@cluster0.b0com.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    #         },
    #         'ENFORCE_SCHEMA': False,
    #     }

}
