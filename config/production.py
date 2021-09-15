from .settings import *

"""
This Django configuration file in used in the production environment.
You can use this configuration file by running:
`python manage.py runserver --settings=config.production`
"""

CHANNEL_LAYERS = {
    'default': {

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

# Database
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases
print('using prod db')
read_endpoint, write_endpoint, port = db.prod_url
MQPASS=os.getenv('MQPASS')
MQUSER=os.getenv('MQUSER')
CELERY_BROKER_URL = f'amqp://{MQUSER}:{MQPASS}@amqp-sg-6wr2clmao003.mq-amqp.cn-hongkong-3568556-b.aliyuncs.com:5672/master'
CELERY_TASK_DEFAULT_QUEUE ='celery'
HEDGE_WORKER_DEFAULT_QUEUE ='hedger'
BROADCAST_WORKER_DEFAULT_QUEUE='broadcaster'
PORTFOLIO_WORKER_DEFAULT_QUEUE='portofolio'
UTILS_WORKER_DEFAULT_QUEUE='utils'
print(read_endpoint)

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

    }

}
FIREBASE_COLLECTION={
    'portfolio':'portfolio',
    'universe':'universe'
}