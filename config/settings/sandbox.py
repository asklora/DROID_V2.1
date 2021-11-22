from .base import *

"""
This Django configuration file in used in the local development environment for tests.
You can use this configuration file by running:
`python manage.py runserver --settings=config.local`
"""

SQLPRINT=True

firebase_admin.initialize_app(
    FIREBASE_DEV_CREDS,
    {
        "databaseURL": "https://asklora-android-default-rtdb.asia-southeast1.firebasedatabase.app/"
    },
)
FIREBASE_STAGGING_APP=firebase_admin.initialize_app(
    FIREBASE_STAGGING_CREDS,
    {
        "databaseURL": "https://asklora-android-default-rtdb.asia-southeast1.firebasedatabase.app/"
    },
    name="stagging-client"
)



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
            "hosts": [("localhost", 6379)],
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
        "LOCATION": "redis://localhost:6379/1",
    }
}

print("using dev db")
read_endpoint, write_endpoint, port = db.dev_url
CELERY_BROKER_URL="redis://localhost:6379/0"
CELERY_SINGLETON_BACKEND_URL = 'localhost://localhost:6379/1'
# CELERY_BROKER_URL = "amqp://rabbitmq:rabbitmq@16.162.110.123:5672"
CELERY_TASK_DEFAULT_QUEUE ='droid_dev'
HEDGE_WORKER_DEFAULT_QUEUE ='droid_dev'
BROADCAST_WORKER_DEFAULT_QUEUE='droid_dev'
PORTFOLIO_WORKER_DEFAULT_QUEUE='droid_dev'
UTILS_WORKER_DEFAULT_QUEUE='droid_dev'
ASKLORA_QUEUE="asklora-dev"
RUN_LOCAL=True
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
    'portfolio':'dev_portfolio',
    'universe':'dev_universe',
    'ranking':'dev_ranking'
}

# CELERY_TASK_ROUTES = {
#     # ===== SHORT INTERVAL =====
#     #websocket ping
#     'core.services.tasks.ping_available_presence': {'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
#     #prune inactive channel
#     'core.services.tasks.channel_prune':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
#     #realtime ranking and portfolio
#    ' core.services.order_services.update_rtdb_user_porfolio':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
#     #market price realtime
#     'datasource.rkd.update_rtdb':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
#     'datasource.rkd.bulk_update_rtdb':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
#     # ===== SHORT INTERVAL =====

#     # ===== ORDER & PORTFOLIO =====
#     # order executor
#     'core.services.order_services.order_executor':{'queue': PORTFOLIO_WORKER_DEFAULT_QUEUE},
#     # ===== ORDER & PORTFOLIO =====
    
#     # ===== HEDGE BOT RELATED =====
#     # weekly topstock and hedge
#     'core.services.tasks.populate_client_top_stock_weekly':{'queue': HEDGE_WORKER_DEFAULT_QUEUE},
#     'core.services.tasks.daily_hedge':{'queue': HEDGE_WORKER_DEFAULT_QUEUE},
#     # ===== HEDGE BOT RELATED =====

#     # ===== UTILITY =====
#     # update ticker weekly
#     'core.services.tasks.weekly_universe_firebase_update':{'queue': UTILS_WORKER_DEFAULT_QUEUE},
#     # exchange hours updater
#     'core.services.exchange_services.init_exchange_check':{'queue': UTILS_WORKER_DEFAULT_QUEUE},
#     'core.services.exchange_services.market_check_routines':{'queue': UTILS_WORKER_DEFAULT_QUEUE},
#     # ===== UTILITY =====

#     # ===== CELERY DEFAULT =====
#     # contains celery beat, user sync, micro service cross language
#     #   . config.celery.app_publish -> for cross language/app producer
#     #   . config.celery.listener  -> for cross language/app consumer

#     }
