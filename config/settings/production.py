from .base import *
import requests

"""
This Django configuration file in used in the production environment.
You can use this configuration file by running:
`python manage.py runserver --settings=config.settings.production`
"""

try:
    EC2_IP_PUBLIC = requests.get('http://169.254.169.254/latest/meta-data/public-ipv4').text
    EC2_IP_LOCAL =requests.get('http://169.254.169.254/latest/meta-data/local-ipv4').text
    ALLOWED_HOSTS.append(EC2_IP_PUBLIC)
    ALLOWED_HOSTS.append(EC2_IP_LOCAL)
except requests.exceptions.RequestException:
    pass

## GCP

firebase_admin.initialize_app(
    FIREBASE_PROD_CREDS,
    {
        "databaseURL": "https://asklora-android-default-rtdb.asia-southeast1.firebasedatabase.app/"
    },
)


SQLPRINT=False
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

# Databases
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases
print('using prod db')
read_endpoint, write_endpoint, port = db.prod_url
MQPASS="NjI0NkZFQzVBQkQwNUE2RERCRjY1QzJGMzA2OUFFMjE1MjAyMkRFMjoxNjMxNjA0MjEwOTY5"
MQUSER="MjphbXFwLXNnLTZ3cjJjbG1hbzAwMzpMVEFJNXRTaGR4VUhxV3ZCVm9MNVR5amE="
CELERY_BROKER_URL = f'amqp://{MQUSER}:{MQPASS}@amqp-sg-6wr2clmao003.mq-amqp.cn-hongkong-3568556-b.aliyuncs.com:5672/master'
CELERY_SINGLETON_BACKEND_URL = 'redis://redis:6379/1'
CELERY_TASK_DEFAULT_QUEUE ='celery'
HEDGE_WORKER_DEFAULT_QUEUE ='hedger'
BROADCAST_WORKER_DEFAULT_QUEUE='broadcaster'
PORTFOLIO_WORKER_DEFAULT_QUEUE='portofolio'
UTILS_WORKER_DEFAULT_QUEUE='utils'
ASKLORA_QUEUE="asklora"

print(read_endpoint)

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

    }

}
FIREBASE_COLLECTION={
    'portfolio':'prod_portfolio',
    'universe':'universe',
    'ranking':'ranking'
}
CELERY_TASK_ROUTES = {
    # ===== SHORT INTERVAL =====
    #websocket ping
    'core.services.tasks.ping_available_presence': {'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
    #prune inactive channel
    'core.services.tasks.channel_prune':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
    #realtime ranking and portfolio
   ' core.services.order_services.update_rtdb_user_porfolio':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
    #market price realtime
    'datasource.rkd.update_rtdb':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
    'datasource.rkd.bulk_update_rtdb':{'queue': BROADCAST_WORKER_DEFAULT_QUEUE},
    # ===== SHORT INTERVAL =====

    # ===== ORDER & PORTFOLIO =====
    # order executor
    'core.services.order_services.order_executor':{'queue': PORTFOLIO_WORKER_DEFAULT_QUEUE},
    # ===== ORDER & PORTFOLIO =====
    
    # ===== HEDGE BOT RELATED =====
    # weekly topstock and hedge
    'core.services.tasks.populate_client_top_stock_weekly':{'queue': HEDGE_WORKER_DEFAULT_QUEUE},
    'core.services.tasks.daily_hedge':{'queue': HEDGE_WORKER_DEFAULT_QUEUE},
    # ===== HEDGE BOT RELATED =====

    # ===== UTILITY =====
    # update ticker weekly
    'core.services.tasks.weekly_universe_firebase_update':{'queue': UTILS_WORKER_DEFAULT_QUEUE},
    # exchange hours updater
    'core.services.exchange_services.init_exchange_check':{'queue': CELERY_TASK_DEFAULT_QUEUE},
    'core.services.exchange_services.market_check_routines':{'queue': CELERY_TASK_DEFAULT_QUEUE},
    # ===== UTILITY =====

    # ===== CELERY DEFAULT =====
    # contains celery beat, user sync, micro service cross language
    #   . config.celery.app_publish -> for cross language/app producer
    #   . config.celery.listener  -> for cross language/app consumer

    }