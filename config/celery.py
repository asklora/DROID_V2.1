import os

from celery import Celery, shared_task
from importlib import import_module
import time
from environs import Env
from celery.signals import worker_ready
from django.conf import settings
from celery.backends.rpc import RPCBackend as CeleryRpcBackend
from dotenv import load_dotenv
from django import db
from core.djangomodule.general import logging
from core.djangomodule.celery_singleton import clear_locks

env = Env()
load_dotenv()
debug = os.environ.get('DJANGO_SETTINGS_MODULE', False)
role = os.environ.get('CELERY_ROLE', 'slave')
if not debug:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
dbdebug = env.bool("DROID_DEBUG")
db.connections.close_all()
logging.warning(f'celery role using -> {role}')
#NOTE AVALAIBLE TASK 13-09-2021
#   . channels_presence.tasks.prune_presence
#   . channels_presence.tasks.prune_rooms
#   . config.celery.app_publish
#   . config.celery.debug_task
#   . config.celery.listener
#   . core.services.exchange_services.init_exchange_check
#   . core.services.exchange_services.market_check_routines
#   . core.services.ingestiontask.get_trkd_data_by_region
#   . core.services.order_services.order_executor
#   . core.services.order_services.update_rtdb_user_porfolio
#   . core.services.tasks.channel_prune
#   . core.services.tasks.daily_hedge
#   . core.services.tasks.daily_hedge_user
#   . core.services.tasks.get_isin_populate_universe
#   . core.services.tasks.get_latest_price
#   . core.services.tasks.order_client_topstock
#   . core.services.tasks.ping_available_presence
#   . core.services.tasks.populate_client_top_stock_weekly
#   . core.services.tasks.send_csv_hanwha
#   . core.services.tasks.weekly_universe_firebase_update
#   . datasource.rkd.bulk_update_rtdb
#   . datasource.rkd.save
#   . datasource.rkd.update_rtdb
#   . portfolio.daily_hedge_classic.classic_position_check
#   . portfolio.daily_hedge_ucdc.ucdc_position_check
#   . portfolio.daily_hedge_uno.uno_position_check
#   . portfolio.daily_hedge_user.user_position_check

app = Celery('core.services')
if debug in ['config.settings.production','config.settings.prodtest']:
    app.conf.broker_login_method = 'PLAIN'
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

@worker_ready.connect
def at_start(sender, **k):
    if role == 'master':
        clear_locks(app)
        with sender.app.connection() as conn:
            sender.app.send_task('core.services.exchange_services.init_exchange_check',connection=conn,queue=settings.BROADCAST_WORKER_DEFAULT_QUEUE)




_RPC = CeleryRpcBackend(app=app)





@app.task(bind=True)
def app_publish(self):
    return {'message': 'hallo'}


@shared_task
def debug_task():
    a = 0
    while True:
        print('running')
        if a >= 20:
            break
        a += 1
        time.sleep(2)


@app.task(bind=True)
def listener(self, data):
    """
    - Data format:
        {
            'type':'function',
            'module':'core.datasource.rkd.RkdData.save',
            'payload': {
                'email':'asklora@publisher.com',
                'password':'r3ddpapapapa'
            }
        }
    """
    if not 'type' in data and not 'module' in data and not 'payload' in data:
        return {'message': f'payload error, must have key type , module and payload', 'received_payload': data}
    if data['type'] == 'function':
        module, function = data['module'].rsplit('.', 1)
        mod = import_module(module)
        func = getattr(mod, function)
        res = func(data['payload'])
        if res:
            return res
    elif data['type'] == 'invoke':
        module, function = data['module'].rsplit('.', 1)
        mod = import_module(module)
        func = getattr(mod, function)
        func()
