import os

from celery import Celery, shared_task
from importlib import import_module
import time
from environs import Env
from celery.signals import worker_ready


from dotenv import load_dotenv

env = Env()
load_dotenv()
debug = os.environ.get('DJANGO_SETTINGS_MODULE', True)
if debug:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.local')
    # app2 = Celery('core.services',broker='amqp://rabbitmq:rabbitmq@16.162.110.123:5672')
dbdebug = env.bool("DROID_DEBUG")


app = Celery('core.services')

app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

@worker_ready.connect
def at_start(sender, **k):
    with sender.app.connection() as conn:
         sender.app.send_task('core.services.exchange_services.init_exchange_check',connection=conn)

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
