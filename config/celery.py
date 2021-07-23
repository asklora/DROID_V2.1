import os

from celery import Celery,shared_task
from importlib import import_module
import time

# set the default Django settings module for the 'celery' program.
debug = os.environ.get('DJANGO_SETTINGS_MODULE',True)
if debug:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.production')
    # app2 = Celery('core.services',broker='amqp://rabbitmq:rabbitmq@16.162.110.123:5672')


app = Celery('core.services')

app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


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
    if data['type'] == 'function':
        module, function = data['module'].rsplit('.', 1)
        mod = import_module(module)
        func = getattr(mod, function)
        func(data['payload'])
    elif data['type'] == 'invoke':
        module, function = data['module'].rsplit('.', 1)
        mod = import_module(module)
        func = getattr(mod, function)
        func()
