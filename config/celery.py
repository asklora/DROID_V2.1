import os

from celery import Celery
from importlib import import_module

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('core.services')
# app.conf.task_routes = {
#     'core.services.ingestiontask.*': {'queue': 'ingestion'}
#     }
# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')

@app.task(bind=True)
def listener(self,data):
    """
    - Data format:
        {
            'type':'function',
            'module':'core.djangomodule.crudlib.user.createuser',
            'payload': {
                'email':'asklora@publisher.com',
                'password':'r3ddpapapapa'
            }
        }
    
    """
    print(data)
    # if data['type'] =='function':
    #     module, function = data['module'].rsplit('.', 1)
    #     mod = import_module(module)
    #     func = getattr(mod, function)
    #     func(data['payload'])