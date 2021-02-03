from config.celery import app



app.conf.task_routes = {
    'services.pc4tasks.*': {'queue': 'pc4'}
    }