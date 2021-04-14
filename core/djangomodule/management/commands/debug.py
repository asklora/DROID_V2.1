from django.core.management.base import BaseCommand, CommandError
from importlib import import_module
from config.celery import debug_task, app
from core.services.ingestiontask import migrate_droid1


class Command(BaseCommand):
    
    def handle(self, *args, **options):
        # lib_name = 'migrate.latest_price'
        # module, function = lib_name.rsplit('.', 1)
        # mod = import_module(module)
        # func = getattr(mod, function)
        # func()
        # debug_task.apply_async(queue='droid')
        # task = app.control.inspect(['droid@dev']).active()
        # print(len(task['droid@dev']))
        migrate_droid1()