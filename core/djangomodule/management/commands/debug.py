from django.core.management.base import BaseCommand, CommandError
from importlib import import_module
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        lib_name = 'core.djangomodule.general.generate_id'
        module, function = lib_name.rsplit('.', 1)
        mod = import_module(module)
        func = getattr(mod, function)
        print(func(5))