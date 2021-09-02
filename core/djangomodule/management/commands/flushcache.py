from django.core.management.base import BaseCommand

from core.djangomodule.general import flush_cache

class Command(BaseCommand):

    def handle(self, *args, **options):
        flush_cache()