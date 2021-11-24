
from django.core.management.base import BaseCommand
from config.celery import app
from core.djangomodule.general import jsonprint

class Command(BaseCommand):
    def handle(self, *args, **options):
        i = app.control.inspect()
        # Show the items that have an ETA or are scheduled for later processing
        jsonprint(i.scheduled())
        # Show tasks that are currently active.
        # i.active()