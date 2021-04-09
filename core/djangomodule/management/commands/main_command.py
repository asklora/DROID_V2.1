from datetime import datetime, date
from django.core.management.base import BaseCommand, CommandError
from config.celery import app
class Command(BaseCommand):

    def handle(self, *args, **options):
        node = app.control.inspect()
        print(app.control.ping(timeout=0.5))
        print(node.scheduled())
