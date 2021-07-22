from django.core.management.base import BaseCommand, CommandError
from config.celery import app_publish, app,debug_task


class Command(BaseCommand):
    def handle(self, *args, **options):
        debug_task.apply_async(queue='broadcaster')
        # app.send_task("djangomodule.aoa.apa", kwargs={
        #               "payload": "222"}, queue='javascript')
