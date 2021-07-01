from django.core.management.base import BaseCommand, CommandError
from config.celery import app_publish,app






class Command(BaseCommand):
    def handle(self, *args, **options):
        app.send_task("hello world",kwargs={"test":"222"},queue='javascript')