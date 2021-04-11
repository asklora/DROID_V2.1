from django.core.management.base import BaseCommand
from core.services import *
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        data = {
            'task':'hellow'
        }
        celery_app.send_task('config.celery.listener',args=(data,),queue='batch')