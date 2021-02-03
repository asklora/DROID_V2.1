from django.core.management.base import BaseCommand, CommandError
from core.services.pc4tasks import tasktest

class Command(BaseCommand):
    
    def handle(self, *args, **options):
        tasktest.delay()