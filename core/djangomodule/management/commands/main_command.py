from datetime import datetime, date
from django.core.management.base import BaseCommand, CommandError
import redis

class Command(BaseCommand):

    def handle(self, *args, **options):
        print('ok')
