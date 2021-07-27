from django_celery_beat.models import PeriodicTask
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-q", "--queue", type=str, help="email")

    def handle(self, *args, **options):
        if options['queue']:
            PeriodicTask.objects.all().update(queue=options['queue'])
        else:
            print('queue not set')
