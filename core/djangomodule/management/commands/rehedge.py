from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge

class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-c", "--currency", type=str, help="currency")

    def handle(self, *args, **options):
        daily_hedge(currency=options['currency'],rehedge={
            'date':'2021-10-25',
            'types':'hedge'
        })
