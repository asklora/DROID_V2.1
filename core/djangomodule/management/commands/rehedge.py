from django.core.management.base import BaseCommand
from core.services.tasks import populate_client_top_stock_weekly, order_client_topstock, daily_hedge, send_csv_hanwha, hedge

class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-c", "--currency", type=str, help="currency")

    def handle(self, *args, **options):
        daily_hedge(currency=options['currency'],rehedge={
            'date':'2021-10-05',
            'types':'hedge'
        })
