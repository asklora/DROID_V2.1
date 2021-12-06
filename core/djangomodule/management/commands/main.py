from ingestion.firestore_migration import firebase_universe_update
from general.sql_query import get_universe_by_region
from django.core.management.base import BaseCommand
from general.date_process import dateNow

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="na", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="ws", default=False)


    def handle(self, *args, **options):
        try:
            status = ""
            if (options["na"]):
                ticker = get_universe_by_region(region_id=["na"])["ticker"].to_list()
                status = "Update Firebase Universe"
                firebase_universe_update(currency_code=["HKD"])

            if (options["ws"]):
                ticker = get_universe_by_region(region_id=["ws"])["ticker"].to_list()
                status = "Update Firebase Universe"
                firebase_universe_update(currency_code=["USD"])

        except Exception as e:
            print("{} : === {} ERROR === : {}".format(dateNow(), status, e))
            