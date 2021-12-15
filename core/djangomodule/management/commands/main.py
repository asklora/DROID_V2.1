from ingestion.firestore_migration import firebase_universe_update
from general.sql_query import get_universe_by_region
from django.core.management.base import BaseCommand
from general.date_process import dateNow, str_to_date

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="na", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="ws", default=False)
        parser.add_argument("-firebase_universe", "--firebase_universe", type=bool, help="firebase_universe", default=False)

    def handle(self, *args, **options):
        try:
            status = ""
            if (options["na"]):
                if(options["firebase_universe"]):
                    status = "Update Firebase Universe"
                    firebase_universe_update(currency_code=["HKD"])
            if (options["ws"]):
                if(options["firebase_universe"]):
                    status = "Update Firebase Universe"
                    firebase_universe_update(currency_code=["USD"])
        except Exception as e:
            print("{} : === {} ERROR === : {}".format(dateNow(), status, e))
            