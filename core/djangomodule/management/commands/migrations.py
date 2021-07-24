from ingestion.data_from_timezone import update_utc_offset_from_timezone
from ingestion.data_from_dsws import interest_update_from_dsws, populate_ibes_table, populate_macro_table
from general.sql_process import do_function
from django.core.management.base import BaseCommand
from migrate import weekly_migrations, daily_migrations
from bot.preprocess import dividend_daily_update, interest_daily_update
from general.sql_process import do_function
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-weekly", "--weekly", help="weekly", type=bool, default=False)
        parser.add_argument("-daily", "--daily", help="daily", type=bool, default=False)

    def handle(self, *args, **options):
        do_function("universe_populate")
        if(options["daily"]):
            
            daily_migrations()
        else:
            weekly_migrations()
        populate_macro_table()
        populate_ibes_table()
        do_function("special_cases_1")
        do_function("master_ohlcvtr_update")
        master_ohlctr_update()
        master_tac_update()
        interest_update_from_dsws()
        dividend_daily_update()
        interest_daily_update()
        update_utc_offset_from_timezone()
        print("Done")