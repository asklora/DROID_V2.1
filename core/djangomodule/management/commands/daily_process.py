from ingestion.data_from_dsws import update_data_dsws_from_dsws
from ingestion.data_from_dss import update_data_dss_from_dss
from general.sql_process import do_function
from ingestion.master_ohlcvtr import master_ohlctr_update
from ingestion.master_tac import master_tac_update
from ingestion.master_multiple import master_multiple_update
from general.sql_query import get_active_universe, get_universe_by_region
from django.core.management.base import BaseCommand

def daily_process_ohlcvtr(region_id = None):
    if(type(region_id) != type(None)):
        ticker = get_universe_by_region(region_id=region_id)
    else:
        ticker = get_active_universe()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    do_function("special_cases_1")
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="north_asia", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="west", default=False)
        
    def handle(self, *args, **options):
        if(options["na"]):
            daily_process_ohlcvtr(region_id=["na"])
        elif(options["ws"]):
            daily_process_ohlcvtr(region_id=["ws"])