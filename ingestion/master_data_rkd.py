from general.table_name import get_universe_table_name
from general.sql_output import upsert_data_to_database
from general.slack import report_to_slack
from general.data_process import remove_null
from general.sql_query import get_active_universe
from general.date_process import datetimeNow
import django
import os
debug = os.environ.get("DJANGO_SETTINGS_MODULE",True)
if debug:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.production") # buat Prod DB
else:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.development") # buat test DB
django.setup()
from datasource.rkd import RkdData

def update_lot_size_from_dss(ticker=None, currency_code=None):
    print("{} : === Lot Size Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["lot_size"])
    ticker = universe["ticker"].to_list()
    field = ["CF_LOTSIZE"]
    rkd = RkdData()
    result = rkd.get_data_from_rkd(ticker, field)
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "CF_LOTSIZE": "lot_size"
        })
        result = remove_null(result, "lot_size")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Lot Size Updated ===".format(datetimeNow()))