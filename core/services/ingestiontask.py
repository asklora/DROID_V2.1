from ingestion.master_multiple import master_multiple_update
from bot.preprocess import dividend_daily_update, interest_daily_update
from ingestion.data_from_rkd import update_currency_price_from_rkd, update_lot_size_from_rkd
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from general.sql_process import do_function
from ingestion.data_from_dsws import interest_update_from_dsws, populate_ibes_table, populate_macro_table, update_data_dsws_from_dsws, update_entity_type_from_dsws, update_ibes_data_monthly_from_dsws, update_macro_data_monthly_from_dsws, update_ticker_name_from_dsws
from ingestion.data_from_dss import populate_latest_price_from_dss, update_data_dss_from_dss, update_ticker_symbol_from_dss
from general.sql_query import get_universe_by_region
from config.celery import app
from datetime import datetime
from main import (daily_ingestion)
import sys
from core.djangomodule.general import aws_batch
from migrate import daily_migrations
from general.slack import report_to_slack
import traceback as trace
from datasource.rkd import RkdData
from core.services.models import ErrorLog
from celery.schedules import crontab
from config.settings import db_debug

if not db_debug:
    app.conf.beat_schedule={
        
        
        "WS-TRKD-DATA": {
            "task": "core.services.ingestionstask.get_trkd_data_by_region",
            "schedule": crontab(minute=30, hour=00, day_of_week="1-5"),
            "kwargs": {"region": "ws"},
        },
        "NA-TRKD-DATA": {
            "task": "core.services.ingestionstask.get_trkd_data_by_region",
            "schedule": crontab(minute=30, hour=16, day_of_week="1-5"),
            "kwargs": {"region": "na"},
        },

    }






@app.task
def get_trkd_data_by_region(region=None):
    try:
        rkd = RkdData()
        universe = get_universe_by_region(region)['ticker'].tolist()
        rkd.get_rkd_data(universe,save=True)
        return {'region':f'{region}','message':'success'}
    except Exception as e:
        err = ErrorLog.objects.create_log(error_description=f"===  ERROR IN POPULATE TRKD DATA FOR REGION {region} ===",error_message=str(e))
        err.send_report_error()
        return {"err": str(e)}

def weekly():
    update_ticker_name_from_dsws()
    do_function("universe_populate")
    update_lot_size_from_rkd()
    update_ticker_symbol_from_dss()
    update_entity_type_from_dsws()

    ticker = get_universe_by_region(region_id="na")
    ticker = ticker["ticker"].to_list()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    
    ticker = get_universe_by_region(region_id="ws")
    ticker = ticker["ticker"].to_list()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()
    update_ibes_data_monthly_from_dsws()
    update_macro_data_monthly_from_dsws()

## DEPRECATED
@aws_batch
@app.task
def batch_weekly():
    """
    Sat at 03:15
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/weekly_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            weekly()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"weekly is updated"}
    except Exception as e:
        return {"err": str(e)}

## DEPRECATED
@app.task
def get_latest_price(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/latest_price_{currency}_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            # triger ingestion function
            populate_latest_price_from_dss(currency_code=currency)
            sys.stdout = original_stdout      
    except Exception as e:
        report_to_slack("===  migrate daily error ===")
        report_to_slack(str(e))
        return {"err": str(e)}
    return {"result": f"latest_price is updated"}




## DEPRECATED
@aws_batch
@app.task
def migrate_na():
    try:
        
        daily_migrations()  # triger ingestion function
        daily_ingestion(region_id="na") # one region
        ticker = get_universe_by_region(region_id="na")
        populate_latest_price_from_dss(ticker=ticker["ticker"])

        populate_macro_table()
        populate_ibes_table()
        do_function("master_ohlcvtr_update")
        master_ohlctr_update()
        master_tac_update()
        update_currency_price_from_rkd()
        interest_update_from_dsws()
        dividend_daily_update()
        interest_daily_update()
        return {"result": f"migrate NA daily done"}
    except Exception as e:
        line_error = trace.format_exc()
        report_to_slack("===  migrate daily NA error ===")
        report_to_slack(str(e))
        report_to_slack(line_error)
        return {"err": line_error}



## DEPRECATED

@aws_batch
@app.task
def migrate_ws():
    try:

        daily_migrations()  # triger ingestion function
        daily_ingestion(region_id="ws")
        ticker = get_universe_by_region(region_id="ws")
        populate_latest_price_from_dss(ticker=ticker["ticker"])

        populate_macro_table()
        populate_ibes_table()
        do_function("master_ohlcvtr_update")
        master_ohlctr_update()
        master_tac_update()
        update_currency_price_from_rkd()
        interest_update_from_dsws()
        dividend_daily_update()
        interest_daily_update()
            
        return {"result": f"migrate daily done"}
    except Exception as e:
        line_error = trace.format_exc()
        report_to_slack("===  migrate daily WS error ===")
        report_to_slack(str(e))
        report_to_slack(line_error)
        return {"err": line_error}


## DEPRECATED
@app.task
def migrate_droid1(region):
    try:

        report_to_slack("===  CREATE AWS BATCH FOR DAILY MIGRATIONS ===")
        if region == 'na':
            migrate_na()  # triger ingestion function
        elif region == 'ws':
            migrate_ws()  # triger ingestion function

        report_to_slack("===  CLOSING AWS BATCH FOR DAILY MIGRATIONS JOB DONE ===")
            
        return {"result": f"migrate daily done"}
    except Exception as e:
        return {"err": str(e)}
