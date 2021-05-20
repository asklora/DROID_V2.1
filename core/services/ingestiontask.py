from ingestion.master_data import update_fundamentals_quality_value
from config.celery import app
from datetime import datetime
from main import (
    daily_ingestion, update_lot_size_from_dss, quandl,
    vix, daily_na, daily_ws,
    weekly, timezones, monthly,
    dlpa_weekly, populate_latest_price,
    update_worldscope_quarter_summary_from_dsws,
    daily_uno_ucdc,
    update_fundamentals_score_from_dsws, populate_macro_table,
    populate_ibes_table,
    update_quandl_orats_from_quandl,
    do_function,
    master_ohlctr_update,
    master_tac_update,
    update_currency_price_from_dss,
    interest_update,
    dividend_daily_update,
    interest_daily_update
)
from core.universe.models import Universe
import sys
from core.djangomodule.general import aws_batch
from migrate import daily_migrations
from general.slack import report_to_slack


@app.task
def get_ohlcvtr_na():
    """
    Mon-Fri at 16:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/ohlcvtr_ws_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            daily_ws()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"Ws market is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_ohlcvtr_ws():
    """
    Tue-Sat at 00:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/ohlcvtr_na_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            daily_na()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"Na market is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def updatelotsize(currency):
    """
    currency -> list or str
    sat at 13 00 utc
    """
    now = datetime.now()
    original_stdout = sys.stdout
    ticker = Universe.manager.get_ticker_list_by_currency(currency)
    try:
        with open(f"logger/lotsize_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            update_lot_size_from_dss(ticker=ticker)
            sys.stdout = original_stdout
        return {"result": f"lotsize updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_quandl():
    """
    Mon-Sat at 13:45
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/quandl_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            quandl()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"quandl is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_vix():
    """
    Mon-Sat at 21:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/vix_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            vix()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"Vix is updated"}
    except Exception as e:
        return {"err": str(e)}


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


@app.task
def get_weekly():
    """
    Sat at 03:15
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/weekly_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            batch_weekly()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"weekly is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_timezones():
    """
    Sun at 20:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/timezone_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            timezones()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"timezone is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_monthly():
    """
    First Saturday Every Month at 03:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/monthly_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            monthly()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"monthly is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_dlpa_weekly():
    """
    Sun at 03:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/dlpa_weekly_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            dlpa_weekly()  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"dlpa_weekly is updated"}
    except Exception as e:
        return {"err": str(e)}


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
            populate_latest_price(currency_code=currency)
            sys.stdout = original_stdout
        return {"result": f"latest_price is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_worldscope_quarter_summary(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/worldscope_quarter_{currency}_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            update_worldscope_quarter_summary_from_dsws(
                currency_code=currency)  # triger ingestion function
            sys.stdout = original_stdout
        return {"result": f"worldscope_quarter is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_fundamentals_score(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/fundamental_score_{currency}_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            update_fundamentals_score_from_dsws(
                currency_code=currency)  # triger ingestion function
            if (currency == ["USD"]):
                update_fundamentals_quality_value()
            sys.stdout = original_stdout
        return {"result": f"fundamental_score is updated"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def get_daily_uno_ucdc(currency, infer=True):
    """
    follow currency time

    """
    now = datetime.now()
    if infer == True or infer == "True":
        infer = True
    else:
        infer = False

    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"logger/daily_uno_ucdc_{currency}_{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            # triger ingestion function
            daily_uno_ucdc(currency_code=currency, infer=infer)
            sys.stdout = original_stdout
        return {"result": f"daily_uno_ucdc {currency} is updated"}
    except Exception as e:
        return {"err": str(e)}


@aws_batch
@app.task
def migrate():
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"files/migrate{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            daily_migrations()  # triger ingestion function
            daily_ingestion(region_id=["na"])
            daily_ingestion(region_id=["ws"])
            populate_macro_table()
            populate_ibes_table()
            do_function("master_ohlcvtr_update")
            master_ohlctr_update()
            master_tac_update()
            update_currency_price_from_dss()
            interest_update()
            dividend_daily_update()
            interest_daily_update()
            sys.stdout = original_stdout
        return {"result": f"migrate daily done"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def migrate_droid1():
    now = datetime.now()
    try:
        original_stdout = sys.stdout  # Save a reference to the original standard output
        with open(f"files/migrate{now}.txt", "w") as f:
            # Change the standard output to the file we created.
            sys.stdout = f
            report_to_slack("===  CREATE AWS BATCH FOR DAILY MIGRATIONS ===")
            migrate()  # triger ingestion function
            report_to_slack("===  CLOSING AWS BATCH FOR DAILY MIGRATIONS JOB DONE ===")
            
            sys.stdout = original_stdout
        return {"result": f"migrate daily done"}
    except Exception as e:
        return {"err": str(e)}
