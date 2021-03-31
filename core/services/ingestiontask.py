from ingestion.master_data import update_fundamentals_quality_value
from config.celery import app
from datetime import datetime
from main import (
    update_lot_size_from_dss,quandl,
    vix,daily_na,daily_ws,
    weekly,timezones,monthly,
    dlpa_weekly,populate_latest_price,
    update_worldscope_quarter_summary_from_dsws,
    daily_uno_ucdc,
    update_fundamentals_score_from_dsws
    )
from core.universe.models import Universe
import sys




@app.task
def get_ohlcvtr_na():
    """
    Mon-Fri at 16:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/ohlcvtr_ws_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            daily_ws() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"Ws market is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_ohlcvtr_ws():
    """
    Tue-Sat at 00:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/ohlcvtr_na_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            daily_na() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"Na market is updated"}
    except Exception as e:
        return {"err":str(e)}

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
                sys.stdout = f # Change the standard output to the file we created.
                update_lot_size_from_dss(ticker=ticker)
                sys.stdout = original_stdout
        return {"result":f"lotsize updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_quandl():
    """
    Mon-Sat at 13:45
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/quandl_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            quandl() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"quandl is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_vix():
    """
    Mon-Sat at 21:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/vix_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            vix() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"Vix is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_weekly():
    """
    Sat at 03:15
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/weekly_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            weekly() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"weekly is updated"}
    except Exception as e:
        return {"err":str(e)}


@app.task
def get_timezones():
    """
    Sun at 20:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/timezone_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            timezones() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"timezone is updated"}
    except Exception as e:
        return {"err":str(e)}



@app.task
def get_monthly():
    """
    First Saturday Every Month at 03:00
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/monthly_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            monthly() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"monthly is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_dlpa_weekly():
    """
    Sun at 03:30
    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/dlpa_weekly_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            dlpa_weekly() # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"dlpa_weekly is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_latest_price(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/latest_price_{currency}_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            populate_latest_price(currency_code=currency) # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"latest_price is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_worldscope_quarter_summary(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/worldscope_quarter_{currency}_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            update_worldscope_quarter_summary_from_dsws(currency_code=currency) # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"worldscope_quarter is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_fundamentals_score(currency):
    """
    follow currency time

    """
    now = datetime.now()
    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/fundamental_score_{currency}_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            update_fundamentals_score_from_dsws(currency_code=currency) # triger ingestion function
            if (currency == ["USD"]):
                update_fundamentals_quality_value()
            sys.stdout = original_stdout
        return {"result":f"fundamental_score is updated"}
    except Exception as e:
        return {"err":str(e)}

@app.task
def get_daily_uno_ucdc(currency,infer=True):
    """
    follow currency time

    """
    now = datetime.now()
    if infer == True or infer == "True":
        infer = True
    else:
        infer = False

    try:
        original_stdout = sys.stdout # Save a reference to the original standard output
        with open(f"logger/daily_uno_ucdc_{currency}_{now}.txt", "w") as f:
            sys.stdout = f # Change the standard output to the file we created.
            daily_uno_ucdc(currency_code=currency,infer=infer) # triger ingestion function
            sys.stdout = original_stdout
        return {"result":f"daily_uno_ucdc {currency} is updated"}
    except Exception as e:
        return {"err":str(e)}