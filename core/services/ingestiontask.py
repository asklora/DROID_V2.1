from config.celery import app
from datetime import datetime
from main import (
    update_ohlcvtr,
    update_quandl_orats_from_quandl,
    update_lot_size_from_dss,
    )
from core.universe.models import Universe
import sys




@app.task
def dailyupdateohlcvtr(currency):
    now = datetime.now()
    try:
        ticker = Universe.manager.get_ticker_list_by_currency(currency)
        original_stdout = sys.stdout # Save a reference to the original standard output

        with open(f'ohlcvtr_{currency}_{now}.txt', 'w') as f:
            sys.stdout = f # Change the standard output to the file we created.
            update_ohlcvtr(ticker=ticker,currency_code=currency)
            sys.stdout = original_stdout
        return {'result':f'{currency} is updated'}
    except Exception as e:
        return {'err':str(e)}

@app.task
def updatequandldsws():
    now = datetime.now()
    original_stdout = sys.stdout
    try:
        with open(f'quandle_daily_{now}.txt', 'w') as f:
                sys.stdout = f # Change the standard output to the file we created.
                update_quandl_orats_from_quandl()
                sys.stdout = original_stdout
        return {'result':f'quandle {now.date()} updated'}
    except Exception as e:
        return {'err':str(e)}

# lotsize sat 13 00
@app.task
def updatelotsize(currency):
    now = datetime.now()
    original_stdout = sys.stdout
    ticker = Universe.manager.get_ticker_list_by_currency(currency)
    try:
        with open(f'quandle_daily_{now}.txt', 'w') as f:
                sys.stdout = f # Change the standard output to the file we created.
                update_lot_size_from_dss(ticker=ticker)
                sys.stdout = original_stdout
        return {'result':f'quandle {now.date()} updated'}
    except Exception as e:
        return {'err':str(e)}

# everyday 21 15
@app.task
def updaterate(currency):
    now = datetime.now()
    original_stdout = sys.stdout
    ticker = Universe.manager.get_ticker_list_by_currency(currency)
    try:
        with open(f'quandle_daily_{now}.txt', 'w') as f:
                sys.stdout = f # Change the standard output to the file we created.
                update_lot_size_from_dss(ticker=ticker)
                sys.stdout = original_stdout
        return {'result':f'quandle {now.date()} updated'}
    except Exception as e:
        return {'err':str(e)}

