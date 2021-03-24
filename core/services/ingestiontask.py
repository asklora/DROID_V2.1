from config.celery import app
from datetime import datetime
from main import update_ohlcvtr,update_quandl_orats_from_quandl
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