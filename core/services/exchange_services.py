from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket
from datetime import datetime
import subprocess
import os

def restart_worker():
    envrion = os.environ.get('DJANGO_SETTINGS_MODULE', False)
    if envrion in ['config.settings.production','config.settings.prodtest']:
        subprocess.Popen(['docker', 'restart', 'Celery','CeleryBroadcaster'])
        return {'response':'restart celery prod'}
    else:
        subprocess.Popen(['docker', 'restart', 'Celery'])
        return {'response':'restart celery staging'}





@app.task(ignore_result=True)
def market_task_checker():
    exchanges = ExchangeMarket.objects.filter(currency_code__in=["HKD","USD"])
    exchanges = exchanges.filter(group='Core')
    fail = []
    for exchange in exchanges:
        if exchange.until < datetime.now():
            fail.append(exchange.mic)
    if fail:
        restart_worker()
    return {'message':fail}


@app.task(ignore_result=True)
def init_exchange_check():
    exchanges = ExchangeMarket.objects.filter(currency_code__in=["HKD","USD"])
    exchanges = exchanges.filter(group='Core')
    for exchange in exchanges:
        market = TradingHours(mic=exchange.mic)
        market.run_market_check()
        if market.time_to_check:
            market_check_routines.apply_async(args=(exchange.mic,),eta=market.time_to_check)


@app.task(ignore_result=True)
def market_check_routines(mic):
    market = TradingHours(mic=mic)
    market.run_market_check()
    if market.time_to_check:
        market_check_routines.apply_async(args=(mic,),eta=market.time_to_check)
