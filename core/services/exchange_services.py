from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket
from datetime import datetime
import subprocess
import os
from django.conf import settings

def restart_worker():
    # tidak perlu conditional lagi karena bisa di mock di test
    envrion = os.environ.get('DJANGO_SETTINGS_MODULE', False)
    if envrion in ['config.settings.production','config.settings.prodtest']:
        subprocess.Popen(['docker', 'restart', 'Celery','CeleryBroadcaster'])
        return {'response':'restart celery prod','code':200}
    elif envrion in ['config.settings.local','config.settings.test','config.settings.development'] :
        subprocess.Popen(['docker', 'restart', 'Celery'])
        return {'response':'restart celery staging','code':200}
    return {'response':'both function not executed','code':400}



def update_due(exchange: ExchangeMarket) -> bool:
    # harus dijadikan fungsi sendiri agar bisa di mock
    from django.utils import timezone  # jare luih aman nganggo iki
    return exchange.until_time < timezone.now()


@app.task(ignore_result=True)
def market_task_checker():
    exchanges = ExchangeMarket.objects.filter(currency_code__in=["HKD","USD"])
    exchanges = exchanges.filter(group='Core')
    fail = []
    for exchange in exchanges:
        if update_due(exchange):
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
