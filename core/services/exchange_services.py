from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket
from django.utils import timezone
import subprocess
import os
from datetime import timedelta
def restart_worker():
    envrion = os.environ.get("DJANGO_SETTINGS_MODULE", False)
    if envrion in ["config.settings.production", "config.settings.prodtest"]:
        subprocess.Popen(["docker", "restart", "CeleryBroadcaster" , "Celery"])
        return {"response": "restart celery prod", "code": 200}
    elif envrion in [
        "config.settings.local",
        "config.settings.test",
        "config.settings.development",
    ]:
        subprocess.Popen(["docker", "restart", "Celery"])
        return {"response": "restart celery staging", "code": 200}
    return {"response": "both function not executed", "code": 400}


def update_due(exchange: ExchangeMarket) -> bool:
    return (exchange.until_time + timedelta(minutes=15)) < timezone.now()


@app.task(ignore_result=True)
def market_task_checker():
    exchanges = ExchangeMarket.objects.filter(currency_code__in=["HKD", "USD"])
    exchanges = exchanges.filter(group="Core")
    fail = []
    for exchange in exchanges:
        if update_due(exchange):
            fail.append(exchange.mic)
    if fail:
        restart_worker()
    return {"message": fail}





@app.task(ignore_result=True)
def init_exchange_check(currency:list=None):
    currency_list = ["HKD", "USD"] if not currency else currency
    exchanges = ExchangeMarket.objects.filter(currency_code__in=currency_list)
    exchanges = exchanges.filter(group="Core")
    initial_id_task=[]
    for exchange in exchanges:
        market_check_routines(exchange)
        initial_id_task.append(exchange.mic)
    return {"message": initial_id_task}


def market_check_routines(exchange):
    if update_due(exchange):
        market = TradingHours(mic=exchange.mic)
        market.run_market_check()
