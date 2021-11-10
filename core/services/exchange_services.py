from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket
from django.utils import timezone
from core.djangomodule.celery_singleton import Singleton
import subprocess
import os



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
    return exchange.until_time < timezone.now()


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


def task_id_maker(mic,time):
    return f"{mic}-{time.strftime('%s')}"


@app.task(base=Singleton)
def init_exchange_check():
    exchanges = ExchangeMarket.objects.filter(currency_code__in=["HKD", "USD"])
    exchanges = exchanges.filter(group="Core")

    for exchange in exchanges:
        market = TradingHours(mic=exchange.mic)
        market.run_market_check()
        if market.time_to_check:
            task_id = task_id_maker(exchange.mic, market.time_to_check)
            market_check_routines.apply_async(
                args=(exchange.mic,task_id), eta=market.time_to_check,task_id=task_id
            )


@app.task(base=Singleton,unique_on=['taskid',],acks_late=True)
def market_check_routines(mic,taskid):
    market = TradingHours(mic=mic)
    market.run_market_check()
    task_id = task_id_maker(mic, market.time_to_check)
    if market.time_to_check :
        market_check_routines.apply_async(args=(mic,task_id), eta=market.time_to_check,task_id=task_id)
