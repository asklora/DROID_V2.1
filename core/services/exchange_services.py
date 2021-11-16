from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket
from django.utils import timezone
from core.djangomodule.celery_singleton import Singleton
import subprocess
import os
from django_celery_results.models import TaskResult


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


@app.task(acks_late=True)
def init_exchange_check(currency:list=None,task_id:str=None):
    currency_list = ["HKD", "USD"] if not currency else currency
    exchanges = ExchangeMarket.objects.filter(currency_code__in=currency_list)
    exchanges = exchanges.filter(group="Core")
    initial_id_task=[]
    for exchange in exchanges:
        market_check_routines.apply_async(
            args=(exchange.mic,),
            kwargs={"task_id": task_id},
        )
        initial_id_task.append(exchange.mic)
    return {"message": initial_id_task}


@app.task()
def market_check_routines(mic,task_id=None):
    if task_id:
        existed_tasks=TaskResult.objects.filter(task_id=task_id,status='SUCCESS').exists()
        if existed_tasks:
            return {"message": f"task {task_id} already existed"}
    market = TradingHours(mic=mic)
    market.run_market_check()
    if not task_id:
        task_id = task_id_maker(mic, market.time_to_check)
    if market.time_to_check:
        init_exchange_check.apply_async(
            kwargs={"currency":[market.exchange.currency_code.currency_code],"task_id": task_id},
            eta=market.time_to_check,
            task_id=task_id
            )
        return {"message": f"task {task_id} scheduled"}
