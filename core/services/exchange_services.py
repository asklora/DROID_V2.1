from config.celery import app
from core.djangomodule.calendar import TradingHours
from core.universe.models import ExchangeMarket




@app.task
def init_exchange_check():
    exchanges = ExchangeMarket.objects.exclude(currency_code=None)
    exchanges = exchanges.filter(group='Core')
    for exchange in exchanges:
        market = TradingHours(mic=exchange.mic)
        market.run_market_check()
        if market.time_to_check:
            market_check_routines.apply_async(args=(exchange.mic,),eta=market.time_to_check)


@app.task
def market_check_routines(mic):
    market = TradingHours(mic=mic)
    market.run_market_check()
    if market.time_to_check:
        market_check_routines.apply_async(args=(mic,),eta=market.time_to_check)
