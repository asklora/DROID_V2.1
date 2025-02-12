# PYTHON TOOLS
from core.djangomodule.general import is_end_of_month
from core.services.notification import send_winner_email
from ingestion.data_from_dsws import update_ibes_currency_from_dsws
import time as tm
import pandas as pd
import io
import asyncio
from asgiref.sync import sync_to_async
from channels.layers import get_channel_layer
from datetime import datetime, timedelta
from pandas.core.series import Series
from django.core.cache import cache
from django.conf import settings
# CELERY APP
from config.celery import app
from celery.schedules import crontab
from celery import group as celery_groups
# MODELS AND UTILS
from core.universe.models import Universe, UniverseConsolidated,ExchangeMarket
from core.Clients.models import ClientTopStock, UniverseClient
from core.user.models import User
from core.djangomodule.serializers import CsvSerializer
# from core.djangomodule.yahooFin import get_quote_index, get_quote_yahoo
from core.master.models import Currency
from core.orders.models import Order, PositionPerformance, OrderPosition
from core.Clients.models import UserClient, Client
from core.services.models import ErrorLog
from .models import ChannelPresence
from channels_presence.models import Presence
from django.core.mail import EmailMessage
from django.conf import settings
from datasource.rkd import RkdData
from bot.calculate_bot import populate_daily_profit, update_monthly_deposit
# RAW SQL QUERY MODULE
from general.sql_process import do_function
# SLACK REPORT
from general.slack import report_to_slack
# date interval
from general.date_process import date_interval
# POPULATE TOPSTOCK MODULE
from portfolio.client_test_pick import populate_fels_bot, test_pick, populate_bot_advisor, populate_bot_tester
# HEDGE MODULE
from portfolio.daily_hedge_user import user_position_check
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
from ingestion import (
    update_quandl_orats_from_quandl,
    update_data_dss_from_dss, 
    update_ticker_symbol_from_dss,
    dividend_updated_from_dsws,
    update_company_desc_from_dsws,
    update_currency_code_from_dsws,
    update_data_dsws_from_dsws,
    update_entity_type_from_dsws,
    update_industry_from_dsws,
    update_lot_size_from_dsws,
    update_mic_from_dsws,
    update_ticker_name_from_dsws,
    update_worldscope_identifier_from_dsws
    )


USD_CUR = Currency.objects.get(currency_code="USD")
HKD_CUR = Currency.objects.get(currency_code="HKD")
KRW_CUR = Currency.objects.get(currency_code="KRW")
CNY_CUR = Currency.objects.get(currency_code="CNY")
EUR_CUR = Currency.objects.get(currency_code="EUR")

channel_layer = get_channel_layer()

# TASK SCHEDULE
app.conf.beat_schedule = {
    "ping-presence": {
        "task": "core.services.tasks.ping_available_presence",
        "schedule": timedelta(seconds=50),
        "options": {
            "expires": 5,
        }
        # "options":{
        #     "queue":"local"
        # }
    },
    "prune-presence": {
        "task": "core.services.tasks.channel_prune",
        "schedule": timedelta(seconds=60),
        "options": {
            "expires": 5,
        }
        # "options":{
        #     "queue":"local"
        # }
    },
    "TradingHours": {
        "task": "core.services.exchange_services.init_exchange_check",
        "schedule": timedelta(seconds=60),
        "options": {
            "expires": 5,
        }
        # "options":{
        #     "queue":"local"
        # }
    },
    # "USD-HEDGE": {
    #     "task": "core.services.tasks.daily_hedge",
    #     "schedule": crontab(minute=USD_CUR.hedge_schedule.minute, hour=USD_CUR.hedge_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "USD"},
    #     "options": {
    #         "expires": 5*60,
    #     }

    # },
    "HKD-HEDGE": {
        "task": "core.services.tasks.daily_hedge",
        "schedule": crontab(minute=HKD_CUR.hedge_schedule.minute, hour=HKD_CUR.hedge_schedule.hour, day_of_week="1-5"),
        "kwargs": {"currency": "HKD"},
        "options": {
            "expires": 5*60,
        }
    },
    # "KRW-HEDGE": {
    #     "task": "core.services.tasks.daily_hedge",
    #     "schedule": crontab(minute=KRW_CUR.hedge_schedule.minute, hour=KRW_CUR.hedge_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "KRW"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "CNY-HEDGE": {
    #     "task": "core.services.tasks.daily_hedge",
    #     "schedule": crontab(minute=CNY_CUR.hedge_schedule.minute, hour=CNY_CUR.hedge_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "CNY"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "EUR-HEDGE": {
    #     "task": "core.services.tasks.daily_hedge",
    #     "schedule": crontab(minute=EUR_CUR.hedge_schedule.minute, hour=EUR_CUR.hedge_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "EUR"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "USD-POPULATE-PICK": {
    #     "task": "core.services.tasks.populate_client_top_stock_weekly",
    #     "schedule": crontab(minute=USD_CUR.top_stock_schedule.minute, hour=USD_CUR.top_stock_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "USD"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "HKD-POPULATE-PICK": {
    #     "task": "core.services.tasks.populate_client_top_stock_weekly",
    #     "schedule": crontab(minute=HKD_CUR.top_stock_schedule.minute, hour=HKD_CUR.top_stock_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "HKD"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "KRW-POPULATE-PICK": {
    #     "task": "core.services.tasks.populate_client_top_stock_weekly",
    #     "schedule": crontab(minute=KRW_CUR.top_stock_schedule.minute, hour=KRW_CUR.top_stock_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "KRW"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "CNY-POPULATE-PICK": {
    #     "task": "core.services.tasks.populate_client_top_stock_weekly",
    #     "schedule": crontab(minute=CNY_CUR.top_stock_schedule.minute, hour=CNY_CUR.top_stock_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "CNY"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "EUR-POPULATE-PICK": {
    #     "task": "core.services.tasks.populate_client_top_stock_weekly",
    #     "schedule": crontab(minute=EUR_CUR.top_stock_schedule.minute, hour=EUR_CUR.top_stock_schedule.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "EUR"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    "EUR-POPULATE-PICK-FELS": {
        "task": "core.services.tasks.populate_client_top_stock_weekly",
        "schedule": crontab(minute=EUR_CUR.top_stock_schedule.minute, hour=EUR_CUR.top_stock_schedule.hour, day_of_week="1-5"),
        "kwargs": {"currency": "EUR", "client_name": "FELS"},
        "options": {
            "expires": 5*60,
        }
    },
    # LATESTPRICE / QUOTES UPDATE
    # "EUR-Latestprice-update": {
    #     "task": "core.services.tasks.get_latest_price",
    #     "schedule": crontab(minute=EUR_CUR.ingestion_time.minute, hour=EUR_CUR.ingestion_time.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "EUR"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    "HKD-Latestprice-update": {
        "task": "core.services.tasks.get_latest_price",
        "schedule": crontab(minute=HKD_CUR.hedge_schedule.minute+15, hour=HKD_CUR.ingestion_time.hour, day_of_week="1-5"),
        "kwargs": {"currency": "HKD"},
        "options": {
            "expires": 5*60,
        }
    },
    # "CNY-Latestprice-update": {
    #     "task": "core.services.tasks.get_latest_price",
    #     "schedule": crontab(minute=CNY_CUR.ingestion_time.minute, hour=CNY_CUR.ingestion_time.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "CNY"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    # "KRW-Latestprice-update": {
    #     "task": "core.services.tasks.get_latest_price",
    #     "schedule": crontab(minute=KRW_CUR.ingestion_time.minute, hour=KRW_CUR.ingestion_time.hour, day_of_week="1-5"),
    #     "kwargs": {"currency": "KRW"},
    #     "options": {
    #         "expires": 5*60,
    #     }
    # },
    "USD-Latestprice-update": {
        "task": "core.services.tasks.get_latest_price",
        "schedule": crontab(minute=USD_CUR.hedge_schedule.minute+15 , hour=USD_CUR.ingestion_time.hour, day_of_week="1-5"),
        "kwargs": {"currency": "USD"},
        "options": {
            "expires": 5*60,
        }
    },

    "Firebase-Universe-update": {
    "task": "core.services.tasks.weekly_universe_firebase_update",
    "schedule": crontab(minute=00, hour=00, day_of_week=5),
    "kwargs": {"currency_code": ["HKD"]},
    "options": {
        "expires": 5*60,
        }
    },
    "USD-MARKET-CHECKER":{
        "task":"core.services.exchange_services.market_task_checker",
        "schedule": crontab(minute=00, hour=4, day_of_week=5),
        "options": {
            "expires": 5*60,
            }
    },
    "HKD-MARKET-CHECKER":{
        "task":"core.services.exchange_services.market_task_checker",
        "schedule": crontab(minute=00, hour=16, day_of_week=5),
        "options": {
            "expires": 5*60,
            }
    },
    # "HealthCheck": {
    #     "task": "core.services.healthcheck.run.run_healthcheck",
    #     "schedule": timedelta(minutes=15),
    #     "options": {
    #         "expires": 5*60,
    #     },
    # },
}
# END TASK SCHEDULE


# FILE BUFFER FUNCtION

def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)
        return buffer.getvalue()
# END FILE BUFFER



# TASK TO PING EXISTING CONNECTION THAT CONNECTED TO WEBSOCKET
def get_presence():
    channels = [c["channel_name"]
                for c in Presence.objects.all().values("channel_name")]
    return channels


async def gather_ping_presence():
    channels = await sync_to_async(get_presence)()
    tasks = []
    if channels:
        for channel in channels:
            tasks.append(asyncio.ensure_future(
                channel_layer.send(
                    channel,
                    {
                        "type": "broadcastmessage",
                        "message": "PING"
                    }
                )
            ))
        await asyncio.gather(*tasks)






@app.task(ignore_result=True)
def ping_available_presence():
    asyncio.run(gather_ping_presence())

# END PING

# MAINTAIN EXISTING CONNECTION


@app.task(ignore_result=True)
def channel_prune():
    ChannelPresence.objects.prune_presences()

# END MAINTAIN


def new_ticker_ingestion(ticker):
    update_ticker_name_from_dsws(ticker=ticker)
    update_ticker_symbol_from_dss(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dsws(ticker=ticker)
    update_currency_code_from_dsws(ticker=ticker)
    update_ibes_currency_from_dsws(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_mic_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)
    update_quandl_orats_from_quandl(ticker=ticker)
    if isinstance(ticker, Series) or isinstance(ticker, list):
        for tick in ticker:
            update_data_dss_from_dss(ticker=tick, history=True)
            update_data_dsws_from_dsws(ticker=tick, history=True)
            dividend_updated_from_dsws(ticker=tick)
    else:
        update_data_dss_from_dss(ticker=ticker, history=True)
        update_data_dsws_from_dsws(ticker=ticker, history=True)
        dividend_updated_from_dsws(ticker=ticker)

@app.task
def weekly_universe_firebase_update(currency_code:list) -> dict:
    from ingestion.firestore_migration import firebase_universe_update
    try:
        firebase_universe_update(currency_code=currency_code)
    except Exception as e:
        err = ErrorLog.objects.create_log(
        error_description=f"===  ERROR IN POPULATE UNIVERSER FIREBASE ===", error_message=str(e))
        err.send_report_error()
        return {'error':str(e)}
    return {'status':'updated firebase update'}


@app.task
def get_latest_price(currency=None):
    if currency:
        tickers = [ ticker['ticker'] for ticker in Universe.objects.prefetch_related('ticker').filter(currency_code=currency,is_active=True).values('ticker')]
        rkd = RkdData()
        rkd.get_rkd_data(tickers, save=True)
        return {'status':'updated','currency':currency}

@app.task
def get_isin_populate_universe(ticker, user_id):
    user = User.objects.get(id=user_id)
    res_celery = []
    symbols = []
    try:
        populate = UniverseConsolidated.ingestion_manager.get_isin_code(
            ticker=ticker)
        triger_sql_populate_once = 0
        if isinstance(ticker, list):
            for tick in ticker:
                triger_sql_populate_once += 1
                print(tick, "")
                ticker_cons = UniverseConsolidated.objects.filter(
                    origin_ticker=tick).distinct("origin_ticker").get()
                if ticker_cons.use_manual:
                    symbol = ticker_cons.origin_ticker
                else:
                    if ticker_cons.consolidated_ticker:
                        symbol = ticker_cons.consolidated_ticker
                    else:
                        symbol = ticker_cons.origin_ticker
                universe = Universe.objects.filter(ticker=symbol)
                if not universe.exists():
                    symbols.append(symbol)
                if triger_sql_populate_once == 1:
                    do_function("universe_populate")
                if populate:
                    relation = UniverseClient.objects.filter(
                        client=user.client_user.all()[0].client.client_uid, ticker=symbol)
                    if relation.exists():
                        res_celery.append(
                            {"result": f"relation {user.client_user.all()[0].client.client_uid} and {tick} exist"})
                    else:
                        if universe.exists():
                            print("create", symbol)
                            UniverseClient.objects.create(client_id=user.client_user.all()[
                                0].client.client_uid, ticker_id=symbol)
                            res_celery.append(
                                {"result": f"relation {user.client_user.all()[0].client.client_uid} and {tick} created"})
                        else:
                            res_celery.append(
                                {"err": f"error cant create ticker {symbol}"})

            if len(symbols) > 0:
                new_ticker_ingestion(ticker=symbols)
            return res_celery
        else:
            new_ticker = []
            ticker_cons = UniverseConsolidated.objects.filter(
                origin_ticker=ticker).distinct("origin_ticker").get()
            if ticker_cons.use_manual:
                symbol = ticker_cons.origin_ticker
            else:
                symbol = ticker_cons.consolidated_ticker
            universe = Universe.objects.filter(ticker=symbol)
            if not universe.exists():
                new_ticker.append(symbol)
            do_function("universe_populate")
            if len(new_ticker) > 0:
                new_ticker_ingestion(ticker=new_ticker)
            if populate:
                relation = UniverseClient.objects.filter(
                    client=user.client_user.all()[0].client.client_uid, ticker=symbol)
                if relation.exists():
                    return {"result": f"relation {user.client_user.all()[0].client.client_uid} and {ticker} exist"}
                else:
                    new_ticker_ingestion(ticker=symbol)
                    UniverseClient.objects.create(
                        client_id=user.client_user.all()[0].client.client_uid, ticker_id=symbol)
                return {"result": f"relation {user.client_user.all()[0].client.client_uid} and {ticker} created"}
    except Exception as e:
        return {"err": str(e)}


@app.task
def populate_client_top_stock_weekly(currency=None, client_name="HANWHA", **options):
    day = datetime.now()
    # POPULATED ONLY/EVERY MONDAY OF THE WEEK
    if day.weekday() == 0:
        interval = date_interval(day)
        if not ClientTopStock.objects.filter(week_of_year=interval, currency_code=currency).exists():
            report_to_slack(
                f"===  POPULATING {client_name} TOP PICK {currency} ===")
            try:
                # skip euro and client hanwha only
                if currency not in ["EUR"] and client_name == "HANWHA":
                    test_pick(currency_code=[currency])
                    populate_bot_advisor(
                        currency_code=[currency], client_name=client_name, capital="small")
                    populate_bot_advisor(
                        currency_code=[currency], client_name=client_name, capital="large")
                    populate_bot_advisor(
                        currency_code=[currency], client_name=client_name, capital="large_margin")
                # skip any currency except bot tester currency and client hanwha only
                if currency in ["USD", "KRW", ] and client_name == "HANWHA":
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="small", bot="UNO", top_pick=1, top_pick_stock=25)
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="small", bot="UCDC", top_pick=1, top_pick_stock=25)
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="small", bot="CLASSIC", top_pick=1, top_pick_stock=25)
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="large", bot="UNO", top_pick=2, top_pick_stock=25)
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="large", bot="UCDC", top_pick=2, top_pick_stock=25)
                    populate_bot_tester(currency_code=[
                                        currency], client_name=client_name, capital="large", bot="CLASSIC", top_pick=2, top_pick_stock=25)
                # skip any currency except  currency and client fels only
                # has own populate schedule ctrl+f search for EUR-POPULATE-PICK-FELS and USD-POPULATE-PICK-FELS
                if currency in ["EUR", ] and client_name == "FELS":
                    test_pick(currency_code=[currency],
                              client_name=client_name)
                    populate_fels_bot(
                        currency_code=[currency], client_name=client_name, top_pick=5)

            except Exception as e:
                err = ErrorLog.objects.create_log(
                    error_description=f"===  ERROR IN POPULATE FOR {currency} ===", error_message=str(e))
                err.send_report_error()
                return {"err": str(e)}

    
    try:
        # WILL RUN EVERY BUSINESS DAY
        # clear any existing cache
        cache.clear()
        # SKIP FELS FOR AUTO ORDER, SINCE FELS USING MANUAL TRIGER ORDER

        if not client_name == "FELS":
            report_to_slack(f"===  START ORDER FOR {client_name} TOP PICK {currency} ===")
            order_client_topstock(
                currency=currency, client_name=client_name, **options)  # bot advisor
            order_client_topstock(
                currency=currency, client_name=client_name, bot_tester=True, **options)  # bot tester
    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"===  ERROR IN ORDER FOR {currency} ===", error_message=str(e))
        err.send_report_error()
        return {"err": str(e)}

    return {"result": f"populate and order {currency} done"}


@app.task
def order_client_topstock(currency=None, client_name="HANWHA", bot_tester=False, **options):
    # NOT USING DSSS
    # try:
    #     populate_intraday_latest_price(currency_code=[currency])
    #     update_index_price_from_dss(currency_code=[currency])
    # except Exception as e:
    #     err = ErrorLog.objects.create_log(error_description=f"=== DSS {str(e)} SKIPPING GET INTRADAY ===",error_message=str(e))
    #     err.send_report_error()
    # if currency == "USD":

    # NOT USING DSSS

    # LOGIN TO TRKD API
    if not "repopulate" in options:
        rkd = RkdData()
        # GET INDEX PRICE AND SAVE/UPDATE TO CURRENCY TABLE
        rkd.get_index_price(currency)
    client = Client.objects.get(client_name=client_name)

    # ONLY PICK RELATED WEEK OF YEAR, WEEK WITH FULL HOLIDAY WILL SKIPPED/IGNORED
    # maybe we need to swap the following two lines
    day = datetime.now()
    now = day.date()
    interval = date_interval(day)

    if bot_tester:
        service_type = "bot_tester"
    else:
        service_type = "bot_advisor"

    topstock = client.client_top_stock.filter(
        has_position=False,  # HERE ARE SAME WITH STATUS, DO WE STILL NEED STATUS??
        service_type=service_type,  # bot advisor / bot tester
        currency_code=currency,
        week_of_year=interval  # WITH THIS WILL AUTO DETECT WEEKLY UNPICK
    ).order_by("service_type", "spot_date", "currency_code", "capital", "rank")
    pos_list = []
    # ONLY EXECUTE IF EXIST / ANY UNPICKED OF THE WEEK
    if topstock.exists():
        report_to_slack(
            f"===  START ORDER FOR UNPICK {client_name} - {currency} ===")
        if not "repopulate" in options:
            tickers = []
            [tickers.append(obj.ticker.ticker)
             for obj in topstock if not obj.ticker.ticker in tickers]
            rkd.get_quote(tickers, save=True, detail=f"populate-{now}")
        for queue in topstock:
            user = UserClient.objects.get(
                currency_code=queue.currency_code,
                extra_data__service_type=queue.service_type,
                extra_data__capital=queue.capital,
                extra_data__type=queue.bot
            )
            # TRADING HOURS CHECK
            market = ExchangeMarket.objects.get(mic=queue.ticker.mic)
            if market.is_open or "repopulate" in options:
                report_to_slack(
                    f"=== {client_name} MARKET {queue.ticker} IS OPEN AND CREATING INITIAL ORDER ===")
                # yahoo finance price
                # if currency == "USD":

                # else:
                #     get_quote_yahoo(queue.ticker.ticker, save=False)
                if "repopulate" in options:
                    from core.master.models import HedgeLatestPriceHistory
                    repopulate = options["repopulate"]
                    backup_price = HedgeLatestPriceHistory.objects.filter(
                        last_date=repopulate["date"], types=repopulate["types"], ticker=queue.ticker)
                    if backup_price.exists():
                        try:
                            backup_price = backup_price.get()
                            price = backup_price.latest_price
                            spot_date = backup_price.intraday_time

                            print("using backup price")
                        except HedgeLatestPriceHistory.DoesNotExist:
                            raise ValueError("no price exist")
                        except HedgeLatestPriceHistory.MultipleObjectsReturned:
                            raise ValueError("dulicate found")
                else:
                    price = queue.ticker.latest_price_ticker.close
                    if queue.ticker.latest_price_ticker.latest_price:
                        price = queue.ticker.latest_price_ticker.latest_price
                    spot_date = datetime.now()

                if(client_name == "HANWHA"):
                    if user.extra_data["service_type"] == "bot_advisor":
                        portnum = 8*1.04
                    elif user.extra_data["service_type"] == "bot_tester":
                        if user.extra_data["capital"] == "small":
                            portnum = 4*1.04
                        else:
                            portnum = 8*1.04
                    investment_amount = min(
                        user.user.current_assets / portnum, user.user.balance / 3)
                elif(client_name == "FELS"):
                    position = OrderPosition.objects.filter(
                        user_id=user.user.user_id)
                    if (len(position) >= 12):
                        investment_amount = min(
                            user.user.current_assets / 12, user.user.balance / 3)  # rebalance rule
                        investment_amount = round(investment_amount, 2)
                    else:
                        investment_amount = 250
                else:
                    investment_amount = min(
                        user.user.current_assets / 12, user.user.balance / 3)

                digits = max(min(5-len(str(int(price))), 2), -1)
                order = Order.objects.create(
                    ticker=queue.ticker,
                    created=spot_date,
                    price=price,
                    bot_id=queue.bot_id,
                    amount=round(investment_amount, digits),
                    user_id=user.user
                )
                if order:
                    order.status='placed'
                    order.placed = True
                    order.placed_at = spot_date
                    order.save()
                if order.status == "pending":
                    order.status = "filled"
                    order.filled_at = spot_date
                    order.save()
                    position_uid = PositionPerformance.objects.get(
                        performance_uid=order.performance_uid).position_uid.position_uid
                    queue.position_uid = position_uid
                    queue.has_position = True
                    queue.execution_date = spot_date
                    queue.save()
                    pos_list.append(str(order.order_uid))
                    report_to_slack(
                        f"=== {client_name} ORDER {queue.ticker} - {queue.service_type} - {queue.capital} IS CREATED ===")
                
                
                populate_daily_profit()
                
            else:
                report_to_slack(
                    f"=== {client_name} MARKET {queue.ticker} IS CLOSE SKIPPING INITIAL ORDER ===")

            del market

        # if pos_list:
        #     send_csv_hanwha(currency=currency, client_name="HANWHA", new={
        #                     "pos_list": pos_list}, bot_tester=bot_tester)
    else:
        report_to_slack(f"=== {client_name} NO TOPSTOCK IN PENDING ===")


@app.task
def daily_hedge_user(currency=None, ingest=False, exclude_client=['HANWHA','FELS']):
    report_to_slack(f"===  START DAILY USER HEDGE FOR {currency} ===")
    try:
        exclude = [user["user"] for user in UserClient.objects.prefetch_related('user_id').filter(client__client_name__in=exclude_client).values("user")]
        user_id = [user["id"] for user in User.objects.filter(is_active=True,current_status='verified').values("id").exclude(id__in=exclude)]
        positions = OrderPosition.objects.prefetch_related('ticker').filter(is_live=True, ticker__currency_code=currency, user_id__in=user_id)
        if positions.exists():
            if ingest:
                now = datetime.now().date()
                ticker_list = [obj.ticker.ticker for obj in positions.distinct("ticker")]
                rkd = RkdData()
                rkd.get_quote(ticker_list, save=True, detail=f"hedge-{now}")
            group_celery_jobs = []
            celery_jobs = celery_groups(group_celery_jobs)
            for position in positions:
                position_uid = position.position_uid
                market = ExchangeMarket.objects.get(mic=position.ticker.mic)
                if market.is_open:
                    if (position.bot.is_uno()):
                        group_celery_jobs.append(uno_position_check.s(position_uid,hedge=True))
                    elif (position.bot.is_ucdc()):
                        group_celery_jobs.append(ucdc_position_check.s(position_uid,hedge=True))
                    elif (position.bot.is_classic()):
                        group_celery_jobs.append(classic_position_check.s(position_uid, hedge=True))
                    elif(position.bot.is_stock()):
                        group_celery_jobs.append(user_position_check.s(position_uid, hedge=True))
                else:
                    report_to_slack(
                        f"===  MARKET {position.ticker} IS CLOSE SKIP DAILY HEDGE ===")
            if group_celery_jobs:
                result = celery_jobs.apply_async()
                retry = 0
                while result.waiting():
                    tm.sleep(2)
                    retry += 1
                    if retry == 10:
                        break
                if result.failed():
                    report_to_slack(f"=== THERE IS FAIL IN DAILY HEDGE TASK ===", channel="#error-log")
    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"===  ERROR IN DAILY HEDGE Function {currency} ===", error_message=str(e))
        err.send_report_error()
        return {"err": str(e)}


def hedge(currency=None, bot_tester=False, **options):
    # clear any existing caches
    cache.clear()
    if bot_tester:
        report_to_slack(f"===  START HEDGE FOR {currency} Bot Tester ===")
    else:
        report_to_slack(f"===  START HEDGE FOR {currency} Bot Advisor ===")

    try:
        # LOGIN TO TRKD API
        if not "rehedge" in options:
            rkd = RkdData()
        if(bot_tester):
            status = "BOT TESTER"
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", extra_data__service_type="bot_tester").values("user")]
        else:
            status = "BOT ADVISOR"
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name__in=["HANWHA", "FELS"], extra_data__service_type__in=["bot_advisor", None]).values("user")]
        # GETTING LIVE POSITION
        positions = OrderPosition.objects.filter(
            is_live=True, ticker__currency_code=currency, user_id__in=hanwha)

        if positions.exists():
            # DISTINCT TICKER TO BE USED IN TRKD
            ticker_list = [
                obj.ticker.ticker for obj in positions.distinct("ticker")]
            # GET NEWEST PRICE FROM TRKD AND SAVE TO LATESTPRICE TABLE
            if not "rehedge" in options:
                now = datetime.now().date()
                rkd.get_quote(ticker_list, save=True, detail=f"hedge-{now}")
            # PREPARE USING MULTIPROCESSING HEDGE GROUPS
            group_celery_jobs = []
            celery_jobs = celery_groups(group_celery_jobs)

            # DO HEDGE
            for position in positions:
                position_uid = position.position_uid
                market = ExchangeMarket.objects.get(mic=position.ticker.mic)
                # MARKET OPEN CHECK TRADINGHOURS, ignore market time if rehedge
                if market.is_open or "rehedge" in options:
                    if (position.bot.is_uno()):
                        # uno_position_check(position_uid)
                        group_celery_jobs.append(uno_position_check.s(position_uid,hedge=True))
                           
                    elif (position.bot.is_ucdc()):
                        # ucdc_position_check(position_uid)
                        group_celery_jobs.append(ucdc_position_check.s(position_uid,hedge=True))
                        
                    elif (position.bot.is_classic()):
                        # classic_position_check(position_uid)
                        group_celery_jobs.append(classic_position_check.s(position_uid, hedge=True))
                else:
                    report_to_slack(
                        f"===  MARKET {position.ticker} IS CLOSE SKIP HEDGE {status} ===")
            if group_celery_jobs:
                result = celery_jobs.apply_async(queue=settings.HEDGE_WORKER_DEFAULT_QUEUE)
                retry = 0
                while result.waiting():
                    tm.sleep(2)
                    retry += 1
                    if retry == 10:
                        break
                if result.failed():
                    report_to_slack(
                        f"=== THERE IS FAIL IN HEDGE TASK ===", channel="#error-log")
                # if result.successful() or "rehedge" in options or result.ready():
                #     send_csv_hanwha(
                #         currency=currency, client_name="HANWHA", bot_tester=bot_tester, **options)

    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"===  ERROR IN HEDGE Function {currency} {status} ===", error_message=str(e))
        err.send_report_error()
        return {"err": str(e)}


@app.task
def daily_hedge(currency=None, **options):
    # try:
    #     populate_intraday_latest_price(currency_code=[currency])
    #     update_index_price_from_dss(currency_code=[currency])
    # except Exception as e:
    #     err = ErrorLog.objects.create_log(error_description=f"=== DSS ERROR : {str(e)} SKIPPING GET INTRADAY ===",error_message=str(e))
    #     err.send_report_error()

    # if currency == "USD":
    # else:
    # GET INDEX PRICE
    # get_quote_index(currency)

    if not "rehedge" in options:
        rkd = RkdData()  # LOGIN
        rkd.get_index_price(currency)  # GET INDEX PRICE
    hedge(currency=currency, **options)  # bot_advisor hanwha and fels
    hedge(currency=currency, bot_tester=True, **options)  # bot_tester
    daily_hedge_user(currency=currency,ingest=True)
    try:
        populate_daily_profit()
    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"===  ERROR IN DAILY profit Function {currency} ===", error_message=str(e))
        err.send_report_error()
    report_to_slack(f"===  HEDGE DAILY FOR {currency} DONE ===")

    return {"result": f"hedge {currency} done"}





@app.task()
def season_update_task():
    if is_end_of_month():
        try:
            update_monthly_deposit()
        except Exception as e:
            err = ErrorLog.objects.create_log(
                error_description=f"===  ERROR IN UPDATE SEASON ===", error_message=str(e))
            err.send_report_error()
        try:
            send_winner_email()
        except Exception as e:
            err = ErrorLog.objects.create_log(
                error_description=f"===  ERROR IN UPDATE SENDING WINNER EMAIL ===", error_message=str(e))
            err.send_report_error()





####### DEPRECATED HANWHA STOPPED ##############






def sending_csv(hanwha, currency=None, client_name=None, new=None, bot_tester=False, bot=None, capital=None, **options):
    if new:
        # NEW SUNDAY MORNING TICKER CSV
        perf = PositionPerformance.objects.filter(
            order_uid__in=new["pos_list"], position_uid__user_id__in=hanwha).order_by("created")
    else:
        # HEDGE TICKER ONLY CSV
        if "rehedge" in options:
            dates = options["rehedge"]["date"]
        else:
            dates = datetime.now().date()
        perf = PositionPerformance.objects.filter(
            position_uid__user_id__in=hanwha, created__gte=dates, position_uid__ticker__currency_code=currency).order_by("created")
        orders = [ids.order_uid for ids in Order.objects.filter(is_init=True)]
        perf = perf.exclude(order_uid__in=orders)

    if perf.exists():
        now = datetime.now()
        datenow = now.date()
        if "rehedge" in options:
            datenow = options["rehedge"]["date"]
        else:
            datenow = datetime.now().date()

        df = pd.DataFrame(CsvSerializer(perf, many=True).data)
        df = df.fillna(0)
        hanwha_df = df.drop(
            columns=["prev_delta", "delta", "v1", "v2", "uuid"])
        csv = export_csv(df)
        hanwha_csv = export_csv(hanwha_df)
        if new:
            if settings.db_debug:
                subject = "TEST NEW PICK"
            else:
                subject = "NEW PICK"
        else:
            if settings.db_debug:
                subject = "TEST HEDGE"
            else:
                subject = "HEDGE"
        if settings.db_debug:
            # if(bot_tester):
            #     LORA_MEMBER=["rede.akbar@loratechai.com","agustian@loratechai.com", "stepchoi@loratechai.com",
            #     "kenson.lau@loratechai.com", "nick.choi@loratechai.com"]
            # else:
            LORA_MEMBER = ["rede.akbar@loratechai.com",
                           "agustian@loratechai.com"]
            HANWHA_MEMBER = LORA_MEMBER
        else:
            LORA_MEMBER = ["rede.akbar@loratechai.com", "stepchoi@loratechai.com", "joseph.chang@loratechai.com",
                           "john.kim@loratechai.com",  "kenson.lau@loratechai.com"]
            HANWHA_MEMBER = ["200200648@hanwha.com", "noblerain72@hanwha.com",
                             "nick.choi@loratechai.com"]

        if(bot_tester):
            stats = f"BOT TESTER"
        else:
            stats = f"BOT ADVISOR"

        draft_email = EmailMessage(
            subject,
            f"ASKLORA {currency} {stats} CSV  - {datenow}",
            "asklora@loratechai.com", LORA_MEMBER
        )
        hanwha_email = EmailMessage(
            subject,
            f"ASKLORA {currency} {stats} CSV - {datenow}",
            "asklora@loratechai.com",
            HANWHA_MEMBER,
        )

        if(bot_tester):
            hanwha_email.attach(
                f"{currency}_bot_tester_{datenow}_asklora.csv", hanwha_csv, mimetype="text/csv")
            draft_email.attach(
                f"{currency}_bot_tester_{datenow}_asklora.csv", csv, mimetype="text/csv")
        else:
            hanwha_email.attach(
                f"{currency}_{datenow}_asklora.csv", hanwha_csv, mimetype="text/csv")
            draft_email.attach(
                f"{currency}_{datenow}_asklora.csv", csv, mimetype="text/csv")

        status = draft_email.send()
        hanwha_status = hanwha_email.send()
        if(new):
            if(status):
                report_to_slack(
                    f"=== NEW PICK {stats} CSV {client_name} EMAIL SENT TO LORA ===")
            if(hanwha_status):
                report_to_slack(
                    f"=== NEW PICK {stats} CSV {client_name} EMAIL SENT TO HANWHA ===")
        else:
            if(status):
                report_to_slack(
                    f"=== {client_name} {stats} EMAIL HEDGE SENT FOR {currency} TO LORA ===")
            if(hanwha_status):
                report_to_slack(
                    f"=== {client_name} {stats} EMAIL HEDGE SENT FOR {currency} TO HANWHA ===")


@app.task
def send_csv_hanwha(currency=None, client_name=None, new=None, bot_tester=False, **options):
    if client_name == "HANWHA":
        if(bot_tester):
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name=client_name,
                                                                         extra_data__service_type="bot_tester").values("user")]
            sending_csv(hanwha, currency=currency, client_name=client_name,
                        new=new, bot_tester=True, **options)
        else:
            hanwha = [user["user"] for user in UserClient.objects.filter(
                client__client_name=client_name, extra_data__service_type="bot_advisor").values("user")]
            sending_csv(hanwha, currency=currency, client_name=client_name,
                        new=new, bot_tester=False, **options)
        return {"result": f"send email {currency} done"}
