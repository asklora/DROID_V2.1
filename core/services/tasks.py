from config.celery import app
from core.universe.models import Universe, UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User
from core.master.models import Currency
from main import new_ticker_ingestion, populate_latest_price, update_index_price_from_dss, populate_intraday_latest_price
from general.sql_process import do_function
from core.orders.models import Order, PositionPerformance, OrderPosition
from core.Clients.models import UserClient, Client
from datetime import datetime
from client_test_pick import test_pick, populate_bot_advisor
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
import pandas as pd
from core.djangomodule.serializers import CsvSerializer
from core.djangomodule.yahooFin import get_quote_index,get_quote_yahoo
from django.core.mail import send_mail, EmailMessage
from celery.schedules import crontab


import io

USD_CUR = Currency.objects.get(currency_code="USD")
HKD_CUR = Currency.objects.get(currency_code="HKD")
KRW_CUR = Currency.objects.get(currency_code="KRW")
CNY_CUR = Currency.objects.get(currency_code="CNY")
app.conf.beat_schedule = {
    'USD-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=USD_CUR.hedge_schedule.minute, hour=USD_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "USD"},
    },
    'HKD-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=HKD_CUR.hedge_schedule.minute, hour=HKD_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "HKD"},
    },
    'KRW-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=KRW_CUR.hedge_schedule.minute, hour=KRW_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "KRW"},
    },
    'CNY-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=CNY_CUR.hedge_schedule.minute, hour=CNY_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "CNY"},
    },
    'USD-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=USD_CUR.top_stock_schedule.minute, hour=USD_CUR.top_stock_schedule.hour, day_of_week="1"),
        'kwargs': {"currency": "USD"},
    },
    'HKD-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=HKD_CUR.top_stock_schedule.minute, hour=HKD_CUR.top_stock_schedule.hour, day_of_week="1"),
        'kwargs': {"currency": "HKD"},
    },
    'KRW-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=KRW_CUR.top_stock_schedule.minute, hour=KRW_CUR.top_stock_schedule.hour, day_of_week="1"),
        'kwargs': {"currency": "KRW"},
    },
    'CNY-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=CNY_CUR.top_stock_schedule.minute, hour=CNY_CUR.top_stock_schedule.hour, day_of_week="1"),
        'kwargs': {"currency": "CNY"},
    },

}


def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)
        return buffer.getvalue()


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
                ticker_cons = UniverseConsolidated.objects.filter(
                    origin_ticker=tick).distinct('origin_ticker').get()
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
                            print('create', symbol)
                            UniverseClient.objects.create(client_id=user.client_user.all()[
                                0].client.client_uid, ticker_id=symbol)
                            res_celery.append(
                                {"result": f"relation {user.client_user.all()[0].client.client_uid} and {tick} created"})
                        else:
                            res_celery.append(
                                {'err': f'error cant create ticker {symbol}'})

            if len(symbols) > 0:
                new_ticker_ingestion(ticker=symbols)
            return res_celery
        else:
            new_ticker = []
            ticker_cons = UniverseConsolidated.objects.filter(
                origin_ticker=ticker).distinct('origin_ticker').get()
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
        return {'err': str(e)}


@app.task
def populate_client_top_stock_weekly(currency=None, client_name=None):
    # client = Client.objects.get(client_name="HANWHA")
    # topstock = client.client_top_stock.filter(
    #     has_position=False, service_type='bot_advisor', currency_code=currency).distinct('spot_date')
    test_pick(currency_code=[currency])
    populate_bot_advisor(
        currency_code=[currency], client_name="HANWHA", capital="small")
    populate_bot_advisor(
        currency_code=[currency], client_name="HANWHA", capital="large")
    populate_bot_advisor(
        currency_code=[currency], client_name="HANWHA", capital="large_margin")
    order_client_topstock(currency=currency)

    return {'result': f'populate and order {currency} done'}


@app.task
def order_client_topstock(currency=None, client_name=None):
    # need to change to client prices
    populate_intraday_latest_price(currency_code=[currency])
    update_index_price_from_dss(currency_code=[currency])
    if currency == "USD":
        get_quote_index(currency)
    client = Client.objects.get(client_name="HANWHA")
    topstock = client.client_top_stock.filter(
        has_position=False, service_type='bot_advisor', currency_code=currency).order_by("service_type", "spot_date", "currency_code", "capital", "rank")
    pos_list = []
    for queue in topstock:
        user = UserClient.objects.get(
            currency_code=queue.currency_code,
            extra_data__service_type=queue.service_type,
            extra_data__capital=queue.capital,
            extra_data__type=queue.bot
        )
        # need to change live price, problem with nan
        # yahoo finance price
        if currency == "USD":
            get_quote_yahoo(queue.ticker.ticker, use_symbol=True)
        else:
            get_quote_yahoo(queue.ticker.ticker, use_symbol=False)
        price = queue.ticker.latest_price_ticker.close
        spot_date = datetime.now()
        if user.extra_data["service_type"] == "bot_advisor":
            portnum = 8*1.04
        elif user.extra_data["service_type"] == "bot_tester":
            if user.extra_data["capital"] == "small":
                portnum = 4*1.04
            else:
                portnum = 8*1.04
        investment_amount = min(
            user.user.current_assets / portnum, user.user.balance / 3)

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
            queue.save()
            pos_list.append(str(order.order_uid))
    send_csv_hanwha.delay(currency=currency, new={'pos_list': pos_list})


@app.task
def daily_hedge(currency=None):
    update_index_price_from_dss(currency_code=[currency])
    populate_intraday_latest_price(currency_code=[currency])
    get_quote_index(currency)
    positions = OrderPosition.objects.filter(
        is_live=True, ticker__currency_code=currency)
    for position in positions:
        position_uid = position.position_uid
        if currency == "USD":
            get_quote_yahoo(position.ticker.ticker_symbol, use_symbol=True)
        else:
            get_quote_yahoo(position.ticker.ticker, use_symbol=False)
    #     if (position.bot.is_uno()):
    #         uno_position_check(position_uid)
    #     elif (position.bot.is_ucdc()):
    #         ucdc_position_check(position_uid)
    #     elif (position.bot.is_classic()):
    #         classic_position_check(position_uid)
    # send_csv_hanwha.delay(currency=currency)
    return {'result': f'hedge {currency} done'}


@app.task
def send_csv_hanwha(currency=None, client_name=None, new=None):
    hanwha = [user["user"] for user in UserClient.objects.filter(
        client__client_name="HANWHA", extra_data__service_type="bot_advisor").values("user")]
    if new:
        perf = PositionPerformance.objects.filter(
            order_uid__in=new['pos_list']).order_by("created")
    else:
        perf = PositionPerformance.objects.filter(
            position_uid__user_id__in=hanwha, created__gte=datetime.now().date(), position_uid__ticker__currency_code=currency).order_by("created")
        # not include new
        orders = [ids.order_uid for ids in Order.objects.filter(is_init=True)]
        perf = perf.exclude(order_uid__in=orders)

    if perf.exists():
        now = datetime.now()
        datenow = now.date()
        df = pd.DataFrame(CsvSerializer(perf, many=True).data)
        df = df.fillna(0)
        hanwha_df = df.drop(
            columns=['prev_delta', 'delta', 'v1', 'v2', 'uuid'])
        csv = export_csv(df)
        hanwha_csv = export_csv(hanwha_df)
        if new:
            subject = 'new bot pick'
        else:
            subject = 'hedge'
        draft_email = EmailMessage(
            subject,
            f'asklora csv {currency} - {datenow}',
            'asklora@loratechai.com',
            ['rede.akbar@loratechai.com', 'stepchoi@loratechai.com', 'joseph.chang@loratechai.com',
                'john.kim@loratechai.com',  'kenson.lau@loratechai.com']
        )
        hanwha_email = EmailMessage(
            subject,
            f'asklora csv {currency} - {datenow}',
            'asklora@loratechai.com',
            ['200200648@hanwha.com', 'noblerain72@hanwha.com',
                'nick.choi@loratechai.com'],
        )
        hanwha_email.attach(f"{currency}_{now}_asklora.csv",
                            hanwha_csv, mimetype="text/csv")
        draft_email.attach(f"{currency}_{now}_asklora.csv",
                           csv, mimetype="text/csv")
        draft_email.send()
        hanwha_email.send()
    return {'result': f'send email {currency} done'}
