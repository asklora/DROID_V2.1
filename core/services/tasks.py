from config.celery import app
from core.universe.models import Universe, UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User
from core.master.models import Currency
from main import new_ticker_ingestion,populate_latest_price,update_index_price_from_dss
from general.sql_process import do_function
from core.orders.models import Order, PositionPerformance,OrderPosition
from core.Clients.models import UserClient, Client
from datetime import datetime
from client_test_pick import test_pick, populate_bot_advisor
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
import pandas as pd
from core.djangomodule.serializers import CsvSerializer
from django.core.mail import send_mail, EmailMessage
from celery.schedules import crontab


import io



USD_CUR=Currency.objects.get(currency_code="USD")
HKD_CUR=Currency.objects.get(currency_code="HKD")
KRW_CUR=Currency.objects.get(currency_code="KRW")
CNY_CUR=Currency.objects.get(currency_code="CNY")
app.conf.beat_schedule ={
    'USD-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=USD_CUR.hedge_schedule.minute,hour=USD_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency":"USD"},
    },
    'HKD-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=21,hour=8, day_of_week="1-5"),
        'kwargs': {"currency":"HKD"},
    },
    'KRW-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=KRW_CUR.hedge_schedule.minute,hour=KRW_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency":"KRW"},
    },
    'CNY-HEDGE': {
        'task': 'core.services.tasks.daily_hedge',
        'schedule': crontab(minute=CNY_CUR.hedge_schedule.minute,hour=CNY_CUR.hedge_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency":"CNY"},
    },
    
}
def export_csv(df):
  with io.StringIO() as buffer:
    df.to_csv(buffer,index=False)
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
def populate_client_top_stock_weekly(currency=None,client_name=None):
    test_pick(currency_code=[currency])
    populate_bot_advisor(currency_code=[currency], client_name="HANWHA", capital="small")
    populate_bot_advisor(currency_code=[currency], client_name="HANWHA", capital="large")
    populate_bot_advisor(currency_code=[currency], client_name="HANWHA", capital="large_margin")

  
@app.task
def order_client_topstock(currency=None,client_name=None):
    # need to change to client price
    populate_latest_price(currency_code=[currency])
    update_index_price_from_dss(currency_code=[currency])
    client = Client.objects.get(client_name="HANWHA")
    topstock = client.client_top_stock.filter(
        has_position=False,service_type='bot_advisor',currency_code=currency).order_by("service_type", "spot_date", "currency_code", "capital", "rank")

    for queue in topstock:
        user = UserClient.objects.get(
            currency_code=queue.currency_code,
            extra_data__service_type=queue.service_type,
            extra_data__capital=queue.capital,
            extra_data__type=queue.bot
        )
        # need to change live price
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
            
            
@app.task
def daily_hedge(currency=None):
    populate_latest_price(currency_code=[currency])
    update_index_price_from_dss(currency_code=[currency])
    positions = OrderPosition.objects.filter(is_live=True,ticker__currency_code=currency)
    for position in positions:
        position_uid = position.position_uid
        if (position.bot.is_uno()):
            uno_position_check.delay(position_uid)
            
        elif (position.bot.is_ucdc()):
            ucdc_position_check.delay(position_uid)
        elif (position.bot.is_classic()):
            classic_position_check.delay(position_uid)
        
            
            
@app.task
def send_csv_hanwha(currency=None,client_name=None):
    hanwha = [user["user"] for user in UserClient.objects.filter(
            client__client_name="HANWHA", extra_data__service_type="bot_advisor").values("user")]
    perf = PositionPerformance.objects.filter(
                    position_uid__user_id__in=hanwha, created=datetime.now().date(), position_uid__ticker__currency_code=currency).order_by("created")
    if perf.exists():
        df = pd.DataFrame(CsvSerializer(perf, many=True).data)
        df = df.fillna(0)
        csv = export_csv(df)
        draft_email = EmailMessage(
            'hedge',
            'asklora csv',
            'asklora@loratechai.com',
            ['ribonred@gmail.com'],
        )
       
        draft_email.attach(f"{currency}_{datetime.now()}_asklora.csv", csv, mimetype="text/csv")
        draft_email.send()
        # csv = df.to_csv(f"files/file_csv/hanwha/{currency}/{currency}_{datetime.now()}_asklora.csv", index=False)
               