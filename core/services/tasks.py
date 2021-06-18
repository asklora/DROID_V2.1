from config.celery import app
from core.universe.models import Universe, UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User
from core.master.models import Currency
from main import new_ticker_ingestion, populate_latest_price, update_index_price_from_dss, populate_intraday_latest_price
from general.sql_process import do_function
from general.slack import report_to_slack
from core.orders.models import Order, PositionPerformance, OrderPosition
from core.Clients.models import UserClient, Client
from datetime import datetime
from client_test_pick import populate_fels_bot, test_pick, populate_bot_advisor,populate_bot_tester
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
import pandas as pd
from core.djangomodule.serializers import CsvSerializer
from core.djangomodule.yahooFin import get_quote_index,get_quote_yahoo
from core.djangomodule.calendar import TradingHours
from django.core.mail import send_mail, EmailMessage
from celery.schedules import crontab
from config.settings import db_debug
import io
from global_vars import bots_list

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
        'schedule': crontab(minute=USD_CUR.top_stock_schedule.minute, hour=USD_CUR.top_stock_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "USD"},
    },
    'HKD-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=HKD_CUR.top_stock_schedule.minute, hour=HKD_CUR.top_stock_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "HKD"},
    },
    'KRW-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=KRW_CUR.top_stock_schedule.minute, hour=KRW_CUR.top_stock_schedule.hour, day_of_week="1-5"),
        'kwargs': {"currency": "KRW"},
    },
    'CNY-POPULATE-PICK': {
        'task': 'core.services.tasks.populate_client_top_stock_weekly',
        'schedule': crontab(minute=CNY_CUR.top_stock_schedule.minute, hour=CNY_CUR.top_stock_schedule.hour, day_of_week="1-5"),
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
                print(tick,'')
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
def populate_client_top_stock_weekly(currency=None, client_name="HANWHA"):
    day = datetime.now()
    ## POPULATED ONLY/EVERY MONDAY OF THE WEEK
    if day.weekday() == 0:
        report_to_slack(f"===  POPULATING {client_name} TOP PICK {currency} ===")
        try:
            test_pick(currency_code=[currency])
            populate_bot_advisor(currency_code=[currency], client_name=client_name, capital="small")
            populate_bot_advisor(currency_code=[currency], client_name=client_name, capital="large")
            populate_bot_advisor(currency_code=[currency], client_name=client_name, capital="large_margin")

            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="UNO", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="UCDC", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="CLASSIC", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="UNO", top_pick=2, top_pick_stock=25)
            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="UCDC", top_pick=2, top_pick_stock=25)
            populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="CLASSIC", top_pick=2, top_pick_stock=25)

            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="UNO", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="UCDC", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="CLASSIC", top_pick=1, top_pick_stock=25)
            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="UNO", top_pick=2, top_pick_stock=25)
            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="UCDC", top_pick=2, top_pick_stock=25)
            populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="CLASSIC", top_pick=2, top_pick_stock=25)
        except Exception as e:
            report_to_slack(f"===  ERROR IN POPULATE FOR {currency} ===")
            report_to_slack(str(e))
            return {'err': str(e)}
    
    report_to_slack(f"===  START ORDER FOR {client_name} TOP PICK {currency} ===")
    try:
        # WILL RUN EVERY BUSINESS DAY
        order_client_topstock(currency=currency, client_name="HANWHA") #bot advisor
        order_client_topstock(currency=currency, client_name="HANWHA", bot_tester=True) #bot tester
    except Exception as e:
        report_to_slack(f"===  ERROR IN ORDER FOR {currency} ===")
        report_to_slack(str(e))
        return {'err': str(e)}

    return {'result': f'populate and order {currency} done'}

@app.task
def order_client_topstock(currency=None, client_name="HANWHA", bot_tester=False):
    # need to change to client prices
    try:
        populate_intraday_latest_price(currency_code=[currency])
        update_index_price_from_dss(currency_code=[currency])
    except Exception as e:
        report_to_slack(f"=== DSS {str(e)} SKIPPING GET INTRADAY ===")
    if currency == "USD":
        get_quote_index(currency)
    client = Client.objects.get(client_name=client_name)

    ## ONLY PICK RELATED WEEK OF YEAR, WEEK WITH FULL HOLIDAY WILL SKIPPED/IGNORED
    day = datetime.now()
    week = day.isocalendar()[1]
    year = day.isocalendar()[0]
    interval = f'{year}{week}'
    if bot_tester:
        topstock = client.client_top_stock.filter(
            has_position=False, # HERE ARE SAME WITH STATUS, DO WE STILL NEED STATUS??
            service_type='bot_tester', # bot advisor and bot tester
            currency_code=currency,
            week_of_year=int(interval) # WITH THIS WILL AUTO DETECT WEEKLY UNPICK
            ).order_by("service_type", "spot_date", "currency_code", "capital", "rank")
    else:
        topstock = client.client_top_stock.filter(
            has_position=False, # HERE ARE SAME WITH STATUS, DO WE STILL NEED STATUS??
            service_type='bot_advisor', # bot advisor and bot tester
            currency_code=currency,
            week_of_year=int(interval) # WITH THIS WILL AUTO DETECT WEEKLY UNPICK
            ).order_by("service_type", "spot_date", "currency_code", "capital", "rank")
    pos_list = []
    ### ONLY EXECUTE IF EXIST / ANY UNPICKED OF THE WEEK
    if topstock.exists():
        report_to_slack(f"===  START ORDER FOR UNPICK {client_name} - {currency} ===")
        for queue in topstock:
            user = UserClient.objects.get(
                currency_code=queue.currency_code,
                extra_data__service_type=queue.service_type,
                extra_data__capital=queue.capital,
                extra_data__type=queue.bot
            )
            market = TradingHours(mic=queue.ticker.mic)
            
            if market.is_open:
                report_to_slack(f"=== {client_name} MARKET {queue.ticker} IS OPEN AND CREATING INITIAL ORDER ===")
                # need to change live price, problem with nan
                # yahoo finance price
                if currency == "USD":
                    get_quote_yahoo(queue.ticker.ticker, use_symbol=True)
                else:
                    get_quote_yahoo(queue.ticker.ticker, use_symbol=False)
                price = queue.ticker.latest_price_ticker.close
                spot_date = datetime.now()
                if(client_name == "HANWHA"):
                    if user.extra_data["service_type"] == "bot_advisor":
                        portnum = 8*1.04
                    elif user.extra_data["service_type"] == "bot_tester":
                        if user.extra_data["capital"] == "small":
                            portnum = 4*1.04
                        else:
                            portnum = 8*1.04
                    investment_amount = min(user.user.current_assets / portnum, user.user.balance / 3)
                elif(client_name == "FELS"):
                    position = OrderPosition.objects.filter(user_id=user.user.user_id)
                    if (len(position) >= 12):
                        investment_amount = min(user.user.current_assets / 12, user.user.balance / 3) #rebalance rule
                        investment_amount = round(investment_amount, 2)
                    else:
                        investment_amount = 250
                else:
                    investment_amount = min(user.user.current_assets / 12, user.user.balance / 3)

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
                    queue.execution_date = spot_date
                    queue.save()
                    pos_list.append(str(order.order_uid))
                    report_to_slack(f"=== {client_name} ORDER {queue.ticker} - {queue.service_type} - {queue.capital} IS CREATED ===")
            else:
                report_to_slack(f"=== {client_name} MARKET {queue.ticker} IS CLOSE SKIPPING INITIAL ORDER ===")
            
            del market

        if pos_list:
            send_csv_hanwha.delay(currency=currency, client_name="HANWHA", new={'pos_list': pos_list})
    else:
        report_to_slack(f"=== {client_name} NO TOPSTOCK IN PENDING ===")

def hedge(currency=None, bot_tester=False):
    report_to_slack(f"===  START HEDGE FOR {currency} ===")
    try:
        if(bot_tester):
            status = "BOT TESTER"
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", extra_data__service_type="bot_tester").values("user")]
        else:
            status = "BOT ADVISOR"
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", extra_data__service_type="bot_advisor").values("user")]
        positions = OrderPosition.objects.filter(is_live=True, ticker__currency_code=currency, user_id__in=hanwha)
        for position in positions:
            position_uid = position.position_uid
            market = TradingHours(mic=position.ticker.mic)
            if market.is_open:
                # if currency == "USD":
                #     get_quote_yahoo(position.ticker.ticker, use_symbol=True)
                # else:
                #     get_quote_yahoo(position.ticker.ticker, use_symbol=False)
                if (position.bot.is_uno()):
                    uno_position_check(position_uid)
                elif (position.bot.is_ucdc()):
                    ucdc_position_check(position_uid)
                elif (position.bot.is_classic()):
                    classic_position_check(position_uid)
            else:
                report_to_slack(f"===  MARKET {position.ticker} IS CLOSE SKIP HEDGE {status} ===")
    except Exception as e:
        report_to_slack(f"===  ERROR IN HEDGE FOR {currency} {status} ===")
        report_to_slack(str(e))
        return {'err': str(e)}
    send_csv_hanwha(currency=currency, client_name="HANWHA", bot_tester=bot_tester)

@app.task
def daily_hedge(currency=None):
    try:
        populate_intraday_latest_price(currency_code=[currency])
        update_index_price_from_dss(currency_code=[currency])
    except Exception as e:
        report_to_slack(f"=== DSS ERROR : {str(e)} SKIPPING GET INTRADAY ===")
    get_quote_index(currency)

    hedge(currency=currency) #bot_advisor
    hedge(currency=currency, bot_tester=True) #bot_tester
    return {'result': f'hedge {currency} done bot tester'}

def sending_csv(hanwha, currency=None, client_name=None, new=None, bot_tester=False, bot=None, capital=None):
    if new:
        perf = PositionPerformance.objects.filter(
            order_uid__in=new['pos_list']).order_by("created")
    else:
        perf = PositionPerformance.objects.filter(position_uid__user_id__in=hanwha, created__gte=datetime.now().date(), position_uid__ticker__currency_code=currency).order_by("created")
        orders = [ids.order_uid for ids in Order.objects.filter(is_init=True)]
        perf = perf.exclude(order_uid__in=orders)

    if perf.exists():
        now = datetime.now()
        datenow = now.date()
        df = pd.DataFrame(CsvSerializer(perf, many=True).data)
        df = df.fillna(0)
        hanwha_df = df.drop(columns=['prev_delta', 'delta', 'v1', 'v2', 'uuid'])
        csv = export_csv(df)
        hanwha_csv = export_csv(hanwha_df)
        if new:
            subject = 'new bot pick'
        else:
            subject = 'hedge'
        if db_debug:
            if(bot_tester):
                LORA_MEMBER=['rede.akbar@loratechai.com','agustian@loratechai.com', 
                'stepchoi@loratechai.com', 
                'kenson.lau@loratechai.com', 'nick.choi@loratechai.com']
            else:
                LORA_MEMBER=['rede.akbar@loratechai.com','agustian@loratechai.com']
            HANWHA_MEMBER=LORA_MEMBER
        else:
            LORA_MEMBER=['rede.akbar@loratechai.com', 'stepchoi@loratechai.com', 'joseph.chang@loratechai.com',
                    'john.kim@loratechai.com',  'kenson.lau@loratechai.com']
            HANWHA_MEMBER=['200200648@hanwha.com', 'noblerain72@hanwha.com',
                    'nick.choi@loratechai.com']
        draft_email = EmailMessage(
            subject,
            f'asklora csv {currency} - {datenow}',
            'asklora@loratechai.com',LORA_MEMBER
        )
        hanwha_email = EmailMessage(
            subject,
            f'asklora csv {currency} - {datenow}',
            'asklora@loratechai.com',
            HANWHA_MEMBER,
        )
        if(bot_tester):
            hanwha_email.attach(f"{currency}_{bot}_{capital}_{now}_asklora.csv", hanwha_csv, mimetype="text/csv")
            draft_email.attach(f"{currency}_{now}_asklora.csv", csv, mimetype="text/csv")
            stats = f"BOT TESTER {bot} {capital}"
        else:
            hanwha_email.attach(f"{currency}_{now}_asklora.csv", hanwha_csv, mimetype="text/csv")
            draft_email.attach(f"{currency}_{now}_asklora.csv", csv, mimetype="text/csv")
            stats = f"BOT ADVISOR"
        status = draft_email.send()
        hanwha_status = hanwha_email.send()
        if(new):
            if(status):
                report_to_slack(f"=== NEW PICK {stats} CSV {client_name} EMAIL SENT TO LORA ===")
            if(hanwha_status):
                report_to_slack(f"=== NEW PICK {stats} CSV {client_name} EMAIL SENT TO HANWHA ===")
        else:
            if(status):
                report_to_slack(f"=== {client_name} {stats} EMAIL HEDGE SENT FOR {currency} TO LORA ===")
            if(hanwha_status):
                report_to_slack(f"=== {client_name} {stats} EMAIL HEDGE SENT FOR {currency} TO HANWHA ===")

@app.task
def send_csv_hanwha(currency=None, client_name=None, new=None, bot_tester=False):
    if(bot_tester):
        for bot in bots_list:
            for capital in ["small", "large"]:
                hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", 
                extra_data__service_type="bot_tester", 
                extra_data__capital=capital, 
                extra_data__type=bot.upper()).values("user")]
                sending_csv(hanwha, currency=None, client_name=None, new=None, bot_tester=False, bot=bot.upper(), capital=capital)
    else:
        hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", extra_data__service_type="bot_advisor").values("user")]
        sending_csv(hanwha, currency=None, client_name=None, new=None, bot_tester=False)
    return {'result': f'send email {currency} done'}
