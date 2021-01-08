import pandas as pd
import sqlalchemy as db
from sqlalchemy.types import Date, BIGINT, TEXT
import numpy as np
from sqlalchemy import create_engine
from general.general import dateNow, backdate_by_day, datetimeNow
from general.SqlProcedure import PgFunctions
from general.slack import report_to_slack
from data_source.DSS import get_data_from_dss, get_intraday_from_dss
from universe import droid_universe_by_index, DroidUniverse
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
from datetime import datetime
from pangres import upsert
import sys
from ingestion.test_account import populate_fels_test_pick_usdeur
import requests


latest_price_table = "latest_price_updates"
droid_universe_table = "droid_universe"
indices_table = 'indices'
master_ohlctr_table = 'master_ohlctr'

def get_latest_price_updates_split(args, indices):
    print('=== Get Ingestion Time ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {latest_price_table} where capital_change is not null and ticker in (select ticker from {droid_universe_table} where index='{indices}' and is_active=True)"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_ingestion_time(args):
    print('=== Get Ingestion Time ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f'select * from {indices_table} where still_live=True order by index'
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def update_capital_change(args, ticker):
    engine_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine_droid.connect() as conn:
        metadata = db.MetaData()
        query = f"update {latest_price_table} set capital_change = null where ticker='{ticker}'"
        result = conn.execute(query)
    engine_droid.dispose()
    print("Capital Change to Null Value")

def update_all_data_by_capital_change(args, ticker, trading_day, capital_change):
    engine_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine_droid.connect() as conn:
        metadata = db.MetaData()

        query = f"update split_record set "
        query += f"data_type = 'DSS', capital_change={capital_change} where ticker = '{ticker}';"
        result = conn.execute(query)

        query = f"update user_portfolio set "
        query += f"entry_price = entry_price * {capital_change}, "
        query += f"max_loss_price = max_loss_price * {capital_change}, "
        query += f"target_profit_price = target_profit_price * {capital_change}, "
        query += f"share_num = share_num / {capital_change} "
        query += f"where stock_selected = '{ticker}' and spot_date < '{trading_day}' and status = False;"
        result = conn.execute(query)

        query = f"update user_portfolio_performance_history set "
        query += f"last_spot_price = last_spot_price * {capital_change}, "
        query += f"last_live_price = last_live_price * {capital_change}, "
        query += f"share_num = share_num / {capital_change} "
        query += f"from (select order_id from user_portfolio where stock_selected='{ticker}' and status = False) result "
        query += f"where user_portfolio_performance_history.created < '{trading_day}' and "
        query += f"user_portfolio_performance_history.order_id=result.order_id;"
        result = conn.execute(query)

        query = f"update executive_uno_option_price set "
        query += f"strike = strike * {capital_change}, "
        query += f"barrier = barrier * {capital_change} "
        query += f"from (select order_id from user_portfolio where stock_selected='{ticker}' and status = False) result "
        query += f"where executive_uno_option_price.portfolio_id=result.order_id;"
        result = conn.execute(query)
    engine_droid.dispose()
    print("All Data Updated (Portfolio, History & Option Price)")

def upsert_latest_price_in_droid_universe(args, result):
    print('=== Update Latest Price to database ===')
    result = result.set_index('ticker')
    result = result.reindex(columns=['last_date', 'open', 
                                     'low', 'high', 'close', 
                                     'latest_price_change', 'intraday_bid',
                                     'intraday_ask', 'intraday_time', 'intraday_date', 'capital_change'])
    dtype={
        'ticker':TEXT,
    }
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=latest_price_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + latest_price_table)
    engines_droid.dispose()
    del result

def check_timezone(ingestion_time):
    timebefore = '{:%H:%M:%S}'.format(datetime.utcnow().time())
    timebefore = datetime.strptime(timebefore, '%H:%M:%S')
    ingestion_time = datetime.strptime(ingestion_time, '%H:%M:%S')
    different_hours = ingestion_time.hour - timebefore.hour
    different_minute = ingestion_time.minute - timebefore.minute + 10
    different_hours = (different_hours * 60) + different_minute
    print(different_hours)
    if (ingestion_time <= timebefore) & (different_hours <= 10) & (different_hours > 0) :
        return True
    else :
        return False

def convert_to_utc(market_close_time, utc_offset, close_ingestion_offset):
    market_close = market_close_time.split(':')
    utc = utc_offset.split(':')
    close_ingestion = close_ingestion_offset.split(':')
    different_hours =int(market_close[0]) + int(utc[0]) + int(close_ingestion[0])
    if(int(utc[0]) < 0):
        different_minutes = int(market_close[1]) - int(utc[1]) + int(close_ingestion[1])
    elif(int(close_ingestion[0]) < 0):
        different_minutes = int(market_close[1]) + int(utc[1]) - int(close_ingestion[1])
    elif(int(utc[0]) < 0 & int(close_ingestion[0]) < 0):
        different_minutes = int(market_close[1]) - int(utc[1]) - int(close_ingestion[1])
    else:
        different_minutes = int(market_close[1]) + int(utc[1]) + int(close_ingestion[1])
    if(different_minutes >= 60):
        different_hours = different_hours + (int(different_minutes / 60))
        different_minutes = different_minutes % 60
    if(different_hours >= 24):
        different_hours = different_hours - 24
    result = str(different_hours) + ":" + str(different_minutes) + ":00"
    return result

def get_latest_price_from_dss(args):
    print(datetime.utcnow().time())
    ingestion_time = get_ingestion_time(args)
    for index, row in ingestion_time.iterrows():
        indices = row['index']
        print(indices)
        market_close_time = row['market_close_time']
        utc_offset = row['utc_offset']
        close_ingestion_offset = row['close_ingestion_offset']
        ingestion = convert_to_utc(market_close_time, utc_offset, close_ingestion_offset)
        print(indices + " " + ingestion + " " + str(check_timezone(ingestion)))
        if(check_timezone(ingestion)):
            jsonFileName = "latest_price.json"
            intradayjsonFileName = "percent_change.json"
            start_date = backdate_by_day(1)
            end_date = dateNow()
            universe = droid_universe_by_index(args, indices)
            ticker = universe['ticker']
            #ticker2 = "/" + universe['ticker']
            data = get_data_from_dss(args, ticker, jsonFileName, start_date, end_date)
            percentage_change =  get_latest_close_price(args, indices)
            if(len(data) > 0):
                datas = data.rename(columns={
                    'RIC': 'ticker',
                    'Trade Date': 'last_date',
                    'Open Price': 'open',
                    'Low Price': 'low',
                    'High Price': 'high',
                    'Universal Close Price': 'close',
                    'Adjustment Value - Capital Change' : 'capital_change'
                })
                datas  =datas.drop(columns=["IdentifierType", "Identifier"])
                datas = pd.DataFrame(datas)

                # new_data = percentage_change.rename(columns={
                #     'RIC': 'ticker',
                #     'Percent Change': 'latest_price_change'
                # })
                # new_data  =new_data.drop(columns=["IdentifierType", "Identifier"])
                # new_data['ticker']=new_data['ticker'].str.replace("/", "")
                # new_data['ticker']=new_data['ticker'].str.strip()
                # new_data = pd.DataFrame(new_data)

                datas["last_date"] = pd.to_datetime(datas["last_date"])
                result = datas.merge(percentage_change, how="left", on="ticker")
                result["yesterday_close"] = np.where(result["yesterday_close"].isnull(), 0, result["yesterday_close"])
                result["latest_price_change"] = round(((result["close"] - result["yesterday_close"]) / result["yesterday_close"]) * 100, 4)
                result = result.drop(columns=["yesterday_close", "trading_day"])
                result = result.dropna(subset=["close"])
                print(result)
                result["latest_price_change"] = np.where(result["latest_price_change"].isnull(), 0, result["latest_price_change"])
                result["latest_price_change"] = np.where(result["latest_price_change"] == np.inf, 0, result["latest_price_change"])
                result["latest_price_change"] = np.where(result["latest_price_change"] == -np.inf, 0, result["latest_price_change"])
        
                if(len(result) > 0):
                    result["intraday_bid"] = result["close"]
                    result["intraday_ask"] = result["close"]
                    result["intraday_time"] = datetimeNow()
                    result["intraday_date"] = result["last_date"]
                    result = result.sort_values(by='last_date', ascending=True)
                    result = result.drop_duplicates(subset='ticker', keep='last')
                    print(result)
                    print(" === Result to CSV === ")
                    upsert_latest_price_in_droid_universe(args, result)
                    PgFunctions(args.db_url_droid_write, 'clean_latest_price_updates')

                    #FELS populate top picks and setting up portfolio
                    if(datetime.utcnow().weekday() == 0):
                        if indices == "0#.SPX" :
                            requests.get('https://api.asklora.ai/api-test/buy-fels-classic/USD')
                            requests.get('https://api.asklora.ai/api-test/buy-fels-classic/USD?frac')

                        if indices == "0#.SXXE" :
                            requests.get('https://api.asklora.ai/api-test/buy-fels-classic/EUR')
                            requests.get('https://api.asklora.ai/api-test/buy-fels-classic/EUR?frac')

                report_to_slack("{} : === {} Latest Price Updated ===".format(str(datetime.now()), indices), args)
                split_order_and_performance(args, indices)

def split_order_and_performance(args, indices):
    latest_price_updates = get_latest_price_updates_split(args, indices)
    if(len(latest_price_updates) > 0): 
        print("Total Split = " + str(len(latest_price_updates)))
        print("Start Splitting Price in Data")
        split_data = latest_price_updates[["ticker", "last_date", "capital_change"]]
        split_data = split_data.loc[split_data["capital_change"] > 0]
        print(split_data)
        for index, row in split_data.iterrows():
            ticker = row["ticker"]
            print(ticker)
            last_date = row["last_date"]
            capital_change = row["capital_change"]
            update_all_data_by_capital_change(args, ticker, last_date, capital_change)
            update_capital_change(args, ticker)
            report_to_slack("{} : === PRICE SPLIT ON TICKER {} with CAPITAL CHANGE {} ===".format(str(datetime.now()), ticker, capital_change), args)

def get_latest_close_price(args, indices):
    print('=== Get Ingestion Time ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ohlctr.ticker, ohlctr.trading_day, ohlctr.close as yesterday_close from {master_ohlctr_table} ohlctr "
        query += f"inner join (select mo.ticker, max(mo.trading_day) as max_date from {master_ohlctr_table} mo where mo.close is not null group by mo.ticker) result "
        query += f"on ohlctr.ticker=result.ticker and ohlctr.trading_day=result.max_date "
        query += f"where ohlctr.ticker in (select ticker from {droid_universe_table} where index='{indices}' and is_active=True);"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def manual_ingestion(args):
    print(datetime.utcnow().time())
    indices = "0#.STI"
    # print(get_latest_close_price(args, indices))
    # sys.exit(1)
    print(indices)
    jsonFileName = "latest_price.json"
    #intradayjsonFileName = "percent_change.json"
    start_date = backdate_by_day(1)
    end_date = dateNow()
    universe = droid_universe_by_index(args, indices)
    ticker = universe['ticker']
    #ticker2 = "/" + universe['ticker']
    data = get_data_from_dss(args, ticker, jsonFileName, start_date, end_date)
    percentage_change =  get_latest_close_price(args, indices)
    if(len(data) > 0):
        datas = data.rename(columns={
            'RIC': 'ticker',
            'Trade Date': 'last_date',
            'Open Price': 'open',
            'Low Price': 'low',
            'High Price': 'high',
            'Universal Close Price': 'close',
            'Adjustment Value - Capital Change' : 'capital_change'
        })
        datas  =datas.drop(columns=["IdentifierType", "Identifier"])
        datas = pd.DataFrame(datas)

        # new_data = percentage_change.rename(columns={
        #     'RIC': 'ticker',
        #     'Percent Change': 'latest_price_change'
        # })
        # new_data  =new_data.drop(columns=["IdentifierType", "Identifier"])
        # new_data['ticker']=new_data['ticker'].str.replace("/", "")
        # new_data['ticker']=new_data['ticker'].str.strip()
        # new_data = pd.DataFrame(new_data)

        datas["last_date"] = pd.to_datetime(datas["last_date"])
        result = datas.merge(percentage_change, how="left", on="ticker")
        result["yesterday_close"] = np.where(result["yesterday_close"].isnull(), 0, result["yesterday_close"])
        result["latest_price_change"] = round(((result["close"] - result["yesterday_close"]) / result["yesterday_close"]) * 100, 4)
        result = result.drop(columns=["yesterday_close", "trading_day"])
        result = result.dropna(subset=["close"])
        print(result)
        result["latest_price_change"] = np.where(result["latest_price_change"].isnull(), 0, result["latest_price_change"])
        result["latest_price_change"] = np.where(result["latest_price_change"] == np.inf, 0, result["latest_price_change"])
        result["latest_price_change"] = np.where(result["latest_price_change"] == -np.inf, 0, result["latest_price_change"])
        if(len(result) > 0):
            result["intraday_bid"] = result["close"]
            result["intraday_ask"] = result["close"]
            result["intraday_time"] = datetimeNow()
            result["intraday_date"] = result["last_date"]
            result = result.sort_values(by='last_date', ascending=True)
            result = result.drop_duplicates(subset='ticker', keep='last')
            print(result)
            print(" === Result to CSV === ")
            upsert_latest_price_in_droid_universe(args, result)
            PgFunctions(args.db_url_droid_write, 'clean_latest_price_updates')
        #report_to_slack("{} : === {} Latest Price Updated ===".format(str(datetime.now()), indices), args)