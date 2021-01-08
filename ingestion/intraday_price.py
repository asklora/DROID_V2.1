import pandas as pd
import sqlalchemy as db
from sqlalchemy.types import Date, BIGINT, TEXT
import numpy as np
from sqlalchemy import create_engine
from general.general import dateNow, backdate_by_day, backdate_by_day
from general.slack import report_to_slack
from data_source.DSS import get_intraday_from_dss
from universe import droid_universe_by_index, DroidUniverse
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
from datetime import datetime
from pangres import upsert
import sys

latest_price_table = "latest_price_updates"
droid_universe_table = "droid_universe"
indices_table = "indices"
user_portfolio_table = "user_portfolio"
split_record_table = 'split_record'

def get_live_ticker_by_index(args, indices):
    print('=== Get Live Ticker ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker, index from {droid_universe_table} where index = '{indices}' and is_active=True and ticker in(select stock_selected from user_portfolio where status = False);"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_latest_price_from_master_ohlctr(args, ticker):
    print('=== Get Yesterday Price ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select master_ohlctr.ticker, master_ohlctr.trading_day, master_ohlctr.close from master_ohlctr "
        query += f"inner join (select mo.ticker, max(mo.trading_day) as max_date from master_ohlctr mo where mo.close is not null group by mo.ticker)result "
        query += f"on master_ohlctr.ticker=result.ticker and master_ohlctr.trading_day=result.max_date "
        query += f"where master_ohlctr.ticker in {ticker};"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_intraday_time(args):
    print('=== Get Intraday Time ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f'select * from {indices_table} where still_live=True order by index'
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def upsert_latest_price_in_droid_universe(args, result):
    print('=== Update Split Temp to database ===')
    result = result.set_index('ticker')
    result = result.reindex(columns=['intraday_date', 'data_type', 
                                     'capital_change', 'price', 
                                     'percent_change'])
    dtype={
        'ticker':TEXT,
    }
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=split_record_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + split_record_table)
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

def convert_to_utc(method, market_close_time, utc_offset, intraday_offset):
    market_time = market_close_time.split(':')
    utc = utc_offset.split(':')
    intraday_time = intraday_offset.split(':')
    
    if(method == "OPEN"):
        different_hours =int(market_time[0]) + int(utc[0]) + int(intraday_time[0])
        if(int(utc[0]) < 0):
            different_minutes = int(market_time[1]) - int(utc[1]) + int(intraday_time[1])
        elif(int(intraday_time[0]) < 0):
            different_minutes = int(market_time[1]) + int(utc[1]) - int(intraday_time[1])
        elif(int(utc[0]) < 0 & int(intraday_time[0]) < 0):
            different_minutes = int(market_time[1]) - int(utc[1]) - int(intraday_time[1])
        else:
            different_minutes = int(market_time[1]) + int(utc[1]) + int(intraday_time[1])
    else:
        different_hours =int(market_time[0]) + int(utc[0]) - int(intraday_time[0])
        if(int(utc[0]) < 0):
            different_minutes = int(market_time[1]) - int(utc[1]) - int(intraday_time[1])
        elif(int(intraday_time[0]) < 0):
            different_minutes = int(market_time[1]) + int(utc[1]) + int(intraday_time[1])
        elif(int(utc[0]) < 0 & int(intraday_time[0]) < 0):
            different_minutes = int(market_time[1]) - int(utc[1]) + int(intraday_time[1])
        else:
            different_minutes = int(market_time[1]) + int(utc[1]) - int(intraday_time[1])

    if(different_minutes >= 60):
        different_hours = different_hours + (int(different_minutes / 60))
        different_minutes = different_minutes % 60
    if(different_minutes < 0):
        different_hours = different_hours - 1
        different_minutes = 60 + different_minutes
    if(different_hours < 0):
        different_hours = 24 + different_hours
    if(different_hours >= 24):
        different_hours = different_hours - 24
    result = str(different_hours) + ":" + str(different_minutes) + ":00"
    return result

def get_split_record_from_dss(args):
    print(datetime.utcnow().time())
    intraday_times = get_intraday_time(args)
    tickerlist = pd.Series([])
    indiceslist = ""
    for index, row in intraday_times.iterrows():
        indices = row['index']
        market_close_time = row['market_open_time']
        utc_offset = row['utc_offset']
        intraday_offset = row['intraday_offset_open']
        intraday_time = convert_to_utc("OPEN", market_close_time, utc_offset, intraday_offset)
        print(indices + " " + intraday_time + " " + str(check_timezone(intraday_time)))
        if(check_timezone(intraday_time)):
            report_to_slack("{} : === MARKET OPEN FOR {} ===".format(str(datetime.now()), indices), args)
            universe = get_live_ticker_by_index(args, indices)
            if(len(universe) > 0):
                ticker = "/" + universe['ticker']
                print(ticker)
                jsonFileName = "intraday_capital_change.json"
                data = get_intraday_from_dss(args, ticker, jsonFileName)
                print("Total Data = " + str(len(data)))
                if(len(data) > 0):
                    datas = data.rename(columns={
                        'RIC': 'ticker',
                        'Trade Date': 'intraday_date',
                        'Last Price': 'price',
                        'Percent Change': 'percent_change'
                    })
                    datas = pd.DataFrame(datas)
                    datas['ticker']=datas['ticker'].str.replace("/", "")
                    datas['ticker']=datas['ticker'].str.strip()
                    datas = datas.reindex(columns=['ticker', 'intraday_date', 'price', 'percent_change'])
                    if(len(datas["ticker"]) == 1):
                        tuple_ticker = "('" + str(datas['ticker'][0]) + "')"
                    else:
                        tuple_ticker = tuple(datas["ticker"].to_list())
                    new_data = get_latest_price_from_master_ohlctr(args, tuple_ticker)
                    datas = datas.merge(new_data, how="left", on="ticker")
                    datas = datas.dropna(subset=["price", "percent_change"])
                    datas["capital_change"] = (datas["price"] / (1 + (datas["percent_change"] / 100))) / datas["close"]
                    datas["data_type"] = "Temp"
                    print(datas)
                    datas = datas.loc[datas["capital_change"] > 1.5]
                    datas =  datas.loc[datas["capital_change"] < 0.95]
                    print(datas)
                    if(len(datas) > 0):
                        upsert_latest_price_in_droid_universe(args, datas)
                        report_to_slack("{} : === SPLIT ON TICKER {} ===".format(str(datetime.now()), tuple(datas["ticker"].to_list())), args)