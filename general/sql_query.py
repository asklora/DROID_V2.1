import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from general.sql_process import db_read, db_write
from general.date_process import backdate_by_day, str_to_date
from general.data_process import tuple_data
from general.table_name import (
    get_vix_table_name,
    get_currency_table_name,
    get_universe_table_name,
    get_universe_rating_table_name,
    get_fundamental_score_table_name,
    get_master_ohlcvtr_table_name, 
    get_report_datapoint_table_name,
    get_universe_consolidated_table_name)

universe_consolidated_table = get_universe_consolidated_table_name()
universe_table = get_universe_table_name()
master_ohlcvtr_table = get_master_ohlcvtr_table_name()
report_datapoint_table = get_report_datapoint_table_name()
vix_table = get_vix_table_name()
currency_table = get_currency_table_name()
fundamentals_score_table = get_fundamental_score_table_name()
universe_rating_table = get_universe_rating_table_name()

def read_query(query, table=universe_table, cpu_counts=False):
    print(f"Get Data From Database on {table} table")
    if(cpu_counts):
        engine = create_engine(db_read, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(db_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("Total Data = " + str(len(data)))
    return data
    
def get_latest_price():
    query = f"select mo.* from master_ohlcvtr mo, "
    query += f"(select master_ohlcvtr.ticker, max(master_ohlcvtr.trading_day) max_date "
    query += f"from master_ohlcvtr where master_ohlcvtr.close is not null " # and master_ohlcvtr.trading_day <= '2020-09-14'
    query += f"group by master_ohlcvtr.ticker) filter "
    query += f"where mo.ticker=filter.ticker and mo.trading_day=filter.max_date; "
    data = read_query(query, table="latest_price")
    return data

def get_data_by_table_name(table):
    query = f"select * from {table}"
    data = read_query(query, table=table)
    return data

def get_data_by_table_name_with_condition(table, condition):
    query = f"select * from {table} where {condition}"
    data = read_query(query, table=table)
    return data

def get_active_currency(currency_code=None):
    query = f"select * from {currency_table} where is_active=True"
    if type(currency_code) != type(None):
        query += f" and currency_code in {tuple_data(currency_code)}"
    #query = f"select currency_code, ric, utc_timezone_location from {currency_table} where is_active=True"
    data = read_query(query, table=currency_table)
    return data

def get_active_currency_ric_not_null():
    query = f"select * from {currency_table} where is_active=True and ric is not null"
    data = read_query(query, table=currency_table)
    return data

def get_active_universe_consolidated_by_field(isin=False, cusip=False, sedol=False, manual=False, ticker=None):
    query = f"select * from {universe_consolidated_table} where is_active=True "
    if isin:
        query += f"and use_isin=True "
    elif cusip:
        query += f"and use_cusip=True "
    elif sedol:
        query += f"and use_sedol=True "
    elif manual:
        query += f"and use_manual=True "

    if type(ticker) != type(None):
        query += f" and origin_ticker in {tuple_data(ticker)} "

    query += " order by origin_ticker "
    data = read_query(query, table=universe_consolidated_table)
    return data

def get_all_universe():
    query = f"select * from {universe_table} order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe(ticker=None, currency_code=None):
    query = f"select * from {universe_table} where is_active=True "
    if type(ticker) != type(None):
        query += f" and ticker in {tuple_data(ticker)} "

    if type(currency_code) != type(None):
        query += f" and currency_code in {tuple_data(currency_code)} "

    query += f"order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_entity_type(ticker=None, currency_code=None, null_entity_type=False):
    query = f"select * from {universe_table} where is_active=True "
    if null_entity_type:
        query += f"and entity_type is null "
    else:
        query += f"and entity_type is not null "
    if type(ticker) != type(None):
        query += f"and ticker in {tuple_data(ticker)} "
    if type(currency_code) != type(None):
        query += f"and currency_code in {tuple_data(currency_code)} " 
    query += f"order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_country_code(country_code, method=True):
    if(method):
        query = f"select * from {universe_table} where is_active=True and country_code='{country_code}' order by ticker"
    else:
        query = f"select * from {universe_table} where is_active=True and country_code!='{country_code}' order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_quandl_symbol(null_symbol=False, ticker=None, quandl_symbol=None):
    if not (null_symbol):
        query = f"select * from {universe_table} where is_active=True and quandl_symbol is not null "

        if type(ticker) != type(None):
            query += f"and ticker in {tuple_data(ticker)} "
    
        if type(quandl_symbol) != type(None):
            query += f"and quandl_symbol in {tuple_data(quandl_symbol)} "

    else:
        query = f"select * from {universe_table} where is_active=True and quandl_symbol is null and currency_code='USD' "
    query += f"order by ticker"
    data = read_query(query, table=universe_table)
    if(len(data) < 1):
        query = f"select ticker, split_part(ticker, '.', 1) as quandl_symbol from {universe_table} where is_active=True "
        
        data = read_query(query, table=universe_table)
    return data

def get_universe_by_region(region_code):
    query = f"select * from {universe_table} where is_active=True and currency_code in (select currency_code from {currency_table} where region_code='{region_code}')"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_company_description_null(ticker=None):
    if type(ticker) != type(None):
        query = f"select * from {universe_table} where is_active=True and ticker in {tuple_data(ticker)} order by ticker;"
    else:
        query = f"select * from {universe_table} where is_active=True and "
        query += f"company_description is null or company_description = 'NA' or company_description = 'N/A' order by ticker;"
    data = read_query(query, table=universe_table)
    return data

def findnewticker(args):
    query = f"select * FROM {report_datapoint_table} where reingested=False"
    data = read_query(query, table=report_datapoint_table)
    return data

def get_master_ohlcvtr_data(trading_day):
    query = f"select * from {master_ohlcvtr_table} where trading_day>='{trading_day}' and ticker in (select ticker from {universe_table} where is_active=True)"
    data = read_query(query, table=master_ohlcvtr_table)
    return data

def get_master_ohlcvtr_start_date():
    query = f"SELECT min(trading_day) as start_date FROM {master_ohlcvtr_table} WHERE day_status is null"
    data = read_query(query, table=master_ohlcvtr_table)
    data = data.loc[0, "start_date"]
    data = str(data)
    data = str_to_date(data)
    if(data == None):
        data = backdate_by_day(7)
    return data

def get_vix(vix_id=None):
    query = f"select * from {vix_table} "
    if type(vix_id) != type(None):
        query += f" where vix_id in {tuple_data(vix_id)}"
    data = read_query(query, table=vix_table)
    return data

def get_universe_rating(ticker=None, currency_code=None):
    query = f"select * from {universe_rating_table} "
    if type(ticker) != type(None):
        query += f" where ticker in (select ticker from {universe_table} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f" where ticker in (select ticker from {universe_table} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    else:
        query += f" where ticker in (select ticker from {universe_table} where is_active=True) "
    query += f"order by ticker"
    data = read_query(query, table=universe_rating_table)
    return data

def get_fundamentals_score(ticker=None, currency_code=None):
    query = f"select * from {fundamentals_score_table} "
    if type(ticker) != type(None):
        query += f" where ticker in (select ticker from {universe_table} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f" where ticker in (select ticker from {universe_table} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    else:
        query += f" where ticker in (select ticker from {universe_table} where is_active=True) "
    query += f"order by ticker"
    data = read_query(query, table=fundamentals_score_table)
    return data

def get_max_last_ingestion_from_universe(ticker=None, currency_code=None):
    query = f"select max(last_ingestion) as max_date from {universe_table} "
    if type(ticker) != type(None):
        query += f" where ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f" where currency_code in {tuple_data(currency_code)} "
    data = read_query(query, table=universe_table)
    return str(data.loc[0, "max_date"])
    
def get_last_close_industry_code(ticker=None, currency_code=None):
    query = f"select mo.ticker, mo.close, mo.currency_code, substring(univ.industry_code from 0 for 3) as industry_code from "
    query += f"{master_ohlcvtr_table} mo inner join {universe_table} univ on univ.ticker=mo.ticker where univ.is_active=True "
    if type(ticker) != type(None):
        query += f"and univ.ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f"and univ.currency_code in {tuple_data(currency_code)} "
    query += f"and exists( select 1 from (select ticker, max(trading_day) max_date "
    query += f"from {master_ohlcvtr_table} where close is not null group by ticker) filter where filter.ticker=mo.ticker "
    query += f"and filter.max_date=mo.trading_day)"
    data = read_query(query, table=master_ohlcvtr_table)
    return data

def get_pred_mean():
    query = f"select distinct avlpf.ticker, avlpf.pred_mean, avlpf.testing_period::date from ai_value_lgbm_pred_final avlpf, "
    query += f"(select ticker, max(testing_period::date) as max_date from ai_value_lgbm_pred_final group by ticker) filter "
    query += f"where filter.ticker=avlpf.ticker and filter.max_date=avlpf.testing_period;"
    data = read_query(query, table=master_ohlcvtr_table)
    return data