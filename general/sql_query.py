from ingestion.master_ohlcvtr import master_ohlctr_update
import time
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from general.sql_process import db_read, db_write
from general.date_process import backdate_by_day
from general.table_name import (
    get_vix_table_name,
    get_currency_table_name,
    get_universe_table_name, 
    get_master_ohlcvtr_table_name, 
    get_report_datapoint_table_name)

universe_table = get_universe_table_name()
master_ohlcvtr_table = get_master_ohlcvtr_table_name()
report_datapoint_table = get_report_datapoint_table_name()
vix_table = get_vix_table_name()
currency_table = get_currency_table_name()

def read_query(query, table=universe_table):
    print(f"Get Data From Database on {table} table")
    engine = create_engine(db_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("Total Data = " + str(len(data)))
    return data

def get_data_by_table_name(table):
    query = f"select * from {table}"
    data = read_query(query, table=table)
    return data

def get_data_by_table_name_with_condition(table, condition):
    query = f"select * from {table} where {condition}"
    data = read_query(query, table=table)
    return data

def get_currency_symbol():
    query = f"select currency_code, ric from {currency_table} where ric is not null"
    data = read_query(query, table=currency_table)
    return data

def get_all_universe():
    query = f"select * from {universe_table} order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe():
    query = f"select * from {universe_table} where is_active=True order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_entity_type():
    query = f"select * from {universe_table} where is_active=True and entity_type is not null order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_currency_entity_type(currency_code, method=True):
    if(method):
        query = f"select * from {universe_table} where is_active=True and currency_code='{currency_code}' and entity_type is not null order by ticker"
    else:
        query = f"select * from {universe_table} where is_active=True and currency_code='{currency_code}' and entity_type is null order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_currency(currency_code):
    query = f"select * from {universe_table} where is_active=True and currency_code='{currency_code}' order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_country_code(country_code, method=True):
    if(method):
        query = f"select * from {universe_table} where is_active=True and country_code='{country_code}' order by ticker"
    else:
        query = f"select * from {universe_table} where is_active=True and country_code!='{country_code}' order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_by_quandl_symbol(method=True):
    if(method):
        query = f"select * from {universe_table} where is_active=True and quandl_symbol is not null order by ticker"
    else:
        query = f"select * from {universe_table} where is_active=True and quandl_symbol is null and country_code='US' order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_universe_by_region(region_code):
    query = f"select * from {universe_table} where is_active=True and currency_code in (select currency_code from {currency_table} where region_code='{region_code}')"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe_company_description_null():
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
    if(data == None):
        data = backdate_by_day(7)
    return data

def get_timezone_area(args):
    print("Get Timezone Area")
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select index, utc_timezone_location from {indices_table} where still_live=True"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_market_close(args):
    print("Get Market Close Time")
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select index, market_close_time, utc_offset, close_ingestion_offset from {indices_table} where still_live=True"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_vix():
    print("Get Vix Index")
    query = f"select * from {vix_table}"
    data = read_query(query, table=vix_table)
    return data

