import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from general.sql_process import db_read, db_write, droid_db_read, dlp_db_read
from general.date_process import backdate_by_day, dateNow, droid_start_date, str_to_date
from general.data_process import tuple_data
from general.table_name import (
    get_data_ibes_monthly_table_name, get_data_macro_monthly_table_name, get_industry_group_table_name, get_industry_table_name, get_latest_price_table_name, get_master_tac_table_name, get_region_table_name, get_vix_table_name,
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

def read_query(query, table=universe_table, cpu_counts=False, droid1=False, dlp=False, prints=True):
    if(prints):
        print(f"Get Data From Database on {table} table")
    if droid1:
        dbcon = droid_db_read
    elif dlp:
        dbcon = dlp_db_read
    else:
        dbcon = db_read
    if(cpu_counts):
        engine = create_engine(dbcon, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(dbcon, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    if(prints):
        print("Total Data = " + str(len(data)))
    return data

def get_active_universe_droid1(ticker=None, currency_code=None):
    query = f"select * from {universe_table} where is_active=True "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=universe_table, dlp=True)
    return data

def check_start_end_date(start_date, end_date):
    if type(start_date) == type(None):
        start_date = str_to_date(droid_start_date())
    if type(end_date) == type(None):
        end_date = str_to_date(dateNow())
    return start_date, end_date
    
def check_ticker_currency_code_query(ticker=None, currency_code=None):
    query = ""
    if type(ticker) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    return query

def get_region():
    table_name = get_region_table_name()
    query = f"select * from {table_name}"
    data = read_query(query, table=table_name)
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
        query += f"and use_manual=True and consolidated_ticker is null "

    if type(ticker) != type(None):
        query += f" and origin_ticker in {tuple_data(ticker)} "

    query += " order by origin_ticker "
    data = read_query(query, table=universe_consolidated_table)
    return data

def get_all_universe(ticker=None, currency_code=None):
    query = f"select * from {universe_table} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    query += f" order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_active_universe(ticker=None, currency_code=None):
    query = f"select * from {universe_table} where is_active=True "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=universe_table)
    return data

def get_universe_rating(ticker=None, currency_code=None):
    table_name = get_universe_rating_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"where " + check
    query += f" order by ticker"
    data = read_query(query, table=table_name)
    return data

def get_active_universe_by_entity_type(ticker=None, currency_code=None, null_entity_type=False):
    query = f"select * from {universe_table} where is_active=True "
    if null_entity_type:
        query += f"and entity_type is null "
    else:
        query += f"and entity_type is not null "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
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
    
        elif type(quandl_symbol) != type(None):
            query += f"and quandl_symbol in {tuple_data(quandl_symbol)} "

    else:
        query = f"select * from {universe_table} where is_active=True and quandl_symbol is null and currency_code='USD' "
    query += f"order by ticker"
    data = read_query(query, table=universe_table)
    if(len(data) < 1):
        query = f"select ticker, split_part(ticker, '.', 1) as quandl_symbol from {universe_table} where is_active=True "
        if type(ticker) != type(None):
            query += f"and ticker in {tuple_data(ticker)} "
        elif type(quandl_symbol) != type(None):
            query += f"and quandl_symbol in {tuple_data(quandl_symbol)} "
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

def get_yesterday_close_price(ticker=None, currency_code=None):
    query = f"select tac.ticker, tac.trading_day, tac.tri_adj_close as yesterday_close from {get_master_tac_table_name()} tac "
    query += f"inner join (select mo.ticker, max(mo.trading_day) as max_date from {get_master_tac_table_name()} mo where mo.tri_adj_close is not null group by mo.ticker) result "
    query += f"on tac.ticker=result.ticker and tac.trading_day=result.max_date "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"where tac." + check
    data = read_query(query, table=master_ohlcvtr_table)
    return data

def get_latest_price_data(ticker=None, currency_code=None):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_latest_price_capital_change(ticker=None, currency_code=None):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} where capital_change is not null "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_ibes_monthly(start_date):
    table_name = get_data_ibes_monthly_table_name()
    query = f"select * from {table_name} "
    query += f"where trading_day in (select max(mcm.trading_day) "
    query += f"from {table_name} mcm where mcm.trading_day>='{start_date}' "
    query += f"group by mcm.period_end, ticker); "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_ibes_new_ticker():
    table_name = universe_table
    query = f"select * from {table_name} "
    query += f"where is_active=True and "
    query += f"entity_type is not null and "
    query += f"ticker not in (select ticker from {get_data_ibes_monthly_table_name()} group by ticker having count(ticker) >= 48)"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_macro_monthly(start_date):
    table_name = get_data_macro_monthly_table_name()
    query = f"select * from {table_name} "
    query += f"where trading_day in (select max(mcm.trading_day) "
    query += f"from {table_name} mcm where mcm.trading_day>='{start_date}' "
    query += f"group by mcm.period_end); "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_industry():
    table_name = get_industry_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table=table_name)
    return data

def get_industry_group():
    table_name = get_industry_group_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table=table_name)
    return data

def get_master_tac_data(start_date=None, end_date=None, ticker=None, currency_code=None):
    start_date, end_date = check_start_end_date(start_date, end_date)
    table_name = get_master_tac_table_name()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_consolidated_data(column, condition, group_field=None):
    table_name = get_universe_consolidated_table_name()
    query = f"select {column} from {table_name} "
    if(type(condition) != type(None)):
        query += f"where {condition} "
    if(type(group_field) != type(None)):
        query += f"group by {group_field} "
    data = read_query(query, table_name, cpu_counts=True)
    return data