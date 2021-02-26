from sqlalchemy.sql.expression import table
from general.date_process import dateNow, droid_start_date, str_to_date
import sys
import datetime as dt
import pandas as pd
import sqlalchemy as db
from datetime import datetime
from multiprocessing import cpu_count
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine, and_
from dateutil.relativedelta import relativedelta
from general import table_name
from global_vars import DB_URL_WRITE, DB_URL_READ
from general.sql_query import read_query
from general.data_process import tuple_data
from general.table_name import (
    get_bot_classic_backtest_table_name, get_bot_data_table_name, get_bot_ucdc_backtest_table_name, get_bot_uno_backtest_table_name, get_calendar_table_name, get_currency_table_name, get_data_dividend_table_name, 
    get_data_ibes_monthly_table_name, get_data_interest_table_name,
    get_data_macro_monthly_table_name, get_data_vix_table_name, 
    get_data_vol_surface_inferred_table_name, 
    get_data_vol_surface_table_name, get_latest_price_table_name, 
    get_universe_table_name,
    get_master_tac_table_name,
    get_master_ohlcvtr_table_name
)
# read_query(query, table=universe_table, cpu_counts=False)
# tuple_data(data)

def check_ticker_currency_code_query(ticker=None, currency_code=None):
    query = ""
    if type(ticker) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    return query

def check_start_end_date(start_date=None, end_date=None):
    if type(start_date) == type(None):
        start_date = droid_start_date()
    if type(end_date) == type(None):
        end_date = dateNow()
    return start_date, end_date
    
def get_bot_data_latest_date(daily=False, history=False):
    if(daily):
        table_name = get_bot_data_table_name()
    else:
        table_name = get_data_vol_surface_inferred_table_name()
    query = f"select ticker, max(trading_day) as max_date from {table_name} group by ticker"
    data = read_query(query, table_name, cpu_counts=True)
    if(len(data) == 0):
        return str_to_date(droid_start_date())
    return min(data["max_date"])

def get_master_tac_price(start_date=None, end_date=None, ticker=None, currency_code=None):
    start_date, end_date = check_start_end_date(start_date=start_date, end_date=end_date)
    table_name = get_master_tac_table_name()
    query = f"select * from {table_name} where trading_day >= {start_date} "
    query += f"and trading_day <= {end_date} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_latest_price(ticker=None, currency_code=None):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_vix_price():
    table_name = get_data_vix_table_name()
    query = f"select * from {table_name}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_currency_data(currency_code=None):
    table_name = get_currency_table_name()
    query = f"select * from {table_name} "
    if type(currency_code) != type(None):
        query += f"where currency_code in {tuple_data(currency_code)} "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_vol_surface_ticker(ticker=None, currency_code=None):
    table_name = get_data_vol_surface_table_name()
    query = f"select distinct ticker from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "where " + check
    data = read_query(query, table_name)
    return data

def get_volatility_latest_date(ticker=None, currency_code=None, infer=True):
    if(infer):
        table_name = get_data_vol_surface_inferred_table_name()
    else:
        table_name = get_data_vol_surface_table_name()
    query = f"select ticker, max(spot_date) as max_date from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "where " + check
    query += f"group by ticker"
    data = read_query(query, table=table_name, cpu_counts=True)
    return data["max_date"].min()

def get_backtest_latest_date(ticker=None, currency_code=None, ucdc=False, uno=False, classic=False, mod=False):
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    else:
        table_name = get_bot_classic_backtest_table_name()

    if(mod):
        table_name += "_mod"
    
    query = f"select ticker, max(spot_date) as max_date from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "where " + check
    query += f"group by ticker"
    data = read_query(query, table=table_name, cpu_counts=True)
    return data

def get_data_vol_surface_inferred_ticker(ticker=None, currency_code=None):
    table_name = get_data_vol_surface_inferred_table_name()
    query = f"select distinct ticker from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "where " + check
    data = read_query(query, table_name)
    return data

def get_new_tickers_from_bot_data(start_date, start_date2, date_identifier, ticker=None, currency_code=None):
    table_name = get_bot_data_table_name()
    query = f"select du.ticker, coalesce(result1.count_data, 0), coalesce(result2.count_price, 0) "
    query += f"from {get_universe_table_name()} du "
    query += f"left join (select ticker, coalesce(count(cb.{date_identifier}), 0) as count_data "
    query += f"from {table_name} cb where cb.{date_identifier}>='{start_date}' group by cb.ticker) result1 on result1.ticker=du.ticker "
    query += f"left join (select ticker, coalesce(count(mo.trading_day), 0) as count_price "
    query += f"from {get_master_tac_table_name()} mo where mo.trading_day>='{start_date2}' group by mo.ticker) "
    query += f"result2 on result2.ticker=du.ticker "
    query += f"where du.is_active=True and "
    if type(ticker) != type(None):
        query += f"du.ticker in {tuple_data(ticker)} and  "
    elif type(currency_code) != type(None):
        query += f"du.currency_code in {tuple_data(currency_code)} and "
    query += f"coalesce(result1.count_data, 0) < coalesce(result2.count_price, 0) - 20 "
    query += f"order by count_data;"
    data = read_query(query, table=get_universe_table_name())
    return data

def get_new_ticker_from_bot_backtest(ticker=None, currency_code=None, ucdc=False, uno=False, classic=False, mod=False):
    start_date = droid_start_date()
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    else:
        table_name = get_bot_classic_backtest_table_name()
    if(mod):
        table_name += "_mod"
    query = f"select du.ticker, index, result1.uno_min_date, result2.ohlctr_min_date, result1.uno_max_date, result2.ohlctr_max_date "
    query += f"from {get_universe_table_name()} du "
    query += f"left join (select ticker, min(cb.spot_date)::date as uno_min_date, max(cb.spot_date)::date as uno_max_date "
    query += f"from {table_name} cb where cb.spot_date>='{start_date}' group by cb.ticker) result1 on result1.ticker=du.ticker  "
    query += f"left join (select ticker, min(mo.trading_day)::date as ohlctr_min_date, max(mo.trading_day)::date as ohlctr_max_date "
    query += f"from {get_master_tac_table_name()} mo where mo.trading_day>='{start_date}' group by mo.ticker) result2 "
    query += f"on result2.ticker=du.ticker  where du.is_active=True and "
    if type(ticker) != type(None):
        query += f"du.ticker in {tuple_data(ticker)} and  "
    elif type(currency_code) != type(None):
        query += f"du.currency_code in {tuple_data(currency_code)} and "
    query += f"result1.uno_min_date > result2.ohlctr_min_date + interval '1 years' "
    query += f"order by du.index;"
    data = read_query(query, table=get_universe_table_name())
    return data

def get_macro_data(start_date, end_date):
    query = f"select trading_day, usinter3_esa, usgbill3_esa, \"EMIBOR3._ESA\", jpmshort_esa, \"EMGBOND._ESA\", \"CHGBOND._ESA\", fred_data "
    query += f"from {get_data_macro_monthly_table_name()} where trading_day >= '{start_date}' and trading_day <= '{end_date}' "
    data = read_query(query, get_data_macro_monthly_table_name(), cpu_counts=False)
    data["ticker"] = "MSFT.O"

    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start_date, end_date, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = result[result["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["ticker", "trading_day"])
    for col in ["usinter3_esa", "usgbill3_esa", "EMIBOR3._ESA", "jpmshort_esa", "EMGBOND._ESA", "CHGBOND._ESA", "fred_data"]:
        result[col] = result[col].bfill().ffill()
    result = result.drop(columns="ticker")
    return result

def get_ibes_data(start_date, end_date, ticker_list):
    query = f"select ticker, trading_day, eps1fd12, eps1tr12, cap1fd12 "
    query += f"from {get_data_ibes_monthly_table_name()} where trading_day >= '{start_date}' and trading_day <= '{end_date}' "
    query += f" and ticker in {tuple_data(ticker_list)} "
    data = read_query(query, get_data_ibes_monthly_table_name(), cpu_counts=False)
    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start_date, end_date, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = result[result["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["ticker", "trading_day"])
    for col in ["eps1fd12", "eps1tr12", "cap1fd12"]:
        result[col] = result[col].bfill().ffill()
    return result

def get_stochatic_data(start_date, end_date, ticker_list):
    query = f"select ticker, trading_day, fast_d, fast_k, rsi "
    query += f"from {get_master_tac_table_name()} where trading_day >= '{start_date}' and trading_day <= '{end_date}' "
    query += f" and ticker in {tuple_data(ticker_list)} "
    data = read_query(query, get_master_tac_table_name(), cpu_counts=False)
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    return data

def get_executive_data_download(start_date, end_date, ticker=None, currency_code=None):
    query = f"select * from {get_bot_data_table_name()} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check
    data = read_query(query, get_bot_data_table_name())
    tickers_list = data["ticker"].unique().tolist()
    macro_data = get_macro_data(start_date, end_date)
    ibes_data = get_ibes_data(start_date, end_date, tickers_list)
    stochatic_data = get_stochatic_data(start_date, end_date, tickers_list)
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    data = data.merge(macro_data, how="left", on=["trading_day"])
    data = data.merge(ibes_data, how="left", on=["ticker", "trading_day"])
    data = data.merge(stochatic_data, how="left", on=["ticker", "trading_day"])
    return data

def get_calendar_data(start_date=None, end_date=None, ticker=None, currency_code=None):
    start_date, end_date = check_start_end_date(start_date=start_date, end_date=end_date)
    table_name = get_calendar_table_name()
    query = f"select * from {table_name} where non_working_day >= '{start_date}' "
    query += f"and non_working_day <= '{end_date}' "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_vol_surface_data(start_date=None, end_date=None, ticker=None, currency_code=None, infer=True):
    if(infer):
        table_name = get_data_vol_surface_inferred_table_name()
    else:
        table_name = get_data_vol_surface_table_name()
    start_date, end_date = check_start_end_date(start_date=start_date, end_date=end_date)
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_interest_rate_data():
    table_name = get_data_interest_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_dividends_data():
    table_name = get_data_dividend_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_backtest_data(start_date=None, end_date=None, day_to_exp=None, ticker=None, currency_code=None, uno=False, ucdc=False, classic=False, mod=False):
    start_date, end_date = check_start_end_date(start_date=start_date, end_date=end_date)
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    else:
        table_name = get_bot_classic_backtest_table_name()
    
    if mod:
        table_name += '_mod'
    
    query = f"select * from {table_name} "
    query += f" "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += "and " + check

def download_production_executive_null_dates_list(args):


    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        print(args.month_horizon)
        query = db.select([table0.columns.spot_date]).where(and_(table0.columns.event == None,
                                                                 table0.columns.ticker.in_(args.tickers_list),
                                                                 table0.columns.month_to_exp.in_(args.month_horizon),
                                                                 table0.columns.inferred == flag,
                                                                 table0.columns.modified == flag_modified))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have null options.")
        sys.exit()
    full_df.columns = columns_list

    return full_df.spot_date.unique()

def get_current_stocks_list(args):
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.executive_data_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker.distinct()])

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have inferred data.")
        sys.exit()
    return full_df[0]




    return full_df

def top_models(args):
    if args.go_production:
        db_url = global_vars.DB_PROD_URL_READ
        table_name = args.production_lgbm_model_data_table_name
    else:
        db_url = global_vars.DB_TEST_URL_READ
        table_name = args.test_lgbm_model_data_table_name

    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

        if args.go_production:
            query = table0.select().where(table0.columns.model_filename.in_(args.models_filenames))
        else:
            query = table0.select().where(table0.columns.spot_date == args.spot_date)

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit(f"We don't have models data in {table_name}.")
    full_df.columns = columns_list

    full_df.sort_values(by=['valid_error'], inplace=True)
    full_df = full_df[full_df.pc_number == args.pc_number]
    full_df2 = full_df.groupby(['model_type']).agg(
        {'model_filename': 'first', 'pc_number': 'first', 'start_date': 'first',
         'spot_date': 'first', 'valid_error': 'first'})
    full_df2.reset_index(inplace=True)
    if len(full_df2) == 0:
        print(f"We don't have models data in {table_name} for {args.spot_date}.")
        sys.exit()
    return full_df2


def tac_data_download(args):
    # The tac data is the TRI adjusted prices.
    start_date = args.start_date
    stop_date = args.end_date

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # table_name = global_vars.droid_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.tac_data_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            ['*']).where(and_(table0.columns.trading_day >= start_date,
                              table0.columns.trading_day <= stop_date, table0.columns.ticker.in_(args.tickers_list)))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have tac data.")
        sys.exit()
    full_df.columns = columns_list

    # tickers = pd.DataFrame(ResultSet2)
    # tickers.columns = columns_list2

    full_df = full_df[full_df.ticker.isin(args.tickers_list)]
    return full_df


def tac_data_download_null_filler(start_date, args):
    # The tac data is the TRI adjusted prices.
    # start_date = args.start_date
    stop_date = args.end_date

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # table_name = global_vars.droid_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.tac_data_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            ['*']).where(and_(table0.columns.trading_day >= start_date,
                              table0.columns.trading_day <= stop_date, table0.columns.ticker.in_(args.tickers_list)))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have tac data.")
        sys.exit()
    full_df.columns = columns_list

    # tickers = pd.DataFrame(ResultSet2)
    # tickers.columns = columns_list2

    full_df = full_df[full_df.ticker.isin(args.tickers_list)]
    return full_df





def download_production_sltp(args):
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select()

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have tac data.")
        sys.exit()
    full_df.columns = columns_list

    return full_df





def get_vol_surface_data_inferred(args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.droid_vol_surface_parameters_inferred_table_name, metadata, autoload=True,
                          autoload_with=conn)

        query = table0.select()
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        full_df = pd.DataFrame(index=range(1), columns=columns_list)
        # sys.exit("We don't have output data.")

    full_df.columns = columns_list
    full_df = full_df[full_df.ticker.isin(args.active_tickers_list)]

    return full_df


def download_production_executive_max_date(args):
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    # if args.add_inferred:
    #     table_name = args.executive_production_table_name_inferred
    # else:
    #     table_name = args.executive_production_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

        # query = table0.select().order_by(table0.columns.spot_date.desc()).limit(1)
        query = table0.select().order_by(table0.columns.spot_date.desc()).where(and_(
            table0.columns.ticker.in_(args.tickers_list)))
        # where(table0.columns.spot_date == max(table0.columns.spot_date))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()
    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have executive data.")
        sys.exit()
    full_df.columns = columns_list
    temp = full_df[['ticker', 'spot_date']].groupby('ticker').agg({'spot_date': ['max']})
    # return full_df
    return temp


def get_active_tickers_list(args):
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table_name = global_vars.droid_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        query2 = db.select([table0.columns.ticker]).where(table0.columns.is_active == True)

        ResultProxy2 = conn.execute(query2)
        ResultSet2 = ResultProxy2.fetchall()
        columns_list2 = ResultProxy2.keys()

    engine.dispose()

    full_df = pd.DataFrame(ResultSet2)

    if len(full_df) == 0:
        print("We don't have tac data.")
        sys.exit()
    full_df.columns = columns_list2

    tickers = pd.DataFrame(ResultSet2)
    tickers.columns = columns_list2

    return tickers.ticker


def download_production_executive_null(args):
    if args.debug_mode or args.option_maker_history_full or args.option_maker_history_full_ucdc:
        db_url = global_vars.DB_TEST_URL_READ  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    else:
        db_url = global_vars.DB_DROID_URL_READ  # if writing to production_data table, i.e., write out the stocks - keep in "live"

    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table_name = args.executive_production_table_name

    if args.option_maker_history_full:
        table_name = table_name + '_full'
    elif args.option_maker_history_full_ucdc:
        table_name = args.executive_production_ucdc_table_name + '_full'
    elif args.option_maker_history_ucdc or args.option_maker_daily_ucdc or args.option_maker_live_ucdc:
        table_name = args.executive_production_ucdc_table_name
    else:
        table_name = table_name

    if args.add_inferred:
        flag = 1
    else:
        flag = 0

    if args.modified:
        table_name = table_name + '_mod'
        flag_modified = 1
    else:
        flag_modified = 0

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select().where(and_(table0.columns.event == None,
                                           table0.columns.ticker.in_(args.tickers_list),
                                           table0.columns.month_to_exp.in_(args.month_horizon),
                                           table0.columns.inferred == flag, table0.columns.spot_date >= args.date_min,
                                           table0.columns.modified == flag_modified,
                                           table0.columns.spot_date <= args.date_max))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print("We don't have null options.")
        sys.exit()
    full_df.columns = columns_list

    return full_df



def executive_benchmark_data_download(args):
    if args.benchmark:
        table_name = args.executive_production_table_name
    else:
        table_name = args.executive_production_ucdc_table_name

    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        query = table0.select().where(and_(table0.columns.event != None,
                                           table0.columns.month_to_exp.in_(args.month_horizon),
                                           table0.columns.spot_date >= dt.date.today()
                                           - relativedelta(months=args.lookback_horizon)))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print(f"We don't have execuive data in {table_name}.")
        sys.exit()
    full_df.columns = columns_list

    return full_df


def get_uno_backtest(flag, args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ

    if flag == 'uno':
        table_name = args.executive_production_table_name
    elif flag == 'ucdc':
        table_name = args.executive_production_ucdc_table_name
    else:  # Classic
        table_name = global_vars.sltp_production_table_name

    if args.modified:
        table_name = table_name + "_mod"

    start_date = args.start_date
    stop_date = args.end_date

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    args.latest_date = get_latest_date(args)

    # db_url = global_vars.DB_DROID_URL
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

        if args.train_model or args.bot_labeler_train:
            query = table0.select().where(and_(table0.columns.ticker.in_(args.tickers_list)))
            if (args.month_horizon) :
                query = table0.select().where(and_(table0.columns.ticker.in_(args.tickers_list),
                                                   table0.columns.month_to_exp.in_(args.month_horizon)))
            ResultProxy = conn.execute(query)
            ResultSet = ResultProxy.fetchall()
            columns_list = ResultProxy.keys()
            full_df = pd.DataFrame(ResultSet)
            if len(full_df) == 0:
                print(f"We don't have data in {table_name}.")
                sys.exit()
            full_df.columns = columns_list
        else:
            # if args.infer_live:
            #     query = table0.select().where(table0.columns.trading_day == args.latest_date)
            # if args.infer_history:
            query = table0.select().where(and_(table0.columns.spot_date >= start_date,
                                               table0.columns.spot_date <= stop_date,
                                               table0.columns.ticker.in_(args.tickers_list)))
            if (args.month_horizon) :
                query = table0.select().where(and_(table0.columns.spot_date >= start_date,
                                                   table0.columns.spot_date <= stop_date,
                                                   table0.columns.ticker.in_(args.tickers_list),
                                                   table0.columns.month_to_exp.in_(args.month_horizon)))

            ResultProxy = conn.execute(query)
            ResultSet = ResultProxy.fetchall()
            columns_list = ResultProxy.keys()
            full_df = pd.DataFrame(ResultSet)
            if len(full_df) == 0:
                print(f"We don't have data in {table_name}.")
                sys.exit()
            full_df.columns = columns_list

            table0 = db.Table(args.droid_universe_table_name, metadata, autoload=True, autoload_with=conn)

            query = table0.select(table0.columns.is_active == True)

            ResultProxy = conn.execute(query)
            ResultSet = ResultProxy.fetchall()
            columns_list = ResultProxy.keys()
            universe_df = pd.DataFrame(ResultSet)
            universe_df.columns = columns_list

            if args.exec_index is not None:
                full_df = full_df[full_df.ticker.isin(universe_df[universe_df['index'].isin(args.exec_index)].ticker)]
            if args.exec_ticker is not None:
                full_df = full_df[full_df.ticker == args.exec_ticker]
        #
        # ResultProxy = conn.execute(query)
        # ResultSet = ResultProxy.fetchall()
        # columns_list = ResultProxy.keys()
    engine.dispose()

    # full_df = pd.DataFrame(ResultSet)
    #
    # if len(full_df) == 0:
    #     sys.exit("We don't have executive data.")
    # full_df.columns = columns_list

    return full_df


def option_stats_func(args):
    date_min = dt.date.today() - relativedelta(years=args.history_num_years)
    date_max = dt.date.today()
    # This is done to find the last run of live mode.
    if args.debug_mode or args.option_maker_history_full or args.option_maker_history_full_ucdc:
        db_url = global_vars.DB_TEST_URL_READ  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    else:
        db_url = global_vars.DB_DROID_URL_READ  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    table_name = args.executive_production_table_name
    if args.option_maker_history_full:
        table_name = table_name + '_full'
    elif args.option_maker_history_full_ucdc:
        table_name = args.executive_production_ucdc_table_name + '_full'
    elif args.option_maker_history_ucdc or args.option_maker_daily_ucdc or args.option_maker_live_ucdc:
        table_name = args.executive_production_ucdc_table_name
    else:
        table_name = table_name
    if args.modified:
        table_name = table_name + '_mod'
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        query = table0.select().order_by(table0.columns.spot_date.desc()). \
            where(and_(table0.columns.spot_date <= date_max, table0.columns.spot_date >= date_min,
                       table0.columns.ticker.in_(args.tickers_list)))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()
    full_df = pd.DataFrame(ResultSet)
    if len(full_df) == 0:
        print("We don't have executive data.")
        sys.exit()
    full_df.columns = columns_list
    full_df = full_df.drop_duplicates(subset='uid', keep="last")
    # print(sum(full_df.uid.isnull()))
    full_df_not_null = full_df[~full_df.event.isna()]
    full_df_null = full_df[full_df.event.isna()]
    # full_df2 = full_df[full_df.groupby(['ticker', 'month_to_exp', 'option_type'])['spot_date'].transform('max') == full_df['spot_date']]
    full_df_1 = full_df_not_null[
        full_df_not_null.groupby(['ticker', 'month_to_exp', 'option_type'])['spot_date'].transform('max') ==
        full_df_not_null['spot_date']]
    full_df_2 = full_df.groupby(['ticker', 'month_to_exp', 'option_type']).agg({'uid': ['count']})
    full_df_2 = full_df_2.reset_index()
    full_df_2.columns = full_df_2.columns.droplevel(1)
    full_df_2.rename(columns={'uid': 'total_options'}, inplace=True)
    full_df_3 = full_df_null.groupby(['ticker', 'month_to_exp', 'option_type']).agg({'uid': ['count']})
    full_df_3 = full_df_3.reset_index()
    full_df_3.columns = full_df_3.columns.droplevel(1)
    full_df_3.rename(columns={'uid': 'total_no_nulls'}, inplace=True)

    final_df = full_df_1[['ticker', 'month_to_exp', 'option_type', 'spot_date']].merge(full_df_2,on=['ticker', 'month_to_exp','option_type'], how='left')
    final_df = final_df.merge(full_df_3, on=['ticker', 'month_to_exp', 'option_type'], how='left')
    final_df['bot_type'] = table_name.replace('_backtest', '')
    # return full_df
    db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    table_name_final = 'options_stats'
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name_final, metadata, autoload=True, autoload_with=conn)
        query = table0.delete().where(and_(table0.columns.bot_type == table_name.replace('_backtest', '')))
        conn.execute(query)
        final_df.to_sql(con=conn, name=table_name_final, schema='public', if_exists='append', index=False)
    # return full_df