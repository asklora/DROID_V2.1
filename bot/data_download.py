from bot.data_process import check_start_end_date
import pandas as pd
from general.sql_query import read_query
from general.data_process import tuple_data
from general.date_process import dateNow, droid_start_date, str_to_date
from general.table_name import (
    get_bot_classic_backtest_table_name, 
    get_bot_data_table_name, get_bot_latest_ranking_table_name, get_bot_option_type_table_name, 
    get_bot_ranking_table_name, 
    get_bot_ucdc_backtest_table_name, 
    get_bot_uno_backtest_table_name, 
    get_calendar_table_name, get_currency_calendar_table_name, 
    get_currency_table_name, get_data_dividend_daily_rates_table_name, 
    get_data_dividend_table_name, 
    get_data_ibes_monthly_table_name, get_data_interest_daily_rates_table_name, 
    get_data_interest_table_name,
    get_data_macro_monthly_table_name, 
    get_data_vix_table_name, 
    get_data_vol_surface_inferred_table_name, 
    get_data_vol_surface_table_name, 
    get_latest_price_table_name, get_latest_vol_table_name, 
    get_universe_table_name,
    get_master_tac_table_name
)

def check_ticker_currency_code_query(ticker=None, currency_code=None):
    query = ""
    if type(ticker) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    return query
    
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
    start_date, end_date = check_start_end_date(start_date, end_date)
    table_name = get_master_tac_table_name()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
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
    query = f"select ticker, max(trading_day) as max_date from {table_name} "
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
    query = f"select du.ticker, du.currency_code, result1.uno_min_date, result2.ohlctr_min_date, result1.uno_max_date, result2.ohlctr_max_date "
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
    query += f"order by du.currency_code;"
    data = read_query(query, table=get_universe_table_name())
    return data

def get_macro_data(start_date, end_date):
    query = f"select trading_day, usinter3, usgbill3, emibor3, jpmshort, emgbond, chgbond, fred_data "
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
    for col in ["usinter3", "usgbill3", "emibor3", "jpmshort", "emgbond", "chgbond", "fred_data"]:
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
    start_date, end_date = check_start_end_date(start_date, end_date)
    table_name = get_calendar_table_name()
    query = f"select * from {table_name} where non_working_day >= '{start_date}' "
    query += f"and non_working_day <= '{end_date}' "
    if type(ticker) != type(None):
        query += f"and currency_code in (select distinct currency_code from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f"and currency_code in (select distinct currency_code from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_vol_surface_data(start_date=None, end_date=None, ticker=None, currency_code=None, infer=True):
    if(infer):
        table_name = get_data_vol_surface_inferred_table_name()
    else:
        table_name = get_data_vol_surface_table_name()
    start_date, end_date = check_start_end_date(start_date, end_date)
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

def get_bot_backtest_data(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, uno=False, ucdc=False, classic=False, mod=False, null_filler=False, not_null=False):
    start_date, end_date = check_start_end_date(start_date, end_date)
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    elif(classic):
        table_name = get_bot_classic_backtest_table_name()
    else:
        table_name = get_bot_uno_backtest_table_name()
    
    if mod:
        table_name += '_mod'
    
    query = f"select * from {table_name} where spot_date >= '{start_date}' and spot_date <= '{end_date}' "
    query += f" "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    if(type(time_to_exp) != type(None)):
        query += f"and time_to_exp in {tuple_data(time_to_exp)} "
    if(null_filler):
        query += f"and event is null "
    if(not_null):
        query += f"and event is not null "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_bot_backtest_data_date_list(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, uno=False, ucdc=False, classic=False, mod=False, null_filler=False):
    start_date, end_date = check_start_end_date(start_date, end_date)
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    else:
        table_name = get_bot_classic_backtest_table_name()
    
    if mod:
        table_name += '_mod'
    
    query = f"select distinct spot_date from {table_name} where spot_date >= '{start_date}' and spot_date <= '{end_date}' "
    query += f" "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    if(type(time_to_exp) != type(None)):
        query += f"and time_to_exp in {tuple_data(time_to_exp)} "
    if(null_filler):
        query += f"and event is null "
    data = read_query(query, table_name, cpu_counts=True)
    return data.spot_date.unique()

def get_bot_ranking_data():
    table_name = get_bot_ranking_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_latest_bot_ranking_data():
    table_name = get_bot_latest_ranking_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_interest_daily(condition=None):
    table_name = get_data_interest_daily_rates_table_name()
    query = f"select * from {table_name} "
    if(type(condition) != type(None)):
        query+= f" where {condition}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_data_dividend_daily_rates(condition=None):
    table_name = get_data_dividend_daily_rates_table_name()
    query = f"select * from {table_name} "
    if(type(condition) != type(None)):
        query+= f" where {condition}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_holiday_by_day_and_currency_code(non_working_day, currency_code):
    table_name = get_currency_calendar_table_name()
    query = f"select * from {table_name} "
    query+= f" where non_working_day='{non_working_day}' and currency_code in {tuple_data(currency_code)}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_latest_vol(condition=None):
    table_name = get_latest_vol_table_name()
    query = f"select * from {table_name} "
    if(type(condition) != type(None)):
        query+= f" where {condition}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_latest_price(condition=None):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} "
    if(type(condition) != type(None)):
        query+= f" where {condition}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

def get_bot_option_type(condition=None):
    table_name = get_bot_option_type_table_name()
    query = f"select * from {table_name} "
    if(type(condition) != type(None)):
        query+= f" where {condition}"
    data = read_query(query, table_name, cpu_counts=True)
    return data