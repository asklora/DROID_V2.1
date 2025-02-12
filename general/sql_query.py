import pandas as pd
from django.conf import settings
from core.djangomodule.general import get_cached_data, set_cache_data
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from general import table_name
from general.sql_process import db_read, alibaba_db_url, DB_URL_ALIBABA_PROD, local_db_url
from general.date_process import (
    backdate_by_day,
    backdate_by_year,
    dateNow,
    date_minus_day,
    droid_start_date,
    str_to_date)
from general.data_process import tuple_data
from general.table_name import (
    get_bot_backtest_table_name,
    get_bot_option_type_table_name,
    get_bot_statistic_table_name,
    get_bot_type_table_name,
    get_data_ibes_monthly_table_name,
    get_data_macro_monthly_table_name,
    get_ai_value_pred_table_name,
    get_data_worldscope_summary_table_name,
    get_industry_group_table_name,
    get_industry_table_name,
    get_latest_bot_update_table_name,
    get_latest_price_table_name,
    get_latest_bot_ranking_table_name,
    get_master_tac_table_name,
    get_orders_position_performance_table_name,
    get_orders_position_table_name,
    get_orders_table_name,
    get_region_table_name,
    get_season_table_name,
    get_universe_client_table_name,
    get_universe_rating_detail_history_table_name,
    get_universe_rating_history_table_name,
    get_user_account_balance_table_name,
    get_user_core_table_name,
    get_user_deposit_history_table_name,
    get_user_profit_history_table_name,
    get_user_transaction_table_name,
    get_vix_table_name,
    get_data_vix_table_name,
    get_currency_table_name,
    get_universe_table_name,
    get_universe_rating_table_name,
    get_fundamental_score_table_name,
    get_master_ohlcvtr_table_name,
    get_report_datapoint_table_name,
    get_universe_consolidated_table_name,
    get_factor_calculation_table_name,
    get_factor_rank_table_name,
    get_ai_score_history_testing_table_name,
    get_factor_current_use_table_name,
    get_historic_fx_rate_table_name,
    get_ingestion_name_source_table_name,
)


from core.djangomodule.general import logging


def read_query(query, table=get_universe_table_name(), cpu_counts=False, alibaba=False, prints=settings.SQLPRINT, local=False):
    """Base function for database query

    Args:
        query (String): Raw SQL query to be performed
        table (String, optional): Database table name. Defaults to get_universe_table_name().
        cpu_counts (bool, optional): Use cpu counts from the running system. Defaults to False.
        dlp (bool, optional): Use DLP database. Defaults to False.
        alibaba (bool, optional): Use Alibaba database. Defaults to False.
        prints (bool, optional): Print detailed data. Defaults to True.

    Returns:
        DataFrame: Resulting data from the query
    """

    if(prints):
        logging.info(f"Get Data From Database on {table} table")
    if alibaba:
        dbcon = alibaba_db_url
    elif local:
        dbcon = local_db_url
    else:
        dbcon = db_read

    if cpu_counts:
        engine = create_engine(
            dbcon, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT"
        )
    else:
        engine = create_engine(dbcon, max_overflow=-1,
                               isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)

    if prints:
        logging.info("Total Data = " + str(len(data)))
    return data


def check_start_end_date(start_date, end_date):
    if type(start_date) == type(None):
        start_date = str_to_date(droid_start_date())
    if type(end_date) == type(None):
        end_date = str_to_date(dateNow())
    return start_date, end_date


def check_ticker_currency_code_query(ticker=None, currency_code=None, active=True):
    query = ""
    if active:
        if type(ticker) != type(None):
            query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
        elif type(currency_code) != type(None):
            query += f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    else:
        if type(ticker) != type(None):
            query += f"ticker in (select ticker from {get_universe_table_name()} where ticker in {tuple_data(ticker)}) "
        elif type(currency_code) != type(None):
            query += f"ticker in (select ticker from {get_universe_table_name()} where currency_code in {tuple_data(currency_code)}) "
    return query


def get_region():
    table_name = get_region_table_name()
    query = f"select * from {table_name}"
    data = read_query(query, table=table_name)
    return data


def get_latest_price(local=False):
    query = f"select mo.* from master_ohlcvtr mo, "
    query += f"(select master_ohlcvtr.ticker, max(master_ohlcvtr.trading_day) max_date "
    # and master_ohlcvtr.trading_day <= '2020-09-14'
    query += f"from master_ohlcvtr where master_ohlcvtr.close is not null "
    query += f"group by master_ohlcvtr.ticker) filter "
    query += f"where mo.ticker=filter.ticker and mo.trading_day=filter.max_date; "
    data = read_query(query, table="latest_price", local=local)
    return data


def get_data_by_table_name(table, local=False):
    query = f"select * from {table}"
    data = read_query(query, table=table, local=local)
    return data


def get_data_by_table_name_with_condition(table, condition, local=False):
    query = f"select * from {table} where {condition}"
    data = read_query(query, table=table, local=local)
    return data


def get_active_currency(currency_code=None):
    query = f"select * from {get_currency_table_name()} where is_active=True"
    if type(currency_code) != type(None):
        query += f" and currency_code in {tuple_data(currency_code)}"
    data = read_query(query, table=get_currency_table_name())
    return data


def get_active_currency_ric_not_null(currency_code=None, active=True):
    query = f"select * from {get_currency_table_name()} where is_active=True and ric is not null "
    if type(currency_code) != type(None):
        query += f" and currency_code in {tuple_data(currency_code)}"
    data = read_query(query, table=get_currency_table_name())
    return data


def get_active_universe_consolidated_by_field(
    isin=False, cusip=False, sedol=False, manual=False, ticker=None
):
    query = (
        f"select * from {get_universe_consolidated_table_name()} where is_active=True "
    )
    if isin:
        query += f"and use_isin=True "
    elif cusip:
        query += f"and use_cusip=True "
    elif sedol:
        query += f"and use_sedol=True "
    elif manual:
        query += f"and use_manual=True and consolidated_ticker is null "

    if type(ticker) != type(None):
        query += f" and (origin_ticker in {tuple_data(ticker)} or consolidated_ticker in {tuple_data(ticker)}) "

    query += " order by origin_ticker "
    data = read_query(query, table=get_universe_consolidated_table_name())
    return data


def get_all_universe(ticker=None, currency_code=None, active=True):
    query = f"select * from {get_universe_table_name()} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"where " + check
    query += f" order by ticker"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_ticker_etf(ticker=None, currency_code=None, active=True):
    table_name = get_currency_table_name()
    query = f"select currency_code, etf_ticker from {table_name} where is_active={active} "
    if(type(currency_code) != type(None)):
        query += f"and currency_code in {tuple_data(currency_code)} "
    elif(type(ticker) != type(None)):
        query += f"and currency_code in (select distinct currency_code from universe where ticker in {tuple_data(ticker)}) "
    data = read_query(query, table=table_name)
    return data


def get_active_universe(ticker=None, currency_code=None, active=True):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active)
    if check != "":
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_active_universe_by_created(created=dateNow()):
    query = f"select * from {get_universe_table_name()} where is_active=True and created='{created}' order by ticker"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_active_position_ticker():
    query = f"select distinct ticker from {get_orders_position_table_name()} where is_live=True"
    data = read_query(query, table=get_orders_position_table_name())
    return data


def get_universe_rating(ticker=None, currency_code=None, active=True):
    table_name = get_universe_rating_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"where " + check
    query += f" order by ticker"
    data = read_query(query, table=table_name)
    return data


def get_active_universe_by_entity_type(ticker=None, currency_code=None, null_entity_type=False, active=True):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    if null_entity_type:
        query += f"and entity_type is null "
    else:
        query += f"and entity_type is not null "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_worldscope_period_end_list(start_date="2000-01-01", end_date=dateNow()):
    table_name = get_data_worldscope_summary_table_name()
    query = f"select distinct period_end from {table_name} where period_end>='{start_date}' and period_end<='{end_date}' order by period_end ASC"
    data = read_query(query, table=table_name)
    return data["period_end"].to_list()


def get_worldscope_summary_latest(quarter_col, year_col):
    ''' get latest record from data_worldscope_summary '''
    table_name = get_data_worldscope_summary_table_name()
    start_date = backdate_by_year(2)
    query = f"select * from {table_name} where period_end>='{start_date}' order by ticker, period_end ASC"
    data = read_query(query, table=table_name)
    num_col = data.select_dtypes(float).columns.to_list()
    # ffill for missing fields
    data[num_col] = data.groupby('ticker')[num_col].ffill()
    for col in quarter_col:
        # add quarter columns for growth ratio calc
        data[col+'_1q'] = data.groupby('ticker')[col].shift(1)
    for col in year_col:
        data[col+'_1y'] = data.groupby('ticker')[col].shift(4)
    data = data.groupby('ticker').last().reset_index()
    return data.drop(columns=['uid', 'worldscope_identifier', 'year', 'fiscal_quarter_end', 'frequency_number', 'period_end', 'report_date'])


def get_ibes_monthly_latest(quarter_col, year_col):
    ''' get latest record from data_ibes_monthly '''
    table_name = get_data_ibes_monthly_table_name()
    start_date = backdate_by_year(2)
    query = f"select * from {table_name} where period_end>='{start_date}' order by ticker, period_end ASC"
    data = read_query(query, table=table_name)
    num_col = data.select_dtypes(float).columns.to_list()
    # ffill for missing fields
    data[num_col] = data.groupby('ticker')[num_col].ffill()
    for col in quarter_col:
        # add quarter columns for growth ratio calc
        data[col + '_1q'] = data.groupby('ticker')[col].shift(1)
    for col in year_col:
        data[col + '_1y'] = data.groupby('ticker')[col].shift(4)
    data = data.groupby('ticker').last().reset_index()
    return data.drop(columns=['uid', 'trading_day', 'period_end'])


def get_missing_field_ticker_list(table_name, field=None, tickers=None, period_end=None):
    '''
    select ticker with missing field & time (suitable for pivot version tables, e.g. data_worldscope_summary,
    data_ibes_monthly, & data_fundamental_score)

    Parameters
    ----------
    table_name :    Str, DB Table Name to search for missing (i.e. Table to be ingested)
    field :         Str, missing field
    ticker :        List, list of ticker where may contain missing field
    period_end :    Date, Timestamp in DB for missing field

    Returns
    -------
    Full dataframe (i.e. all columns) of tickers with missing fields
    '''

    # test if period_end exists in Table
    query1 = f"SELECT ticker FROM {table_name} WHERE period_end='{period_end}'"
    data1 = read_query(query1, table=table_name)

    if len(data1) == 0:
        # if period_end not exists
        query_miss = f"SELECT * FROM {table_name} LIMIT 1"
        data_miss = read_query(query_miss, table=table_name)
        data_miss = pd.DataFrame(index=range(
            len(tickers)), columns=data_miss.columns.to_list())
        data_miss['ticker'] = tickers
        data_miss['period_end'] = period_end
        return data_miss
    else:
        # if period_end exists
        if len(tickers) == 1:
            query = f"SELECT * FROM {table_name} WHERE {field} IS NULL AND period_end='{period_end}' AND ticker='{tickers[0]}'"
        else:
            query = f"SELECT * FROM {table_name} WHERE {field} IS NULL AND period_end='{period_end}' AND ticker IN {tuple(tickers)}"
        data = read_query(query, table=table_name)
        return data


def get_active_universe_by_country_code(country_code, method=True):
    if method:
        query = f"select * from {get_universe_table_name()} where is_active=True and country_code='{country_code}' order by ticker"
    else:
        query = f"select * from {get_universe_table_name()} where is_active=True and country_code!='{country_code}' order by ticker"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_active_universe_by_quandl_symbol(
    null_symbol=False, ticker=None, quandl_symbol=None
):
    if not (null_symbol):
        query = f"select * from {get_universe_table_name()} where is_active=True and quandl_symbol is not null "

        if type(ticker) != type(None):
            query += f"and ticker in {tuple_data(ticker)} "

        elif type(quandl_symbol) != type(None):
            query += f"and quandl_symbol in {tuple_data(quandl_symbol)} "

    else:
        query = f"select * from {get_universe_table_name()} where is_active=True and quandl_symbol is null and currency_code='USD' "
    query += f"order by ticker"
    data = read_query(query, table=get_universe_table_name())
    if len(data) < 1:
        query = f"select ticker, split_part(ticker, '.', 1) as quandl_symbol from {get_universe_table_name()} where is_active=True "
        if type(ticker) != type(None):
            query += f"and ticker in {tuple_data(ticker)} "
        elif type(quandl_symbol) != type(None):
            query += f"and quandl_symbol in {tuple_data(quandl_symbol)} "
        data = read_query(query, table=get_universe_table_name())
    return data


def get_universe_client(client_uid=None):
    query = f"select * from {get_universe_client_table_name()} "
    if type(client_uid) != type(None):
        query += f"where client_uid in {tuple_data(client_uid)} "
    data = read_query(query, table=get_universe_table_name())
    return data


def get_universe_by_region(region_id=None):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    if type(region_id) != type(None):
        query += f"and currency_code in (select currency_code from {get_currency_table_name()} where region_id in {tuple_data(region_id)}) and is_active=true"
    data = read_query(query, table=get_universe_table_name())
    return data


def get_active_universe_company_description_null(ticker=None):
    if type(ticker) != type(None):
        query = f"select * from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)} order by ticker;"
    else:
        query = f"select * from {get_universe_table_name()} where is_active=True and "
        query += f"company_description is null or company_description = 'NA' or company_description = 'N/A' order by ticker;"
    data = read_query(query, table=get_universe_table_name())
    return data


def findnewticker(args):
    query = f"select * FROM {get_report_datapoint_table_name()} where reingested=False"
    data = read_query(query, table=get_report_datapoint_table_name())
    return data


def get_master_ohlcvtr_data(trading_day, local=False):
    query = f"select * from {get_master_ohlcvtr_table_name()} where trading_day>='{trading_day}' and ticker in (select ticker from {get_universe_table_name()} where is_active=True)"
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_master_ohlcvtr_start_date(local=False):
    query = f"SELECT min(trading_day) as start_date FROM {get_master_ohlcvtr_table_name()} WHERE day_status is null"
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    data = data.loc[0, "start_date"]
    data = str(data)
    data = str_to_date(data)
    if data == None:
        data = backdate_by_day(7)
    return data


def get_vix(vix_id=None):
    query = f"select * from {get_vix_table_name()} "
    if type(vix_id) != type(None):
        query += f" where vix_id in {tuple_data(vix_id)}"
    data = read_query(query, table=get_vix_table_name())
    return data


def get_vix_since(vix_id=None, date=None):
    query = f"select * from {get_data_vix_table_name()} WHERE trading_day > '{date}'"
    if type(vix_id) != type(None):
        query += f" where vix_id in {tuple_data(vix_id)}"
    data = read_query(query, table=get_data_vix_table_name())
    return data


def get_fundamentals_score(ticker=None, currency_code=None):
    query = f"select * from {get_fundamental_score_table_name()} "
    if type(ticker) != type(None):
        query += f" where ticker in (select ticker from {get_universe_table_name()} where is_active=True and ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f" where ticker in (select ticker from {get_universe_table_name()} where is_active=True and currency_code in {tuple_data(currency_code)}) "
    else:
        query += f" where ticker in (select ticker from {get_universe_table_name()} where is_active=True) "
    query += f"order by ticker"
    data = read_query(query, table=get_fundamental_score_table_name())
    return data


def get_max_last_ingestion_from_universe(ticker=None, currency_code=None):
    query = f"select max(last_ingestion) as max_date from {get_universe_table_name()} "
    if type(ticker) != type(None):
        query += f" where ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f" where currency_code in {tuple_data(currency_code)} "
    data = read_query(query, table=get_universe_table_name())
    return str(data.loc[0, "max_date"])


def get_last_close_industry_code(ticker=None, currency_code=None, local=False):
    query = f"select mo.ticker, mo.close, mo.currency_code, substring(univ.industry_code from 0 for 7) as industry_code from "
    query += f"{get_master_ohlcvtr_table_name()} mo inner join {get_universe_table_name()} univ on univ.ticker=mo.ticker where univ.is_active=True "
    if type(ticker) != type(None):
        query += f"and univ.ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f"and univ.currency_code in {tuple_data(currency_code)} "
    query += f"and exists( select 1 from (select ticker, max(trading_day) max_date "
    query += f"from {get_master_ohlcvtr_table_name()} where close is not null group by ticker) filter where filter.ticker=mo.ticker "
    query += f"and filter.max_date=mo.trading_day)"
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_pred_mean():
    query = f"select distinct avlpf.ticker, avlpf.pred_mean, avlpf.testing_period::date, avlpf.update_time from ai_value_lgbm_pred_final avlpf, "
    query += f"(select ticker, max(testing_period::date) as max_date from ai_value_lgbm_pred_final group by ticker) filter "
    query += (
        f"where filter.ticker=avlpf.ticker and filter.max_date=avlpf.testing_period;"
    )
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), alibaba=True)
    data = data.sort_values(by=["ticker", "update_time"], ascending=False)
    data = data.drop_duplicates(subset=["ticker"], keep="first")
    data = data.drop(columns=["update_time"])
    return data


def get_ai_value_pred_final():
    query = f"SELECT * FROM {get_ai_value_pred_table_name()}"
    data = read_query(
        query, table=get_ai_value_pred_table_name(), alibaba=True)
    data = pd.pivot_table(data, index=["ticker"], columns=[
                          "y_type"], values="final_pred")
    data.columns = ["ai_value_"+x for x in data.columns]
    return data.reset_index()


def get_specific_tri(trading_day, tri_name="tri", local=False):
    query = f"select price.ticker, price.total_return_index as {tri_name} from {get_master_ohlcvtr_table_name()} price "
    query += f"inner join (select ohlcvtr.ticker, max(ohlcvtr.trading_day) as max_date from {get_master_ohlcvtr_table_name()} ohlcvtr "
    query += f"where ohlcvtr.total_return_index is not null and ohlcvtr.trading_day <= '{trading_day}' group by ohlcvtr.ticker) result "
    query += f"on price.ticker=result.ticker and price.trading_day=result.max_date "
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_specific_tri_avg(trading_day, avg_days=7, tri_name="tri", local=False):
    query = f"SELECT a.ticker, avg(a.tri) as {tri_name} FROM (SELECT ticker, total_return_index as tri FROM master_ohlcvtr "
    query += f"WHERE trading_day < '{trading_day}' AND trading_day >= '{date_minus_day(start_date=trading_day, days=avg_days)}') a GROUP BY ticker"
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_specific_volume_avg(trading_day, avg_days=7, volume_name="volume", local=False):
    query = f"SELECT a.ticker, avg(a.volume) as {volume_name} FROM (SELECT ticker, volume FROM master_ohlcvtr "
    query += f"WHERE trading_day < '{trading_day}' AND trading_day >= '{date_minus_day(start_date=trading_day, days=avg_days)}') a GROUP BY ticker"
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_factor_calculation_formula():
    query = f"SELECT * FROM {get_factor_calculation_table_name()} WHERE is_active"
    data = read_query(
        query, table=get_factor_calculation_table_name(), alibaba=True)
    return data


def get_factor_current_used():
    query = f"SELECT * FROM {get_factor_current_use_table_name()}"
    data1 = read_query(
        query+"_weekly1", table=get_factor_current_use_table_name(), alibaba=True).set_index('index')
    data2 = read_query(
        query+"_monthly1", table=get_factor_current_use_table_name(), alibaba=True).set_index('index')
    data = data1+','+data2
    return data


def get_ingestion_name_source():
    query = f"SELECT * FROM {get_ingestion_name_source_table_name()} WHERE is_active"
    data = read_query(
        query, table=get_ingestion_name_source_table_name(), alibaba=True)
    return data


def get_ingestion_name_macro_source():
    query = f"SELECT * FROM {get_ingestion_name_source_table_name()}_macro"
    data = read_query(
        query, table=get_ingestion_name_source_table_name()+"_macro", alibaba=True)
    return data


def get_factor_rank(score_type):
    query = f"SELECT * FROM {get_factor_rank_table_name()}_{score_type}"
    data = read_query(query, table=get_factor_rank_table_name(), alibaba=True)
    return data


def get_yesterday_close_price(ticker=None, currency_code=None, active=True, local=False):
    query = f"select tac.ticker, tac.trading_day, tac.tri_adj_close as yesterday_close from {get_master_tac_table_name()} tac "
    query += f"inner join (select mo.ticker, max(mo.trading_day) as max_date from {get_master_tac_table_name()} mo where mo.tri_adj_close is not null group by mo.ticker) result "
    query += f"on tac.ticker=result.ticker and tac.trading_day=result.max_date "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"where tac." + check
    data = read_query(
        query, table=get_master_ohlcvtr_table_name(), local=local)
    return data


def get_latest_price_data(ticker=None, currency_code=None, active=True):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_latest_price_capital_change(ticker=None, currency_code=None, active=True):
    table_name = get_latest_price_table_name()
    query = f"select * from {table_name} where capital_change is not null "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
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
    table_name = get_universe_table_name()
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


def get_master_tac_data(
    start_date=None, end_date=None, ticker=None, currency_code=None, active=True, local=False
):
    start_date, end_date = check_start_end_date(start_date, end_date)
    table_name = get_master_tac_table_name()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True, local=local)
    return data


def get_consolidated_universe_data():
    table_name = get_universe_consolidated_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_consolidated_data(column, condition, group_field=None):
    table_name = get_universe_consolidated_table_name()
    query = f"select {column} from {table_name} "
    if type(condition) != type(None):
        query += f"where {condition} "
    if type(group_field) != type(None):
        query += f"group by {group_field} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_ai_score_testing_history(backyear=1):
    """ get ai_score / ai_score2 history from universe rating """
    query = f"SELECT trading_day, h.ticker, currency_code, ai_score_unscaled, ai_score2_unscaled "
    query += f"FROM {get_universe_rating_history_table_name()} h "
    query += f"INNER JOIN {get_universe_table_name()} u ON u.ticker=h.ticker "
    query += f"WHERE trading_day > '{backdate_by_year(backyear)}' "
    query += f"AND ai_score_unscaled IS NOT NULL "
    data = read_query(
        query, table=get_ai_score_history_testing_table_name(), alibaba=False)
    return data


def get_currency_fx_rate_dict():
    """ get ai_score / ai_score2 history from universe rating """
    table_name = get_currency_table_name()
    query = f"select currency_code, last_price, last_date from {table_name} cph "
    query += f"where exists (select 1 from (select filters.currency_code, max(filters.last_date) max_date "
    query += f"from {table_name} as filters group by filters.currency_code) result "
    query += f"where result.currency_code=cph.currency_code and result.max_date=cph.last_date); "
    # query =  f"SELECT currency_code, last_price FROM {get_currency_table_name()} "
    data = read_query(query, table=get_ai_score_history_testing_table_name())
    return data.set_index("currency_code")["last_price"].to_dict()


def get_currency_code_ibes_ws():
    """ get ai_score / ai_score2 history from universe rating """
    query = f"SELECT ticker, currency_code_ibes, currency_code_ws FROM {get_universe_table_name()}"
    data = read_query(query, table=get_universe_table_name(), alibaba=False)
    return data


def get_iso_currency_code_map():
    """ get ai_score / ai_score2 history from universe rating """
    query = f"SELECT currency_code, nation_code FROM iso_currency_code"
    data = read_query(query, table="iso_currency_code", alibaba=True)
    return data.set_index("nation_code")["currency_code"].to_dict()


def get_universe_rating_history(ticker=None, currency_code=None, active=True):
    table_name = get_universe_rating_history_table_name()
    query = f"select * from {table_name} where trading_day=(select max(urh.trading_day) from {table_name} urh) "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_universe_rating_detail_history(ticker=None, currency_code=None, active=True):
    table_name = get_universe_rating_detail_history_table_name()
    query = f"select * from {table_name} where trading_day=(select max(urh.trading_day) from {table_name} urh) "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "and " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_type(condition=None):
    table_name = get_bot_type_table_name()
    query = f"select * from {table_name} "
    if type(condition) != type(None):
        query += f" where {condition}"
    cached_data = get_cached_data(f"{table_name}_{condition}", df=True)
    if not isinstance(cached_data, pd.DataFrame):
        data = read_query(query, table_name, cpu_counts=True)
        set_cache_data(table_name, data.to_dict("records"))
        return data
    return cached_data


def get_latest_bot_update_data(ticker=None, currency_code=None):
    table_name = get_latest_bot_update_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code)
    if check != "":
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_statistic_data(ticker=None, currency_code=None):
    table_name = get_bot_statistic_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code)
    if check != "":
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_backtest(
    start_date=None, end_date=None, ticker=None, currency_code=None, bot_id=None
):
    table_name = get_bot_backtest_table_name()
    start_date, end_date = check_start_end_date(start_date, end_date)
    table_name = get_master_tac_table_name()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    query += f"and trading_day <= '{end_date}' "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code)
    if check != "":
        query += f"and " + check
    if type(bot_id) != type(None):
        query += f"and bot_id='{bot_id}'"
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_option_type(condition=None):
    table_name = get_bot_option_type_table_name()
    query = f"select * from {table_name} "
    if type(condition) != type(None):
        query += f" where {condition}"
    cached_data = get_cached_data(f"{table_name}_{condition}", df=True)
    if not isinstance(cached_data, pd.DataFrame):
        data = read_query(query, table_name, cpu_counts=True)
        set_cache_data(table_name, data.to_dict("records"))
        return data
    return cached_data


def get_latest_ranking(ticker=None, currency_code=None, active=True):
    table_name = get_latest_bot_ranking_table_name()
    query = f"select * from {table_name} rank  "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_latest_ranking_rank_1(ticker=None, currency_code=None, active=True):
    table_name = get_latest_bot_ranking_table_name()
    query = f"select * from {table_name} rank  "
    query += f"where EXISTS (select 1 from {table_name} rank2  "
    query += f"where rank2.ranking = 1 and rank.uid=rank2.uid  "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "and " + check
    query += f"group by rank2.ticker, rank2.time_to_exp); "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_order_performance_by_ticker(ticker=None, trading_day=None):
    table_name = get_orders_position_performance_table_name()
    query = f"select * from {table_name} "
    if type(trading_day) != type(None):
        query += f"where created < '{trading_day}' and position_uid in  "
    else:
        query += f"where position_uid in "
    query += f"(select position_uid from {get_orders_position_table_name()} "
    if type(ticker) != type(None):
        query += f"where ticker='{ticker}' "
    query += f") and order_summary is not null;"
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_data_from_table_name(table_name, ticker=None, currency_code=None, active=True):
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_all_user_core():
    table_name = get_user_core_table_name()
    query = f"select * from {table_name} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_user_core(currency_code=None, user_id=None, field="*", conditions=["is_active=True", "is_superuser=False", "is_test=False"]):
    table_name = get_user_core_table_name()
    query = f"select {field} from {table_name} where {' AND '.join(conditions)} "
    if type(user_id) != type(None):
        if len(user_id) != 0:
            query += f"and id in {tuple_data(user_id)}  "
    elif type(currency_code) != type(None):
        query += f"and id in (select user_id as id from {get_user_account_balance_table_name()} where currency_code in {tuple_data(currency_code)}) "
    query += "order by id "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_user_profit_history(user_id=None, field="*"):
    table_name = get_user_profit_history_table_name()
    query = f"select {field} from {table_name} uph "
    query += f"where uph.trading_day = (select max(filters.trading_day) max_date from user_profit_history filters) "
    if type(user_id) != type(None):
        query += f"and user_id in {tuple_data(user_id)}  "
    query += "order by user_id "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_user_account_balance(currency_code=None, user_id=None, field="*"):
    table_name = get_user_account_balance_table_name()
    query = f"select {field} from {table_name} where amount is not null "
    if type(user_id) != type(None):
        query += f"and user_id in {tuple_data(user_id)}  "
    elif type(currency_code) != type(None):
        query += f"and currency_code in {tuple_data(currency_code)} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_user_deposit(user_id=None):
    table_name = get_user_deposit_history_table_name()
    query = f"select user_id, deposit from {table_name} udh "
    query += f"where exists (select 1 from (select filters.user_id, max(filters.trading_day) max_date from {table_name} as filters "
    query += f"group by filters.user_id) result where result.user_id=udh.user_id and result.max_date::date=udh.trading_day) "
    if type(user_id) != type(None):
        query += f"and user_id in {tuple_data(user_id)} "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_orders_position(user_id=None, ticker=None, currency_code=None, position_uid=None, field="*", active=True):
    table_name = get_orders_position_table_name()
    query = f"select {field} from {table_name} where is_live={active} "
    if type(user_id) != type(None):
        query += f"and user_id in {tuple_data(user_id)} "
    elif type(position_uid) != type(None):
        query += f"where position_uid in {tuple_data(position_uid)} "
    elif type(ticker) != type(None):
        query += f"and ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f"and ticker in (select ticker from universe where currency_code in {tuple_data(currency_code)}) "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_orders_group_by_user_id(user_id=None, ticker=None, currency_code=None, stock=False):
    filter = ""
    if type(user_id) != type(None):
        filter = f"and user_id in {tuple_data(user_id)} "
    elif type(ticker) != type(None):
        filter = f"and ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        filter = f"and ticker in (select ticker from universe where currency_code in {tuple_data(currency_code)}) "

    table_name = get_orders_table_name()
    if(stock):
        query = f"select user_id, sum(stock_pending_amount) as stock_pending_amount from ("
        query += f"select user_id, amount * exchange_rate as stock_pending_amount from {table_name} where status='pending' "
        query += f"and side='buy' and canceled_at is null and bot_id = 'STOCK_stock_0' {filter}) as result2 group by user_id; "
    else:
        query = f"select user_id ,sum(bot_pending_amount) as bot_pending_amount from ("
        query += f"select user_id, ((performance ->> 'current_bot_cash_balance')::double precision + amount) "
        query += f"* exchange_rate  as bot_pending_amount from ( "
        query += f"select user_id, (setup ->> 'performance')::json as performance, amount, exchange_rate "
        query += f"from {table_name} where status='pending' and side='buy' and canceled_at is null  "
        query += f"and bot_id != 'STOCK_stock_0' {filter}) as result) as result2 group by user_id ;"
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_count_orders_position(user_id=None, ticker=None, currency_code=None):
    filter = ""
    if type(user_id) != type(None):
        filter = f"where user_id in {tuple_data(user_id)} "
    elif type(ticker) != type(None):
        filter = f"where ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        filter = f"where ticker in (select ticker from universe where currency_code in {tuple_data(currency_code)}) "

    table_name = get_orders_position_table_name()
    query = f"select user_id, count(position_uid) as total_position from  {table_name} {filter} group by user_id; "
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_orders_position_performance(user_id=None, ticker=None, currency_code=None, position_uid=None, field="*", active=True, latest=False):
    table_name = get_orders_position_performance_table_name()
    query = f"select {field} from {table_name} opp "
    if type(user_id) != type(None):
        query += f"where position_uid in (select position_uid from {get_orders_position_table_name()} where is_live={active} and "
        query += f"user_id in {tuple_data(user_id)}) "
    elif type(position_uid) != type(None):
        query += f"where position_uid in {tuple_data(position_uid)} "
    elif type(ticker) != type(None):
        query += f"where position_uid in (select position_uid from {get_orders_position_table_name()} where is_live={active} and "
        query += f"ticker in {tuple_data(ticker)}) "
    elif type(currency_code) != type(None):
        query += f"where position_uid in (select position_uid from {get_orders_position_table_name()} where is_live={active} and "
        query += f"ticker in (select ticker from universe where currency_code in {tuple_data(currency_code)})) "
    if(latest):
        query += "and exists (select 1 from (select filters.position_uid, max(filters.created) max_date "
        query += "from orders_position_performance as filters group by filters.position_uid) result "
        query += "where result.position_uid=opp.position_uid and result.max_date::date=opp.created::date) "
        query += "order by created DESC;"
    data = read_query(query, table_name, cpu_counts=True)
    return data


def rename_table_columns():
    ''' obsolete '''
    from global_vars import DB_URL_ALIBABA_DEV
    engine = create_engine(
        DB_URL_ALIBABA_DEV, max_overflow=-1, isolation_level="AUTOCOMMIT")

    df = get_ingestion_name_source()
    filter_field = df.loc[df['ibes_monthly'], ['dsws_name', 'our_name']].values
    table_name = 'factor_formula_ratios'

    with engine.connect() as conn:
        df = pd.read_sql(f'SELECT * From {table_name}', conn)
        for col in ['field_num', 'field_denom']:
            x = df[col].to_list()
            for field_dsws, field_rename in filter_field:
                # old_name = 'fn_'+str(int(field_dsws[2:-1]))
                old_name = field_dsws.lower()
                x = [i.replace(old_name, field_rename) if i else i for i in x]
            df[col] = x
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    engine.dispose()


def get_latest_season():
    table_name = get_season_table_name()
    query = f"select * from {table_name} where end_date = (select max(end_date) as end_date from {table_name})"
    data = read_query(query, table_name, cpu_counts=True)
    return data
