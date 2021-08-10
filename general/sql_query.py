import pandas as pd
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from general.sql_process import db_read, dlp_db_read, alibaba_db_url
from general.date_process import backdate_by_day, dateNow, droid_start_date, str_to_date
from general.data_process import tuple_data
from general.table_name import (
    get_bot_backtest_table_name,
    get_bot_option_type_table_name,
    get_bot_statistic_table_name,
    get_bot_type_table_name,
    get_data_ibes_monthly_table_name,
    get_data_macro_monthly_table_name,
    get_industry_group_table_name,
    get_industry_table_name,
    get_latest_bot_update_table_name,
    get_latest_price_table_name,
    get_latest_bot_ranking_table_name,
    get_master_tac_table_name,
    get_orders_position_performance_table_name,
    get_orders_position_table_name,
    get_region_table_name,
    get_universe_rating_detail_history_table_name,
    get_universe_rating_history_table_name,
    get_vix_table_name,
    get_currency_table_name,
    get_universe_table_name,
    get_universe_rating_table_name,
    get_fundamental_score_table_name,
    get_master_ohlcvtr_table_name,
    get_report_datapoint_table_name,
    get_universe_consolidated_table_name,
)


def read_query(
    query,
    table=get_universe_table_name(),
    cpu_counts=False,
    dlp=False,
    alibaba=False,
    prints=True,
):
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
    if prints:
        print(f"Get Data From Database on {table} table")

    if dlp:
        dbcon = dlp_db_read
    elif alibaba:
        dbcon = alibaba_db_url
    else:
        dbcon = db_read

    if cpu_counts:
        engine = create_engine(
            dbcon, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT"
        )
    else:
        engine = create_engine(dbcon, max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)

    if prints:
        print("Total Data = " + str(len(data)))
    return data


def get_active_universe_droid1(ticker=None, currency_code=None, active=True):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=get_universe_table_name(), dlp=True)
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


def get_latest_price():
    query = f"select mo.* from master_ohlcvtr mo, "
    query += f"(select master_ohlcvtr.ticker, max(master_ohlcvtr.trading_day) max_date "
    # and master_ohlcvtr.trading_day <= '2020-09-14'
    query += f"from master_ohlcvtr where master_ohlcvtr.close is not null "
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


def get_active_universe(ticker=None, currency_code=None, active=True):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"and " + check
    query += f"order by ticker"
    data = read_query(query, table=get_universe_table_name())
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


def get_active_universe_by_entity_type(
    ticker=None, currency_code=None, null_entity_type=False, active=True
):
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


def get_universe_by_region(region_id=None):
    query = f"select * from {get_universe_table_name()} where is_active=True "
    if type(region_id) != type(None):
        query += f"and currency_code in (select currency_code from {get_currency_table_name()} where region_id in {tuple_data(region_id)}) "
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


def get_master_ohlcvtr_data(trading_day):
    query = f"select * from {get_master_ohlcvtr_table_name()} where trading_day>='{trading_day}' and ticker in (select ticker from {get_universe_table_name()} where is_active=True)"
    data = read_query(query, table=get_master_ohlcvtr_table_name())
    return data


def get_master_ohlcvtr_start_date():
    query = f"SELECT min(trading_day) as start_date FROM {get_master_ohlcvtr_table_name()} WHERE day_status is null"
    data = read_query(query, table=get_master_ohlcvtr_table_name())
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


def get_last_close_industry_code(ticker=None, currency_code=None):
    query = f"select mo.ticker, mo.close, mo.currency_code, substring(univ.industry_code from 0 for 7) as industry_code from "
    query += f"{get_master_ohlcvtr_table_name()} mo inner join {get_universe_table_name()} univ on univ.ticker=mo.ticker where univ.is_active=True "
    if type(ticker) != type(None):
        query += f"and univ.ticker in {tuple_data(ticker)} "
    elif type(currency_code) != type(None):
        query += f"and univ.currency_code in {tuple_data(currency_code)} "
    query += f"and exists( select 1 from (select ticker, max(trading_day) max_date "
    query += f"from {get_master_ohlcvtr_table_name()} where close is not null group by ticker) filter where filter.ticker=mo.ticker "
    query += f"and filter.max_date=mo.trading_day)"
    data = read_query(query, table=get_master_ohlcvtr_table_name())
    return data


def get_pred_mean():
    query = f"select distinct avlpf.ticker, avlpf.pred_mean, avlpf.testing_period::date, avlpf.update_time from ai_value_lgbm_pred_final avlpf, "
    query += f"(select ticker, max(testing_period::date) as max_date from ai_value_lgbm_pred_final group by ticker) filter "
    query += (
        f"where filter.ticker=avlpf.ticker and filter.max_date=avlpf.testing_period;"
    )
    data = read_query(query, table=get_master_ohlcvtr_table_name(), alibaba=True)
    data = data.sort_values(by=["ticker", "update_time"], ascending=False)
    data = data.drop_duplicates(subset=["ticker"], keep="first")
    data = data.drop(columns=["update_time"])
    return data


def get_specific_tri(trading_day, tri_name="tri"):
    query = f"select price.ticker, price.total_return_index as {tri_name} from {get_master_ohlcvtr_table_name()} price "
    query += f"inner join (select ohlcvtr.ticker, max(ohlcvtr.trading_day) as max_date from {get_master_ohlcvtr_table_name()} ohlcvtr "
    query += f"where ohlcvtr.total_return_index is not null and ohlcvtr.trading_day <= '{trading_day}' group by ohlcvtr.ticker) result "
    query += f"on price.ticker=result.ticker and price.trading_day=result.max_date "
    data = read_query(query, table=get_master_ohlcvtr_table_name())
    return data


def get_yesterday_close_price(ticker=None, currency_code=None, active=True):
    query = f"select tac.ticker, tac.trading_day, tac.tri_adj_close as yesterday_close from {get_master_tac_table_name()} tac "
    query += f"inner join (select mo.ticker, max(mo.trading_day) as max_date from {get_master_tac_table_name()} mo where mo.tri_adj_close is not null group by mo.ticker) result "
    query += f"on tac.ticker=result.ticker and tac.trading_day=result.max_date "
    check = check_ticker_currency_code_query(
        ticker=ticker, currency_code=currency_code, active=active
    )
    if check != "":
        query += f"where tac." + check
    data = read_query(query, table=get_master_ohlcvtr_table_name())
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
    start_date=None, end_date=None, ticker=None, currency_code=None, active=True
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
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_latest_bot_update_data(ticker=None, currency_code=None):
    table_name = get_latest_bot_update_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if check != "":
        query += "where " + check
    data = read_query(query, table_name, cpu_counts=True)
    return data


def get_bot_statistic_data(ticker=None, currency_code=None):
    table_name = get_bot_statistic_table_name()
    query = f"select * from {table_name} "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
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
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
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
    data = read_query(query, table_name, cpu_counts=True)
    return data


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
