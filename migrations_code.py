from general.sql_query import get_active_universe
from general.date_process import backdate_by_month, dateNow, str_to_date
import pandas as pd
from pangres import upsert
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from sqlalchemy.types import DATE, BIGINT, TEXT, INTEGER, BOOLEAN

db_backtest = "postgres://backtest_tmp:TU1HB5c5rTvuRr2u@pgm-3nse9b275d7vr3u18o.pg.rds.aliyuncs.com:1921/backtest_tmp"
db_local = "postgres:AskLORAv2@localhost:5433/postgres"
db_droid = "postgres://postgres:ml2021#LORA@droid-v2-production-cluster.cluster-ro-cy4dofwtnffp.ap-east-1.rds.amazonaws.com:5432/postgres"
db_quant = "postgresql://asklora:AskLORAv2@pgm-3nscoa6v8c876g5xlo.pg.rds.aliyuncs.com:1924/postgres"

def read_query(query, quant=False, local=False, backtest=False):
    if(quant):
        engine = create_engine(db_quant, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    elif(local):
        engine = create_engine(db_local, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    elif(backtest):
        engine = create_engine(db_backtest, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(db_droid, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn, chunksize=20000)
        data = pd.concat(data, axis=0, ignore_index=False)
    engine.dispose()
    data = pd.DataFrame(data)
    print(data)
    return data

def upsert_to_database(data, table, primary_key, how="update", Text=False, Date=False, Int=False, Bigint=False, Bool=False):
    print(data)
    try:
        engine = create_engine(db_backtest, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
        if how in ["replace", "append"]:
            with engine.connect() as conn:
                extra = {'con': conn, 'index': False, 'if_exists': how, 'method': 'multi', 'chunksize': 20000}
                data.to_sql(table, **extra)
        else:
            print(f"=== Upsert Data to Database on Table {table} ===")
            data = data.drop_duplicates(subset=[primary_key], keep="first", inplace=False)
            data = data.dropna(subset=[primary_key])
            data = data.set_index(primary_key)
            if(Text):
                data_type={primary_key:TEXT}
            elif(Date):
                data_type={primary_key:DATE}
            elif(Int):
                data_type={primary_key:INTEGER}
            elif(Bigint):
                data_type={primary_key:BIGINT}
            elif(Bool):
                data_type={primary_key:BOOLEAN}
            else:
                data_type={primary_key:TEXT}

            upsert(engine=engine,
                df=data,
                table_name=table,
                if_row_exists=how,
                chunksize=20000,
                dtype=data_type,
                add_new_columns=True)
            print(f"DATA UPSERT TO {table}")
            engine.dispose()
    except Exception as e:
        print(f"===  ERROR IN UPSERT DB === Error : {e}")

def region():
    table_name = "region"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "region_id", how="update", Text=True)

def vix():
    table_name = "vix"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "vix_id", how="update", Text=True)

def currency():
    table_name = "currency"
    query = f"select * from {table_name}"
    query += f" where currency_code in ('USD', 'CNY', 'HKD')"
    data = read_query(query)
    upsert_to_database(data, table_name, "currency_code", how="update", Text=True)

def currency_calendar():
    table_name = "currency_calendar"
    query = f"select * from {table_name}"
    query += f" where currency_code in ('USD', 'CNY', 'HKD')"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def industry_group():
    table_name = "industry_group"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "industry_group_code", how="update", Text=True)

def industry():
    table_name = "industry"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "industry_code", how="update", Text=True)

def industry_worldscope():
    table_name = "industry_worldscope"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "wc_industry_code", how="update", Text=True)

def universe():
    table_name = "universe"
    query = f"select * from {table_name}"
    query += f" where is_active=True and currency_code in ('USD', 'CNY', 'HKD')"
    data = read_query(query)
    upsert_to_database(data, table_name, "ticker", how="update", Text=True)

def data_dividend():
    table_name = "data_dividend"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def master_ohlcvtr():
    table_name = "master_ohlcvtr"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    query += f" and trading_day >= '2020-02-10'"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def master_tac():
    table_name = "master_tac"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    query += f" and trading_day >= '2020-02-10'"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def data_interest():
    table_name = "data_interest"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "ticker_interest", how="update", Text=True)

def data_vix():
    table_name = "data_vix"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def universe_rating():
    table_name = "universe_rating"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    data = read_query(query)
    upsert_to_database(data, table_name, "ticker", how="update", Text=True)

def bot_option_type():
    table_name = "bot_option_type"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "bot_id", how="update", Text=True)

def bot_type():
    table_name = "bot_type"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "bot_type", how="update", Text=True)

def data_dividend_daily_rates():
    table_name = "data_dividend_daily_rates"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def data_ibes_monthly():
    table_name = "data_ibes_monthly"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def data_interest_daily_rates():
    table_name = "data_interest_daily_rates"
    query = f"select * from {table_name}"
    query += f" where currency_code in ('USD', 'CNY', 'HKD')"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def data_macro_monthly():
    table_name = "data_macro_monthly"
    query = f"select * from {table_name}"
    data = read_query(query)
    upsert_to_database(data, table_name, "trading_day", how="update", Text=True)

def data_vol_surface():
    table_name = "data_vol_surface"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    query += f" and trading_day >= '2021-02-10'"
    data = read_query(query)
    upsert_to_database(data, table_name, "uid", how="update", Text=True)

def latest_price():
    table_name = "latest_price"
    query = f"select * from {table_name}"
    query += f" where ticker in (select ticker from universe where is_active=True and currency_code in ('USD', 'CNY', 'HKD'))"
    data = read_query(query)
    upsert_to_database(data, table_name, "ticker", how="update", Text=True)

def get_data_tri(ticker=None, start_date=None):
    table_name = "data_tri"
    conditions = ["True"]
    if start_date:
        conditions.append(f"trading_day >= '{start_date}'")
    if ticker:
        conditions.append(f"ticker in {tuple(ticker)}")
    query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)}"
    data = read_query(query.replace(",)", ")"), quant=True)
    return data

def get_data_ohlcv(ticker=None, start_date=None):
    table_name = "data_ohlcv"
    conditions = ["True"]
    if start_date:
        conditions.append(f"trading_day >= '{start_date}'")
    if ticker:
        conditions.append(f"ticker in {tuple(ticker)}")
    query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)}"
    data = read_query(query.replace(",)", ")"), quant=True)
    return data

def get_master_ohlcvtr_data(trading_day):
    query = f"select * from master_ohlcvtr where trading_day>='{trading_day}' and "
    query += f"ticker in (select ticker from universe where is_active=True) "
    data = read_query(query, backtest=True)
    return data

def get_master_ohlcvtr_data_quant():
    universe = get_active_universe()[["ticker", "currency_code"]]
    ticker = universe["ticker"].to_list()
    start_date = backdate_by_month(7)
    end_date = dateNow()
    tri = get_data_tri(ticker, start_date)
    ohlcv = get_data_ohlcv(ticker, start_date).drop(columns=["Error"])
    master_data = pd.merge(tri, ohlcv, on=["trading_day", "ticker", "uid"], how="outer")
    master_data = master_data.merge(universe, on=["ticker"], how="inner")
    return master_data

def master_ohlctr_update():
    from master_ohlcvtr import FillMissingDay, CountDatapoint
    universe = get_active_universe()[["ticker", "currency_code"]]
    ticker = universe["ticker"].to_list()
    master_ohlcvtr_data = get_master_ohlcvtr_data_quant()
    start_date = backdate_by_month(7)
    end_date = dateNow()
    print(f"Calculation Start From {start_date}")
    print("Filling All Missing Days")
    master_ohlcvtr_data = FillMissingDay(master_ohlcvtr_data, start_date, dateNow())
    print("Calculate Datapoint & Day_status")
    master_ohlcvtr_data = CountDatapoint(master_ohlcvtr_data)
    print(master_ohlcvtr_data)
    upsert_to_database(master_ohlcvtr_data, "master_ohlcvtr", "uid", how="update", Text=True)

# def master_ohlctr_update():
#     from ingestion.master_ohlcvtr import FillMissingDay, CountDatapoint, FillDayStatus
#     print("Get Start Date")
#     start_date = str_to_date(backdate_by_month(7))
#     print(f"Calculation Start From {start_date}")
#     print("Getting OHLCVTR Data")
#     master_ohlcvtr_data = get_master_ohlcvtr_data_quant()
#     print("OHLCTR Done")
#     print("Filling All Missing Days")
#     master_ohlcvtr_data = FillMissingDay(master_ohlcvtr_data, start_date, dateNow())
#     print("Calculate Datapoint")
#     master_ohlcvtr_data, new_ticker = CountDatapoint(master_ohlcvtr_data)
#     print("Fill Day Status")
#     master_ohlcvtr_data = FillDayStatus(master_ohlcvtr_data)
#     master_ohlcvtr_data = master_ohlcvtr_data.drop(columns=["point", "fulldatapoint"])
#     print(master_ohlcvtr_data)
#     if(len(master_ohlcvtr_data) > 0):
#         upsert_to_database(master_ohlcvtr_data, "master_ohlcvtr", "uid", how="update", Text=True)
#         del master_ohlcvtr_data

def master_tac_update():
    from ingestion.master_tac import DeleteHolidayStatus, ForwardBackwardFillNull, rolling_apply, roundPrice, get_rsi, get_stochf
    print("Getting OHLCVTR Data")
    start_date = backdate_by_month(7)
    data = get_master_ohlcvtr_data(start_date)
    print("OHLCTR Done")
    #data = data.rename(columns={"ticker_id" : "ticker", "currency_code_id" : "currency_code"})
    print("Delete Holiday Status")
    data = DeleteHolidayStatus(data)
    data = data.drop(columns=["datapoint_per_day", "datapoint"])
    print("Fill Null Data Forward & Backward")
    data = ForwardBackwardFillNull(data, ["open", "high", "low", "close", "total_return_index"])
    print("Calculate TAC")
    result = data.copy()
    result = result.drop(columns=["day_status"])
    data = data[["uid", "ticker", "trading_day", "volume", "currency_code", "day_status"]]
    result =  result.rename(columns={"close":"tri_adj_close",
        "low":"tri_adj_low",
        "high":"tri_adj_high",
        "open":"tri_adj_open"})
    result = result.sort_values(by="trading_day", ascending=False)
    result["tac"] =  result.groupby("ticker")["total_return_index"].shift(-1) / result.total_return_index
    result["tri_adj_close"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_close")["tri_adj_close"]
    result["tri_adj_low"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_low")["tri_adj_low"]
    result["tri_adj_high"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_high")["tri_adj_high"]
    result["tri_adj_open"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_open")["tri_adj_open"]
    print("Round Price")
    result["tri_adj_close"] = result["tri_adj_close"].apply(roundPrice)
    result["tri_adj_low"] = result["tri_adj_low"].apply(roundPrice)
    result["tri_adj_high"] = result["tri_adj_high"].apply(roundPrice)
    result["tri_adj_open"] = result["tri_adj_open"].apply(roundPrice)
    result = result.sort_values(by="trading_day", ascending=True)
    result = result.drop(columns=["trading_day", "ticker", "tac", "volume", "currency_code"])
    result = result.merge(data, on=["uid"], how="inner")
    print("Calculate RSI")
    result["rsi"] = result.groupby("ticker")["tri_adj_close"].transform(get_rsi)
    print("Calculate STOCHATIC")
    result = result.groupby("ticker").apply(get_stochf)
    print("Calculate TAC Done")
    upsert_to_database(result, "master_tac", "uid", how="update", Text=True)
    del result

if __name__ == "__main__":
    print("Start Process")
    # bot_classic_backtest()
    # bot_ucdc_backtest()
    # bot_uno_backtest()
    # bot_data()
    # bot_ranking()
    # bot_statistic()
    # data_vol_surface_inferred()

    # data_vol_surface()
    # bot_option_type()
    # bot_type()
    # data_dividend_daily_rates()
    # data_ibes_monthly()
    # data_interest_daily_rates()
    # data_macro_monthly()
    # latest_price()
    # currency()
    # currency_calendar()
    # data_dividend()
    # data_interest()
    # data_vix()
    # industry()
    # industry_group()
    # industry_worldscope()
    # master_ohlcvtr()
    # master_tac()
    # region()
    # currency()
    # vix()
    universe()
    universe_rating()
    data_vol_surface()
    master_ohlctr_update()
    master_tac_update()