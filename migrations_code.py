import pandas as pd
from pangres import upsert
from sqlalchemy import create_engine
from multiprocessing import cpu_count
from sqlalchemy.types import DATE, BIGINT, TEXT, INTEGER, BOOLEAN

db_dst = "postgres://backtest_tmp:TU1HB5c5rTvuRr2u@pgm-3nse9b275d7vr3u18o.pg.rds.aliyuncs.com:1921/backtest_tmp"
db_src = "postgres://postgres:ml2021#LORA@droid-v2-production-cluster.cluster-ro-cy4dofwtnffp.ap-east-1.rds.amazonaws.com:5432/postgres"

def read_query(query):
    engine = create_engine(db_src, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
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
        engine = create_engine(db_dst, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
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
    master_ohlcvtr()
    master_tac()
    # region()
    # universe()
    # universe_rating()
    # vix()