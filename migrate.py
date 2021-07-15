import sys
import time
import pandas as pd
import numpy as np
import sqlalchemy as db
from pangres import upsert
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import DATE
from sqlalchemy.types import Date, BIGINT, TEXT
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

dlp = "postgres://postgres:ml2021#LORA@dlp-prod.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
droid = "postgres://postgres:ml2021#LORA@droid-v1-cluster.cluster-ro-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
droid2 = "postgres://postgres:ml2021#LORA@droid-v2-prod-cluster.cluster-cy4dofwtnffp.ap-east-1.rds.amazonaws.com:5432/postgres"
datenow = datetime.now().date().strftime("%Y-%m-%d")
start_date = datetime.now().date() - relativedelta(days=15)

def uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day", date=True):
    data[trading_day] = data[trading_day].astype(str)
    data[uid]=data[trading_day] + data[ticker]
    data[uid]=data[uid].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
    data[uid]=data[uid].str.strip()
    if(date):
        data[trading_day] = pd.to_datetime(data[trading_day])
    return data

def PgFunctions(db_url, func):
    engine = create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    try:
        print(f'trigger functions {func}')
        cursor.callproc(func)
        cursor.close()
        connection.commit()
    except Exception as e:
        print(f'error: {e}')
    finally:
        connection.close()
        print("COMPLETE")
    engine.dispose()

def upsert_data_to_database(index, index_type, data, table_name, method="update"):
    data = data.drop_duplicates(subset=[index], keep="first", inplace=False)
    print(f'=== Upsert data {table_name} to database ===')
    data = data.set_index(index)
    print(data)
    dtype={
        index:index_type,
    }
    engine = create_engine(droid2, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engine,
            df=data,
            chunksize=10000,
            table_name=table_name,
            if_row_exists=method,
            dtype=dtype)
    print("DATA INSERTED TO " + table_name)
    engine.dispose()
    
def insert_data_to_database(db_url, data, table):
    print('=== Insert Data To Database ===')
    engines = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    try:
        with engines.connect() as engine:
            metadata = db.MetaData()
            data.to_sql(
                table,
                if_exists="append",
                index=False,
                chunksize=20000,
                con=engine
            )
        engines.dispose()
        print("DATA INSERTED TO " + table)
    except Exception as ex:
        print("error: ", ex)

def insert_data_to_database_manually(db_url, field, data, table, index):
    engine_prod = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine_prod.connect() as conn:
        metadata = db.MetaData()
        try:
            query = f"INSERT INTO {table} {field} VALUES {data} ON CONFLICT ({index}) DO NOTHING"
            result = conn.execute(query)
        except Exception as e:
            print(e)
    engine_prod.dispose()

def get_data_from_database_condition(db_url, table, condition):
    print("Get Data From Database")
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {table} where {condition}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return data

def get_ticker_from_new_droid():
    print("Get Data From Database")
    engine = create_engine(droid2, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker from universe where is_active=True"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return tuple(data["ticker"].to_list())

def get_universe_from_new_droid():
    print("Get Data From Database")
    engine = create_engine(droid2, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker from universe where is_active=True"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return data

def get_data_from_database(db_url, table):
    print("Get Data From Database")
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {table}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return data

def market_region():
    table = "market_region"
    column = ["region_id",
        "region_name",
        "ingestion_time"]
    data = get_data_from_database(droid, table)
    data = data.rename(columns={'region_code': 'region_id'})
    data = data[column]
    print(data)
    # insert_data_to_database(droid2, data, "region")
    upsert_data_to_database("region_id", TEXT, data, "region", method="update")
    
    print(f"Get {table} = True")

def country():
    table = "country"
    column = ["country_code", "country_name", "ds_country_code"]
    data = get_data_from_database(droid, table)
    data = data[column]
    exclude = ["IN", "TH", "AU", "ID", "MY"]
    data = data[~data['country_code'].isin(exclude)]
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("country_code", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_dividend():
    table = "dividends"
    column = ["ticker", "ex_dividend_date", "amount"]
    #data = get_data_from_database(droid, table)
    data = get_data_from_database_condition(droid, table, f" ticker in {get_ticker_from_new_droid()} ")
    data = data[column]
    data["ex_dividend_date"] = data["ex_dividend_date"].astype(str)
    data["uid"]=data["ex_dividend_date"] + data["ticker"]
    data["uid"]=data["uid"].str.replace("-", "").str.replace(".", "").str.replace(" ", "")
    data["uid"]=data["uid"].str.strip()
    data["ex_dividend_date"] = pd.to_datetime(data["ex_dividend_date"])
    print(data)
    upsert_data_to_database("uid", TEXT, data, "data_dividend", method="update")
    print(f"Get {table} = True")

def data_universe_detail():
    initial_data = get_data_from_database_condition(droid2, "universe", f" ticker in {get_ticker_from_new_droid()} ")
    initial_data = initial_data.drop(columns=["quandl_symbol", "ticker_name", "ticker_fullname", "lot_size", "entity_type", "industry_code", "wc_industry_code", "company_description", "fiscal_year_end", "icb_code", "worldscope_identifier"])
    print(initial_data)
    table = "droid_universe"
    column = ["ticker", "quandl_symbol", "ticker_name", "ticker_fullname", "lot_size", "entity_type", "industry_code", "wc_industry_code"]
    data = get_data_from_database_condition(droid, table, f" ticker in {get_ticker_from_new_droid()} ")
    data = data.rename(columns={'wc_industry_group': 'wc_industry_code'})
    data = data[column]


    table = "company_descriptions"
    column = ["ticker", "company_description"]
    data2 = get_data_from_database_condition(droid, table, f" ticker in {get_ticker_from_new_droid()} ")
    data2 = data2.rename(columns={'long_des': 'company_description'})
    data2 = data2[column]

    table = "universe"
    column = ["ticker", "fiscal_year_end", "icb_code", "worldscope_identifier"]
    data3 = get_data_from_database_condition(dlp, table, f" ticker in {get_ticker_from_new_droid()} ")
    data3 = data3.rename(columns={'identifier': 'worldscope_identifier'})
    data3 = data3[column]
    initial_data  = initial_data.merge(data, how="left", on="ticker")
    initial_data = initial_data.merge(data2, how="left", on="ticker")
    initial_data = initial_data.merge(data3, how="left", on="ticker")
    print(initial_data)
    upsert_data_to_database("ticker", TEXT, initial_data, table, method="update")
    print(f"Get {table} = True")

def country_calendar():
    table = "calendar"
    column = ["uid", "non_working_day", "description", "country_code"]
    data = get_data_from_database(droid, table)
    data = data[column]
    exclude = ["IN", "TH", "AU", "ID", "MY"]
    data = data[~data['country_code'].isin(exclude)]
    print(data)

    table = "country_calendar"
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def currency_calendar():
    column = ["non_working_day", "description", "country_code"]
    data = get_data_from_database(droid, "calendar")
    data = data[column]
    exclude = ["IN", "TH", "AU", "ID", "MY"]
    data = data[~data['country_code'].isin(exclude)]
    data = data.rename(columns={'country_code': 'currency_code'})
    print(data)
    data["currency_code"] = np.where(data["currency_code"] == "KR", "KRW", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "US", "USD", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "HK", "HKD", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "SG", "SGD", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "CN", "CNY", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "TW", "TWD", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "JP", "JPY", data["currency_code"])
    data["currency_code"] = np.where(data["currency_code"] == "GB", "GBP", data["currency_code"])
    data["currency_code"] = np.where(~data["currency_code"].isin(["KRW", "USD", "HKD", "SGD", "CNY", "TWD", "JPY", "GBP"]), "EUR", data["currency_code"])
    data = uid_maker(data, trading_day="non_working_day", ticker="currency_code")
    print(data)
    table = "currency_calendar"
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    # sys.exit(1)
    print(f"Get {table} = True")

def vix():
    table = "vix"
    column = ["vix_id"]
    data = get_data_from_database(droid, table)
    data = data.rename(columns={'vix_index': 'vix_id'})
    data = data[column]
    print(data)

    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("vix_id", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def vix_data():
    table = "vix_master"
    column = ["uid", "trading_day", "vix_value", "vix_id"]
    data = get_data_from_database(droid, table)
    data = data.rename(columns={'vix_index': 'vix_id'})
    data = data[column]
    print(data)

    table = "data_vix"
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def industry_group():
    table = "industry_group"
    column = ["industry_group_code",
        "industry_group_name",
        "industry_group_img"]
    data = get_data_from_database(droid, table)
    data = data.rename(columns={'industry_group_path': 'industry_group_img'})
    data = data[column]
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("industry_group_code", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def industry():
    table = "industry"
    column = ["industry_code",
        "industry_name",
        "industry_group_code"]
    data = get_data_from_database(droid, table)
    data = data[column]
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("industry_code", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def industry_worldscope():
    table = "industry_worldscope"
    column = ["wc_industry_code", 
        "wc_industry_name"]
    data = get_data_from_database(droid, table)
    data = data[column]
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("wc_industry_code", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def currency_member_type():
    table = "currency_member_type"
    data = pd.DataFrame({"member_type_id":[1, 2, 3],"type_description":["Retrieve Member", "Custom Member", "User Defined Member"]}, index=[0, 1, 2])
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("member_type_id", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def currency():
    column = ["currency_code",
        "currency_name",
        "ric",
        "is_decimal",
        "amount",
        "last_price",
        "last_date",
        "utc_timezone_location",
        "ingestion_time",
        "utc_offset",
        "market_close_time",
        "market_open_time",
        "close_ingestion_offset",
        "intraday_offset_close",
        "intraday_offset_open",
        "region_id",
        "vix_id"]
    table = "currency"
    indices = get_data_from_database(droid, "indices")
    currency = get_data_from_database(droid, "currency")
    data = currency.merge(indices, how="left", on=["currency_code"])
    data = data.rename(columns={'vix_index': 'vix_id', 'region_code': 'region_id'})
    print(data)
    data = data[column]
    data["is_active"] = True
    data = data.drop_duplicates(subset=["currency_code"], keep="first", inplace=False)
    data = data.dropna(subset=["vix_id"])
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("currency_code", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def universe_consolidated():
    column = ["consolidated_ticker",
        "origin_ticker",
        "isin",
        "is_consolidated",
        "still_live",
        "created",
        "updated",
        "has_isin"]

    table = "universe_consolidated"
    SPX = get_data_from_database_condition(dlp, "universe", "is_active=True and index_id='0#.SPX'")
    consolidated_spx = get_data_from_database(dlp, "consolidated_ticker")
    SPX = SPX.merge(consolidated_spx, how="left", left_on=["ticker"], right_on=["consolidated_ticker"])
    SPX = SPX.drop(columns=["consolidated_ticker"])
    SPX = SPX.rename(columns={'primary_exchange_ticker': 'origin_ticker', 
    'ticker': 'consolidated_ticker',
    'is_active': 'still_live',
    'last_updated': 'updated'})
    SPX = SPX[["consolidated_ticker", "origin_ticker", "isin", "still_live", "updated"]]
    SPX["is_consolidated"] = True
    SPX["has_isin"] = True
    print(SPX)
    
    retrieve_ticker = get_data_from_database_condition(dlp, "universe", "is_active=True and index_id not in ('0#.SPX')")
    retrieve_ticker = retrieve_ticker.rename(columns={'ticker': 'consolidated_ticker',
        'is_active': 'still_live',
        'last_updated': 'updated'})
    retrieve_ticker = retrieve_ticker[["consolidated_ticker", "still_live", "updated"]]
    retrieve_ticker["origin_ticker"] = retrieve_ticker["consolidated_ticker"]
    retrieve_ticker["is_consolidated"] = False
    retrieve_ticker["isin"] = "0"
    retrieve_ticker["has_isin"] = False
    print(retrieve_ticker)

    data = SPX.append(retrieve_ticker)
    data["created"] = data["updated"]
    data = data[column]
    data = data.drop_duplicates(subset=["consolidated_ticker"], keep="first", inplace=False)
    print(data)
    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("consolidated_ticker", TEXT, data, table, method="update")
    print(f"Get {table} = True")
    sys.exit(1)

def universe_excluded():
    column = ["ticker",
        "exclude_dss",
        "exclude_dsws"]
    index = "ticker"
    table = "universe_excluded"
    excluded_tickers = get_data_from_database(dlp, "excluded_tickers")
    excluded_tickers = excluded_tickers.rename(columns={'excluded_ticker': 'ticker'})
    excluded_tickers = excluded_tickers["ticker"]
    print(excluded_tickers)

    for ticker in excluded_tickers:
        insert_data_to_database_manually(droid2, tuple(column), (ticker, True, False), table, index)

    dsws_excluded_ticker = get_data_from_database(dlp, "dsws_excluded_ticker")
    dsws_excluded_ticker = dsws_excluded_ticker["ticker"]
    print(dsws_excluded_ticker)

    for ticker in dsws_excluded_ticker:
        insert_data_to_database_manually(droid2, tuple(column), (ticker, False, True), table, index)

    print(f"Get {table} = True")

def universe_rating():
    column = ["ticker",
        "fundamentals_quality",
        "fundamentals_value",
        "dlp_1m",
        "dlp_3m",
        "wts_rating",
        "wts_rating2"]
    index = "ticker"
    table = "universe_rating"
    
    data = get_data_from_database_condition(droid, "droid_universe", f"is_active=True and ticker in {get_ticker_from_new_droid()} ")
    data = data[column]
    print(data)

    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("ticker", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_dss():
    column = ["dss_id",
        "trading_day",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ticker"]
    table = "data_dss"
    data = get_data_from_database_condition(dlp, "dss_data", f"ticker in {get_ticker_from_new_droid()} and trading_day >='{start_date}'")
    data = data.rename(columns={'uid': 'dss_id'})
    data = data[column]
    print(data)
    upsert_data_to_database("dss_id", TEXT, data, table, method="update")
    #insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def data_dsws():
    column = ["dsws_id",
        "trading_day",
        "total_return_index",
        "ticker"]
    table = "data_dsws"
    data = get_data_from_database_condition(dlp, "dsws_data", f"ticker in {get_ticker_from_new_droid()} and trading_day >='{start_date}'")
    data = data.rename(columns={'uid': 'dsws_id'})
    data = data[column]
    print(data)

    upsert_data_to_database("dsws_id", TEXT, data, table, method="update")
    #insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def data_quandl():
    column = ["uid",
        "trading_day",
        "stockpx",
        "iv30",
        "iv60",
        "iv90",
        "m1atmiv",
        "m1dtex",
        "m2atmiv",
        "m2dtex",
        "m3atmiv",
        "m3dtex",
        "m4atmiv",
        "m4dtex",
        "slope",
        "deriv",
        "slope_inf",
        "deriv_inf",
        "ticker"]
    table = "data_quandl"
    data = get_data_from_database_condition(droid, "droid_quandl_orats_vols", f"ticker in {get_ticker_from_new_droid()}  and trading_day >='{start_date}'")
    data = data[column]
    print(data)

    # insert_data_to_database(droid2, data, table)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_fundamental_score():
    column = ["ticker",
        "eps",
        "bps",
        "ttm_rev",
        "mkt_cap",
        "ttm_ebitda",
        "ttm_capex",
        "net_debt",
        "roe",
        "cfps",
        "peg",
        "bps1fd12",
        "ebd1fd12",
        "evt1fd12",
        "eps1fd12",
        "sal1fd12",
        "cap1fd12"]
    table = "data_fundamental_score"
    data = get_data_from_database_condition(droid, "fundamentals_score", f"ticker in {get_ticker_from_new_droid()}")
    data = data[column]
    print(data)
    upsert_data_to_database("ticker", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_ibes():
    column = ["uid",
    "period_end",
    "epsi1md",
    "i0eps",
    "cap1fd12",
    "ebd1fd12",
    "eps1fd12",
    "eps1tr12",
    "ticker"]
    table = "data_ibes"
    data = get_data_from_database_condition(dlp, "ibes_data", f" ticker in {get_ticker_from_new_droid()} ")
    data = uid_maker(data, uid="uid", ticker="ticker", trading_day="period_end", date=True)
    data = data[column]
    print(data)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_ibes_monthly():
    column = ["uid",
    "trading_day",
    "period_end",
    "eps1fd12",
    "eps1tr12",
    "cap1fd12",
    "epsi1md",
    "i0eps",
    "ebd1fd12",
    "ticker"]
    table = "data_ibes_monthly"
    data = get_data_from_database_condition(dlp, "ibes_data_monthly", f" ticker in {get_ticker_from_new_droid()} ")
    data = uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day", date=True)
    data = data[column]
    print(data)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_macro():
    column = ["period_end",
        "chgdp",
        "jpgdp",
        "usgdp",
        "emgdp",
        "emibor3",
        "emgbond",
        "chgbond",
        "usinter3",
        "usgbill3",
        "jpmshort",
        "fred_data"]
    table = "data_macro"
    data = get_data_from_database(dlp, "macro_data")
    data = data.rename(columns={"usinter3_esa" : "usinter3",
        "usgbill3_esa" : "usgbill3",
        "EMIBOR3._ESA" : "emibor3",
        "jpmshort_esa" : "jpmshort",
        "EMGBOND._ESA" : "emgbond",
        "CHGBOND._ESA" : "chgbond",
        "CHGDP...C_ESA" : "chgdp",
        "JPGDP...D_ESA" : "jpgdp",
        "USGDP...D_ESA" : "usgdp",
        "EMGDP...D_ESA" : "emgdp"})
    data = data[column]
    print(data)
    upsert_data_to_database("period_end", DATE, data, table, method="update")
    print(f"Get {table} = True")

def data_macro_monthly():
    column = ["trading_day", 
        "period_end",
        "chgdp",
        "jpgdp",
        "usgdp",
        "emgdp",
        "emibor3",
        "emgbond",
        "chgbond",
        "usinter3",
        "usgbill3",
        "jpmshort",
        "fred_data"]
    table = "data_macro_monthly"
    data = get_data_from_database(dlp, "macro_data_monthly")
    data = data.rename(columns={"usinter3_esa" : "usinter3",
        "usgbill3_esa" : "usgbill3",
        "EMIBOR3._ESA" : "emibor3",
        "jpmshort_esa" : "jpmshort",
        "EMGBOND._ESA" : "emgbond",
        "CHGBOND._ESA" : "chgbond",
        "CHGDP...C_ESA" : "chgdp",
        "JPGDP...D_ESA" : "jpgdp",
        "USGDP...D_ESA" : "usgdp",
        "EMGDP...D_ESA" : "emgdp"})
    data = data[column]
    print(data)
    upsert_data_to_database("trading_day", DATE, data, table, method="update")
    print(f"Get {table} = True")

def data_fred():
    column = ["trading_day", "data"]
    table = "data_fred"
    data = get_data_from_database(dlp, "fred_data")
    data = data[column]
    print(data)
    upsert_data_to_database("trading_day", DATE, data, table, method="update")
    # insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")


def data_vol_surface_inferred():
    column = ["uid",
    "trading_day",
    "atm_volatility_spot",
    "atm_volatility_one_year",
    "atm_volatility_infinity",
    "slope",
    "deriv",
    "slope_inf",
    "deriv_inf",
    "ticker"]
    table = "data_vol_surface_inferred"
    data = get_data_from_database_condition(droid, "droid_vol_surface_parameters_inferred", f" ticker in {get_ticker_from_new_droid()} ")
    data = uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day", date=True)
    data = data[column]
    print(data)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def latest_price():
    column = ["ticker", "classic_vol", "open", "high", "low", "close", 
        "intraday_date", "intraday_ask", "intraday_bid", "latest_price_change", 
        "intraday_time", "last_date", "capital_change"]
    table = "latest_price"
    data = get_data_from_database_condition(droid, "latest_price_updates", f" ticker in {get_ticker_from_new_droid()} ")
    data = data[column]
    print(data)
    upsert_data_to_database("ticker", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_interest():
    column = ["ticker_interest", "rate", "raw_data", "days_to_maturity", "ingestion_field", "maturity", "updated", "currency_code"]
    table = "data_interest"
    data = get_data_from_database(droid, "interest_rate")
    data = data.rename(columns={"ticker" : "ticker_interest", "currency" : "currency_code", "update_date" : "updated"})
    data = data[column]
    print(data)
    upsert_data_to_database("ticker_interest", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def data_worldscope_summary():
    column = ["uid", "worldscope_identifier", "year", "frequency_number", "fiscal_quarter_end", "period_end", "report_date", 
        "fn_2001", "fn_2101", "fn_2201", "fn_2501", "fn_3101", "fn_5085", "fn_8001", "fn_18100", "fn_18158", "fn_18199", "fn_18262", 
        "fn_18263", "fn_18264", "fn_18265", "fn_18266", "fn_18267", "fn_18269", "fn_18304", "fn_18308", "fn_18309", "fn_18310", "fn_18311", 
        "fn_18312", "fn_18313", "fn_3501", "fn_3255", "fn_18271", "fn_2999", "fn_5192", "ticker"]
    table = "data_worldscope_summary"
    data = get_data_from_database_condition(dlp, "worldscope_quarter_summary", f" period_end>='{start_date}'::date - interval '3 months' and ticker in {get_ticker_from_new_droid()} ")
    data = data.rename(columns={"identifier" : "worldscope_identifier"})
    data = uid_maker(data, uid="uid", ticker="ticker", trading_day="period_end", date=True)
    data = data[column]
    print(data)
    upsert_data_to_database("uid", TEXT, data, table, method="update")
    print(f"Get {table} = True")

def ai_value_lgbm_pred():
    table = "ai_value_lgbm_pred"
    data = get_data_from_database(dlp, table)
    print(data)
    insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def ai_value_lgbm_pred_final():
    table = "ai_value_lgbm_pred_final"
    data = get_data_from_database(dlp, table)
    print(data)
    insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def ai_value_lgbm_pred_final_eps():
    table = "ai_value_lgbm_pred_final_eps"
    data = get_data_from_database(dlp, table)
    print(data)
    insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def ai_value_lgbm_score():
    table = "ai_value_lgbm_score"
    data = get_data_from_database(dlp, table)
    data = data.rename(columns={"verbose" : "lgbm_verbose"})
    print(data)
    insert_data_to_database(droid2, data, table)
    print(f"Get {table} = True")

def daily_migrations():
    data_dss()
    data_dsws()

def weekly_migrations():
    # currency()
    # universe_rating()
    # data_universe_detail()
    # vix_data()
    # data_dss()
    # data_dsws()
    # data_quandl()
    # latest_price()
    # data_interest()
    # data_ibes()
    data_ibes_monthly()
    # data_fred()
    # data_macro()
    data_macro_monthly()
    # data_dividend()
    # data_fundamental_score()
    # data_worldscope_summary()

def new_ticker_migration():
    data_vol_surface_inferred()
    
if __name__ == '__main__':
    print("Do Process")
    # daily_migrations()
    # dlp_ticker = get_data_from_database_condition(dlp, "universe", "is_active = True")
    # print(dlp_ticker)
    # droid_ticker = get_ticker_from_new_droid()
    # print(droid_ticker)
    # dlp_ticker = dlp_ticker.loc[dlp_ticker["ticker"].isin(droid_ticker["ticker"].to_list())]
    # print(dlp_ticker)
    # droid_ticker = droid_ticker.loc[~droid_ticker["ticker"].isin(dlp_ticker["ticker"].to_list())]
    # print(droid_ticker["ticker"].to_list())
    # print(result["ticker"].to_list())
    # data_worldscope_summary()
    # ai_value_lgbm_pred()
    # ai_value_lgbm_pred_final()
    # ai_value_lgbm_pred_final_eps()
    # ai_value_lgbm_score()
    # data = "0#.SPX"
    # data = data.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    # data = data.split(",")
    # data = tuple(data)
    # print(data)
    #market_region()
    #country()
    #country_calendar()
    # currency_calendar()
    # vix()
    
    # industry_group()
    # industry()
    # industry_worldscope()
    #currency_member_type()
    # currency()
    #universe_consolidated()
    #PgFunctions(droid2, "universe_update")
    #universe_excluded()
    # universe_rating()
    # data_dividend()
    
    # data_universe_detail()
    # PgFunctions(droid2, "data_vol_surface_update")
    # data_fundamental_score()
    
    # top_stock_models()
    # top_stock_models_stock()
    # data_ibes()
    # data_ibes_monthly()
    # data_fred()
    # data_macro()
    # data_macro_monthly()
    # data_vol_surface_inferred()
    # currency_calendar
    
    # data_dividend_daily_rates
    # data_interest
    # data_interest_daily_rates
    # data_split
    # data_vol_surface_inferred
    # master_multiple
    # master_ohlcvtr
    # master_tac
    # report_datapoint
    # report_new_datapoint
    print("DONE")