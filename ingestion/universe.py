import os
import numpy as np
import pandas as pd
from general.date_process import (
    datetimeNow, 
    droid_start_date, 
    backdate_by_day, 
    dateNow)
from general.slack import report_to_slack
from general.data_process import uid_maker, remove_null
from general.sql_query import (
    get_vix, 
    get_currency_symbol,
    get_active_universe, 
    get_active_universe_company_description_null)
from general.sql_output import upsert_data_to_database, fill_null_company_desc_with_ticker_name
from datasource.dsws import get_data_static_from_dsws, get_data_history_from_dsws
from datasource.dss import get_data_from_dss
from general.table_name import (
    get_currency_table_name,
    get_universe_table_name, 
    get_industry_table_name,
    get_country_table_name,
    get_data_dividend_table_name,
    get_industry_worldscope_table_name,
    get_vix_table_name)

def update_ticker_name():
    print("{} : === Ticker Name Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe[["ticker"]]
    filter_field = ["WC06003", "NAME"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe, identifier, filter_field, use_ticker=True, split_number=40)
    result = result.rename(columns={"WC06003": "ticker_name", "NAME" : "ticker_fullname"})
    result["ticker_name"]=result["ticker_name"].str.replace("'", "")
    result["ticker_name"]=result["ticker_name"].str.strip()
    result["ticker_fullname"]=result["ticker_fullname"].str.replace("'", "")
    result["ticker_fullname"]=result["ticker_fullname"].str.strip()
    print(result)
    if(len(result)) > 0 :
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Ticker Name Updated ===".format(datetimeNow()))

def update_entity_type():
    print("{} : === Entity Type Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe[["ticker"]]
    print(universe)
    filter_field = ["WC06100"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe, identifier, filter_field, use_ticker=True, split_number=40)
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"WC06100": "entity_type"})
        result = remove_null(result, "entity_type")
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Entity Type Updated ===".format(datetimeNow()))

def get_lot_size_from_dss():
    print("{} : === Lot Size Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    tickerlist = get_active_universe()
    tickerlist["ticker"] = "/" + tickerlist["ticker"]
    ticker = tickerlist["ticker"]
    jsonFileName = "files/file_json/lot_size.json"
    result = get_data_from_dss("start_date", "end_date", ticker, jsonFileName, report=os.getenv("REPORT_INTRADAY"))
    result = result.rename(columns={
        "RIC": "ticker",
        "Lot Size": "lot_size"
    })
    print(result)
    if(len(result)) > 0 :
        result["ticker"]=result["ticker"].str.replace("/", "")
        result["ticker"]=result["ticker"].str.strip()
        result = remove_null(result, "lot_size")
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Lot Size Updated ===".format(datetimeNow()))

def update_vix_from_dsws():
    print("{} : === Vix Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_day(3)
    universe = get_vix()
    universe = universe[["vix_index"]]
    identifier="vix_index"
    filter_field = ["PI"]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=False, split_number=1)
    if(len(result)) > 0 :
        result = result.rename(columns={"PI": "vix_value"})
        result = uid_maker(result, uid="uid", ticker="vix_index", trading_day="trading_day")
        upsert_data_to_database(result, get_vix_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_company_desc_from_dsws(args):
    print("{} : === Company Description Ingestion ===".format(datetimeNow()))
    universe = get_active_universe_company_description_null()
    universe = universe[["ticker"]]
    identifier = "ticker"
    filter_field = ["WC06092"]
    result, error_ticker = get_data_static_from_dsws(universe, identifier, filter_field, use_ticker=True, split_number=1)
    if(len(result) > 0):
        result = result.rename(columns={"WC06092": "company_description"})
        result = remove_null(result, "company_description")
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        fill_null_company_desc_with_ticker_name()
        report_to_slack("{} : === Company Description Updated ===".format(datetimeNow()))

def update_country_from_dsws():
    print("{} : === Country & Industry Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe[["ticker"]]
    identifier="ticker"
    filter_field = ["GGISO", "WC07040", "WC06011"]
    result = get_data_static_from_dsws(universe, identifier, filter_field, use_ticker=True, split_number=40)
    print(result)
    if(len(result)>0):
        result = result.rename(columns={"index":"ticker",
            "GGISO":"country_code", 
            "WC07040":"industry_code", 
            "WC06011":"wc_industry_group"})
        
        country = result[["country_code"]]
        country = country.drop_duplicates(keep="first")
        country["country_name"] = "NA"
        upsert_data_to_database(country, get_country_table_name(), identifier, how="ignore", Text=True)

        industry = result[["industry_code"]]
        industry = industry.drop_duplicates(keep="first")
        industry["industry_name"] = "NA"
        upsert_data_to_database(industry, get_industry_table_name(), identifier, how="ignore", Text=True)

        wc_industry = result[["wc_industry_code"]]
        wc_industry = wc_industry.drop_duplicates(keep="first")
        wc_industry["wc_industry_name"] = "NA"
        upsert_data_to_database(wc_industry, get_industry_worldscope_table_name(), identifier, how="ignore", Text=True)

        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Country & Industry Updated ===".format(datetimeNow()))

def get_currency_price_from_dss():
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_currency_symbol()
    currency = currencylist["ric"]
    print(currency)
    print(currencylist)
    jsonFileName = "files/file_json/currency_price.json"
    result = get_data_from_dss("start_date", "end_date", currency, jsonFileName, report=os.getenv("REPORT_INTRADAY"))
    if(len(result) > 0):
        result = result.rename(columns={
            "RIC": "ric",
            "Universal Bid Ask Date": "last_date",
            "Universal Ask Price": "ask_price",
            "Universal Bid Price": "bid_price",
        })
        result = result.merge(currencylist, how="left", on="ric")
        result["last_price"] = (result["ask_price"] + result["bid_price"]) / 2
        result = result[["currency_code","last_date","last_price"]]
        print(result)
        upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))