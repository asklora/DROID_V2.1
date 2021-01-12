import os
from dotenv import load_dotenv
load_dotenv()
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
    get_active_universe, 
    get_active_universe_company_description_null)
from general.sql_output import upsert_data_to_database, fill_null_company_desc_with_ticker_name
from datasource.dsws import get_data_static_from_dsws, get_data_history_from_dsws
from datasource.dss import get_data_from_dss
from general.table_name import (
    get_universe_table_name, 
    get_industry_table_name,
    get_country_table_name,
    get_data_dividend_table_name,
    get_industry_worldscope_table_name,
    get_vix_table_name)

def update_ticker_name_from_dsws():
    print("{} : === Ticker Name Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe.drop(columns=["ticker_name", "ticker_fullname"])
    print(universe)
    filter_field = ["WC06003", "NAME"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=40)
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"WC06003": "ticker_name", "NAME" : "ticker_fullname", "index":"ticker"})
        result["ticker_name"]=result["ticker_name"].str.replace("'", "").strip()
        result["ticker_fullname"]=result["ticker_fullname"].str.replace("'", "").strip()
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Ticker Name Updated ===".format(datetimeNow()))

def update_entity_type_from_dsws():
    print("{} : === Entity Type Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe.drop(columns=["entity_type"])
    filter_field = ["WC06100"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=40)
    result = result.rename(columns={"WC06100": "entity_type", "index":"ticker"})
    result = remove_null(result, "entity_type")
    print(result)
    if(len(result)) > 0 :
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Entity Type Updated ===".format(datetimeNow()))

def update_lot_size_from_dss():
    print("{} : === Lot Size Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    universe = get_active_universe()
    universe = universe.drop(columns=["lot_size"])
    ticker = "/" + universe["ticker"]
    jsonFileName = "files/file_json/lot_size.json"
    result = get_data_from_dss("start_date", "end_date", ticker, jsonFileName, report=os.getenv("REPORT_INTRADAY"))
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Lot Size": "lot_size"
        })
        result["ticker"]=result["ticker"].str.replace("/", "")
        result["ticker"]=result["ticker"].str.strip()
        result = remove_null(result, "lot_size")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
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
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"PI": "vix_value", "index" : "trading_day"})
        result = uid_maker(result, uid="uid", ticker="vix_index", trading_day="trading_day")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        #upsert_data_to_database(result, get_vix_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_company_desc_from_dsws():
    print("{} : === Company Description Ingestion ===".format(datetimeNow()))
    universe = get_active_universe_company_description_null()
    universe = universe.drop(columns=["company_description"])
    identifier = "ticker"
    filter_field = ["WC06092"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=1)
    if(len(result) > 0):
        result = result.rename(columns={"WC06092": "company_description", "index" : "ticker"})
        result = remove_null(result, "company_description")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        fill_null_company_desc_with_ticker_name()
        report_to_slack("{} : === Company Description Updated ===".format(datetimeNow()))

def update_country_from_dsws():
    print("{} : === Country & Industry Ingestion ===".format(datetimeNow()))
    universe = get_active_universe()
    universe = universe.drop(columns=["country_code", "industry_code", "wc_industry_code"])
    identifier="ticker"
    filter_field = ["GGISO", "WC07040", "WC06011"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=40)
    print(result)
    if(len(result)>0):
        result = result.rename(columns={"index":"ticker",
            "GGISO":"country_code", 
            "WC07040":"industry_code", 
            "WC06011":"wc_industry_code"})
        
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
        
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Country & Industry Updated ===".format(datetimeNow()))
    
def update_worldscope_identifier_from_dsws():
    print("{} : === Worldscope Identifier Ingestion ===".format(datetimeNow()))
    universe = get_active_universe().head(2)
    universe = universe.drop(columns=["worldscope_identifier", "icb_code", "fiscal_year_end"])
    identifier="ticker"
    filter_field = ["WC06035", "WC07040", "WC05352"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=1)
    print(result)
    if (len(result) > 0):
        result = result.rename(columns={"WC06035": "worldscope_identifier", "WC07040": "icb_code", "WC05352": "fiscal_year_end", "index" : "ticker"})
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Worldscope Identifier Updated ===".format(datetimeNow()))
    