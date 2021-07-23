from general.sql_process import do_function
import numpy as np
from general.date_process import (
    backdate_by_day, datetimeNow,
    dateNow)
from general.slack import report_to_slack
from general.data_process import remove_null
from general.sql_query import (
    get_active_universe, 
    get_active_universe_consolidated_by_field,
    get_active_universe_company_description_null,
    get_all_universe)
from general.sql_output import update_universe_where_currency_code_null, upsert_data_to_database, fill_null_company_desc_with_ticker_name
from datasource.dsws import get_data_static_from_dsws
from datasource.dss import get_data_from_dss
from general.table_name import (
    get_universe_table_name, 
    get_industry_table_name,
    get_industry_worldscope_table_name,
    get_universe_consolidated_table_name
    )
from global_vars import REPORT_HISTORY, REPORT_INTRADAY

def populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=None):
    print("{} : === Ticker ISIN Start Ingestion ===".format(datetimeNow()))
    manual_universe = get_active_universe_consolidated_by_field(manual=True, ticker=ticker)
    manual_universe = manual_universe.loc[manual_universe["use_manual"] == True]
    manual_universe["consolidated_ticker"] = manual_universe["origin_ticker"]

    universe = get_active_universe_consolidated_by_field(isin=True, ticker=ticker)
    universe = universe.loc[universe["use_manual"] == False]
    if(len(universe) > 0):
        universe = universe.drop(columns=["isin", "consolidated_ticker", "sedol", "is_active", "cusip", "permid", "updated"])
        print(universe)
        identifier="origin_ticker"
        filter_field = ["ISIN", "SECD", "WC06004", "IPID"]
        result, error_ticker = get_data_static_from_dsws(universe[["origin_ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 20))
        result = result.rename(columns={"ISIN": "isin", "index":"origin_ticker", "SECD": "sedol", "WC06004": "cusip", "IPID": "permid"})
        
        print(result)

        isin_list = result[["isin"]]
        isin_list = isin_list.drop_duplicates(keep="first", inplace=False)
        result2, error_ticker = get_data_static_from_dsws(isin_list, "isin", ["RIC", "SECD"], use_ticker=False, split_number=min(len(isin_list), 20))
        result2 = result2.rename(columns={"RIC": "consolidated_ticker", "index":"isin", "SECD": "sedol"})
        print(result2)
        result = result.merge(result2, how="left", on=["isin", "sedol"])
        result = result.loc[result["consolidated_ticker"] != "FGBSM2^1"]
        null_result = result.loc[result["isin"] == "NA"]
        null_result["use_manual"] = True
        null_result["isin"] = None
        null_result["sedol"] = None
        null_result["cusip"] = None
        null_result["permid"] = None
        null_result["is_active"] = True
        null_result["use_isin"] = False
        null_result["consolidated_ticker"] = null_result["origin_ticker"]
        print(null_result)
        result = result.loc[result["consolidated_ticker"] != "NA"]
        print(result)
        if(len(result)) > 0 :
            consolidated_ticker = result.loc[result["consolidated_ticker"].notnull()]
            consolidated_ticker["is_active"] = True
            null_consolidated_ticker = result.loc[result["consolidated_ticker"].isnull()]
            #null_consolidated_ticker = remove_null(null_consolidated_ticker)
            null_consolidated_ticker["is_active"] = False
            for index, row in null_consolidated_ticker.iterrows():
                origin_ticker = row["origin_ticker"]
                isin = row["isin"]
                sedol = row["sedol"]
                #find the same isin
                same_isin = consolidated_ticker.loc[consolidated_ticker["isin"] == isin]
                if(len(same_isin) > 0 and isin != "NA"):
                    #find the same sedol
                    same_sedol = same_isin.loc[same_isin["sedol"] == sedol]
                    if(len(same_sedol) == 0 and isin != "NA"):
                        null_consolidated_ticker.loc[index, "consolidated_ticker"] = origin_ticker
                        null_consolidated_ticker.loc[index, "is_active"] = True
            result = consolidated_ticker.append(null_consolidated_ticker)
            result = result.merge(universe, how="left", on=["origin_ticker"])
            
            if(len(manual_universe) > 0):
                result = result.append(manual_universe)
            
            if(len(null_result) > 0):
                result = result.append(null_result)
    else:
        result = manual_universe
    result["updated"] = dateNow()
    print(result)
    upsert_data_to_database(result, get_universe_consolidated_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Ticker ISIN Updated ===".format(datetimeNow()))
    #Should remove FGBSM2^1 this ticker

def update_ticker_name_from_dsws(ticker=None, currency_code=None):
    print("{} : === Ticker Name Start Ingestion ===".format(datetimeNow()))
    universe = get_all_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["ticker_name", "ticker_fullname"])
    print(universe)
    filter_field = ["WC06003", "NAME"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"WC06003": "ticker_name", "NAME" : "ticker_fullname", "index":"ticker"})
        result["ticker_name"]=result["ticker_name"].str.replace("'", "", regex=True)
        result["ticker_fullname"]=result["ticker_fullname"].str.replace("'", "", regex=True)
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        do_function("universe_populate")
        report_to_slack("{} : === Ticker Name Updated ===".format(datetimeNow()))

def update_entity_type_from_dsws(ticker=None, currency_code=None):
    print("{} : === Entity Type Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["entity_type"])
    filter_field = ["WC06100"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    result = result.rename(columns={"WC06100": "entity_type", "index":"ticker"})
    result = remove_null(result, "entity_type")
    print(result)
    if(len(result)) > 0 :
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Entity Type Updated ===".format(datetimeNow()))

def update_lot_size_from_dss(ticker=None, currency_code=None):
    print("{} : === Lot Size Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["lot_size"])
    ticker = "/" + universe["ticker"]
    jsonFileName = "files/file_json/lot_size.json"
    result = get_data_from_dss("start_date", "end_date", ticker, jsonFileName, report=REPORT_INTRADAY)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Lot Size": "lot_size"
        })
        result["ticker"]=result["ticker"].str.replace("/", "", regex=True)
        result["ticker"]=result["ticker"].str.strip()
        result = remove_null(result, "lot_size")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Lot Size Updated ===".format(datetimeNow()))

def update_currency_code_from_dss(ticker=None, currency_code=None):
    print("{} : === Currency Code Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["currency_code"])
    ticker = "/" + universe["ticker"]
    jsonFileName = "files/file_json/currency.json"
    result = get_data_from_dss("start_date", "end_date", ticker, jsonFileName, report=REPORT_INTRADAY)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Currency Code": "currency_code"
        })
        result["ticker"]=result["ticker"].str.replace("/", "", regex=True)
        result["ticker"]=result["ticker"].str.strip()
        result = remove_null(result, "currency_code")
        result = universe.merge(result, how="left", on=["ticker"])
        result["currency_code"] = result["currency_code"].str.upper()
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Currency Code Updated ===".format(datetimeNow()))
        update_universe_where_currency_code_null()

def update_mic_from_dss(ticker=None, currency_code=None):
    print("{} : === MIC Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["mic"])
    jsonFileName = "files/file_json/test_eod.json"
    result = get_data_from_dss("start_date", "end_date", universe["ticker"], jsonFileName, report=REPORT_INTRADAY)
    print(result)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Market MIC": "mic"
        })
        result["mic"] = np.where(result["mic"] == "XETB", "XETA", result["mic"])
        result["mic"] = np.where(result["mic"] == "XXXX", "XNAS", result["mic"])
        result["mic"] = np.where(result["mic"] == "MTAA", "XMIL", result["mic"])
        result["mic"] = np.where(result["mic"] == "WBAH", "XEUR", result["mic"])
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === MIC Updated ===".format(datetimeNow()))

def update_ticker_symbol_from_dss(ticker=None, currency_code=None):
    print("{} : === Ticker Symbol Start Ingestion ===".format(datetimeNow()))
    identifier="ticker"
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["ticker_symbol"])
    start_date = backdate_by_day(1)
    end_date = dateNow()
    jsonFileName = "files/file_json/ticker_symbol.json"
    result = get_data_from_dss(start_date, end_date, universe["ticker"], jsonFileName, report=REPORT_HISTORY)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Ticker": "ticker_symbol"
        })
        result = universe.merge(result, how="left", on=["ticker"])
        result1 = result.loc[result["currency_code"] == "HKD"]
        if(len(result1) > 0):
            for index, row in result1.iterrows():
                result1.loc[index, "ticker_symbol"] = str(("0" * (4 - len(str(row["ticker_symbol"])))) + str(row["ticker_symbol"]))
        result2 = result.loc[result["currency_code"] != "HKD"]
        if(len(result1) > 0):
            print(result1)
            upsert_data_to_database(result1, get_universe_table_name(), identifier, how="update", Text=True)
        if(len(result2) > 0):
            print(result2)
            upsert_data_to_database(result2, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Ticker Symbol Updated ===".format(datetimeNow()))

def update_company_desc_from_dsws(ticker=None, currency_code=None):
    print("{} : === Company Description Ingestion ===".format(datetimeNow()))
    universe = get_active_universe_company_description_null(ticker=ticker)
    universe = universe.drop(columns=["company_description"])
    if(len(universe) > 0):
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

def update_industry_from_dsws(ticker=None, currency_code=None):
    print("{} : === Country & Industry Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    #universe = universe.drop(columns=["country_code", "industry_code", "wc_industry_code"])
    universe = universe.drop(columns=["industry_code", "wc_industry_code"])
    identifier="ticker"
    #filter_field = ["GGISO", "WC07040", "WC06011"]
    filter_field = ["WC07040", "WC06011"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    print(result)
    if(len(result)>0):
        result = result.rename(columns={"index":"ticker",
            #"GGISO":"country_code", 
            "WC07040":"industry_code", 
            "WC06011":"wc_industry_code"})
        print(result)
        # country = result[["country_code"]]
        # country = country.drop_duplicates(keep="first")
        # country["country_name"] = "NA"
        # upsert_data_to_database(country, get_country_table_name(), identifier, how="ignore", Text=True)

        industry = result[["industry_code"]]
        industry = industry.drop_duplicates(keep="first")
        industry["industry_name"] = "NA"
        upsert_data_to_database(industry, get_industry_table_name(), "industry_code", how="ignore", Text=True)

        wc_industry = result[["wc_industry_code"]]
        wc_industry = wc_industry.drop_duplicates(keep="first")
        wc_industry["wc_industry_name"] = "NA"
        upsert_data_to_database(wc_industry, get_industry_worldscope_table_name(), "wc_industry_code", how="ignore", Text=True)
        
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Country & Industry Updated ===".format(datetimeNow()))
    
def update_worldscope_identifier_from_dsws(ticker=None, currency_code=None):
    print("{} : === Worldscope Identifier Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["worldscope_identifier", "icb_code", "fiscal_year_end"])
    identifier="ticker"
    filter_field = ["WC06035", "WC07040", "WC05352"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 10))
    print(result)
    if (len(result) > 0):
        result = result.rename(columns={"WC06035": "worldscope_identifier", "WC07040": "icb_code", "WC05352": "fiscal_year_end", "index" : "ticker"})
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Worldscope Identifier Updated ===".format(datetimeNow()))
    