import pandas as pd
from general.sql_query import get_active_universe
from general.date_process import backdate_by_day, backdate_by_year, dateNow, datetimeNow
from global_vars import REPORT_INTRADAY, REPORT_HISTORY, REPORT_EOD, REPORT_INDEXMEMBER
from datasource.dss import get_data_from_dss
import firebase_admin
from firebase_admin import firestore, credentials, db

def get_intraday():
    print("Do Process")
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    # currencylist = get_active_universe(ticker=[".KS200", ".SPX", ".HSLI", ".CSI300"])
    currencylist = pd.DataFrame({"ticker" : ["0005.HK", "601398.SS"]}, index=[0, 1])
    jsonFileName = "files/file_json/test_intraday.json"
    currencylist["ticker"] = "/" + currencylist["ticker"]
    print(currencylist)
    end_date = dateNow()
    start_date = backdate_by_year(1)
    result = get_data_from_dss("start_date", "end_date", currencylist["ticker"], jsonFileName, report=REPORT_INTRADAY)
    print(result)
    # result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)

def get_history():
    print("Do Process")
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    # currencylist = get_active_universe(ticker=[".KS200", ".SPX", ".HSLI", ".CSI300"])
    currencylist = pd.DataFrame({"ticker" : ["0005.HK", "601398.SS"]}, index=[0, 1])
    jsonFileName = "files/file_json/test_history.json"
    print(currencylist)
    end_date = dateNow()
    start_date = backdate_by_year(1)
    result = get_data_from_dss(start_date, end_date, currencylist["ticker"], jsonFileName, report=REPORT_HISTORY)
    print(result)
    # result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)

def get_index_member():
    currencylist = pd.DataFrame({"index" : ["0#.HSLMI"]}, index=[0])
    jsonFileName = "files/file_json/index_member.json"
    print(currencylist)
    end_date = dateNow()
    start_date = backdate_by_year(1)
    result = get_data_from_dss("start_date", "end_date", currencylist["index"], jsonFileName, report=REPORT_INDEXMEMBER)
    print(result)
    result["origin_ticker"] = result["RIC"]
    result = result.drop(columns=["IdentifierType", "Identifier", "RIC"])
    result["source_id"] = "DSS"
    result["use_isin"] = 0
    result["use_manual"] = 1
    print(result)
    result.to_csv("index_member_HSLMI.csv")

if __name__ == "__main__":
    identifier="ticker"
    ticker = ["MSFT.O", "AAPL.O"]
    universe = get_active_universe()
    start_date = backdate_by_day(1)
    end_date = dateNow()
    jsonFileName = "files/file_json/test_corporate_action.json"
    result = get_data_from_dss(start_date, end_date, universe["ticker"], jsonFileName, report=REPORT_INTRADAY)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    result = result.rename(columns={"RIC" : "ticker"})
    print(result)
    columns_list = ["Capital Additive Adjustment Factor", "Capital Change Event Type", "Capital Change Event Type Description", 
        "Capital Change Ex Date", "Dividend Ex Date", "Dividend Frequency", "Dividend Frequency Description", "Dividend Rate", 
        "Earnings Announcement Date", "EPS Amount"]
    result2 = universe[["ticker"]]
    for col in columns_list:
        temp = result[["ticker", col]].dropna()
        if len(temp) <= 0:
            temp = pd.DataFrame({"ticker" : [] , col : []}, index=[])
        result2 = result2.merge(temp, how="left", on=["ticker"])
    print(result2)
    result2 = result2.drop_duplicates(subset=columns_list, keep="first")
    print(result2)
    result2.to_csv("/home/loratech/corporate_action.csv")
    

    