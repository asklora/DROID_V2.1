import pandas as pd
from general.sql_query import get_active_universe
from general.date_process import backdate_by_day, backdate_by_year, dateNow, datetimeNow
from global_vars import REPORT_INTRADAY, REPORT_HISTORY, REPORT_EOD, REPORT_INDEXMEMBER
from datasource.dss import get_data_from_dss

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

if __name__ == '__main__':
    get_index_member()