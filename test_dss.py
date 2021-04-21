
from general.sql_query import get_active_universe
from general.date_process import datetimeNow
from global_vars import REPORT_INTRADAY, REPORT_HISTORY
from datasource.dss import get_data_from_dss

if __name__ == '__main__':
    print("Do Process")
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_universe(ticker=["005930.KS", "1299.HK", "0700.HK", "AAPL.O"])
    jsonFileName = "files/file_json/test_intraday.json"
    currencylist["ticker"] = "/" + currencylist["ticker"]
    result = get_data_from_dss("start_date", "end_date", currencylist["ticker"], jsonFileName, report=REPORT_INTRADAY)
    print(result)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)