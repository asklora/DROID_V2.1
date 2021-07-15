import pandas as pd
from general.sql_query import get_active_universe
from general.date_process import backdate_by_day, backdate_by_year, dateNow, datetimeNow
from global_vars import REPORT_INTRADAY, REPORT_HISTORY
from datasource.dss import get_data_from_dss

if __name__ == '__main__':
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