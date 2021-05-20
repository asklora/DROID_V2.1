from general.slack import report_to_slack
from general.sql_output import upsert_data_to_database
import pandas as pd
from general.sql_query import get_active_universe, get_active_universe_droid1
from general.date_process import backdate_by_day, dateNow, datetimeNow
from global_vars import REPORT_INTRADAY, REPORT_HISTORY
from datasource.dss import get_data_from_dss

if __name__ == '__main__':
    print("Do Process")
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_universe(ticker=[".KS200", ".SPX", ".HSLI", ".CSI300"])
    currencylist = pd.DataFrame({"ticker" : [".KS200", ".SPX", ".HSLI", ".CSI300"]}, index=[0, 1, 2, 3])
    jsonFileName = "files/file_json/test_intraday.json"
    currencylist["ticker"] = "/" + currencylist["ticker"]
    print(currencylist)
    result = get_data_from_dss("start_date", "end_date", currencylist["ticker"], jsonFileName, report=REPORT_INTRADAY)
    print(result)
    # result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    # universe = get_active_universe_droid1()
    # start_date = backdate_by_day(1)
    # end_date = dateNow()
    # jsonFileName = "files/file_json/test.json"
    # result = get_data_from_dss(start_date, end_date, universe["ticker"], jsonFileName, report=REPORT_HISTORY)
    # print(result)
    # result = result.drop(columns=["IdentifierType", "Identifier"])
    # print(result)
    # if (len(result) > 0 ):
    #     result = result.rename(columns={
    #         "RIC": "ticker",
    #         "Exchange Code": "exchange_code",
    #         "Exchange Description" : "exchange_name"
    #     })
    #     result = result.drop(columns=["ticker", "Error"])
    #     result = result.dropna()
    #     print(result)
    #     upsert_data_to_database(result, "exchange_code", "exchange_code", how="update", Text=True)
    #     report_to_slack("{} : === Exchange Code Updated ===".format(datetimeNow()))