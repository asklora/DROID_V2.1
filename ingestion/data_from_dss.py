import numpy as np
import pandas as pd
from general.data_process import remove_null, uid_maker
from general.slack import report_to_slack
from general.sql_output import (
    clean_latest_price, 
    update_all_data_by_capital_change, 
    update_capital_change, 
    upsert_data_to_database)
from general.table_name import (
    get_data_dss_table_name, 
    get_latest_price_table_name, 
    get_universe_table_name)
from datasource.dss import get_data_from_dss
from general.sql_query import (
    get_active_universe, 
    get_latest_price_capital_change, 
    get_latest_price_data, 
    get_max_last_ingestion_from_universe, 
    get_yesterday_close_price)
from general.date_process import (
    backdate_by_day, 
    dateNow, 
    datetimeNow, 
    dlp_start_date, 
    str_to_date)
from global_vars import REPORT_HISTORY

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

def update_data_dss_from_dss(ticker=None, currency_code=None, history=False, manual=False):
    print("{} : === DSS Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    
    if(history):
        start_date = dlp_start_date()
    elif(manual):
        start_date = backdate_by_day(1)
    else:
        start_date = get_max_last_ingestion_from_universe(ticker=ticker, currency_code=currency_code)
        if(start_date=="None"):
            start_date = dlp_start_date()
    
    print(f"Ingestion Start From {start_date}")
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    jsonFileName = "files/file_json/historyAPI.json"
    result = get_data_from_dss(start_date, end_date, universe["ticker"].to_list(), jsonFileName, report=REPORT_HISTORY)
    print(result)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Trade Date": "trading_day",
            "Open Price": "open",
            "High Price": "high",
            "Low Price": "low",
            "Universal Close Price": "close",
            "Accumulated Volume Unscaled": "volume"
        })
        result = uid_maker(result, uid="dss_id", ticker="ticker", trading_day="trading_day")
        print(result)
        upsert_data_to_database(result, get_data_dss_table_name(), "dss_id", how="update", Text=True)
        report_to_slack("{} : === DSS Updated ===".format(datetimeNow()))

def split_order_and_performance(ticker=None, currency_code=None):
    latest_price_updates = get_latest_price_capital_change(ticker=ticker, currency_code=currency_code)
    if(len(latest_price_updates) > 0): 
        print("Total Split = " + str(len(latest_price_updates)))
        print("Start Splitting Price in Data")
        split_data = latest_price_updates[["ticker", "last_date", "capital_change"]]
        split_data = split_data.loc[split_data["capital_change"] > 0]
        print(split_data)
        for index, row in split_data.iterrows():
            ticker = row["ticker"]
            print(ticker)
            last_date = row["last_date"]
            capital_change = row["capital_change"]
            price = row["close"]
            percent_change = row["latest_price_change"]
            update_all_data_by_capital_change(ticker, last_date, capital_change, price, percent_change)
            update_capital_change(ticker)
            report_to_slack("{} : === PRICE SPLIT ON TICKER {} with CAPITAL CHANGE {} ===".format(str(dateNow()), ticker, capital_change))
           
           
def populate_latest_price_from_dss(ticker=None, currency_code=None):
    jsonFileName = "files/file_json/latest_price.json"
    start_date = backdate_by_day(1)
    end_date = dateNow()
    latest_price = get_latest_price_data(ticker=ticker, currency_code=currency_code)
    last_price = latest_price[["ticker", "classic_vol"]]
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    ticker_list =  universe["ticker"]
    data = get_data_from_dss(start_date, end_date, ticker_list, jsonFileName, report=REPORT_HISTORY)
    print(data)
    percentage_change =  get_yesterday_close_price(ticker=ticker_list, currency_code=currency_code)
    print(percentage_change)
    data  =data.drop(columns=["IdentifierType", "Identifier"])
    null_ticker = []
    if(len(data) > 0):
        data = data.rename(columns={
            "RIC": "ticker",
            "Trade Date": "last_date",
            "Open Price": "open",
            "Low Price": "low",
            "High Price": "high",
            "Universal Close Price": "close",
            "Adjustment Value - Capital Change" : "capital_change"
        })
        data["ticker"]=data["ticker"].str.replace("/", "", regex=True)
        data["ticker"]=data["ticker"].str.strip()
        data = pd.DataFrame(data)
        holiday = data.copy()
        holiday = data.dropna(subset=["close"])
        if(len(holiday) == 0):
            report_to_slack("{} : === {} is Holiday. Latest Price Not Updated ===".format(dateNow(), currency_code))
            return False
        data["last_date"] = pd.to_datetime(data["last_date"])
        result = data.merge(percentage_change, how="left", on="ticker")
        result = result.merge(last_price, on=["ticker"], how="left")
        result["yesterday_close"] = np.where(result["yesterday_close"].isnull(), 0, result["yesterday_close"])
        result["latest_price_change"] = round(((result["close"] - result["yesterday_close"]) / result["yesterday_close"]) * 100, 4)
        result = result.drop(columns=["yesterday_close", "trading_day"])
        result = result.dropna(subset=["close"])
        result = remove_null(result, "latest_price_change")
        if(len(result) > 0):
            result["intraday_bid"] = result["close"]
            result["intraday_ask"] = result["close"]
            result["intraday_time"] = datetimeNow()
            result["intraday_date"] = result["last_date"]
            result["last_date"] = str_to_date(dateNow())
            result = result.sort_values(by="last_date", ascending=True)
            result = result.drop_duplicates(subset="ticker", keep="last")
            print(result)
            upsert_data_to_database(result, get_latest_price_table_name(), "ticker", how="update", Text=True)
            clean_latest_price()
            if(type(ticker) != type(None)):
                report_to_slack("{} : === {} Latest Price Updated ===".format(dateNow(), ticker))
            elif(type(currency_code) != type(None)):
                report_to_slack("{} : === {} Latest Price Updated ===".format(dateNow(), currency_code))
            else:
                report_to_slack("{} : === Latest Price Updated ===".format(dateNow()))
            null_ticker = result["ticker"].tolist()
            
            split_order_and_performance(ticker=ticker, currency_code=currency_code)

    latest_price = latest_price.loc[~latest_price["ticker"].isin(null_ticker)]
    print(latest_price)
    try:
        if(len(latest_price) > 0):
            thislist = list(latest_price.columns)
            thislist.remove("trading_day")
            thislist.remove("yesterday_close")
            latest_price = latest_price[thislist]
            print(last_price)
            latest_price = latest_price.merge(percentage_change, how="left", on="ticker")
            latest_price["close"] = latest_price["yesterday_close"]
            latest_price["high"] = latest_price["yesterday_close"]
            latest_price["low"] = latest_price["yesterday_close"]
            latest_price["open"] = latest_price["yesterday_close"]
            latest_price["intraday_bid"] = latest_price["yesterday_close"]
            latest_price["intraday_ask"] = latest_price["yesterday_close"]
            latest_price["last_date"] = str_to_date(dateNow())
            latest_price = latest_price.drop(columns=["trading_day", "yesterday_close"])
            print(latest_price)
            upsert_data_to_database(latest_price, get_latest_price_table_name(), "ticker", how="update", Text=True)
    except Exception as e:
        print("{} : === LATEST PRICE ERROR === : {}".format(dateNow(), e))