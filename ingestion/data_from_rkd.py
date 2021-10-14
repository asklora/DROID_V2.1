import django
import os
debug = os.environ.get("DJANGO_SETTINGS_MODULE",True)
if debug:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.production")
else:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.development")
django.setup()
import numpy as np
import pandas as pd
from datetime import datetime
from general.data_process import remove_null
from general.slack import report_to_slack
from general.table_name import get_currency_table_name, get_latest_price_table_name, get_universe_table_name
from general.sql_output import clean_latest_price, update_universe_where_currency_code_null, upsert_data_to_database
from general.sql_query import get_active_currency, get_active_currency_ric_not_null, get_active_universe, get_latest_price_data, get_yesterday_close_price
from general.date_process import dateNow, datetimeNow, str_to_date
from datasource import rkd as RKD

# def update_currency_code_from_rkd(ticker=None, currency_code=None):
#     print("{} : === Currency Code Start Ingestion ===".format(datetimeNow()))
#     identifier="ticker"
#     universe = get_active_universe(ticker=ticker, currency_code=currency_code)
#     universe = universe.drop(columns=["currency_code"])
#     ticker = universe["ticker"].to_list()
#     field = ["CF_CURRENCY"]
#     rkd = RKD.RkdData()
#     result = rkd.get_data_from_rkd(ticker, field)
#     print(result)
#     if (len(result) > 0 ):
#         result = result.rename(columns={"CF_CURRENCY": "currency_code"})
#         result = remove_null(result, "currency_code")
#         result = universe.merge(result, how="left", on=["ticker"])
#         result["currency_code"] = result["currency_code"].str.upper()
#         print(result)
#         upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
#         report_to_slack("{} : === Currency Code Updated ===".format(datetimeNow()))
#         update_universe_where_currency_code_null()

def update_currency_price_from_rkd(currency_code=None):
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_currency_ric_not_null(currency_code=currency_code)
    currencylist = currencylist.drop(columns=["last_date", "last_price"])
    currency = currencylist["ric"].to_list()
    field = ["CF_DATE", "CF_ASK", "CF_BID"]
    rkd = RKD.RkdData()
    result = rkd.get_data_from_rkd(currency, field)
    print(result)
    if(len(result) > 0):
        result = result.rename(columns={
            "ticker" : "ric",
            "CF_DATE": "last_date",
            "CF_ASK": "ask_price",
            "CF_BID": "bid_price",
        })
        result["last_date"] = pd.to_datetime(result["last_date"])
        result["ask_price"] = result["ask_price"].astype(float)
        result["bid_price"] = result["bid_price"].astype(float)
        result = result.merge(currencylist, how="left", on="ric")
        result["last_price"] = (result["ask_price"] + result["ask_price"]) / 2
        result = result.drop(columns=["ask_price", "bid_price"])
        print(result)
        upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))
    
def update_index_price_from_rkd(currency_code=None):
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_currency(currency_code=currency_code)
    currencylist = currencylist.drop(columns=["index_price"])
    currency = currencylist["ric"].to_list()
    field = ["CF_LAST"]
    rkd = RKD.RkdData()
    result = rkd.get_data_from_rkd(currency, field)
    print(result)
    if(len(result) > 0):
        result = result.rename(columns={
            "ticker": "ric",
            "CF_LAST": "index_price"
        })
        result["index_price"] = result["index_price"].astype(float)
        result = result.dropna(subset=["index_price"])
        result = result.merge(currencylist, how="left", on="ric")
        print(result)
        upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))

def populate_intraday_latest_price_from_rkd(ticker=None, currency_code=None,use_index=False):
    latest_price = get_latest_price_data(ticker=ticker, currency_code=currency_code)
    last_price = latest_price[["ticker", "classic_vol", "capital_change"]]
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    ticker = universe["ticker"].to_list()
    field = ["CF_BID", "CF_ASK", "CF_OPEN", "CF_HIGH", "CF_LOW", "CF_CLOSE", "PCTCHNG", "TRADE_DATE", "CF_VOLUME", "CF_LAST", "CF_NETCHNG"]
    rkd = RKD.RkdData()
    result = rkd.get_data_from_rkd(ticker, field)
    print(result)
    percentage_change =  get_yesterday_close_price(ticker=universe["ticker"], currency_code=currency_code)
    print(result)
    if(len(result) > 0):
        result = result.rename(columns={
            "CF_BID": "intraday_bid",
            "CF_ASK": "intraday_ask",
            "CF_OPEN": "open",
            "CF_HIGH": "high", 
            "CF_LOW": "low",
            "CF_CLOSE": "close",
            "PCTCHNG": "latest_price_change",
            "TRADE_DATE": "last_date",
            "CF_VOLUME": "volume",
            "CF_LAST": "latest_price",
            "CF_NETCHNG": "latest_net_change"
        })
        # result["last_date"] = str(datetime.now().date())
        result["intraday_time"] = str(datetime.now())
        result["intraday_bid"] = result["intraday_bid"].astype(float)
        result["intraday_ask"] = result["intraday_ask"].astype(float)
        result["open"] = result["open"].astype(float)
        result["high"] = result["high"].astype(float)
        result["low"] = result["low"].astype(float)
        result["close"] = result["close"].astype(float)
        result["last_date"] = pd.to_datetime(result["last_date"])
        # result["intraday_time"] =  result["last_date"].astype(str) + " " + result["intraday_time"]
        print(result)
        result = result.dropna(subset=["close"])
        holiday = result.copy()
        if(len(holiday) == 0):
            report_to_slack("{} : === {} is Holiday. Latest Price Not Updated ===".format(dateNow(), currency_code))
            return False
        result["last_date"] = pd.to_datetime(result["last_date"])
        result["intraday_time"] = pd.to_datetime(result["intraday_time"])
        result = result.merge(percentage_change, how="left", on="ticker")
        result = result.merge(last_price, on=["ticker"], how="left")
        result["yesterday_close"] = np.where(result["yesterday_close"].isnull(), 0, result["yesterday_close"])
        result["latest_price_change"] = round(((result["close"] - result["yesterday_close"]) / result["yesterday_close"]) * 100, 4)
        result = result.drop(columns=["yesterday_close", "trading_day"])
        result = result.dropna(subset=["close"])
        result = remove_null(result, "latest_price_change")
        print(result)
        if(len(result) > 0):
            result["intraday_date"] = result["last_date"]
            result = result.sort_values(by="last_date", ascending=True)
            result = result.drop_duplicates(subset="ticker", keep="last")
            print(result)
            upsert_data_to_database(result, get_latest_price_table_name(), "ticker", how="update", Text=True)
            clean_latest_price()
            if(type(currency_code) != type(None)):
                report_to_slack("{} : === {} Intraday Price Updated ===".format(dateNow(), currency_code))
            elif(type(ticker) != type(None)):
                report_to_slack("{} : === {} Intraday Price Updated ===".format(dateNow(), ticker))
            else:
                report_to_slack("{} : === Intraday Price Updated ===".format(dateNow()))
            # split_order_and_performance(ticker=ticker, currency_code=currency_code)
        latest_price = latest_price.loc[~latest_price["ticker"].isin(result["ticker"].tolist())]
        if(len(latest_price) > 0):
            print(latest_price)
            latest_price["last_date"] = str_to_date(dateNow())
            print(latest_price)
            upsert_data_to_database(latest_price, get_latest_price_table_name(), "ticker", how="update", Text=True)

# def update_lot_size_from_rkd(ticker=None, currency_code=None):
#     print("{} : === Lot Size Start Ingestion ===".format(datetimeNow()))
#     universe = get_active_universe(ticker=ticker, currency_code=currency_code)
#     universe = universe.drop(columns=["lot_size"])
#     ticker = universe["ticker"].to_list()
#     field = ["CF_LOTSIZE"]
#     rkd = RKD.RkdData()
#     result = rkd.get_data_from_rkd(ticker, field)
#     print(result)
#     if (len(result) > 0 ):
#         result = result.rename(columns={"CF_LOTSIZE": "lot_size"})
#         result["lot_size"] = result["lot_size"].astype(float)
#         result = remove_null(result, "lot_size")
#         result = universe.merge(result, how="left", on=["ticker"])
#         print(result)
#         upsert_data_to_database(result, get_universe_table_name(), "ticker", how="update", Text=True)
#         report_to_slack("{} : === Lot Size Updated ===".format(datetimeNow()))

# def update_mic_from_rkd(ticker=None, currency_code=None):
#     print("{} : === MIC Start Ingestion ===".format(datetimeNow()))
#     identifier="ticker"
#     universe = get_active_universe(ticker=ticker, currency_code=currency_code)
#     universe = universe.drop(columns=["mic"])
#     ticker = universe["ticker"].to_list()
#     field = ["CF_LAST"]
#     rkd = RKD.RkdData()
#     result = rkd.get_data_from_rkd(ticker, field)
#     print(result)
#     if (len(result) > 0 ):
#         result = result.rename(columns={
#             "Market MIC": "mic"
#         })
#         result["mic"] = np.where(result["mic"] == "XETB", "XETA", result["mic"])
#         result["mic"] = np.where(result["mic"] == "XXXX", "XNAS", result["mic"])
#         result["mic"] = np.where(result["mic"] == "MTAA", "XMIL", result["mic"])
#         result["mic"] = np.where(result["mic"] == "WBAH", "XEUR", result["mic"])
#         result = universe.merge(result, how="left", on=["ticker"])
#         print(result)
#         upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
#         report_to_slack("{} : === MIC Updated ===".format(datetimeNow()))