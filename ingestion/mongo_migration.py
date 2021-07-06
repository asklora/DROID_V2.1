import json
import numpy as np
import pandas as pd
from bot.data_download import get_bot_backtest, get_bot_option_type, get_bot_statistic_data, get_latest_bot_update_data, get_latest_price
from general.sql_query import get_active_currency, get_active_universe, get_consolidated_data, get_industry, get_industry_group, get_latest_price_data, get_master_tac_data, get_region, get_universe_rating
from general.mongo_query import create_collection, update_to_mongo, change_date_to_str, update_specific_to_mongo
from general.date_process import backdate_by_month
import sys

def mongo_create_currency():
    collection = json.load(open("files/file_json/validator_currency.json"))
    print(collection)
    table = "currency"
    create_collection(collection, table)

def mongo_currency_update():
    region = get_region()
    region = region.rename(columns={"ingestion_time" : "region_time"})
    currency = get_active_currency()
    result = currency.merge(region, on="region_id", how="left")
    result = result[["currency_code", "currency_name", "last_price", "utc_offset", "utc_timezone_location", 
        "classic_schedule", "region_id", "region_name", "region_time"]]
    result = result.rename(columns={"classic_schedule" : "close_time", "utc_timezone_location" : "timezone"})
    print(result)
    update_to_mongo(data=result, index="currency_code", table="currency", dict=False)

def mongo_universe_update():
    #Populate Universe
    universe = get_active_universe()
    industry = get_industry()
    industry_group = get_industry_group()
    result = universe.merge(industry, on="industry_code", how="left")
    result = result.merge(industry_group, on="industry_group_code", how="left")
    
    result = result[["ticker", "currency_code", "ticker_name", "company_description", "industry_code", "industry_group_img", 
    "ticker_symbol", "exchange_code", "quandl_symbol", "lot_size", "is_active"]]
    # ["isin", "cusip", "sedol", "permid", origin_ticker"]
    #Populate Price
    isin = get_consolidated_data("consolidated_ticker as ticker, isin", "isin is not null", "consolidated_ticker, isin")
    isin = isin.drop_duplicates(subset=["ticker"], keep="first")

    cusip = get_consolidated_data("consolidated_ticker as ticker, cusip", "cusip is not null", "consolidated_ticker, cusip")
    cusip = cusip.drop_duplicates(subset=["ticker"], keep="first")

    sedol = get_consolidated_data("consolidated_ticker as ticker, sedol", "sedol is not null", "consolidated_ticker, sedol")
    sedol = sedol.drop_duplicates(subset=["ticker"], keep="first")

    permid = get_consolidated_data("consolidated_ticker as ticker, permid", "permid is not null", "consolidated_ticker, permid")
    permid = permid.drop_duplicates(subset=["ticker"], keep="first")

    origin_ticker = get_consolidated_data("consolidated_ticker as ticker, origin_ticker, source_id", "origin_ticker is not null", "consolidated_ticker, origin_ticker, source_id")

    result = result.merge(isin, on="ticker", how="left")
    result = result.merge(cusip, on="ticker", how="left")
    result = result.merge(sedol, on="ticker", how="left")
    result = result.merge(permid, on="ticker", how="left")

    origin_ticker_df = pd.DataFrame({"ticker":[], "origin_ticker":[]}, index=[])
    for tick in result["ticker"].unique():
        ticker_data = origin_ticker.loc[origin_ticker["ticker"] == tick]
        if(len(ticker_data) > 0):
            ticker_data = ticker_data[["origin_ticker", "source_id"]].to_dict("records")
            ticker_data = pd.DataFrame({"ticker":[tick], "origin_ticker":[ticker_data]}, index=[0])
            origin_ticker_df = origin_ticker_df.append(ticker_data)
    result = result.merge(origin_ticker_df, on="ticker", how="left")
    print(result)
    update_to_mongo(data=result, index="ticker", table="universe", dict=False)

def mongo_universe_rating_update():
    #Populate Universe
    rating = get_universe_rating()
    result = rating[["ticker", "fundamentals_quality", "fundamentals_value", "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2"]]
    result["ST"] = np.where((result["wts_rating"] + result["dlp_1m"]) >= 11, True, False)
    result["MT"] = np.where(result["dlp_3m"] >= 7, True, False)
    result["GQ"] = np.where(result["fundamentals_quality"] >= 5, True, False)
    result["GV"] = np.where(result["fundamentals_value"] >= 5, True, False)
    print(result)
    update_to_mongo(data=result, index="ticker", table="universe_rating", dict=False)

def mongo_latest_price_update():
    result = get_latest_price_data()
    result = result[["ticker", "close", "latest_price_change", "last_date"]]
    print(result)
    update_specific_to_mongo(data=result, index="ticker", table="universe", column=["close", "latest_price_change", "intraday_ask", "intraday_bid", "last_date"], dict=False)

def mongo_price_update():
    start_date = backdate_by_month(1)
    ticker = ["AAPL.O"]
    result = get_master_tac_data(start_date=start_date, ticker=ticker)
    result = result[["ticker", "trading_day", "tri_adj_close", "day_status"]]
    result = result.rename(columns={"tri_adj_close" : "price"})
    result = change_date_to_str(result)
    print(result["ticker"].unique())
    for tick in result["ticker"].unique():
        price_data = result.loc[result["ticker"] == tick]
        price_data = price_data.sort_values(by="trading_day", ascending=False)
        price_data = price_data[["trading_day", "price"]].to_dict("records")
        price_data = pd.DataFrame({"ticker":[tick], "price_data":[price_data]}, index=[0])
        print(price_data)
        update_specific_to_mongo(data=result, index="ticker", table="universe", column=["price_data"], dict=False)

def mongo_bot_data_update():
    universe = get_active_universe()
    bot_data = get_latest_bot_update_data()
    for time_exp in bot_data["time_to_exp_str"].unique():
        bot_data.loc[bot_data["time_to_exp_str"] == time_exp, "expiry_date"] = bot_data[f"expiry_{time_exp}"]
    bot_data = bot_data[["ticker", "bot_id", "potential_max_loss", "targeted_profit", "expiry_date", "ranking"]]
    print(bot_data)
    bot_option_type = get_bot_option_type()
    bot_data = bot_data.merge(bot_option_type, on="bot_id", how="left")
    bot_data = change_date_to_str(bot_data)
    for tick in universe["ticker"].unique():
        result = bot_data.loc[bot_data["ticker"] == tick]
        result = result.sort_values(by="ranking", ascending=False)
        result = result[["potential_max_loss", "targeted_profit", "bot_type", "bot_option_type", "bot_option_name", "expiry_date", "ranking"]]
        result = result.to_dict("records")
        result = pd.DataFrame({"ticker":[tick], "bot_data":[result]}, index=[0])
        update_to_mongo(data=result, index="ticker", table="bot_data", dict=False)