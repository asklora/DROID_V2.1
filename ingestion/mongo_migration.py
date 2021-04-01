from general.date_process import backdate_by_month
from main import latest_price
import numpy as np
import pandas as pd
from general.table_name import get_industry_group_table_name
from bot.data_download import get_latest_price
from general.sql_query import get_active_currency, get_active_universe, get_industry, get_industry_group, get_latest_price_data, get_master_tac_data, get_region, get_universe_rating
from general.mongo_query import update_to_mongo, change_date_to_str, update_specific_to_mongo

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
    rating = get_universe_rating()
    industry = get_industry()
    industry_group = get_industry_group()
    latest_price = get_latest_price()
    result = universe.merge(rating, on="ticker", how="left")
    result = result.merge(latest_price, on="ticker", how="left")
    result = result.merge(industry, on="industry_code", how="left")
    result = result.merge(industry_group, on="industry_group_code", how="left")
    result["ST"] = np.where((result["wts_rating"] + result["dlp_1m"]) >= 11, True, False)
    result["MT"] = np.where(result["dlp_3m"] >= 7, True, False)
    result["GQ"] = np.where(result["fundamentals_quality"] >= 5, True, False)
    result["GV"] = np.where(result["fundamentals_value"] >= 5, True, False)
    result = result[["ticker", "close", "latest_price_change", "last_date", 
        "ticker_name", "company_description", "currency_code", "fundamentals_quality", "fundamentals_value", 
        "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2", "industry_group_img", "ST", "MT", "GQ", "GV"]]
    
    #Populate Price
    start_date = backdate_by_month(1)
    price = get_master_tac_data(start_date=start_date)
    price = price[["ticker", "trading_day", "tri_adj_close", "day_status"]]
    price = price.rename(columns={"tri_adj_close" : "price"})
    price = change_date_to_str(price)
    print(price)
    for tick in result["ticker"].unique():
        price_data = price.loc[price["ticker"] == tick]
        price_data = price_data.sort_values(by="trading_day", ascending=False)
        price_data = price_data[["trading_day", "price"]].to_dict("records")
        price_data = pd.DataFrame({"ticker":[tick], "price_data":[price_data]}, index=[0])
        result = result.merge(price_data, on="ticker", how="left")
    print(result)
    update_to_mongo(data=result, index="ticker", table="universe", dict=False)

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
        update_to_mongo(data=price_data, index="ticker", table="price_data", dict=False)