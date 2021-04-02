from general.date_process import backdate_by_month
from main import latest_price
import numpy as np
import pandas as pd
from general.table_name import get_industry_group_table_name
from bot.data_download import get_bot_backtest, get_bot_option_type, get_bot_statistic_data, get_latest_bot_update_data, get_latest_price
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
    result = result[["ticker", "close", "latest_price_change", "last_date", "ticker_name", "company_description", 
        "currency_code", "industry_group_img", "ST", "MT", "GQ", "GV"]]
    
    #Populate Price
    start_date = backdate_by_month(1)
    price = get_master_tac_data(start_date=start_date)
    price = price[["ticker", "trading_day", "tri_adj_close", "day_status"]]
    price = price.rename(columns={"tri_adj_close" : "price"})
    price = change_date_to_str(price)
    print(price)
    price_df = pd.DataFrame({"ticker":[], "price_data":[]}, index=[])
    for tick in result["ticker"].unique():
        price_data = price.loc[price["ticker"] == tick]
        price_data = price_data.sort_values(by="trading_day", ascending=False)
        price_data = price_data[["trading_day", "price"]].to_dict("records")
        price_data = pd.DataFrame({"ticker":[tick], "price_data":[price_data]}, index=[0])
        price_df = price_df.append(price_data)
    result = result.merge(price_df, on="ticker", how="left")
    print(result)
    update_to_mongo(data=result, index="ticker", table="universe", dict=False)

def mongo_universe_rating_update():
    #Populate Universe
    rating = get_universe_rating()
    result = rating[["ticker", "fundamentals_quality", "fundamentals_value", "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2"]]
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
    
def mongo_statistic_backtest_update():
    universe = get_active_universe()
    bot_option_type = get_bot_option_type()
    backtest_data = get_bot_backtest(start_date=None, end_date=None, ticker=None, currency_code=None, bot_id=None)
    backtest_data = backtest_data[["ticker", "bot_id", "spot_price", "spot_date", "potential_max_loss", "targeted_profit", "bot_return"]]
    backtest_data.loc[backtest_data["bot_return"] >= 0, "event"] = "TP"
    backtest_data.loc[backtest_data["bot_return"] < 0, "event"] = "SL"
    backtest_data.loc[backtest_data["bot_return"] < 0, "event"] = "RUN"

    bot_statistic = get_bot_statistic_data(ticker=None, currency_code=None)
    
#     bot_data = change_date_to_str(bot_data)
#     for tick in universe["ticker"].unique():
#         result = bot_data.loc[bot_data["ticker"] == tick]
#         result = result.sort_values(by="ranking", ascending=False)
#         result = result[["potential_max_loss", "targeted_profit", "bot_type", "bot_option_type", "bot_option_name", "expiry_date", "ranking"]]
#         result = result.to_dict("records")
#         result = pd.DataFrame({"ticker":[tick], "bot_data":[result]}, index=[0])
#         update_to_mongo(data=result, index="ticker", table="bot_data", dict=False)
#     bot_backtest
# uid
# spot_date
# bot_type
# bot_id
# option_type
# time_to_exp
# spot_price
# potential_max_loss
# targeted_profit
# bot_return
# event
# ticker

#     bot_statistic
# uid
# option_type
# bot_type
# lookback
# time_to_exp
# avg_days
# pct_profit
# pct_losses
# avg_profit
# avg_loss
# avg_return
# pct_max_profit
# pct_max_loss
# ann_avg_return
# ann_avg_return_bm
# avg_return_bm
# max_loss_bot
# max_loss_bm
# avg_days_max_profit
# avg_days_max_loss
# ticker