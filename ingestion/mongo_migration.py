from bot.data_download import get_currency_data
import json
from asgiref.sync import sync_to_async
import numpy as np
import pandas as pd
from general.date_process import backdate_by_month
from general.mongo_query import (
    change_null_to_zero, 
    create_collection, 
    update_to_mongo, 
    change_date_to_str, 
    update_specific_to_mongo,
    get_price_data_firebase
    )
from general.sql_query import (
    get_active_currency, 
    get_active_universe,
    get_bot_type, 
    get_industry, 
    get_industry_group, 
    get_latest_price_data,
    get_latest_ranking,
    get_latest_ranking_rank_1, 
    get_master_tac_data,
    get_orders_position,
    get_orders_position_performance, 
    get_region, 
    get_universe_rating, 
    get_universe_rating_detail_history, 
    get_universe_rating_history,
    get_bot_backtest, 
    get_bot_option_type, 
    get_bot_statistic_data, 
    get_latest_bot_update_data,
    get_user_account_balance,
    get_user_core,
    get_user_profit_history,
    get_factor_calculation_formula,
    get_factor_current_used)
import asyncio
from asgiref.sync import sync_to_async
from typing import List



def NonetoZero(value):
    return value
    if value is None or value == np.nan:
        return 0
    else:
        return value

# def factor_column_name():
#     return ["earnings_yield_minmax_currency_code", "earnings_yield_minmax_industry",
#         "book_to_price_minmax_currency_code", "book_to_price_minmax_industry",
#         "ebitda_to_ev_minmax_currency_code", "ebitda_to_ev_minmax_industry",
#         "fwd_bps_minmax_currency_code", "fwd_bps_minmax_industry",
#         "fwd_ebitda_to_ev_minmax_currency_code", "fwd_ebitda_to_ev_minmax_industry",
#         "roe_minmax_currency_code", "roe_minmax_industry",
#         "roic_minmax_currency_code", "roic_minmax_industry",
#         "cf_to_price_minmax_currency_code", "cf_to_price_minmax_industry",
#         "eps_growth_minmax_currency_code", "eps_growth_minmax_industry",
#         "fwd_ey_minmax_industry", "fwd_ey_minmax_currency_code",
#         "fwd_roic_minmax_industry", "fwd_sales_to_price_minmax_industry",
#         "earnings_pred_minmax_industry", "earnings_pred_minmax_currency_code",
#         "momentum_minmax_currency_code",
#         "sales_to_price_minmax_currency_code", "sales_to_price_minmax_industry",
#         "fwd_sales_to_price_minmax_currency_code", "fwd_roic_minmax_currency_code",
#         "environment_minmax_currency_code", "social_minmax_currency_code",
#         "goverment_minmax_currency_code", "environment_minmax_industry",
#         "social_minmax_industry", "goverment_minmax_industry"]
#
# def fundamentals_value_factor_column_name():
#     return ["earnings_yield_minmax_currency_code", "earnings_yield_minmax_industry",
#         "book_to_price_minmax_currency_code", "book_to_price_minmax_industry",
#         "ebitda_to_ev_minmax_currency_code", "ebitda_to_ev_minmax_industry",
#         "fwd_bps_minmax_currency_code", "fwd_bps_minmax_industry",
#         "fwd_ebitda_to_ev_minmax_currency_code", "fwd_ebitda_to_ev_minmax_industry",
#         "roe_minmax_currency_code", "roe_minmax_industry"]
#
# def fundamentals_quality_factor_column_name():
#     return ["roic_minmax_currency_code", "roic_minmax_industry",
#         "cf_to_price_minmax_currency_code", "cf_to_price_minmax_industry",
#         "eps_growth_minmax_currency_code", "eps_growth_minmax_industry",
#         "fwd_ey_minmax_industry", "fwd_ey_minmax_currency_code",
#         "fwd_sales_to_price_minmax_industry", "fwd_roic_minmax_industry",
#         "earnings_pred_minmax_industry", "earnings_pred_minmax_currency_code"]
#
# def other_fundamentals_column_name():
#     return ["sales_to_price_minmax_currency_code", "sales_to_price_minmax_industry",
#         "fwd_sales_to_price_minmax_currency_code", "fwd_roic_minmax_currency_code",
#         "environment_minmax_currency_code", "social_minmax_currency_code",
#         "goverment_minmax_currency_code", "environment_minmax_industry",
#         "social_minmax_industry", "goverment_minmax_industry"]
#
# def fundamentals_value_factor_name():
#     return ["P/E", "P/E (ind.)", "Book Value", "Book Value (ind.)",
#         "EV/EBITDA", "EV/EBITDA (ind.)", "Forward Book Value", "Forward Book Value (ind.)",
#         "Forward EV/EBITDA", "Forward EV/EBITDA (ind.)", "ROE", "ROE (ind.)"]
#
# def fundamentals_quality_factor_name():
#     return ["ROIC", "ROIC (ind.)", "Free Cash Flow", "Free Cash Flow (ind.)",
#         "PEG ratio", "PEG ratio (ind.)", "Forward P/E","Forward P/E (ind.)",
#         "Forward Sales (ind.)", "Forward ROIC (ind.)", "Earning Momentum", "Earnings Momentum (ind.)"]
#
# def momentum_factor_name():
#     return ["Price Momentum"]
#
# def other_fundamentals_name():
#     return ["Sales", "Sales (ind.)", "Forward Sales", "Forward ROIC",
#         "Environment", "Environment (ind.)", "Social", "Social (ind.)",
#         "Goverment", "Goverment (ind.)"]

def factor_column_name_changes():
    ''' map factor name used in DB to name shown on APP '''

    name_df = get_factor_calculation_formula()
    name_df['index'] = np.where(name_df['scaler'].notnull(),
                                [f"{x}_{y}_currency_code" for x, y in name_df[["name","scaler"]].to_numpy()],
                                name_df["name"].values)
    name_dict = name_df.set_index('index')['app_name'].to_dict()
    esg_name = esg_factor_name_change()
    name_dict.update(esg_name)

    return name_dict

def esg_factor_name_change():
    return {"environment_minmax_currency_code": "Environment",
            "environment_minmax_industry": "Environment (ind.)",
            "social_minmax_currency_code": "Social",
            "social_minmax_industry": "Social (ind.)",
            "goverment_minmax_currency_code": "Goverment",
            "goverment_minmax_industry": "Goverment (ind.)"}

def rolling_apply(group, field):
    adjusted_price = [group[field].iloc[0]]
    for x in group.tac[:-1]:
        adjusted_price.append(adjusted_price[-1] *  x )
    group[field] = adjusted_price
    return group

def mongo_universe_update(ticker=None, currency_code=None):
    ''' update mongo for:
    1. APP "Details" from universe_rating

    1. ai_score from universe_rating
    '''

    # Populate Universe
    all_universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    industry = get_industry()
    industry_group = get_industry_group()
    result = all_universe.merge(industry, on="industry_code", how="left")
    result = result.merge(industry_group, on="industry_group_code", how="left")
    universe = result[["ticker"]]
    print(result)

    # detail_df of static info dict of {Companies Name, Industry, Currency, Description, Lot Size}
    result = change_null_to_zero(result)
    detail_df = pd.DataFrame({"ticker":[], "detail":[]}, index=[])
    for tick in universe["ticker"].unique():
        detail_data = result.loc[result["ticker"] == tick]
        detail_data = detail_data[["currency_code", "ticker_name", "ticker_fullname", "company_description", 
            "industry_code", "industry_name", "industry_group_code", "industry_group_name", "ticker_symbol", "lot_size", "mic"]].to_dict("records")
        details = pd.DataFrame({"ticker":[tick], "detail":[detail_data[0]]}, index=[0])
        detail_df = detail_df.append(details)
    detail_df = detail_df.reset_index(inplace=False)
    detail_df = detail_df.drop(columns=["index"])
    universe = universe.merge(detail_df, how="left", on=["ticker"])
    print(universe)

    # detail_df of financial info dict of {price, pe, pb, ...} <- from Universe
    price = result[["ticker", "ebitda", "free_cash_flow", "market_cap", "pb", "pe_forecast", "pe_ratio", "revenue_per_share", "wk52_high", "wk52_low"]]
    latest_price = get_latest_price_data(ticker=ticker, currency_code=currency_code)
    latest_price = price.merge(latest_price, how="left", on=["ticker"])
    latest_price = change_date_to_str(latest_price)
    latest_price = change_null_to_zero(latest_price)
    price_df = pd.DataFrame({"ticker":[], "price":[]}, index=[])
    for tick in latest_price["ticker"].unique():
        price_data = latest_price.loc[latest_price["ticker"] == tick]
        price_data = price_data[["ebitda", "free_cash_flow", "market_cap", 
        "pb", "pe_forecast", "pe_ratio", "revenue_per_share", "wk52_high", "wk52_low", 
        "open", "high", "low", "close", "intraday_date", "intraday_ask", "intraday_bid", 
        "latest_price_change", "intraday_time", "last_date", "capital_change", 
        "latest_price", "volume"]].to_dict("records")
        price = pd.DataFrame({"ticker":[tick], "price":[price_data[0]]}, index=[0])
        price_df = price_df.append(price)
    price_df = price_df.reset_index(inplace=False)
    price_df = price_df.drop(columns=["index"])
    universe = universe.merge(price_df, how="left", on=["ticker"])
    print(universe)

    # detail_df of financial info dict of {price, pe, pb, ...} <- from Universe
    rating = result[["ticker"]]
    universe_rating = get_universe_rating_history(ticker=ticker, currency_code=currency_code)
    universe_rating_detail = get_universe_rating_detail_history(ticker=ticker, currency_code=currency_code)
    universe_rating_detail = universe_rating_detail.merge(universe_rating[["ticker", "dlp_1m", "wts_rating"]], how="left", on=["ticker"])
    # universe_rating_detail = universe_rating_detail.rename(columns={"momentum" : "momentum_minmax_currency_code"})
    universe_rating_detail_df = pd.DataFrame({"ticker":[], "factor_name":[], "score":[]}, index=[])
    for col in factor_column_name_changes().keys():
        temp = universe_rating_detail[["ticker", col]]
        temp = temp.rename(columns={col : "score"})
        temp["factor_name"] = factor_column_name_changes()[col]
        universe_rating_detail_df = universe_rating_detail_df.append(temp)

    df = get_factor_current_used()

    universe_rating_positive_negative = pd.DataFrame({"ticker":[], "positive_factor":[], "negative_factor":[]}, index=[])
    for tick in universe_rating_detail_df["ticker"].unique():
        positive_factor = []
        negative_factor = []
        temp = universe_rating_detail_df.loc[universe_rating_detail_df["ticker"] == tick]

        lb, hb = (temp["score"].mean() - temp["score"].std()), (temp["score"].mean() + temp["score"].std())
        positive = temp.loc[temp["score"] > hb]
        positive = positive.sort_values(by=["score"], ascending=False)
        rule1 = positive.loc[positive["factor_name"].isin(fundamentals_value_factor_name())].head(2)
        rule2 = positive.loc[positive["factor_name"].isin(fundamentals_quality_factor_name())].head(2)
        rule3 = positive.loc[positive["factor_name"].isin(momentum_factor_name())].head(1)
        rule4 = positive.loc[positive["factor_name"].isin(other_fundamentals_name())].head(2)
        rule1 = rule1.append(rule2)
        rule1 = rule1.append(rule3)
        rule1 = rule1.append(rule4)

        negative = temp.loc[temp["score"] < lb]
        negative = negative.sort_values(by=["score"])
        rule1_min = negative.loc[negative["factor_name"].isin(fundamentals_value_factor_name())].head(2)
        rule2_min = negative.loc[negative["factor_name"].isin(fundamentals_quality_factor_name())].head(2)
        rule3_min = negative.loc[negative["factor_name"].isin(momentum_factor_name())].head(1)
        rule4_min = negative.loc[negative["factor_name"].isin(other_fundamentals_name())].head(2)
        rule1_min = rule1_min.append(rule2_min)
        rule1_min = rule1_min.append(rule3_min)
        rule1_min = rule1_min.append(rule4_min)
        for index, row in rule1.iterrows():
            positive_factor.append(row["factor_name"])
        for index, row in rule1_min.iterrows():
            negative_factor.append(row["factor_name"])
        positive_negative_result = pd.DataFrame({"ticker":[tick], "positive_factor":[positive_factor], "negative_factor":[negative_factor]}, index=[0])
        universe_rating_positive_negative = universe_rating_positive_negative.append(positive_negative_result)
    universe_rating = rating.merge(universe_rating, how="left", on=["ticker"])
    universe_rating = universe_rating.merge(universe_rating_positive_negative, how="left", on=["ticker"])
    universe_rating = change_date_to_str(universe_rating)
    universe_rating = change_null_to_zero(universe_rating)
    print(universe_rating)
    rating_df = pd.DataFrame({"ticker":[], "rating":[], "ai_score":[], "ai_score2":[]}, index=[])
    for tick in universe_rating["ticker"].unique():
        rating_data = universe_rating.loc[universe_rating["ticker"] == tick]
        rating_data["final_score"] = rating_data["ai_score"]
        ai_score = rating_data["ai_score"].to_list()[0]
        ai_score2 = rating_data["ai_score2"].to_list()[0]
        rating_data = rating_data[["final_score", "ai_score", "ai_score2", "fundamentals_quality", "fundamentals_value", 
        "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2", "esg", "momentum", "technical", "positive_factor", "negative_factor"]].to_dict("records")
        rating = pd.DataFrame({"ticker":[tick], "rating":[rating_data[0]], "ai_score":[ai_score], "ai_score2":[ai_score2]}, index=[0])
        rating_df = rating_df.append(rating)
    rating_df = rating_df.reset_index(inplace=False, drop=True)
    universe = universe.merge(rating_df, how="left", on=["ticker"])
    universe = universe.sort_values(by=["ai_score", "ticker"], ascending=[False, True])
    universe = universe.reset_index(inplace=False)
    universe = universe.drop(columns=["index", "ai_score", "ai_score2"])
    universe = universe.reset_index(inplace=False, drop=True)
    print(universe)

    # bot ranking & statistics
    ranking = result[["ticker"]]
    duration_list = ["2 Weeks", "4 Weeks"]
    bot_ranking = get_latest_ranking(ticker=ticker, currency_code=currency_code)
    bot_type = get_bot_type()[["bot_type", "bot_apps_name", "bot_apps_description"]]
    bot_option_type = get_bot_option_type()[["bot_id", "duration"]]
    bot_ranking = bot_ranking.merge(bot_type, how="left", on=["bot_type"])
    bot_ranking = bot_ranking.merge(bot_option_type, how="left", on=["bot_id"])
    bot_ranking = bot_ranking[["ticker", "bot_id", "ranking", "bot_type", "bot_option_type", "bot_apps_name", "bot_apps_description", "duration", "time_to_exp", "time_to_exp_str"]]
    bot_ranking = bot_ranking.loc[bot_ranking["duration"].isin(duration_list)]
    bot_ranking["duration"] = bot_ranking["duration"].str.replace(" ", "-", regex=True)
    bot_ranking = bot_ranking.sort_values(by=["ticker", "duration", "ranking"])
    bot_ranking = bot_ranking.drop_duplicates(subset=["ticker", "duration", "bot_type"], keep="first")

    bot_statistic = get_bot_statistic_data(ticker=ticker, currency_code=currency_code)
    bot_statistic = bot_statistic.rename(columns={"option_type" : "bot_option_type"})
    bot_statistic = bot_statistic[["ticker", "time_to_exp", "lookback", "bot_type", "bot_option_type", "pct_profit", "avg_return", "avg_loss"]]
    time_to_exp = bot_ranking["time_to_exp"].unique().tolist()
    bot_statistic = bot_statistic.loc[bot_statistic["time_to_exp"].isin(time_to_exp)]
    bot_statistic = bot_statistic.loc[bot_statistic["lookback"] == 6]
    bot_statistic = bot_statistic.reset_index(inplace=False, drop=True)
    bot_statistic["pct_profit"] = np.where(bot_statistic["pct_profit"].isnull(), 0, bot_statistic["pct_profit"])
    bot_statistic["avg_return"] = np.where(bot_statistic["avg_return"].isnull(), 0, bot_statistic["avg_return"])
    bot_statistic["avg_loss"] = np.where(bot_statistic["avg_loss"].isnull(), 0, bot_statistic["avg_loss"])
    bot_statistic["win_rate"] = bot_statistic["pct_profit"]
    for index, row in bot_statistic.iterrows():
        bot_statistic.loc[index, "bot_return"] = max(min(row["avg_return"], 0.3), -0.2) / 0.5
        bot_statistic.loc[index, "risk_moderation"] = max(0.3 + row["avg_loss"], 0) / 0.3
    bot_ranking = bot_ranking.merge(bot_statistic[["ticker", "time_to_exp", "bot_type", 
        "bot_option_type", "win_rate", "bot_return", "risk_moderation"]], 
        how="left", on=["ticker", "bot_type", "bot_option_type", "time_to_exp"])
    print(bot_ranking)
    ranking = pd.DataFrame({"ticker":[], "ranking":[]}, index=[])
    for tick in bot_ranking["ticker"].unique():
        ranking_data = bot_ranking.loc[bot_ranking["ticker"] == tick]
        ranking_data = ranking_data.sort_values(by=["ticker", "ranking", "duration"])
        print(ranking_data)
        ranking_df = []
        for index, row in ranking_data.iterrows():
            rank_data = ranking_data.loc[ranking_data["duration"] == row["duration"]]
            rank_data = rank_data.loc[rank_data["bot_id"] == row["bot_id"]]
            rank_data = rank_data[["bot_id", "bot_apps_name", "bot_apps_description", "duration", "win_rate", "bot_return", "risk_moderation"]]
            rank_data = rank_data.to_dict("records")[0]
            ranking_df.append(rank_data)
        rank = pd.DataFrame({"ticker":[tick], "ranking":[ranking_df]}, index=[0])
        ranking = ranking.append(rank)
    ranking = ranking.reset_index(inplace=False, drop=True)
    print(ranking)
    universe = universe.merge(ranking, how="left", on=["ticker"])
    universe = universe.reset_index(inplace=False, drop=True)
    print(universe)
    update_to_mongo(data=universe, index="ticker", table="universe", dict=False)


async def gather_task(position_data:pd.DataFrame,bot_option_type:pd.DataFrame,user_core:pd.DataFrame)-> List[pd.DataFrame]:
    tasks=[]
    users= user_core["user_id"].unique().tolist()
    for user in users:
        tasks.append(asyncio.ensure_future(do_task(
            position_data,bot_option_type,user,user_core
        )))
    active_portfolio = await asyncio.gather(*tasks)
    return active_portfolio
        

async def do_task(position_data:pd.DataFrame,bot_option_type:pd.DataFrame,user:str,user_core:pd.DataFrame)->pd.DataFrame:
        user_core =  user_core.loc[user_core["user_id"] == user]
        
        orders_position = position_data.loc[position_data["user_id"] == user]

        orders_position = orders_position.reset_index(inplace=False)
        #TOP LEVEL
        orders_position["status"] = "LIVE"
        orders_position["current_values"] = (orders_position["bot_cash_balance"] + (orders_position["share_num"] * orders_position["price"]))
        orders_position["current_ivt_amt"] = (orders_position["share_num"] * orders_position["price"])
        #MARGIN
        # orders_position["margin_amount"] = (orders_position["margin"] * orders_position["investment_amount"]) - orders_position["investment_amount"]
        orders_position["margin_amount"] = (orders_position["margin"] - 1) * orders_position["investment_amount"]
        orders_position["bot_cash_balance"] = orders_position["bot_cash_balance"] + orders_position["margin_amount"]
        orders_position["threshold"] = (orders_position["margin_amount"] + orders_position["investment_amount"]) - orders_position["bot_cash_balance"]

        #PROFIT
        # orders_position["profit"] = orders_position["investment_amount"] - orders_position["current_values"]
        # orders_position["pct_profit"] =  orders_position["profit"] / orders_position["investment_amount"]
        orders_position["profit"] =orders_position["current_values"] - orders_position["investment_amount"]
        orders_position["pct_profit"] =  orders_position["profit"] / orders_position["investment_amount"] 

        #BOT POSITION
        # orders_position["pct_cash"] =  (orders_position["bot_cash_balance"] / orders_position["investment_amount"] * 100).round(0)
        # orders_position["pct_stock"] =  100 - orders_position["pct_cash"]
        orders_position["pct_cash"] =  (orders_position["bot_cash_balance"] / orders_position["current_values"] * 100).round(0)
        orders_position["pct_stock"] =  100 - orders_position["pct_cash"]

        #USER POSITION
        # total_invested_amount = sum(orders_position["investment_amount"].to_list())
        # total_bot_invested_amount	= sum(orders_position.loc[orders_position["bot_id"] != "STOCK_stock_0"]["investment_amount"].to_list())
        # total_user_invested_amount = sum(orders_position.loc[orders_position["bot_id"] == "STOCK_stock_0"]["investment_amount"].to_list())
        # pct_total_bot_invested_amount = int(round(total_bot_invested_amount / total_invested_amount, 0) * 100)
        # pct_total_user_invested_amount = int(round(total_user_invested_amount / total_invested_amount, 0) * 100)
        # total_profit_amount = sum(orders_position["profit"].to_list())
        total_invested_amount = NonetoZero(sum(orders_position["current_values"].to_list()))
        total_bot_invested_amount = NonetoZero(sum(orders_position.loc[orders_position["bot_id"] != "STOCK_stock_0"]["current_values"].to_list()))
        total_user_invested_amount = NonetoZero(sum(orders_position.loc[orders_position["bot_id"] == "STOCK_stock_0"]["current_values"].to_list()))
        pct_total_bot_invested_amount = NonetoZero(int(round(total_bot_invested_amount / total_invested_amount, 0) * 100))
        pct_total_user_invested_amount = NonetoZero(int(round(total_user_invested_amount / total_invested_amount, 0) * 100))
        total_profit_amount = NonetoZero(sum(orders_position["profit"].to_list()))

        active_df = []
        orders_position = change_date_to_str(orders_position)
        # orders_position_field = "position_uid, bot_id, ticker, expiry, spot_date, bot_cash_balance, margin, entry_price, investment_amount, user_id"
        # orders_position = get_orders_position(user_id=[user], active=False, field=orders_position_field)
        orders_position = orders_position.sort_values(by=["profit"])
        for index, row in orders_position.iterrows():
            act_df = orders_position.loc[orders_position["position_uid"] == row["position_uid"]]
            bot = bot_option_type.loc[bot_option_type["bot_id"] == row["bot_id"]][["bot_id", "bot_option_type", "bot_apps_name", "bot_apps_description", "duration"]]
            act_df = act_df.reset_index(inplace=False)
            act_df.loc[0, "bot_details"] = bot.to_dict("records")
            act_df["position_uid"] = act_df["position_uid"].astype(str)
            act_df["order_uid"] = act_df["order_uid"].astype(str)
            act_df = act_df.drop(columns=["bot_id", "bot_apps_name", "duration"])
            act_df = act_df.to_dict("records")[0]
            active_df.append(act_df)
        active = pd.DataFrame({"user_id":[user], "total_invested_amount":[total_invested_amount], "total_bot_invested_amount":[total_bot_invested_amount], 
            "total_user_invested_amount":[total_user_invested_amount], "pct_total_bot_invested_amount":[pct_total_bot_invested_amount], "pct_total_user_invested_amount":[pct_total_user_invested_amount], 
            "total_profit_amount":[total_profit_amount], "active_portfolio":[active_df]}, index=[0])
        result = user_core.merge(active, how="left", on=["user_id"])
        result = result.rename(columns={"currency_code" : "currency"})
        
        await sync_to_async(update_to_mongo)(data=result, index="user_id", table="portfolio", dict=False)

        return active


def firebase_user_update(user_id=None, currency_code=None):
    # import time
    # start = time.time()

    print("Start User Populate")
    bot_type = get_bot_type()
    bot_option_type = get_bot_option_type()
    bot_option_type = bot_option_type.merge(bot_type, how="left", on=["bot_type"])
    currency = get_currency_data(currency_code=currency_code)
    currency = currency[["currency_code", "is_decimal"]]

    user_core = get_user_core(currency_code=currency_code, user_id=user_id, field="id as user_id, username")
    user_daily_profit = get_user_profit_history(user_id=user_id, field="user_id, daily_profit, daily_profit_pct")
    user_balance = get_user_account_balance(currency_code=currency_code, user_id=user_id, field="user_id, amount as balance, currency_code")
    user_core = user_core.merge(user_balance, how="left", on=["user_id"])
    user_core = user_core.merge(user_daily_profit, how="left", on=["user_id"])
    user_core = user_core.merge(currency, how="left", on=["currency_code"])
    user_core["balance"] = np.where(user_core["balance"].isnull(), 0, user_core["balance"])
    user_core.loc[user_core["is_decimal"] == True, "balance"] = round(user_core.loc[user_core["is_decimal"] == True, "balance"], 2)
    user_core.loc[user_core["is_decimal"] == False, "balance"] = round(user_core.loc[user_core["is_decimal"] == False, "balance"], 0)
    print(user_core)

    orders_position_field = "position_uid, bot_id, ticker, expiry, spot_date, bot_cash_balance, margin, entry_price, investment_amount, user_id"
    position_data = get_orders_position(user_id=user_core["user_id"].to_list(), active=True, field=orders_position_field)
    
    universe = get_active_universe(ticker = position_data["ticker"].unique())[["ticker", "ticker_name"]]
    latest_price = get_price_data_firebase(position_data["ticker"].unique().tolist())
    print(latest_price)
    latest_price = latest_price.rename(columns={"last_date" : "trading_day", "latest_price" : "price"})

    position_data = position_data.merge(latest_price, how="left", on=["ticker"])
    position_data["price"] = np.where(position_data["price"].isnull(), position_data["entry_price"], position_data["price"])
    position_data["trading_day"] = np.where(position_data["trading_day"].isnull(), position_data["spot_date"], position_data["trading_day"])
    position_data = position_data.merge(universe, how="left", on=["ticker"])

    orders_performance_field = "position_uid, share_num, order_uid"
    performance_data = get_orders_position_performance(position_uid=position_data["position_uid"].to_list(), field=orders_performance_field, latest=True)
    position_data = position_data.merge(performance_data, how="left", on=["position_uid"])
    position_data = position_data.merge(bot_option_type[["bot_id", "bot_apps_name", "duration"]], how="left", on=["bot_id"])

    # print(position_data)
    active_portfolio = pd.DataFrame({"user_id":[], "total_invested_amount":[], "total_bot_invested_amount":[], "total_user_invested_amount":[], 
        "pct_total_bot_invested_amount":[], "pct_total_user_invested_amount":[], "total_profit_amount":[], "active_portfolio":[]}, index=[])
    
    
    # concurent calculation
    gather_active_portfolios = asyncio.run(gather_task(
        position_data,bot_option_type ,user_core
        ))
    # end = time.time()
    # print(f"time consumed : {end-start}")


    # active_portfolio = pd.concat(gather_active_portfolios)
    # active_portfolio = active_portfolio.reset_index(inplace=False, drop=True)
    # result = user_core.merge(active_portfolio, how="left", on=["user_id"])
    # result = result.rename(columns={"currency_code" : "currency"})
    # print(result)
    # update_to_mongo(data=result, index="user_id", table="portfolio", dict=False)










    

# def mongo_create_currency():
#     collection = json.load(open("files/file_json/validator_currency.json"))
#     print(collection)
#     table = "currency"
#     create_collection(collection, table)

# def mongo_currency_update():
#     region = get_region()
#     region = region.rename(columns={"ingestion_time" : "region_time"})
#     currency = get_active_currency()
#     result = currency.merge(region, on="region_id", how="left")
#     result = result[["currency_code", "currency_name", "last_price", "utc_offset", "utc_timezone_location", 
#         "classic_schedule", "region_id", "region_name", "region_time"]]
#     result = result.rename(columns={"classic_schedule" : "close_time", "utc_timezone_location" : "timezone"})
#     print(result)
#     update_to_mongo(data=result, index="currency_code", table="currency", dict=False)

# def mongo_universe_update():
#     #Populate Universe
#     universe = get_active_universe()
#     industry = get_industry()
#     industry_group = get_industry_group()
#     result = universe.merge(industry, on="industry_code", how="left")
#     result = result.merge(industry_group, on="industry_group_code", how="left")
    
#     result = result[["ticker", "currency_code", "ticker_name", "company_description", "industry_code", "industry_group_img", 
#     "ticker_symbol", "exchange_code", "quandl_symbol", "lot_size", "is_active"]]
#     # ["isin", "cusip", "sedol", "permid", origin_ticker"]
#     #Populate Price
#     isin = get_consolidated_data("consolidated_ticker as ticker, isin", "isin is not null", "consolidated_ticker, isin")
#     isin = isin.drop_duplicates(subset=["ticker"], keep="first")

#     cusip = get_consolidated_data("consolidated_ticker as ticker, cusip", "cusip is not null", "consolidated_ticker, cusip")
#     cusip = cusip.drop_duplicates(subset=["ticker"], keep="first")

#     sedol = get_consolidated_data("consolidated_ticker as ticker, sedol", "sedol is not null", "consolidated_ticker, sedol")
#     sedol = sedol.drop_duplicates(subset=["ticker"], keep="first")

#     permid = get_consolidated_data("consolidated_ticker as ticker, permid", "permid is not null", "consolidated_ticker, permid")
#     permid = permid.drop_duplicates(subset=["ticker"], keep="first")

#     origin_ticker = get_consolidated_data("consolidated_ticker as ticker, origin_ticker, source_id", "origin_ticker is not null", "consolidated_ticker, origin_ticker, source_id")

#     result = result.merge(isin, on="ticker", how="left")
#     result = result.merge(cusip, on="ticker", how="left")
#     result = result.merge(sedol, on="ticker", how="left")
#     result = result.merge(permid, on="ticker", how="left")

#     origin_ticker_df = pd.DataFrame({"ticker":[], "origin_ticker":[]}, index=[])
#     for tick in result["ticker"].unique():
#         ticker_data = origin_ticker.loc[origin_ticker["ticker"] == tick]
#         if(len(ticker_data) > 0):
#             ticker_data = ticker_data[["origin_ticker", "source_id"]].to_dict("records")
#             ticker_data = pd.DataFrame({"ticker":[tick], "origin_ticker":[ticker_data]}, index=[0])
#             origin_ticker_df = origin_ticker_df.append(ticker_data)
#     result = result.merge(origin_ticker_df, on="ticker", how="left")
#     print(result)
#     update_to_mongo(data=result, index="ticker", table="universe", dict=False)

# def mongo_universe_rating_update():
#     #Populate Universe
#     rating = get_universe_rating()
#     result = rating[["ticker", "fundamentals_quality", "fundamentals_value", "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2"]]
#     result["ST"] = np.where((result["wts_rating"] + result["dlp_1m"]) >= 11, True, False)
#     result["MT"] = np.where(result["dlp_3m"] >= 7, True, False)
#     result["GQ"] = np.where(result["fundamentals_quality"] >= 5, True, False)
#     result["GV"] = np.where(result["fundamentals_value"] >= 5, True, False)
#     print(result)
#     update_to_mongo(data=result, index="ticker", table="universe_rating", dict=False)

# def mongo_latest_price_update():
#     result = get_latest_price_data()
#     result = result[["ticker", "close", "latest_price_change", "last_date"]]
#     print(result)
#     update_specific_to_mongo(data=result, index="ticker", table="universe", column=["close", "latest_price_change", "intraday_ask", "intraday_bid", "last_date"], dict=False)

# def mongo_price_update():
#     start_date = backdate_by_month(1)
#     ticker = ["AAPL.O"]
#     result = get_master_tac_data(start_date=start_date, ticker=ticker)
#     result = result[["ticker", "trading_day", "tri_adj_close", "day_status"]]
#     result = result.rename(columns={"tri_adj_close" : "price"})
#     result = change_date_to_str(result)
#     print(result["ticker"].unique())
#     for tick in result["ticker"].unique():
#         price_data = result.loc[result["ticker"] == tick]
#         price_data = price_data.sort_values(by="trading_day", ascending=False)
#         price_data = price_data[["trading_day", "price"]].to_dict("records")
#         price_data = pd.DataFrame({"ticker":[tick], "price_data":[price_data]}, index=[0])
#         print(price_data)
#         update_specific_to_mongo(data=result, index="ticker", table="universe", column=["price_data"], dict=False)

# def mongo_bot_data_update():
#     universe = get_active_universe()
#     bot_data = get_latest_bot_update_data()
#     for time_exp in bot_data["time_to_exp_str"].unique():
#         bot_data.loc[bot_data["time_to_exp_str"] == time_exp, "expiry_date"] = bot_data[f"expiry_{time_exp}"]
#     bot_data = bot_data[["ticker", "bot_id", "potential_max_loss", "targeted_profit", "expiry_date", "ranking"]]
#     print(bot_data)
#     bot_option_type = get_bot_option_type()
#     bot_data = bot_data.merge(bot_option_type, on="bot_id", how="left")
#     bot_data = change_date_to_str(bot_data)
#     for tick in universe["ticker"].unique():
#         result = bot_data.loc[bot_data["ticker"] == tick]
#         result = result.sort_values(by="ranking", ascending=False)
#         result = result[["potential_max_loss", "targeted_profit", "bot_type", "bot_option_type", "bot_option_name", "expiry_date", "ranking"]]
#         result = result.to_dict("records")
#         result = pd.DataFrame({"ticker":[tick], "bot_data":[result]}, index=[0])
#         update_to_mongo(data=result, index="ticker", table="bot_data", dict=False)
    
# def mongo_statistic_backtest_update():
#     universe = get_active_universe()
#     bot_option_type = get_bot_option_type()
#     backtest_data = get_bot_backtest(start_date=None, end_date=None, ticker=None, currency_code=None, bot_id=None)
#     backtest_data = backtest_data[["ticker", "bot_id", "spot_price", "spot_date", "potential_max_loss", "targeted_profit", "bot_return"]]
#     backtest_data.loc[backtest_data["bot_return"] >= 0, "event"] = "TP"
#     backtest_data.loc[backtest_data["bot_return"] < 0, "event"] = "SL"
#     backtest_data.loc[backtest_data["bot_return"].isnull(), "event"] = "RUN"
#     bot_statistic = get_bot_statistic_data(ticker=None, currency_code=None)
    
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


# start_date = backdate_by_month(1)
#     price = get_master_tac_data(start_date=start_date)
#     price = price[["ticker", "trading_day", "tri_adj_close", "day_status"]]
#     price = price.rename(columns={"tri_adj_close" : "price"})
#     price = change_date_to_str(price)
#     print(price)
#     price_df = pd.DataFrame({"ticker":[], "price_data":[]}, index=[])
#     for tick in result["ticker"].unique():
#         price_data = price.loc[price["ticker"] == tick]
#         price_data = price_data.sort_values(by="trading_day", ascending=False)
#         price_data = price_data[["trading_day", "price"]].to_dict("records")
#         price_data = pd.DataFrame({"ticker":[tick], "price_data":[price_data]}, index=[0])
#         price_df = price_df.append(price_data)
#     result = result.merge(price_df, on="ticker", how="left")
#     print(result)