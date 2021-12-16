from general.date_process import datetimeNow
from core.user.convert import ConvertMoney
import random
import numpy as np
import pandas as pd
from core.djangomodule.general import formatdigit
from django.conf import settings
from typing import List
import asyncio
from asgiref.sync import sync_to_async
from general.data_process import NoneToZero, dateNow
from bot.data_download import get_currency_data
from asgiref.sync import sync_to_async
from general.slack import report_to_slack, report_to_slack_factor
from general.firestore_query import (
    change_null_to_zero,
    delete_firestore_universe,
    delete_firestore_user,
    get_all_portfolio_from_firestore,
    get_all_universe_from_firestore, 
    update_to_firestore, 
    change_date_to_str, 
    get_price_data_firebase
    )
from general.sql_query import (
    get_active_currency, 
    get_active_universe,
    get_all_universe,
    get_all_user_core,
    get_bot_type, 
    get_industry, 
    get_industry_group, 
    get_latest_price_data,
    get_latest_ranking,
    get_orders_group_by_user_id,
    get_orders_position,
    get_orders_position_performance,
    get_universe_rating_detail_history, 
    get_universe_rating_history,
    get_bot_option_type, 
    get_bot_statistic_data, 
    get_user_account_balance,
    get_user_core,
    get_user_profit_history,
    get_factor_calculation_formula,
    get_factor_current_used)
from es_logging.logger import log2es

def firebase_user_delete():
    user = get_user_core()
    user = user.loc[user["is_joined"] == True]
    user = user.loc[user["current_status"] == "verified"]
    user = user["id"].to_list()

    firebase_portfolio = get_all_portfolio_from_firestore()
    firebase_delete = firebase_portfolio.loc[~firebase_portfolio["user_id"].isin(user)]["user_id"].to_list()
    for user_id in firebase_delete:
        delete_firestore_user(str(user_id))

def firebase_universe_delete():
    universe = get_all_universe(active=True)
    active_universe = universe.loc[universe["is_active"] == True]["ticker"].to_list()

    firebase_universe = get_all_universe_from_firestore()
    firebase_delete = firebase_universe.loc[~firebase_universe["ticker"].isin(active_universe)]["ticker"].to_list()

    for ticker in firebase_delete:
        delete_firestore_universe(ticker)

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


@log2es("ai_score")
def firebase_universe_update(ticker=None, currency_code=None,update_firebase=True):

    ''' update mongo for:
    1. static information
    2. price/financial ratios
    3. positive / negative factors
    4. ai_ratings
    5. bot informations
    '''
    if(update_firebase):
        firebase_universe_delete()
    # Populate Universe
    all_universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    currency = get_active_currency(currency_code=currency_code)

    all_universe = all_universe.loc[~all_universe["ticker"].isin(currency["index_ticker"].to_list())]
    all_universe = all_universe.loc[~all_universe["ticker"].isin(currency["etf_ticker"].to_list())]
    all_universe = all_universe.loc[~all_universe["ticker"].isin([".NDX"])]
    
    currency = currency[["currency_code", "country"]]
    industry = get_industry()
    industry_group = get_industry_group()
    result = all_universe.merge(industry, on="industry_code", how="left")
    result = result.merge(currency, on="currency_code", how="left")
    result = result.merge(industry_group, on="industry_group_code", how="left")
    universe = result[["ticker"]]

    # 1. static info dict of {Companies Name, Industry, Currency, Description, Lot Size}
    result = change_null_to_zero(result)
    detail_df = pd.DataFrame({"ticker":[], "detail":[]}, index=[])
    for tick in universe["ticker"].unique():
        detail_data = result.loc[result["ticker"] == tick]
        detail_data = detail_data[["currency_code", "ticker_name", "ticker_fullname", "company_description",
            "industry_code", "industry_name", "industry_group_code", "industry_group_name", "ticker_symbol", 
            "schi_name", "tchi_name", "lot_size", "mic", "country"]].to_dict("records")
        details = pd.DataFrame({"ticker":[tick], "detail":[detail_data[0]]}, index=[0])
        detail_df = detail_df.append(details)
    detail_df = detail_df.reset_index(inplace=False)
    detail_df = detail_df.drop(columns=["index"])
    universe = universe.merge(detail_df, how="left", on=["ticker"])
    # print(universe)

    # 2. detail_df of financial info dict of {price, pe, pb, ...} <- from Universe
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

    # 3. positive / negative factor -> factor = factor used in ai_score calculation
    rating = result[["ticker"]]
    universe_rating = get_universe_rating_history(ticker=ticker, currency_code=currency_code)
    universe_rating_detail = get_universe_rating_detail_history(ticker=ticker, currency_code=currency_code)
    universe_rating_detail = universe_rating_detail.merge(universe_rating[["ticker", "dlp_1m", "wts_rating"]], how="left", on=["ticker"])

    universe_rating_positive_negative = pd.DataFrame({"ticker":[], "positive_factor":[], "negative_factor":[]}, index=[])
    name_map = factor_column_name_changes()

    # factor currently used in ai_score calculation
    factor_use = get_factor_current_used().transpose().to_dict(orient='list')
    for cur, v in factor_use.items():   # work on different currency separately -> can use different factors
        lst = [x for x in ','.join(v).split(',') if len(x) > 0]
        lst = list(set(lst))
        lst += [x + '_minmax_currency_code' for x in lst]

        # select dataframe for given industry
        curr_ticker = result.loc[result['currency_code']==cur,'ticker'].to_list()
        curr_details = universe_rating_detail.loc[universe_rating_detail['ticker'].isin(curr_ticker)]

        # unstack dataframe
        cols = list(set(lst) & set(universe_rating_detail.columns.to_list()))
        curr_details = curr_details.set_index(["ticker"])[cols].unstack().reset_index()
        curr_details.columns = ["factor_name", "ticker", "score"]
        curr_details["factor_name"] = curr_details["factor_name"].map(name_map)

        # rules for positive / negative factors
        curr_details['score'] = pd.to_numeric(curr_details['score'])
        curr_details = curr_details.dropna(how='any')
        curr_des = curr_details.groupby(['factor_name'])['score'].agg(['mean','std'])
        curr_pos = (curr_des['mean'] + 0.4*curr_des['std']).to_dict()       # positive = factors > mean + 0.4std
        curr_neg = (curr_des['mean'] - 0.4*curr_des['std']).to_dict()       # negative = factors < mean - 0.4std

        for tick in curr_details['ticker'].unique():
            positive_factor = []
            negative_factor = []
            temp = curr_details.loc[curr_details["ticker"] == tick]

            positive = temp.loc[temp["score"] > temp["factor_name"].map(curr_pos)]
            positive = positive.sort_values(by=["score"], ascending=False).head(5)
            # if len(positive) == 0:
            #     positive = temp.nlargest(1,'score')     # if no factor > mean + 0.4*std -> use highest score one as pos

            negative = temp.loc[temp["score"] < temp["factor_name"].map(curr_neg)]
            negative = negative.sort_values(by=["score"]).head(5)
            # if len(negative) == 0:
            #     positive = temp.nsmallest(1,'score')

            for index, row in positive.iterrows():
                positive_factor.append(row["factor_name"])      # positive/negative only first 5 factor
            for index, row in negative.iterrows():
                negative_factor.append(row["factor_name"])
            positive_negative_result = pd.DataFrame({"ticker":[tick], "positive_factor":[positive_factor], "negative_factor":[negative_factor]}, index=[0])
            universe_rating_positive_negative = universe_rating_positive_negative.append(positive_negative_result)

    universe_rating = rating.merge(universe_rating, how="left", on=["ticker"])
    universe_rating = universe_rating.merge(universe_rating_positive_negative, how="left", on=["ticker"])
    universe_rating = change_date_to_str(universe_rating)
    universe_rating = change_null_to_zero(universe_rating)
    # print(universe_rating)

    # 4. ai_ratings
    rating_df = pd.DataFrame({"ticker":[], "rating":[], "ai_score":[], "ai_score2":[]}, index=[])
    for tick in universe_rating["ticker"].unique():
        rating_data = universe_rating.loc[universe_rating["ticker"] == tick]
        rating_data["final_score"] = rating_data["ai_score"]
        ai_score = rating_data["ai_score"].to_list()[0]
        ai_score2 = rating_data["ai_score2"].to_list()[0]
        rating_data = rating_data[["final_score", "ai_score", "ai_score2", "fundamentals_quality", "fundamentals_value", "fundamentals_extra",
        "dlp_1m", "dlp_3m", "wts_rating", "wts_rating2", "esg", "positive_factor", "negative_factor"]].to_dict("records")
        rating = pd.DataFrame({"ticker":[tick], "rating":[rating_data[0]], "ai_score":[ai_score], "ai_score2":[ai_score2]}, index=[0])
        rating_df = rating_df.append(rating)
    rating_df = rating_df.reset_index(inplace=False, drop=True)
    universe = universe.merge(rating_df, how="left", on=["ticker"])
    universe = universe.sort_values(by=["ai_score", "ticker"], ascending=[False, True])
    universe = universe.reset_index(inplace=False)
    universe = universe.drop(columns=["index", "ai_score", "ai_score2"])
    universe = universe.reset_index(inplace=False, drop=True)
    # print(universe)

    # 3/4 testing: What tickers has no postive/negative tickers?
    for cur in factor_use.keys():
        curr_ticker = result.loc[result['currency_code'] == cur, 'ticker'].to_list()
        df_cur = universe_rating.loc[universe_rating['ticker'].isin(list(curr_ticker))]
        for i in ['positive_factor', 'negative_factor']:
            df = df_cur.loc[df_cur[i].astype(str) == '[]']
            if cur in ['USD', 'HKD']:
                if(update_firebase):
                    report_to_slack_factor("*{} : === [{}] without {}: {}/{} ===*".format(dateNow(), cur, i, len(df), len(df_cur)))
                    report_to_slack_factor('```'+', '.join(["{0:<10}: {1:<5}".format(x,y) for x, y in df[['ticker', 'ai_score']].values])+'```')

    # 5. bot ranking & statistics
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
    bot_statistic = bot_statistic[["ticker", "time_to_exp", "lookback", "bot_type", "bot_option_type", "pct_profit", "avg_profit", "avg_loss"]]
    time_to_exp = bot_ranking["time_to_exp"].unique().tolist()
    bot_statistic = bot_statistic.loc[bot_statistic["time_to_exp"].isin(time_to_exp)]
    bot_statistic = bot_statistic.loc[bot_statistic["lookback"] == 6]
    bot_statistic = bot_statistic.reset_index(inplace=False, drop=True)
    bot_statistic["pct_profit"] = np.where(bot_statistic["pct_profit"].isnull(), float(0), bot_statistic["pct_profit"])
    bot_statistic["avg_profit"] = np.where(bot_statistic["avg_profit"].isnull(), float(0), bot_statistic["avg_profit"])
    bot_statistic["avg_loss"] = np.where(bot_statistic["avg_loss"].isnull(), float(0), bot_statistic["avg_loss"])
    bot_statistic["win_rate"] = bot_statistic["pct_profit"]
    for index, row in bot_statistic.iterrows():
        if(row["bot_type"] == "CLASSIC"):
            avg_profit = row["avg_profit"]
            avg_loss = row["avg_loss"]
        else:
            avg_profit = row["avg_profit"] * 2
            avg_loss = row["avg_loss"] * 2
        bot_statistic.loc[index, "bot_return"] = max(min(avg_profit, 0.5), 0) / 0.5
        bot_statistic.loc[index, "risk_moderation"] = max(0.5 + avg_loss, float(0)) / 0.5
    bot_ranking = bot_ranking.merge(bot_statistic[["ticker", "time_to_exp", "bot_type", 
        "bot_option_type", "win_rate", "bot_return", "risk_moderation"]], 
        how="left", on=["ticker", "bot_type", "bot_option_type", "time_to_exp"])
    ranking = pd.DataFrame({"ticker":[], "ranking":[]}, index=[])
    for tick in bot_ranking["ticker"].unique():
        ranking_data = bot_ranking.loc[bot_ranking["ticker"] == tick]
        ranking_data = ranking_data.sort_values(by=["ticker", "ranking", "duration"])
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
    universe = universe.merge(ranking, how="left", on=["ticker"])
    universe = universe.reset_index(inplace=False, drop=True)
    universe = change_date_to_str(universe)
    universe = universe.reset_index(inplace=False, drop=True)
    if(update_firebase):
        update_to_firestore(data=universe, index="ticker", table=settings.FIREBASE_COLLECTION['universe'], dict=False)
        report_to_slack("{} : === FIREBASE UNIVERSE UPDATED ===".format(datetimeNow()))
    else:
        return universe


async def gather_task(position_data:pd.DataFrame,bot_option_type:pd.DataFrame,user_core:pd.DataFrame, update_firebase=True)-> List[pd.DataFrame]:
    tasks=[]
    users= user_core["user_id"].unique().tolist()
    for user in users:
        tasks.append(
            asyncio.ensure_future(
            do_task(position_data, bot_option_type, user, user_core, update_firebase=update_firebase)
            ))
    active_portfolio = await asyncio.gather(*tasks)
    return active_portfolio
        

async def do_task(position_data:pd.DataFrame, bot_option_type:pd.DataFrame, user:str, user_core:pd.DataFrame, update_firebase=True)->pd.DataFrame:
        user_core =  user_core.loc[user_core["user_id"] == user]
        user_core = user_core.reset_index(inplace=False, drop=True)
        rounded = 0
        if(user_core.loc[0, "is_decimal"]):
            rounded = 2
        orders_position = position_data.loc[position_data["user_id"] == user]
        if(len(orders_position) > 0):
            orders_position = orders_position.reset_index(inplace=False, drop=True)
            #TOP LEVEL
            orders_position["margin"] = orders_position["margin"].astype(int)
            orders_position["status"] = "LIVE"
            orders_position["current_ivt_amt"] = (orders_position["share_num"] * orders_position["price"]).round(rounded)
            orders_position["current_values"] = orders_position["bot_cash_balance"] + orders_position["current_ivt_amt"]
            orders_position["current_ivt_amt"] = (orders_position["current_ivt_amt"] * orders_position["exchange_rate"]).round(2)
            orders_position["current_values"] = (orders_position["current_values"] * orders_position["exchange_rate"]).round(2)
            orders_position["bot_cash_balance"] = (orders_position["bot_cash_balance"] * orders_position["exchange_rate"]).round(2)

            #MARGIN
            orders_position["margin_amount"] = (orders_position["margin"] - 1) * orders_position["investment_amount"]
            orders_position["bot_cash_balance"] = orders_position["bot_cash_balance"] + orders_position["margin_amount"]
            orders_position["threshold"] = (orders_position["margin_amount"] + orders_position["investment_amount"]) - orders_position["bot_cash_balance"]
            orders_position["margin_amount"] = orders_position["bot_cash_balance"] - orders_position["margin_amount"]
            orders_position["margin_amount"] = np.where(orders_position["margin_amount"] >= 0, float(0), orders_position["margin_amount"] * -1)
            
            #PROFIT
            orders_position["profit"] = (orders_position["current_values"] - orders_position["investment_amount"]).round(rounded)
            orders_position["pct_profit"] =  (orders_position["profit"] / orders_position["investment_amount"]  * 100).round(2)

            #BOT POSITION
            orders_position["pct_cash"] =  (orders_position["bot_cash_balance"] / (orders_position["bot_cash_balance"] + orders_position["current_values"]) * 100).round(0)
            orders_position["pct_cash"] = np.where(orders_position["bot_id"] == "STOCK_stock_0", float(0), orders_position["pct_cash"])
            orders_position["pct_stock"] =  100 - orders_position["pct_cash"]
            

            #USER POSITION
            total_invested_amount = float(NoneToZero(np.nansum(orders_position["current_values"].to_list())))
            daily_live_profit = float(NoneToZero(total_invested_amount + user_core.loc[0, "pending_amount"] - user_core.loc[0, "daily_invested_amount"]))
            total_bot_invested_amount = float(NoneToZero(np.nansum(orders_position.loc[orders_position["bot_id"] != "STOCK_stock_0"]["current_values"].to_list())))
            total_user_invested_amount = float(NoneToZero(np.nansum(orders_position.loc[orders_position["bot_id"] == "STOCK_stock_0"]["current_values"].to_list())))
            pct_total_bot_invested_amount = float(NoneToZero(round((total_bot_invested_amount / total_invested_amount) * 100, 2)))
            pct_total_user_invested_amount = float(NoneToZero(round((total_user_invested_amount / total_invested_amount) * 100, 2)))
            total_profit_amount = NoneToZero(np.nansum(orders_position["profit"].to_list()))

            active_df = []
            orders_position = orders_position.reset_index(inplace=False, drop=True)
            orders_position = change_date_to_str(orders_position)
            orders_position = orders_position.sort_values(by=["profit"])
            for index, row in orders_position.iterrows():
                act_df = orders_position.loc[orders_position["position_uid"] == row["position_uid"]]
                bot = bot_option_type.loc[bot_option_type["bot_id"] == row["bot_id"]][["bot_id", "bot_option_type", "bot_apps_name", "bot_apps_description", "duration"]]
                act_df = act_df.reset_index(inplace=False, drop=True)
                act_df.loc[0, "bot_details"] = bot.to_dict("records")
                act_df["position_uid"] = act_df["position_uid"].astype(str)
                act_df["order_uid"] = act_df["order_uid"].astype(str)
                act_df = act_df.drop(columns=["bot_id", "bot_apps_name", "duration"])
                act_df = act_df.to_dict("records")[0]
                active_df.append(act_df)
            total_invested_amount = float(formatdigit(total_invested_amount, user_core.loc[0, "is_decimal"]))
            total_bot_invested_amount = float(formatdigit(total_bot_invested_amount, user_core.loc[0, "is_decimal"]))
            total_user_invested_amount = float(formatdigit(total_user_invested_amount, user_core.loc[0, "is_decimal"]))
            pct_total_bot_invested_amount = float(formatdigit(pct_total_bot_invested_amount, user_core.loc[0, "is_decimal"]))
            daily_live_profit = float(formatdigit(daily_live_profit, user_core.loc[0, "is_decimal"]))
            total_profit_amount = float(formatdigit(total_profit_amount, user_core.loc[0, "is_decimal"]))
            total_portfolio = float(formatdigit(total_invested_amount, user_core.loc[0, "is_decimal"]))
            active = pd.DataFrame({"user_id":[user], "total_invested_amount":[total_invested_amount], "total_bot_invested_amount":[total_bot_invested_amount], 
                "total_user_invested_amount":[total_user_invested_amount], "pct_total_bot_invested_amount":[pct_total_bot_invested_amount], 
                "pct_total_user_invested_amount":[pct_total_user_invested_amount], "total_profit_amount":[total_profit_amount], "daily_live_profit":[daily_live_profit], 
                "total_portfolio":[total_portfolio], "active_portfolio":[active_df]}, index=[0])
        else:
            zero = float(0)
            active = pd.DataFrame({"user_id":[user], "total_invested_amount":[zero], "total_bot_invested_amount":[zero], 
                "total_user_invested_amount":[zero], "pct_total_bot_invested_amount":[zero], "pct_total_user_invested_amount":[zero], 
                "total_profit_amount":[zero], "daily_live_profit":[zero], "total_portfolio":[zero], "active_portfolio":[[]]}, index=[0])
        # print(active)
        profile = user_core[["username", "first_name", "last_name", "email", "phone", "birth_date", "is_joined", "gender"]]
        profile["birth_date"] = profile["birth_date"].astype(str)
        
        user_core = user_core.drop(columns=profile.columns.tolist())
        
        profile = profile.to_dict("records")[0]
        profile = pd.DataFrame({"user_id":[user_core.loc[0, "user_id"]], "profile":[profile]}, index=[0])

        user_core = user_core.merge(profile, how="left", on=["user_id"])
        result = user_core.merge(active, how="left", on=["user_id"])
        result = result.rename(columns={"currency_code" : "currency"})
        result["current_asset"] = result["balance"] + result["total_portfolio"] + result["pending_amount"]
        result = change_date_to_str(result, exception=["rank"])
        result["rank"] = np.where(result["rank"].isnull(), None, result["rank"])

        result["current_asset"]  = result["current_asset"].astype(float).round(2)
        result["balance"]  = result["balance"].astype(float).round(2)
        result["total_portfolio"]  = result["total_portfolio"].astype(float).round(2)
        result["pending_amount"]  = result["pending_amount"].astype(float).round(2)
        result["daily_profit"]  = result["daily_profit"].astype(float).round(2)
        result["bot_pending_amount"]  = result["bot_pending_amount"].astype(float).round(2)
        result["stock_pending_amount"]  = result["stock_pending_amount"].astype(float).round(2)
        result["total_invested_amount"]  = result["total_invested_amount"].astype(float).round(2)
        result["total_bot_invested_amount"]  = result["total_bot_invested_amount"].astype(float).round(2)
        result["total_user_invested_amount"]  = result["total_user_invested_amount"].astype(float).round(2)
        result["total_profit_amount"]  = result["total_profit_amount"].astype(float).round(2)
        result["daily_live_profit"]  = result["daily_live_profit"].astype(float).round(2)
        result = result.reset_index(inplace=False, drop=True)
        if(update_firebase):
            await sync_to_async(update_to_firestore)(data=result, index="user_id", table=settings.FIREBASE_COLLECTION['portfolio'], dict=False)
        return result

def firebase_user_update(user_id=None, currency_code=None, update_firebase=True):
    firebase_ranking_update()
    firebase_user_delete()
    if not user_id and not currency_code:
        return
    convert = ConvertMoney("USD", "HKD")
    exchange_rate = convert.get_exchange_rate()
    print("Start Populate portfolio")
    bot_type = get_bot_type()
    bot_option_type = get_bot_option_type()
    bot_option_type = bot_option_type.merge(bot_type, how="left", on=["bot_type"])
    currency = get_currency_data(currency_code=currency_code)
    currency = currency[["currency_code", "is_decimal"]]

    user_core = get_user_core(currency_code=currency_code, user_id=user_id, field="id as user_id, username, is_joined, current_status, first_name, last_name, email, phone, birth_date, gender")
    user_core = user_core.loc[user_core["current_status"] == "verified"]
    user_core = user_core.loc[user_core["is_joined"] == True]
    user_core = user_core.drop(columns=["current_status"])
    if user_core.empty:
        return
    user_daily_profit = get_user_profit_history(user_id=user_id, field="user_id, daily_profit, daily_profit_pct, daily_invested_amount, rank::integer, total_profit, total_profit_pct")
    # user_daily_profit["rank"]  = user_daily_profit["rank"].astype(int)
    user_balance = get_user_account_balance(currency_code=currency_code, user_id=user_id, field="user_id, amount as balance, currency_code")
    
    user_core = user_core.merge(user_balance, how="left", on=["user_id"])
    user_core = user_core.merge(user_daily_profit, how="left", on=["user_id"])
    user_core = user_core.merge(currency, how="left", on=["currency_code"])
    user_core["balance"] = np.where(user_core["balance"].isnull(), float(0), user_core["balance"])
    user_core.loc[user_core["is_decimal"] == True, "balance"] = round(user_core.loc[user_core["is_decimal"] == True, "balance"], 2)
    user_core.loc[user_core["is_decimal"] == False, "balance"] = round(user_core.loc[user_core["is_decimal"] == False, "balance"], 0)
    if(len(user_core["user_id"].to_list()) > 0):
        bot_order_pending = get_orders_group_by_user_id(user_id=user_core["user_id"].to_list(), stock=False)

        user_core = user_core.merge(bot_order_pending, how="left", on=["user_id"])
        user_core["bot_pending_amount"] = np.where(user_core["bot_pending_amount"].isnull(), float(0), user_core["bot_pending_amount"])
        stock_order_pending = get_orders_group_by_user_id(user_id=user_core["user_id"].to_list(), stock=True)
        user_core = user_core.merge(stock_order_pending, how="left", on=["user_id"])
        user_core["stock_pending_amount"] = np.where(user_core["stock_pending_amount"].isnull(), float(0), user_core["stock_pending_amount"])
        user_core["pending_amount"] = user_core["stock_pending_amount"] + user_core["bot_pending_amount"]
        user_core["bot_pending_amount"]  = user_core["bot_pending_amount"].astype(float)
        user_core["stock_pending_amount"]  = user_core["stock_pending_amount"].astype(float)
        user_core["pending_amount"]  = user_core["pending_amount"].astype(float)

        orders_position_field = "position_uid, bot_id, ticker, expiry, spot_date, margin"
        orders_position_field += ", entry_price, investment_amount, user_id"
        orders_position_field += ", target_profit_price, max_loss_price, exchange_rate"
        position_data = get_orders_position(user_id=user_core["user_id"].to_list(), active=True, field=orders_position_field)
        position_data["exchange_rate"] = np.where(position_data["exchange_rate"].isnull(), 1, position_data["exchange_rate"])
        position_data = position_data.rename(columns={"target_profit_price" : "take_profit", "max_loss_price" : "stop_loss"})
        position_data["investment_amount"] = (position_data["investment_amount"] * position_data["exchange_rate"]).round(2)
        # print(position_data)
        position_data["expiry"]=position_data["expiry"].astype(str)
        if(len(position_data) > 0):
            universe = get_active_universe(ticker = position_data["ticker"].unique())[["ticker", "ticker_name", "ticker_fullname", "schi_name", "tchi_name", "currency_code"]]
            latest_price = get_price_data_firebase(position_data["ticker"].unique().tolist())
            latest_price = latest_price.rename(columns={"last_date" : "trading_day", "latest_price" : "price"})
            position_data = position_data.merge(latest_price, how="left", on=["ticker"])
            position_data["price"] = np.where(position_data["price"].isnull(), position_data["entry_price"], position_data["price"])
            position_data["price"]  = position_data["price"].astype(float).round(2)
            position_data["trading_day"] = np.where(position_data["trading_day"].isnull(), position_data["spot_date"], position_data["trading_day"])
            position_data = position_data.merge(universe, how="left", on=["ticker"])
            position_data["exchange_rate"] = np.where((position_data["exchange_rate"] == 1) & (position_data["currency_code"] == "USD"), exchange_rate, position_data["exchange_rate"])
            orders_performance_field = "distinct created, position_uid, share_num, order_uid, barrier, current_bot_cash_balance as bot_cash_balance"
            performance_data = get_orders_position_performance(position_uid=position_data["position_uid"].to_list(), field=orders_performance_field, latest=True)
            performance_data["created"] = performance_data["created"].dt.date
            performance_data = performance_data.drop_duplicates(subset=["created", "position_uid"], keep="first")
            performance_data = performance_data.drop(columns=["created"])
            position_data = position_data.merge(performance_data, how="left", on=["position_uid"])
            position_data = position_data.merge(bot_option_type[["bot_id", "bot_apps_name", "duration"]], how="left", on=["bot_id"])
            position_data["exchange_rate"] = 1
            position_data["exchange_rate"] = np.where(position_data["currency_code"] == "USD", exchange_rate, position_data["exchange_rate"])
            position_data["expiry"] = position_data["expiry"].astype(str)
            position_data["spot_date"] = position_data["spot_date"].astype(str)
            position_data["trading_day"] = position_data["trading_day"].astype(str)
        user_core["birth_date"] = user_core["birth_date"].astype(str)
        result = asyncio.run(gather_task(position_data, bot_option_type, user_core, update_firebase=update_firebase))
        if(not update_firebase):
            portfolio = []
            for pst_result in result:
                if(len(portfolio) == 0):
                    portfolio = pst_result
                else:
                    portfolio = portfolio.append(pst_result)
            return portfolio

def firebase_ranking_update(update_firebase=True):
    rank = get_user_profit_history(field="user_id, rank::integer as ranking, rank::integer, total_profit_pct")
    rank = rank.sort_values(by=["rank"], ascending=True).head(6)
    user_core = get_user_core(user_id=rank["user_id"].to_list(), field="id as user_id, username, current_status, is_joined, first_name, last_name, email")
    user_core = user_core.loc[user_core["current_status"] == "verified"]
    user_core = user_core.loc[user_core["is_joined"] == True]
    user_core = user_core.drop(columns=["current_status", "is_joined"])
    rank = rank.merge(user_core, how="left", on=["user_id"])
    rank["ranking"] = (rank["ranking"].astype(int).astype(str) * 4)
    rank = rank.reset_index(inplace=False, drop=True)
    if(update_firebase):
        update_to_firestore(data=rank, index="ranking", table=settings.FIREBASE_COLLECTION['ranking'], dict=False)
    else:
        return rank

def firebase_ranking_update_random(update_firebase=True):
    user_core = get_user_core(field="id as user_id, username, current_status, is_joined, first_name, last_name, email")
    user_core = user_core.loc[user_core["current_status"] == "verified"]
    user_core = user_core.loc[user_core["is_joined"] == True]
    user_core = user_core.drop(columns=["current_status", "is_joined"])
    rank = get_user_profit_history(field="user_id, total_profit_pct")
    rank = rank.merge(user_core, how="left", on=["user_id"])
    rank = rank.loc[~rank["username"].isnull()]
    for index, row in rank.iterrows():
        rank.loc[index, "total_profit_pct"] = abs(row["total_profit_pct"]) * (random.random() * 2)
    rank = rank.sort_values(by=["total_profit_pct"], ascending=[False])
    rank = rank.reset_index(inplace=False, drop=True)
    rank = rank.reset_index(inplace=False)
    rank = rank.rename(columns={"index" : "rank"})
    rank["rank"] = rank["rank"] + 1
    rank["ranking"] = rank["rank"]
    rank["total_profit_pct"] = rank["total_profit_pct"].round(4)
    rank = rank.sort_values(by=["rank"], ascending=True)
    rank = rank.head(6)
    rank["ranking"] = (rank["ranking"].astype(int).astype(str) * 4)
    rank = rank.reset_index(inplace=False, drop=True)
    if(update_firebase):
        update_to_firestore(data=rank, index="ranking", table=settings.FIREBASE_COLLECTION['ranking'], dict=False)
    else:
        return rank