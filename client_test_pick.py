import sys
from numpy import append
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from general.data_process import tuple_data
from general.sql_query import read_query
from general.date_process import dateNow
from general.slack import report_to_slack
from general.data_process import uid_maker, NoneToZero
from general.sql_output import upsert_data_to_database
from general.table_name import (
    get_bot_option_type_table_name,
    get_bot_ranking_table_name,
    get_client_table_name,
    get_client_test_pick_table_name,
    get_client_top_stock_table_name,
    get_currency_table_name,
    get_master_ohlcvtr_table_name,
    get_master_tac_table_name,
    get_orders_position_table_name,
    get_universe_rating_history_table_name, 
    get_universe_table_name, 
    get_universe_client_table_name, 
    get_user_account_balance_table_name,
    get_user_clients_table_name)
from bot.calculate_bot import get_expiry_date

def check_currency_code(currency_code, client_uid):
    query = ""
    if (type(currency_code) != type(None)):
        query = f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and "
        query+= f"currency_code in {tuple_data(currency_code)} and "
        query += f"ticker in (select ticker from {get_universe_client_table_name()} where client_uid='{client_uid}')) "
    return query

def get_bot_option_type(time_to_exp):
    table_name = get_bot_option_type_table_name()
    query = f"select * from {table_name} where time_to_exp in {tuple_data(time_to_exp)};"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def get_client_universe(currency_code, client_uid):
    table_name = get_universe_table_name()
    query = f"select * from {table_name} "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"where {check} "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def get_currency_rate(currency_code):
    table_name = get_currency_table_name()
    if(currency_code == "EUR"):
        return 1
    else:
        query = f"select * from {table_name} where currency_code in ('{currency_code}', 'EUR')"
        data = read_query(query, table=table_name, cpu_counts=True, prints=False)
        return data.loc[0, "last_price"] / data.loc[1, "last_price"]

def get_client_uid(client_name="HANWHA"):
    table_name = get_client_table_name()
    query = f"select * from {table_name} where client_name = '{client_name}';"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data.loc[0, "client_uid"]

def get_user_id(client_uid, currency_code, tester=False, advisor=False, capital="small", bot="UNO"):
    table_name = get_user_clients_table_name()
    query = f"select user_id, extra_data-> 'type' as bot_type, "
    query += f"extra_data-> 'capital' as capital, extra_data-> 'service_type' as service_type "
    query += f"from {table_name} where client_uid='{client_uid}' and currency_code in {tuple_data(currency_code)} "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    if(advisor):
        data = data.loc[data["capital"] == capital]
        data = data.loc[data["service_type"] == "bot_advisor"]
        # query += f"and capital=\"{capital}\" and service_type=\"bot_advisor\""
    if(tester):
        data = data.loc[data["bot_type"] == bot.upper()]
        data = data.loc[data["service_type"] == "bot_tester"]
        data = data.loc[data["capital"] == capital]
        # query += f"and bot_type = \"{bot.lower()}\" and capital = \"{capital}\" and service_type = \"bot_tester\""
    data = data.reset_index(inplace=False, drop=True)
    print(data)
    if(len(data) >= 1):
        return data.loc[0, "user_id"]
    else:
        sys.exit("There is no Account for this user")

def get_client_test_pick_ticker(client_uid, currency_code):
    table_name = get_client_test_pick_table_name()
    query = f"select ctp.* from {table_name} ctp "
    query += f"inner join (select client_uid, currency_code, max(ctp1.spot_date) max_date "
    query += f"from {table_name} ctp1 group by client_uid, currency_code) filter on ctp.client_uid = filter.client_uid "
    query += f"and ctp.currency_code = filter.currency_code and ctp.spot_date = filter.max_date "
    query += f"where ctp.client_uid='{client_uid}' and ctp.currency_code  in {tuple_data(currency_code)}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    result = pd.DataFrame({"client_uid":[], "currency_code":[], "spot_date":[], "ticker":[], "rank":[]}, index=[])
    for col in data.columns:
        if(col not in ["uid", "client_uid", "currency_code", "spot_date"]):
            rank = col.split("_")
            temp = pd.DataFrame({"client_uid":[client_uid], "currency_code":[currency_code[0]], 
            "spot_date":[data.loc[0, "spot_date"]], "ticker":[data.loc[0, col]], "rank":[rank[1]]}, index=[0])
            result = result.append(temp)
    result = result.reset_index(inplace=False, drop=True)
    return result

def get_industry_code():
    table_name = get_universe_table_name()
    query = f"select ticker, industry_code from {table_name}"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def get_portfolio_ticker_list(user_id):
    # get stocks already in portfolio
    table_name = get_orders_position_table_name()
    query = f"select distinct op.ticker, u.industry_code from {table_name} op inner join universe u on op.ticker = u.ticker "
    query += f"where user_id='{user_id}' and op.is_live=True; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

# def get_old_bot_ticker_pick(client_uid, currency_code, tester=False, advisor=False, capital="small", bot="UNO"):
#     #????
#     table_name = get_client_top_stock_table_name()
#     query = f"select ticker from client_top_stock where currency_code in {tuple_data(currency_code)} and client_uid = '{client_uid}' "
#     if(advisor):
#         query += f"and service_type = 'bot_advisor' and capital = '{capital}' "
#     if(tester):
#         query += f"and service_type = 'bot_tester' and capital = '{capital}' and bot = '{bot}' "
#     data = read_query(query, table=table_name, cpu_counts=True, prints=False)
#     return data["ticker"].to_list()

def get_bot_ranking(ticker, spot_date):
    table_name = get_bot_ranking_table_name()
    query = f"select br.* from {table_name} br inner join (select br1.ticker, max(br1.spot_date) as spot_date  "
    query += f"from {table_name} br1 where spot_date<='{spot_date}' group by br1.ticker) as filter "
    query += f"on filter.ticker=br.ticker and filter.spot_date=br.spot_date "
    query += f"where br.ticker in {tuple_data(ticker)}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def get_newest_price(ticker, spot_date, master_ohlcvtr=False):
    if(master_ohlcvtr):
        table_name = get_master_ohlcvtr_table_name()
        query = f"select tac.ticker, tac.trading_day, tac.close as tri_adj_close from {table_name} tac "
        query += f"inner join (select tac1.ticker, max(tac1.trading_day) as max_date "
        query += f"from {table_name} tac1 where trading_day<='{spot_date}' and tac1.close is not null group by tac1.ticker) as filter "
        query += f"on filter.ticker=tac.ticker and filter.max_date=tac.trading_day "
        query += f"where tac.ticker in {tuple_data(ticker)}; "
    else:
        table_name = get_master_tac_table_name()
        query = f"select tac.ticker, tac.trading_day, tac.tri_adj_close from {table_name} tac "
        query += f"inner join (select tac1.ticker, max(tac1.trading_day) as max_date "
        query += f"from {table_name} tac1 where trading_day<='{spot_date}' and tac1.tri_adj_close is not null group by tac1.ticker) as filter "
        query += f"on filter.ticker=tac.ticker and filter.max_date=tac.trading_day "
        query += f"where tac.ticker in {tuple_data(ticker)}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data
    
def get_current_assets(user_id):
    table_name = get_orders_position_table_name()
    query = f"select sum(current_values) as current_value from {table_name} where user_id='{user_id}' and is_live=True;"
    total_current_value = NoneToZero(read_query(query, table=table_name, cpu_counts=True, prints=False).loc[0, "current_value"])
    print(total_current_value)
    table_name = get_user_account_balance_table_name()
    query = f"select amount from {table_name} where user_id='{user_id}';"
    user_balance = NoneToZero(read_query(query, table=table_name, cpu_counts=True, prints=False).loc[0, "amount"])
    current_assets = round(total_current_value + user_balance, 2)
    return current_assets

def another_top_stock(currency_code, client_uid, top_distinct_ticker_list, top_pick_distinct, threshold, top_pick_stock):
    #top stocks without distince industries (excluding [top_distinct_ticker_list])
    table_name = get_universe_rating_history_table_name()

    query = f"select ticker, ai_score, trading_day, industry_code, (now())::date as forward_date, rn from ( "
    query += f"select univ.ticker, urh.ai_score, urh.trading_day, univ.industry_code, "
    query += f"row_number() OVER (PARTITION BY univ.industry_code ORDER BY urh.ai_score DESC) AS rn "
    query += f"from {get_universe_table_name()} univ inner join {table_name} urh "
    query += f"on urh.ticker=univ.ticker and urh.trading_day=(select max(urh2.trading_day) from {table_name} urh2) "
    if(type(threshold) != type(None)):
        query += f"inner join (select ohlcv.ticker, ohlcv.trading_day,ohlcv.close from {get_master_ohlcvtr_table_name()} ohlcv inner join "
        query += f"(select mo.ticker, max(mo.trading_day) as trading_day from {get_master_ohlcvtr_table_name()} mo where mo.close is not null group by mo.ticker) mo "
        query += f"on ohlcv.ticker = mo.ticker and ohlcv.trading_day=mo.trading_day where ohlcv.close<{threshold}) filter "
        query += f"on univ.ticker = filter.ticker "
    query += f"where urh.ticker not in {tuple_data(top_distinct_ticker_list)} "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"and urh.{check}"
    query += f"order by ai_score DESC) result "
    query += f"where result.rn = 1 "
    query += f"order by ai_score DESC limit {top_pick_stock-top_pick_distinct}; "
    # print(query)

    # table_name = get_universe_rating_table_name()
    # query = f"select f3.ticker, f3.industry_code, f3.ribbon_score, f3.wts_rating, f3.wts_score, (now())::date as forward_date "
    # query += f"from (select f2.ticker, f2.industry_code, f2.wts_rating, f2.wts_score, (f2.st + f2.mt + f2.gq) as ribbon_score "
    # query += f"from (select f1.ticker, f1.industry_code, "
    # query += f"CASE WHEN (f1.wts_rating) >= 5 THEN 1 ELSE 0 END AS st, "
    # query += f"CASE WHEN (f1.dlp_1m) >= 5 THEN 1 ELSE 0 END AS mt, "
    # query += f"CASE WHEN (f1.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
    # query += f"f1.wts_rating + f1.dlp_1m + f1.fundamentals_quality AS wts_score, f1.wts_rating "
    # query += f"from (select ur.ticker, ur.wts_rating, ur.dlp_1m, ur.fundamentals_quality, u.industry_code "
    # query += f"from {table_name} ur inner join {get_universe_table_name()} u on u.ticker = ur.ticker "
    # if(type(threshold) != type(None)):
    #     query += f"inner join (select ohlcv.ticker, ohlcv.trading_day,ohlcv.close from {get_master_ohlcvtr_table_name()} ohlcv inner join  "
    #     query += f"(select mo.ticker, max(mo.trading_day) as trading_day "
    #     query += f"from {get_master_ohlcvtr_table_name()} mo where mo.close is not null group by mo.ticker) mo on ohlcv.ticker = mo.ticker and  "
    #     query += f"ohlcv.trading_day=mo.trading_day where ohlcv.close<{threshold}) filter "
    #     query += f"on u.ticker = filter.ticker "
    # query += f"where ur.ticker not in {tuple_data(top_distinct_ticker_list)} "
    # check = check_currency_code(currency_code, client_uid)
    # if (check != ""):
    #     query += f"and ur.{check}"
    # query += f") f1) f2) f3 "
    # #top total ribbons, then wts score, then combined score
    # query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit {top_pick_stock-top_pick_distinct} "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def top_stock_distinct_industry(currency_code, client_uid, top_pick_distinct, threshold, top_pick_stock):
    # top (top_pick_distinct) stocks with distinct industries
    table_name = get_universe_rating_history_table_name()

    query = f"select ticker, ai_score, trading_day, industry_code, (now())::date as forward_date, rn from ( "
    query += f"select univ.ticker, urh.ai_score, urh.trading_day, univ.industry_code, "
    query += f"row_number() OVER (PARTITION BY univ.industry_code ORDER BY urh.ai_score DESC) AS rn "
    query += f"from {get_universe_table_name()} univ inner join {table_name} urh "
    query += f"on urh.ticker=univ.ticker and urh.trading_day=(select max(urh2.trading_day) from {table_name} urh2) "
    if(type(threshold) != type(None)):
        query += f"inner join (select ohlcv.ticker, ohlcv.trading_day,ohlcv.close from {get_master_ohlcvtr_table_name()} ohlcv inner join "
        query += f"(select mo.ticker, max(mo.trading_day) as trading_day from {get_master_ohlcvtr_table_name()} mo where mo.close is not null group by mo.ticker) mo "
        query += f"on ohlcv.ticker = mo.ticker and ohlcv.trading_day=mo.trading_day where ohlcv.close<{threshold}) filter "
        query += f"on univ.ticker = filter.ticker "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"where urh.{check}"
    query += f"order by ai_score DESC) result "
    query += f"where result.rn = 1 "
    query += f"order by ai_score DESC limit {top_pick_distinct}; "
    # print(query)
    # table_name = get_universe_rating_table_name()
    # query = f"select f5.ticker, f5.industry_code, f5.ribbon_score, f5.wts_rating, f5.wts_score, (now())::date as forward_date "
    # query += f"from (select distinct on (f4.industry_code) f4.ticker, f4.industry_code, f4.ribbon_score, f4.wts_rating, f4.wts_score "
    # query += f"from (select f3.ticker, f3.industry_code, f3.ribbon_score, f3.wts_rating, f3.wts_score,  "
    # query += f"row_number() OVER (PARTITION BY f3.industry_code ORDER BY "
    # query += f"f3.ribbon_score DESC, f3.wts_rating DESC, f3.wts_score DESC, f3.ticker ASC) AS rn "
    # query += f"from (select f2.ticker, f2.industry_code, f2.wts_rating, f2.wts_score, (f2.st + f2.mt + f2.gq) as ribbon_score "
    # query += f"from (select f1.ticker, f1.industry_code, "
    # query += f"CASE WHEN (f1.wts_rating) >= 5 THEN 1 ELSE 0 END AS st, "
    # query += f"CASE WHEN (f1.dlp_1m) >= 5 THEN 1 ELSE 0 END AS mt, "
    # query += f"CASE WHEN (f1.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
    # query += f"f1.wts_rating + f1.dlp_1m + f1.fundamentals_quality AS wts_score, f1.wts_rating "
    # query += f"from (select ur.ticker, ur.wts_rating, ur.dlp_1m, ur.fundamentals_quality, u.industry_code "
    # query += f"from {table_name} ur inner join {get_universe_table_name()} u on u.ticker = ur.ticker "
    # if(type(threshold) != type(None)):
    #     query += f"inner join (select ohlcv.ticker, ohlcv.trading_day,ohlcv.close from {get_master_ohlcvtr_table_name()} ohlcv inner join  "
    #     query += f"(select mo.ticker, max(mo.trading_day) as trading_day "
    #     query += f"from {get_master_ohlcvtr_table_name()} mo where mo.close is not null group by mo.ticker) mo on ohlcv.ticker = mo.ticker and  "
    #     query += f"ohlcv.trading_day=mo.trading_day where ohlcv.close<{threshold}) filter "
    #     query += f"on u.ticker = filter.ticker "
    # check = check_currency_code(currency_code, client_uid)
    # if (check != ""):
    #     query += f"where ur.{check}"
    # query += f") f1) f2) f3 "
    # query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC) f4 where rn=1) f5 "
    # #top total ribbons, then wts score, then combined score
    # query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit {top_pick_distinct}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def populate_bot_advisor(currency_code=None, client_name="HANWHA", top_pick_stock=7, time_to_exp=[0.07692], top_pick = 2, capital="small"):
    client_uid = get_client_uid(client_name=client_name)
    user_id = get_user_id(client_uid, currency_code, advisor=True, capital=capital)
    print(client_uid)
    print(user_id)
    portfolio_ticker_list = get_portfolio_ticker_list(user_id) #current portfolio stocks
    # old_advisor_ticker_list = get_old_bot_ticker_pick(client_uid, currency_code, advisor=True, capital=capital)
    top_stock = get_client_test_pick_ticker(client_uid, currency_code)
    #eliminate stocks already in portfolio
    available_pick = top_stock.loc[~top_stock["ticker"].isin(portfolio_ticker_list["ticker"].to_list())]
    # available_pick = available_pick.loc[~available_pick["ticker"].isin(old_advisor_ticker_list)]
    available_pick = available_pick.reset_index(inplace=False, drop=True).head(top_pick_stock)
    bot_ranking = get_bot_ranking(available_pick["ticker"], top_stock.loc[0, "spot_date"])
    price = get_newest_price(available_pick["ticker"], top_stock.loc[0, "spot_date"])
    filter_field = ["ticker"]
    rename = {}
    bot_option_type = get_bot_option_type(time_to_exp)
    for index, row in bot_option_type.iterrows():
        option_type = row["bot_option_type"]
        if(option_type == "CLASSIC") : option_type = option_type.lower()
        bot_type = row["bot_type"].lower()
        time_to_exp_str = row["time_to_exp_str"]
        filter_field.append(f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob")
        temp = {f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob" : f"{bot_type.upper()}_{option_type}_{time_to_exp_str}"}
        rename.update(temp)
    bot_ranking = bot_ranking[filter_field]
    bot_ranking = bot_ranking.rename(columns=rename)
    bot_advisor_pick = pd.DataFrame({"ticker":[], "spot_date":[], "bot_id":[], "prob":[], "bot":[]}, index=[])
    for index, row in bot_ranking.iterrows():
        for col in bot_ranking.columns:
            if(col != "ticker"):
                bot = col.split("_")
                temp = pd.DataFrame({"ticker":[row["ticker"]], "spot_date":[top_stock.loc[0, "spot_date"]], 
                    "bot_id":[col], "prob":[row[col]], "bot":[bot[0]]}, index=[0])
                bot_advisor_pick = bot_advisor_pick.append(temp)
    bot_advisor_pick = bot_advisor_pick.sort_values(by=["prob"], ascending=[False])
    bot_advisor_pick = bot_advisor_pick.merge(price, how="left", on="ticker")
    bot_advisor_pick = bot_advisor_pick.reset_index(inplace=False, drop=True)
    print(bot_advisor_pick)
    current_assets = get_current_assets(user_id)
    advisor_pick = pd.DataFrame({"created":[], "updated":[], "uid":[],"spot_date":[], "expiry_date":[], "has_position":[], 
        "position_uid":[],"execution_date":[], "completed_date":[], "event":[], "rank":[], "client_uid":[], "ticker":[], 
        "bot_id":[], "currency_code":[], "service_type":[], "bot":[], "status":[]}, index=[])
    count = 1
    last_ticker = [""]
    for index, row in bot_advisor_pick.iterrows():
        ticker = row["ticker"]
        bot_id = row["bot_id"]
        bot = row["bot"]
        tri_adj_close = row["tri_adj_close"]
        if(count > top_pick):
            break
        status = True
        if(bot == "UNO" or bot == "UCDC"):
            if(tri_adj_close > current_assets/100):
                status=False
        if(status and ticker not in last_ticker):
            service_type = "bot_advisor"
            spot_date = top_stock.loc[0, "spot_date"]
            uid = f"{client_uid}_{currency_code[0]}_{ticker}_{spot_date}_{service_type}_{capital}"
            expiry_date = get_expiry_date(time_to_exp[0], str(spot_date), currency_code[0])
            uid = uid.replace("-", "").replace(".", "").replace(" ", "")
            #### ADD INTERVAL WEEK OF YEAR ####
            dates = pd.to_datetime(spot_date)
            week = dates.isocalendar()[1]
            year = dates.isocalendar()[0]
            interval = f'{year}{week}'
            ##### ================ #####
            temp = pd.DataFrame({"created":[spot_date], "updated":[spot_date], "uid":[uid],"spot_date":[spot_date], "bot":[None], 
            "expiry_date":[expiry_date], "has_position":["False"], "position_uid":[None],"execution_date":[None], "completed_date":[None], "event":[None],
            "rank":[count], "client_uid":[client_uid], "ticker":[ticker],"bot_id":[bot_id], "currency_code":[currency_code[0]], "service_type":[service_type], 
            "capital":[capital], 
            "week_of_year":[int(interval)] # ADD INTERVAL TO DATAFRAME
            }, index=[0])
            last_ticker.append(ticker)
            count+=1
            advisor_pick = advisor_pick.append(temp)
    print(advisor_pick)
    # advisor_pick.to_csv("advisor_pick.csv")
    upsert_data_to_database(advisor_pick, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)

def populate_bot_tester(currency_code=None, client_name="HANWHA", top_pick_stock=7, time_to_exp=[0.07692], top_pick = 2, capital="small", bot="UNO"):
    client_uid = get_client_uid(client_name=client_name)
    user_id = get_user_id(client_uid, currency_code, tester=True, capital=capital, bot=bot)
    print(client_uid)
    print(user_id)
    portfolio_ticker_list = get_portfolio_ticker_list(user_id)
    # old_tester_ticker_list = get_old_bot_ticker_pick(client_uid, currency_code, tester=True)
    top_stock = get_client_test_pick_ticker(client_uid, currency_code)
    industry_code = get_industry_code()
    top_stock = top_stock.merge(industry_code, how="left", on="ticker")
    available_pick = top_stock.loc[~top_stock["ticker"].isin(portfolio_ticker_list["ticker"].to_list())]
    available_pick = top_stock.loc[~top_stock["industry_code"].isin(portfolio_ticker_list["industry_code"].unique())]
    # available_pick = available_pick.loc[~available_pick["ticker"].isin(old_tester_ticker_list)]
    available_pick = available_pick.reset_index(inplace=False, drop=True).head(top_pick_stock)
    bot_ranking = get_bot_ranking(available_pick["ticker"], top_stock.loc[0, "spot_date"])
    price = get_newest_price(available_pick["ticker"], top_stock.loc[0, "spot_date"])
    filter_field = ["ticker"]
    rename = {}
    bot_option_type = get_bot_option_type(time_to_exp)
    for index, row in bot_option_type.iterrows():
        option_type = row["bot_option_type"]
        if(option_type == "CLASSIC") : option_type = option_type.lower()
        bot_type = row["bot_type"].lower()
        time_to_exp_str = row["time_to_exp_str"]
        filter_field.append(f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob")
        temp = {f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob" : f"{bot_type.upper()}_{option_type}_{time_to_exp_str}"}
        rename.update(temp)
    bot_ranking = bot_ranking[filter_field]
    bot_ranking = bot_ranking.rename(columns=rename)
    bot_tester_pick = pd.DataFrame({"ticker":[], "spot_date":[], "bot_id":[], "prob":[], "bot":[]}, index=[])
    for index, row in bot_ranking.iterrows():
        for col in bot_ranking.columns:
            if(col != "ticker"):
                bot_type = col.split("_")
                temp = pd.DataFrame({"ticker":[row["ticker"]], "spot_date":[top_stock.loc[0, "spot_date"]], 
                    "bot_id":[col], "prob":[row[col]], "bot":[bot_type[0]]}, index=[0])
                bot_tester_pick = bot_tester_pick.append(temp)
    bot_tester_pick = bot_tester_pick.sort_values(by=["prob"], ascending=[False])
    bot_tester_pick = bot_tester_pick.merge(price, how="left", on="ticker")
    bot_tester_pick = bot_tester_pick.reset_index(inplace=False, drop=True)
    print(bot_tester_pick)
    current_assets = get_current_assets(user_id)
    tester_pick = pd.DataFrame({"created":[], "updated":[], "uid":[],"spot_date":[], "expiry_date":[], "has_position":[], 
        "position_uid":[],"execution_date":[], "completed_date":[], "event":[], "rank":[], "client_uid":[], "ticker":[], 
        "bot_id":[], "currency_code":[], "service_type":[], "bot":[], "status":[]}, index=[])
    count = 1
    last_ticker = [""]
    last_industry_code = [""]
    bot_tester_pick = bot_tester_pick.loc[bot_tester_pick["bot"] == bot]
    bot_tester_pick = bot_tester_pick.merge(industry_code, how="left", on="ticker")
    print(bot_tester_pick)
    for index, row in bot_tester_pick.iterrows():
        ticker = row["ticker"]
        industry_code = row["industry_code"]
        bot_id = row["bot_id"]
        bot = row["bot"]
        tri_adj_close = row["tri_adj_close"]
        if(count > top_pick):
            break
        status = True
        if(bot == "UNO" or bot == "UCDC"):
            if(tri_adj_close > current_assets/100):
                status=False
        if(status and ticker not in last_ticker and industry_code not in last_industry_code):
            service_type = "bot_tester"
            spot_date = top_stock.loc[0, "spot_date"]
            uid = f"{client_uid}_{currency_code[0]}_{ticker}_{spot_date}_{bot}_{service_type}_{capital}"
            expiry_date = get_expiry_date(time_to_exp[0], str(spot_date), currency_code[0])
            uid = uid.replace("-", "").replace(".", "").replace(" ", "")
            #### ADD INTERVAL WEEK OF YEAR ####
            dates = pd.to_datetime(spot_date)
            week = dates.isocalendar()[1]
            year = dates.isocalendar()[0]
            interval = f'{year}{week}'
            ##### ================ #####
            temp = pd.DataFrame({"created":[spot_date], "updated":[spot_date], "uid":[uid],"spot_date":[spot_date], 
            "expiry_date":[expiry_date], "has_position":["False"], "position_uid":[None],"execution_date":[None], "completed_date":[None], "event":[None],
            "rank":[count], "client_uid":[client_uid], "ticker":[ticker],"bot_id":[bot_id],"bot":[bot], "currency_code":[currency_code[0]], "service_type":[service_type], 
            "capital":[capital], "week_of_year":[int(interval)]}, index=[0])
            last_ticker.append(ticker)
            last_industry_code.append(industry_code)
            count+=1
            tester_pick = tester_pick.append(temp)
    print(tester_pick)
    # tester_pick.to_csv("tester_pick.csv")
    upsert_data_to_database(tester_pick, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)

def populate_fels_bot(currency_code=None, client_name="FELS", time_to_exp=[0.07692], top_pick = 5, bot="CLASSIC"):
    client_uid = get_client_uid(client_name=client_name)
    user_id = get_user_id(client_uid, currency_code)
    print(client_uid)
    print(user_id)
    portfolio_ticker_list = get_portfolio_ticker_list(user_id)
    top_stock = get_client_test_pick_ticker(client_uid, currency_code)
    available_pick = top_stock.loc[~top_stock["ticker"].isin(portfolio_ticker_list["ticker"].to_list())]
    # available_pick = available_pick.loc[~available_pick["ticker"].isin(old_tester_ticker_list)]
    available_pick = available_pick.reset_index(inplace=False, drop=True)
    price = get_newest_price(available_pick["ticker"], top_stock.loc[0, "spot_date"])
    available_pick = available_pick.merge(price, how="left", on="ticker")
    bot_option_type = get_bot_option_type(time_to_exp)
    bot_option_type = bot_option_type.loc[bot_option_type["bot_type"] == bot]
    bot_option_type = bot_option_type.reset_index(inplace=False, drop=True)
    print(available_pick)
    print(bot_option_type)
    last_ticker = [""]
    count = 1
    fels_pick = pd.DataFrame({"created":[], "updated":[], "uid":[],"spot_date":[], "expiry_date":[], "has_position":[], 
        "position_uid":[],"execution_date":[], "completed_date":[], "event":[], "rank":[], "client_uid":[], "ticker":[], 
        "bot_id":[], "currency_code":[], "service_type":[], "bot":[], "status":[]}, index=[])
    for index, row in available_pick.iterrows():
        ticker = row["ticker"]
        spot_date = row["spot_date"]
        bot_id = bot_option_type.loc[0, "bot_id"]
        tri_adj_close = row["tri_adj_close"]
        if(count > top_pick):
            break
        if(ticker not in last_ticker):
            uid = f"{client_uid}_{currency_code[0]}_{ticker}_{spot_date}_null_null"
            expiry_date = get_expiry_date(time_to_exp[0], str(spot_date), currency_code[0])
            uid = uid.replace("-", "").replace(".", "").replace(" ", "")
            #### ADD INTERVAL WEEK OF YEAR ####
            dates = pd.to_datetime(spot_date)
            week = dates.isocalendar()[1]
            year = dates.isocalendar()[0]
            interval = f'{year}{week}'
            ##### ================ #####
            temp = pd.DataFrame({"created":[spot_date], "updated":[spot_date], "uid":[uid],"spot_date":[spot_date], 
            "expiry_date":[expiry_date], "has_position":["False"], "position_uid":[None],"execution_date":[None], "completed_date":[None], "event":[None],
            "rank":[count], "client_uid":[client_uid], "ticker":[ticker],"bot_id":[bot_id],"bot":[bot], "currency_code":[currency_code[0]], "service_type":[None], 
            "capital":[None], "week_of_year":[int(interval)]}, index=[0])
            last_ticker.append(ticker)
            count+=1
            fels_pick = fels_pick.append(temp)
    print(fels_pick)
    fels_pick.to_csv("fels_pick.csv")
    upsert_data_to_database(fels_pick, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)

def test_pick(currency_code=None, client_name="HANWHA", top_pick_distinct=7, threshold=300, top_pick_stock=25):
    print("{} : === CLIENT WEEKLY PICK STARTED ===".format(dateNow()))
    client_uid = get_client_uid(client_name=client_name)
    if(client_name == "FELS"):
        currency_rate = get_currency_rate(currency_code[0])
        threshold = threshold * currency_rate
    else:
        threshold = None
    # get the top (top_pick_distinct=7) distinct industry stocks
    top_stock_distinct = top_stock_distinct_industry(currency_code, client_uid, top_pick_distinct, threshold, top_pick_stock)
    print(top_stock_distinct)
    # get the rest (indusries can overlap)
    top_stock_not_distinct = another_top_stock(currency_code, client_uid, top_stock_distinct["ticker"], top_pick_distinct, threshold, top_pick_stock)
    print(top_stock_not_distinct)
    top_stock = top_stock_distinct.append(top_stock_not_distinct)
    top_stock = top_stock.reset_index(inplace=False, drop=True)
    print(top_stock)
    result = pd.DataFrame({"spot_date":[top_stock.loc[0, "forward_date"]],
                "ticker_1":[top_stock.loc[0, "ticker"]],
                "ticker_2":[top_stock.loc[1, "ticker"]],
                "ticker_3":[top_stock.loc[2, "ticker"]],
                "ticker_4":[top_stock.loc[3, "ticker"]],
                "ticker_5":[top_stock.loc[4, "ticker"]],
                "ticker_6":[top_stock.loc[5, "ticker"]],
                "ticker_7":[top_stock.loc[6, "ticker"]],
                "ticker_8":[top_stock.loc[7, "ticker"]],
                "ticker_9":[top_stock.loc[8, "ticker"]],
                "ticker_10":[top_stock.loc[9, "ticker"]],
                "ticker_11":[top_stock.loc[10, "ticker"]],
                "ticker_12":[top_stock.loc[11, "ticker"]],
                "ticker_13":[top_stock.loc[12, "ticker"]],
                "ticker_14":[top_stock.loc[13, "ticker"]],
                "ticker_15":[top_stock.loc[14, "ticker"]],
                "ticker_16":[top_stock.loc[15, "ticker"]],
                "ticker_17":[top_stock.loc[16, "ticker"]],
                "ticker_18":[top_stock.loc[17, "ticker"]],
                "ticker_19":[top_stock.loc[18, "ticker"]],
                "ticker_20":[top_stock.loc[19, "ticker"]],
                "ticker_21":[top_stock.loc[20, "ticker"]],
                "ticker_22":[top_stock.loc[21, "ticker"]],
                "ticker_23":[top_stock.loc[22, "ticker"]],
                "ticker_24":[top_stock.loc[23, "ticker"]],
                "ticker_25":[top_stock.loc[24, "ticker"]]}, index=[0])
    result["client_uid"] = client_uid
    result["currency_code"] = currency_code[0]
    # result = result.merge(advisor_pick, how="left", on="currency_code")
    today = datetime.now().date()
    print(today)
    print(today.weekday())
    while (today.weekday() != 0):
        today =  today - relativedelta(days=1)
    print(today)
    # sys.exit(1)
    result["spot_date"] = today
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="client_uid", date=False)
    result = uid_maker(result, uid="uid", ticker="uid", trading_day="spot_date", date=True)
    print(result)
    print("{} : === CLIENT WEEKLY PICK COMPLETED ===".format(dateNow()))
    upsert_data_to_database(result, get_client_test_pick_table_name(), "uid", how="update", cpu_count=True, Text=True)
    report_to_slack("{} : === {} CLIENT WEEKLY PICK COMPLETED ===".format(dateNow(), client_name))

# def bot_tester_manual_populate(client_name="HANWHA", time_to_exp=[0.07692]):
#     all = []
#     bot_option_type = get_bot_option_type(time_to_exp)
#     industry_code = get_industry_code()
#     stock_pick = pd.DataFrame({"ticker":[""], "spot_date":[], "currency_code":[]}, index=[])
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["000880.KS", "000660.KS"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["KRW", "KRW"]}, index=[0, 1]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["AIG", "AMD.O"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["USD", "USD"]}, index=[2, 3]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["601398.SS", "000001.SZ"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["CNY", "CNY"]}, index=[4, 5]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["0005.HK", "0700.HK"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["HKD", "HKD"]}, index=[6, 7]))

#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["003550.KS", "004020.KS"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["KRW", "KRW"]}, index=[8, 9]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["F", "AAL.O"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["USD", "USD"]}, index=[10, 11]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["600115.SS", "600600.SS"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["CNY", "CNY"]}, index=[12, 13]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["1398.HK", "1928.HK"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["HKD", "HKD"]}, index=[14, 15]))

#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["004170.KS", "006800.KS"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["KRW", "KRW"]}, index=[16, 17]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["AA", "CAT"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["USD", "USD"]}, index=[18, 19]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["601009.SS", "601111.SS"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["CNY", "CNY"]}, index=[20, 21]))
#     # stock_pick = stock_pick.append(pd.DataFrame({"ticker":["2318.HK", "9618.HK"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["HKD", "HKD"]}, index=[22, 23]))
#     print(stock_pick)
#     for spot_date in stock_pick["spot_date"].unique():
#         for currency_code in stock_pick["currency_code"].unique():
#             for capital in ["small", "large"]:
#                 if(capital == "small"):
#                     top_pick=1
#                 else:
#                     top_pick=2
#                 available_pick = stock_pick.loc[stock_pick["currency_code"] == currency_code]
#                 available_pick = available_pick.loc[ available_pick["spot_date"] == spot_date]
#                 print(available_pick)
#                 client_uid = get_client_uid(client_name=client_name)
#                 user_id = get_user_id(client_uid, currency_code, tester=True, capital=capital)
#                 print(client_uid)
#                 print(user_id)
#                 bot_ranking = get_bot_ranking(available_pick["ticker"], spot_date)
#                 price = get_newest_price(available_pick["ticker"], spot_date, master_ohlcvtr=True)
#                 print(spot_date)
#                 rename = {}
#                 bot_option_type = get_bot_option_type(time_to_exp)
#                 filter_field = ["ticker"]
#                 for index, row in bot_option_type.iterrows():
#                     option_type = row["bot_option_type"]
#                     if(option_type == "CLASSIC") : option_type = option_type.lower()
#                     bot_type = row["bot_type"].lower()
#                     time_to_exp_str = row["time_to_exp_str"]
#                     filter_field.append(f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob")
#                     temp = {f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob" : f"{bot_type.upper()}_{option_type}_{time_to_exp_str}"}
#                     rename.update(temp)
#                 bot_ranking = bot_ranking[filter_field]
#                 bot_ranking = bot_ranking.rename(columns=rename)
#                 bot_tester_pick = pd.DataFrame({"ticker":[], "spot_date":[], "bot_id":[], "prob":[], "bot":[]}, index=[])
#                 for index, row in bot_ranking.iterrows():
#                     for col in bot_ranking.columns:
#                         if(col != "ticker"):
#                             bot_type = col.split("_")
#                             pick_temp = pd.DataFrame({"ticker":[row["ticker"]], "spot_date":[spot_date], "bot_id":[col], "prob":[row[col]], "bot":[bot_type[0]]}, index=[0])
#                             print(pick_temp)
#                             bot_tester_pick = bot_tester_pick.append(pick_temp)
#                 bot_tester_pick = bot_tester_pick.sort_values(by=["prob"], ascending=[False])
#                 bot_tester_pick = bot_tester_pick.merge(price, how="left", on="ticker")
#                 bot_tester_pick = bot_tester_pick.reset_index(inplace=False, drop=True)
#                 current_assets = get_current_assets(user_id)
#                 count = 1
#                 last_ticker = [""]
#                 last_industry_code = [""]
#                 bot_tester_pick = bot_tester_pick.merge(industry_code, how="left", on="ticker")
#                 tester_pick = pd.DataFrame({"created":[], "updated":[], "uid":[],"spot_date":[], "expiry_date":[], "has_position":[], 
#                     "position_uid":[],"execution_date":[], "completed_date":[], "event":[], "rank":[], "client_uid":[], "ticker":[], 
#                     "bot_id":[], "currency_code":[], "service_type":[], "bot":[]}, index=[])
#                 for bot in ["UNO", "UCDC", "CLASSIC"]:
#                     test = bot_tester_pick.loc[bot_tester_pick["bot"] == bot]
#                     print(test)
#                     for index, row in test.iterrows():
#                         ticker = row["ticker"]
#                         industry_code_field = row["industry_code"]
#                         bot_id = row["bot_id"]
#                         bot = row["bot"]
#                         tri_adj_close = row["tri_adj_close"]
#                         print("current_assets")
#                         print(current_assets)
#                         print(tri_adj_close)
#                         if(count > top_pick):
#                             break
#                         status = True
#                         if(bot == "UNO" or bot == "UCDC"):
#                             if(tri_adj_close > current_assets/100):
#                                 status=False
#                         if(status and ticker not in last_ticker and industry_code_field not in last_industry_code):
#                             service_type = "bot_tester"
#                             spot_date = spot_date
#                             uid = f"{client_uid}_{currency_code[0]}_{ticker}_{spot_date}_{service_type}_{capital}"
#                             expiry_date = get_expiry_date(time_to_exp[0], str(spot_date), currency_code)
#                             uid = uid.replace("-", "").replace(".", "").replace(" ", "")
#                             temp = pd.DataFrame({"created":[spot_date], "updated":[spot_date], "uid":[uid],"spot_date":[spot_date], 
#                             "expiry_date":[expiry_date], "has_position":["False"], "position_uid":[None],"execution_date":[None], "completed_date":[None], "event":[None],
#                             "rank":[count], "client_uid":[client_uid], "ticker":[ticker],"bot_id":[bot_id],"bot":[bot], "currency_code":[currency_code], "service_type":[service_type], "capital":[capital]}, index=[0])
#                             last_ticker.append(ticker)
#                             last_industry_code.append(industry_code_field)
#                             count+=1
#                             # upsert_data_to_database(temp, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)
#                             tester_pick = tester_pick.append(temp)
#                 print("tester_pick")
#                 print(tester_pick)
#                 all.append(tester_pick)
#     data = []
#     for frame in all:
#         df = frame.reset_index()
#         data.append(df)
#     data = pd.concat(data)
#     print(data)
#     data = data.drop(columns=["index"])
#     upsert_data_to_database(data, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)

# def bot_advisor_manual_populate(client_name="HANWHA", time_to_exp=[0.07692], top_pick = 2):
#     all = []
#     bot_option_type = get_bot_option_type(time_to_exp)
#     industry_code = get_industry_code()
#     stock_pick = pd.DataFrame({"ticker":[""], "spot_date":[], "currency_code":[]}, index=[])
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["000880.KS", "000660.KS"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["KRW", "KRW"]}, index=[0, 1]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["AIG", "AMD.O"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["USD", "USD"]}, index=[2, 3]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["601398.SS", "000001.SZ"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["CNY", "CNY"]}, index=[4, 5]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["0005.HK", "0700.HK"], "spot_date":["2021-04-05", "2021-04-05"], "currency_code":["HKD", "HKD"]}, index=[6, 7]))

#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["003550.KS", "004020.KS"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["KRW", "KRW"]}, index=[8, 9]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["F", "AAL.O"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["USD", "USD"]}, index=[10, 11]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["600115.SS", "600600.SS"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["CNY", "CNY"]}, index=[12, 13]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["1398.HK", "1928.HK"], "spot_date":["2021-04-12", "2021-04-12"], "currency_code":["HKD", "HKD"]}, index=[14, 15]))

#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["004170.KS", "006800.KS"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["KRW", "KRW"]}, index=[16, 17]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["AA", "CAT"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["USD", "USD"]}, index=[18, 19]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["601009.SS", "601111.SS"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["CNY", "CNY"]}, index=[20, 21]))
#     stock_pick = stock_pick.append(pd.DataFrame({"ticker":["2318.HK", "9618.HK"], "spot_date":["2021-04-19", "2021-04-19"], "currency_code":["HKD", "HKD"]}, index=[22, 23]))
#     print(stock_pick)
#     for spot_date in stock_pick["spot_date"].unique():
#         for currency_code in stock_pick["currency_code"].unique():
#             for capital in ["small", "large", "large_margin"]:
#                 available_pick = stock_pick.loc[stock_pick["currency_code"] == currency_code]
#                 available_pick = available_pick.loc[ available_pick["spot_date"] == spot_date]
#                 print(available_pick)
#                 client_uid = get_client_uid(client_name=client_name)
#                 user_id = get_user_id(client_uid, currency_code, advisor=True, capital=capital)
#                 print(client_uid)
#                 print(user_id)
#                 bot_ranking = get_bot_ranking(available_pick["ticker"], spot_date)
#                 price = get_newest_price(available_pick["ticker"], spot_date, master_ohlcvtr=True)
#                 print(spot_date)
#                 filter_field = ["ticker"]
#                 rename = {}
#                 bot_option_type = get_bot_option_type(time_to_exp)
#                 for index, row in bot_option_type.iterrows():
#                     option_type = row["bot_option_type"]
#                     if(option_type == "CLASSIC") : option_type = option_type.lower()
#                     bot_type = row["bot_type"].lower()
#                     time_to_exp_str = row["time_to_exp_str"]
#                     filter_field.append(f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob")
#                     temp = {f"{bot_type}_{option_type}_{time_to_exp_str}_pnl_class_prob" : f"{bot_type.upper()}_{option_type}_{time_to_exp_str}"}
#                     rename.update(temp)
#                 bot_ranking = bot_ranking[filter_field]
#                 bot_ranking = bot_ranking.rename(columns=rename)
#                 bot_advisor_pick = pd.DataFrame({"ticker":[], "spot_date":[], "bot_id":[], "prob":[], "bot":[]}, index=[])
#                 for index, row in bot_ranking.iterrows():
#                     for col in bot_ranking.columns:
#                         if(col != "ticker"):
#                             bot = col.split("_")
#                             temp = pd.DataFrame({"ticker":[row["ticker"]], "spot_date":[spot_date], 
#                                 "bot_id":[col], "prob":[row[col]], "bot":[bot[0]]}, index=[0])
#                             bot_advisor_pick = bot_advisor_pick.append(temp)
#                 bot_advisor_pick = bot_advisor_pick.sort_values(by=["prob"], ascending=[False])
#                 bot_advisor_pick = bot_advisor_pick.merge(price, how="left", on="ticker")
#                 bot_advisor_pick = bot_advisor_pick.reset_index(inplace=False, drop=True)
#                 print(bot_advisor_pick)
#                 current_assets = get_current_assets(user_id)
#                 advisor_pick = pd.DataFrame({"created":[], "updated":[], "uid":[],"spot_date":[], "expiry_date":[], "has_position":[], 
#                     "position_uid":[],"execution_date":[], "completed_date":[], "event":[], "rank":[], "client_uid":[], "ticker":[], 
#                     "bot_id":[], "currency_code":[], "service_type":[], "bot":[]}, index=[])
#                 count = 1
#                 last_ticker = [""]
#                 for index, row in bot_advisor_pick.iterrows():
#                     ticker = row["ticker"]
#                     bot_id = row["bot_id"]
#                     bot = row["bot"]
#                     tri_adj_close = row["tri_adj_close"]
#                     if(count > top_pick):
#                         break
#                     status = True
#                     if(bot == "UNO" or bot == "UCDC"):
#                         if(tri_adj_close > current_assets/100):
#                             status=False
#                     if(status and ticker not in last_ticker):
#                         service_type = "bot_advisor"
#                         spot_date = spot_date
#                         uid = f"{client_uid}_{currency_code[0]}_{ticker}_{spot_date}_{service_type}_{capital}"
#                         expiry_date = get_expiry_date(time_to_exp[0], str(spot_date), currency_code)
#                         uid = uid.replace("-", "").replace(".", "").replace(" ", "")
#                         temp = pd.DataFrame({"created":[spot_date], "updated":[spot_date], "uid":[uid],"spot_date":[spot_date], "bot":[None], 
#                         "expiry_date":[expiry_date], "has_position":["False"], "position_uid":[None],"execution_date":[None], "completed_date":[None], "event":[None],
#                         "rank":[count], "client_uid":[client_uid], "ticker":[ticker],"bot_id":[bot_id], "currency_code":[currency_code], "service_type":[service_type], "capital":[capital]}, index=[0])
#                         last_ticker.append(ticker)
#                         count+=1
#                         advisor_pick = advisor_pick.append(temp)
#                 print(advisor_pick)
#                 print("advisor_pick")
#                 print(advisor_pick)
#                 all.append(advisor_pick)
#     data = []
#     for frame in all:
#         df = frame.reset_index()
#         data.append(df)
#     data = pd.concat(data)
#     data = data.drop(columns=["index"])
#     print(data)
#     upsert_data_to_database(data, get_client_top_stock_table_name(), "uid", how="ignore", cpu_count=True, Text=True)


if __name__ == '__main__':
    print("Do Process")
    # bot_advisor_manual_populate()
    # bot_tester_manual_populate()
    # test_pick(currency_code=["USD"], client_name="FELS")
    # test_pick(currency_code=["EUR"], client_name="FELS")
    # test_pick(currency_code=["USD"], client_name="HANWHA")
    # test_pick(currency_code=["KRW"], client_name="HANWHA")
    test_pick(currency_code=["HKD"], client_name="HANWHA")
    # test_pick(currency_code=["CNY"], client_name="HANWHA")

    # populate_fels_bot(currency_code=["USD"], client_name="FELS", top_pick = 5)
    # populate_fels_bot(currency_code=["EUR"], client_name="FELS", top_pick = 5)

    # populate_bot_advisor(currency_code=["USD"], client_name="HANWHA", capital="small")
    # populate_bot_advisor(currency_code=["USD"], client_name="HANWHA", capital="large")
    # populate_bot_advisor(currency_code=["USD"], client_name="HANWHA", capital="large_margin")
    # populate_bot_advisor(currency_code=["KRW"], client_name="HANWHA", capital="small")
    # populate_bot_advisor(currency_code=["KRW"], client_name="HANWHA", capital="large")
    # populate_bot_advisor(currency_code=["KRW"], client_name="HANWHA", capital="large_margin")
    # populate_bot_advisor(currency_code=["HKD"], client_name="HANWHA", capital="small")
    # populate_bot_advisor(currency_code=["HKD"], client_name="HANWHA", capital="large")
    # populate_bot_advisor(currency_code=["HKD"], client_name="HANWHA", capital="large_margin")
    # populate_bot_advisor(currency_code=["CNY"], client_name="HANWHA", capital="small")
    # populate_bot_advisor(currency_code=["CNY"], client_name="HANWHA", capital="large")
    # populate_bot_advisor(currency_code=["CNY"], client_name="HANWHA", capital="large_margin")

    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="UNO", top_pick=1, top_pick_stock=25)
    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="UCDC", top_pick=1, top_pick_stock=25)
    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="small", bot="CLASSIC", top_pick=1, top_pick_stock=25)

    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="UNO", top_pick=2, top_pick_stock=25)
    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="UCDC", top_pick=2, top_pick_stock=25)
    # populate_bot_tester(currency_code=["USD"], client_name="HANWHA", capital="large", bot="CLASSIC", top_pick=2, top_pick_stock=25)

    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="UNO", top_pick=1, top_pick_stock=25)
    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="UCDC", top_pick=1, top_pick_stock=25)
    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="small", bot="CLASSIC", top_pick=1, top_pick_stock=25)
    
    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="UNO", top_pick=2, top_pick_stock=25)
    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="UCDC", top_pick=2, top_pick_stock=25)
    # populate_bot_tester(currency_code=["KRW"], client_name="HANWHA", capital="large", bot="CLASSIC", top_pick=2, top_pick_stock=25)
