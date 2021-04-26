import sys
from botocore import hooks
from numpy import append
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from general.data_process import tuple_data
from general.sql_query import get_master_tac_data, read_query
from general.date_process import dateNow
from general.slack import report_to_slack
from general.data_process import uid_maker, NoneToZero
from general.sql_output import upsert_data_to_database
from general.table_name import (
    get_bot_option_type_table_name,
    get_bot_ranking_table_name,
    get_client_table_name,
    get_master_tac_table_name,
    get_orders_position_table_name, 
    get_universe_table_name, 
    get_universe_client_table_name, 
    get_universe_rating_table_name,
    get_user_account_balance_table_name,
    get_user_clients_table_name)

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

def get_client_uid(client_name="HANWHA"):
    table_name = get_client_table_name()
    query = f"select * from {table_name} where client_name = '{client_name}';"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data.loc[0, "client_uid"]

def get_user_id(client_uid, currency_code):
    table_name = get_user_clients_table_name()
    filter = '{"capital": "small", "service_type": "bot_advisor"}'
    query = f"select user_id from {table_name} where client_uid='{client_uid}' and extra_data='{filter}' and currency_code in {tuple_data(currency_code)};"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    if(len(data) >= 1):
        return data.loc[0, "user_id"]
    else:
        sys.exit("There is no Account for this user")

def get_portolio_ticker_list(user_id):
    table_name = get_orders_position_table_name()
    query = f"select distinct ticker from {table_name} where user_id='{user_id}';"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data["ticker"].to_list()

def get_old_advisor_ticker_pick(client_uid):
    table_name = get_orders_position_table_name()
    query = f"select ticker_rank1 as ticker from client_test_pick where client_uid = '{client_uid}' "
    query += f"UNION "
    query += f"select ticker_rank2 as ticker from client_test_pick where client_uid = '{client_uid}';"
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data["ticker"].to_list()

def get_bot_ranking(ticker, spot_date):
    table_name = get_bot_ranking_table_name()
    query = f"select br.* from {table_name} br inner join (select br1.ticker, max(br1.spot_date) as spot_date  "
    query += f"from {table_name} br1 where spot_date<='{spot_date}' group by br1.ticker) as filter "
    query += f"on filter.ticker=br.ticker and filter.spot_date=br.spot_date "
    query += f"where br.ticker in {tuple_data(ticker)}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def get_newest_price(ticker, spot_date):
    table_name = get_master_tac_table_name()
    query = f"select tac.ticker, tac.trading_day, tac.tri_adj_close from {table_name} tac "
    query += f"inner join (select tac1.ticker, max(tac1.trading_day) as max_date "
    query += f"from {table_name} tac1 where trading_day<='{spot_date}' group by tac1.ticker) as filter "
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

def another_top_stock(currency_code, client_uid, top_five_ticker_list, top_pick_distinct):
    table_name = get_universe_rating_table_name()
    query = f"select f3.ticker, f3.industry_code, f3.ribbon_score, f3.wts_rating, f3.wts_score, (now())::date as forward_date "
    query += f"from (select f2.ticker, f2.industry_code, f2.wts_rating, f2.wts_score, (f2.st + f2.mt + f2.gq) as ribbon_score "
    query += f"from (select f1.ticker, f1.industry_code, "
    query += f"CASE WHEN (f1.wts_rating) >= 5 THEN 1 ELSE 0 END AS st, "
    query += f"CASE WHEN (f1.dlp_1m) >= 5 THEN 1 ELSE 0 END AS mt, "
    query += f"CASE WHEN (f1.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
    query += f"f1.wts_rating + f1.dlp_1m + f1.fundamentals_quality AS wts_score, f1.wts_rating "
    query += f"from (select ur.ticker, ur.wts_rating, ur.dlp_1m, ur.fundamentals_quality, u.industry_code "
    query += f"from {table_name} ur inner join {get_universe_table_name()} u on u.ticker = ur.ticker "
    query += f"where ur.ticker not in {tuple_data(top_five_ticker_list)} " 
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"and ur.{check}"
    query += f") f1) f2) f3 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit {25-top_pick_distinct} "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def top_stock_distinct_industry(currency_code, client_uid, top_pick_distinct):
    table_name = get_universe_rating_table_name()
    query = f"select f5.ticker, f5.industry_code, f5.ribbon_score, f5.wts_rating, f5.wts_score, (now())::date as forward_date "
    query += f"from (select distinct on (f4.industry_code) f4.ticker, f4.industry_code, f4.ribbon_score, f4.wts_rating, f4.wts_score "
    query += f"from (select f3.ticker, f3.industry_code, f3.ribbon_score, f3.wts_rating, f3.wts_score,  "
    query += f"row_number() OVER (PARTITION BY f3.industry_code ORDER BY "
    query += f"f3.ribbon_score DESC, f3.wts_rating DESC, f3.wts_score DESC, f3.ticker ASC) AS rn "
    query += f"from (select f2.ticker, f2.industry_code, f2.wts_rating, f2.wts_score, (f2.st + f2.mt + f2.gq) as ribbon_score "
    query += f"from (select f1.ticker, f1.industry_code, "
    query += f"CASE WHEN (f1.wts_rating) >= 5 THEN 1 ELSE 0 END AS st, "
    query += f"CASE WHEN (f1.dlp_1m) >= 5 THEN 1 ELSE 0 END AS mt, "
    query += f"CASE WHEN (f1.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
    query += f"f1.wts_rating + f1.dlp_1m + f1.fundamentals_quality AS wts_score, f1.wts_rating "
    query += f"from (select ur.ticker, ur.wts_rating, ur.dlp_1m, ur.fundamentals_quality, u.industry_code "
    query += f"from {table_name} ur inner join {get_universe_table_name()} u on u.ticker = ur.ticker "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"where ur.{check}"
    query += f") f1) f2) f3 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC) f4 where rn=1) f5 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit {top_pick_distinct}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def top_stock_distinct_industry(currency_code, client_uid, top_pick_distinct):
    table_name = get_universe_rating_table_name()
    query = f"select f5.ticker, f5.industry_code, f5.ribbon_score, f5.wts_rating, f5.wts_score, (now())::date as forward_date "
    query += f"from (select distinct on (f4.industry_code) f4.ticker, f4.industry_code, f4.ribbon_score, f4.wts_rating, f4.wts_score "
    query += f"from (select f3.ticker, f3.industry_code, f3.ribbon_score, f3.wts_rating, f3.wts_score,  "
    query += f"row_number() OVER (PARTITION BY f3.industry_code ORDER BY "
    query += f"f3.ribbon_score DESC, f3.wts_rating DESC, f3.wts_score DESC, f3.ticker ASC) AS rn "
    query += f"from (select f2.ticker, f2.industry_code, f2.wts_rating, f2.wts_score, (f2.st + f2.mt + f2.gq) as ribbon_score "
    query += f"from (select f1.ticker, f1.industry_code, "
    query += f"CASE WHEN (f1.wts_rating) >= 5 THEN 1 ELSE 0 END AS st, "
    query += f"CASE WHEN (f1.dlp_1m) >= 5 THEN 1 ELSE 0 END AS mt, "
    query += f"CASE WHEN (f1.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
    query += f"f1.wts_rating + f1.dlp_1m + f1.fundamentals_quality AS wts_score, f1.wts_rating "
    query += f"from (select ur.ticker, ur.wts_rating, ur.dlp_1m, ur.fundamentals_quality, u.industry_code "
    query += f"from {table_name} ur inner join {get_universe_table_name()} u on u.ticker = ur.ticker "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"where ur.{check}"
    query += f") f1) f2) f3 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC) f4 where rn=1) f5 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit {top_pick_distinct}; "
    data = read_query(query, table=table_name, cpu_counts=True, prints=False)
    return data

def populate_bot_advisor(client_uid, currency_code, top_pick_distinct, top_stock, time_to_exp):
    user_id = get_user_id(client_uid, currency_code)
    portolio_ticker_list = get_portolio_ticker_list(user_id)
    old_advisor_ticker_list = get_old_advisor_ticker_pick(client_uid)
    available_pick = top_stock.loc[~top_stock["ticker"].isin(portolio_ticker_list)]
    available_pick = available_pick.loc[~available_pick["ticker"].isin(old_advisor_ticker_list)]
    available_pick = available_pick.reset_index(inplace=False, drop=True).head(top_pick_distinct)
    bot_ranking = get_bot_ranking(available_pick["ticker"], top_stock.loc[0, "forward_date"])
    price = get_newest_price(available_pick["ticker"], top_stock.loc[0, "forward_date"])
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
                temp = pd.DataFrame({"ticker":[row["ticker"]], "spot_date":[top_stock.loc[0, "forward_date"]], 
                    "bot_id":[col], "prob":[row[col]], "bot":[bot[0]]}, index=[0])
                bot_advisor_pick = bot_advisor_pick.append(temp)
    bot_advisor_pick = bot_advisor_pick.sort_values(by=["prob"], ascending=[False])
    bot_advisor_pick = bot_advisor_pick.merge(price, how="left", on="ticker")
    bot_advisor_pick = bot_advisor_pick.reset_index(inplace=False, drop=True)
    print(bot_advisor_pick)
    current_assets = get_current_assets(user_id)
    advisor_pick = pd.DataFrame({"currency_code":[currency_code[0]]}, index=[0])
    count = 1
    last_ticker = [""]
    for index, row in bot_advisor_pick.iterrows():
        ticker = row["ticker"]
        bot_id = row["bot_id"]
        bot = row["bot"]
        tri_adj_close = row["tri_adj_close"]
        if(count > 2):
            break
        status = True
        if(bot == "UNO" or bot == "UCDC"):
            if(tri_adj_close > current_assets/100):
                status=False
        if(status):
            advisor_pick[f"ticker_rank{count}"] = ticker
            advisor_pick[f"bot_rank{count}"] = bot_id
            last_ticker.append(ticker)
            count+=1
    print(advisor_pick)
    return advisor_pick

def test_pick(currency_code=None, client_name="HANWHA", top_pick_distinct=7, time_to_exp=[0.07692]):
    print("{} : === CLIENT WEEKLY PICK STARTED ===".format(dateNow()))
    client_uid = get_client_uid(client_name=client_name)
    top_stock_distinct = top_stock_distinct_industry(currency_code, client_uid, top_pick_distinct)
    print(top_stock_distinct)
    top_stock_not_distinct = another_top_stock(currency_code, client_uid, top_stock_distinct["ticker"], top_pick_distinct)
    print(top_stock_not_distinct)
    top_stock = top_stock_distinct.append(top_stock_not_distinct)
    top_stock = top_stock.reset_index(inplace=False, drop=True)
    print(top_stock)
    advisor_pick = populate_bot_advisor(client_uid, currency_code, top_pick_distinct, top_stock, time_to_exp)
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
    result = result.merge(advisor_pick, how="left", on="currency_code")
    today = datetime.now().date()
    print(today)
    print(today.weekday())
    while (today.weekday() != 0):
        today =  today - relativedelta(days=1)
    result["spot_date"] = today
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="client_uid", date=False)
    result = uid_maker(result, uid="uid", ticker="uid", trading_day="spot_date", date=True)
    print(result)
    print("{} : === CLIENT WEEKLY PICK COMPLETED ===".format(dateNow()))
    upsert_data_to_database(result, "client_test_pick", "uid", how="ignore", cpu_count=True, Text=True)
    report_to_slack("{} : === CLIENT WEEKLY PICK COMPLETED ===".format(dateNow()))

if __name__ == '__main__':
    print("Do Process")
    # test_pick(currency_code=["USD"], client_name="FELS")
    # test_pick(currency_code=["EUR"], client_name="FELS")
    test_pick(currency_code=["USD"], client_name="HANWHA")
    test_pick(currency_code=["KRW"], client_name="HANWHA")
    test_pick(currency_code=["HKD"], client_name="HANWHA")
    test_pick(currency_code=["CNY"], client_name="HANWHA")