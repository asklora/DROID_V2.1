
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from general.data_process import tuple_data
from general.sql_query import read_query
from general.date_process import dateNow
from general.slack import report_to_slack
from general.data_process import uid_maker
from general.sql_output import upsert_data_to_database
from general.table_name import (
    get_client_table_name, 
    get_universe_table_name, 
    get_universe_client_table_name, 
    get_universe_rating_table_name)

def check_currency_code(currency_code, client_uid):
    query = ""
    if (type(currency_code) != type(None)):
        query = f"ticker in (select ticker from {get_universe_table_name()} where is_active=True and "
        query+= f"currency_code in {tuple_data(currency_code)} and "
        query += f"ticker in (select ticker from {get_universe_client_table_name()} where client_uid='{client_uid}')) "
    return query

def get_client_universe(currency_code, client_uid):
    table_name = get_universe_table_name()
    query = f"select * from {table_name} "
    check = check_currency_code(currency_code, client_uid)
    if (check != ""):
        query += f"where {check} "
    data = read_query(query, table=table_name, cpu_counts=True)
    return data

def get_client_uid(client_name="HANWHA"):
    table_name = get_client_table_name()
    query = f"select * from {table_name} where client_name = '{client_name}';"
    data = read_query(query, table=table_name, cpu_counts=True)
    return data.loc[0, "client_uid"]

def another_top_stock(currency_code, client_uid, top_five_ticker_list):
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
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit 18 "
    data = read_query(query, table=table_name, cpu_counts=True)
    return data

def top_stock_distinct_industry(currency_code, client_uid):
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
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit 30) f4 where rn=1) f5 "
    query += f"order by ribbon_score DESC, wts_rating DESC, wts_score DESC, ticker ASC limit 7; "
    data = read_query(query, table=table_name, cpu_counts=True)
    return data

def hanwha_test_pick(currency_code=None):
    print("{} : === HANWHA WEEKLY PICK STARTED ===".format(dateNow()))
    client_uid = get_client_uid()
    universe = get_client_universe(currency_code, client_uid)
    top_five = top_stock_distinct_industry(currency_code, client_uid)
    print(top_five)
    top_ten = another_top_stock(currency_code, client_uid, top_five["ticker"])
    print(top_ten)
    top_stock = top_five.append(top_ten)
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
    today = datetime.now().date()
    print(today)
    print(today.weekday())
    while (today.weekday() != 0):
        today =  today - relativedelta(days=1)
    result["spot_date"] = today
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="client_uid", date=False)
    result = uid_maker(result, uid="uid", ticker="uid", trading_day="spot_date", date=True)
    print(result)
    print("{} : === HANWHA WEEKLY PICK COMPLETED ===".format(dateNow()))
    upsert_data_to_database(result, "client_test_pick", "uid", how="ignore", cpu_count=True, Text=True)
    report_to_slack("{} : === HANWHA WEEKLY PICK COMPLETED ===".format(dateNow()))

if __name__ == '__main__':
    print("Do Process")
    hanwha_test_pick(currency_code=["USD"])
    hanwha_test_pick(currency_code=["KRW"])
    hanwha_test_pick(currency_code=["HKD"])
    hanwha_test_pick(currency_code=["CNY"])