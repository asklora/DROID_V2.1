import os
import pandas as pd
from general.date_process import (
    relativedelta,
    str_to_date,
    datetimeNow,
    droid_start_date,
    dateNow)
from general.slack import report_to_slack
from general.data_process import uid_maker, remove_null
from general.sql_query import (get_active_universe, get_data_by_table_name)
from general.sql_output import upsert_data_to_database, insert_data_to_database
from datasource.dsws import get_data_history_from_dsws
from general.table_name import (get_data_dividend_table_name, get_data_dividend_daily_table_name, get_latest_price_table_name)

data_dividend_table = get_data_dividend_table_name()
data_dividend_daily_table = get_data_dividend_daily_table_name()
latest_price_table = get_latest_price_table_name()

def get_dividends_from_dsws():
    print("{} : === Dividens Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    identifier="ticker"
    filter_field = ["UDDE"]
    universe = get_active_universe()
    universe = universe[["ticker"]]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1)
    if(len(result)) > 0 :
        result = result.rename(columns={"UDDE": "amount"})
        result = result.dropna(subset=["amount"])
        result = remove_null(result, "amount")
        result = uid_maker(result, uid="uid", ticker="vix_index", trading_day="ex_dividend_date")
        print(result)
        upsert_data_to_database(result, get_data_dividend_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Dividens Updated ===".format(datetimeNow()))

def cal_q_daily():
    dividend_data = get_data_by_table_name(data_dividend_table)
    universe = get_active_universe()
    intraday_price = get_data_by_table_name(latest_price_table)
    dividend_data = dividend_data[dividend_data.ticker.isin(universe.ticker)]
    dividend_data = dividend_data.merge(universe[['ticker','index']], on='ticker', how='left')
    days_q = pd.DataFrame(range(1, os.getenv("DAILY_Q_DAYS")))
    dates_temp = days_q
    dates_temp['spot_date'] = str_to_date(dateNow())
    def calculate_expiry(row):
        return (row['spot_date'] + relativedelta(days=row[0])).date()
    dates_temp['expiry_date'] = dates_temp.apply(calculate_expiry, axis=1)
    dates_temp3 = dates_temp.copy()
    result = pd.DataFrame()
    for ticker in dividend_data.ticker.unique():
        temp_q = pd.DataFrame()
        dates_temp = pd.DataFrame(dates_temp3)
        dates_temp2 = dates_temp.copy()
        dates_temp = dates_temp2
        dates_temp['id'] = 2
        dividend_data['id'] = 2
        dates_temp = pd.merge(dates_temp, dividend_data[dividend_data.ticker == ticker], on='id', how='outer')
        dates_temp = dates_temp[(dates_temp.spot_date <= dates_temp.ex_dividend_date) &(dates_temp.ex_dividend_date <= dates_temp.expiry_date)]
        dates_temp = dates_temp.groupby(['expiry_date'])['amount'].sum().reset_index()
        dates_temp2 = dates_temp2.merge(dates_temp, on='expiry_date', how='left').fillna(0)
        temp_q['q'] = dates_temp2['amount'] / intraday_price.loc[intraday_price.ticker == ticker, 'close'].values
        temp_q['ticker'] = ticker
        temp_q['spot_date'] = dates_temp2['spot_date']
        temp_q['expiry_date'] = dates_temp2['expiry_date']
        temp_q['t'] = days_q[0]
        result = result.append(temp_q)

    result = result.merge(universe[['ticker', 'index']], on='ticker', how='left')

    result = uid_maker(result, uid="uid", ticker="ticker", trading_day="t")
    insert_data_to_database(result, data_dividend_daily_table, how="replace")
    report_to_slack("{} : === Daily Dividend Updated ===".format(datetimeNow()))