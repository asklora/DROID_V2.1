import sys
import gc
import logging
import numpy as np
np.seterr(divide="ignore", invalid="ignore")
import pandas as pd
pd.options.mode.chained_assignment = None
from tqdm import tqdm
from datetime import date
from general.data_process import tuple_data
from general.sql_query import read_query
from django.core.management.base import BaseCommand
from general.date_process import dateNow
from general.table_name import get_bot_classic_backtest_table_name, get_bot_ucdc_backtest_table_name, get_bot_uno_backtest_table_name
from bot.data_process import check_start_end_date, check_time_to_exp
from bot.preprocess import cal_interest_rate, cal_q
from bot.black_scholes import (find_vol, deltaUnOC)
from bot.data_download import (
    check_ticker_currency_code_query,
    get_master_tac_price, 
    get_interest_rate_data,
    get_dividends_data)

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-run_number", "--run_number", help="run_number", type=int, default=0)

    def handle(self, *args, **options):
        ticker = "TSLA.O"
        start_date = "2021-12-15"
        end_date = dateNow()
        time_to_exp = 0.07692
        option_type = "ITM"
        generate_hedging(start_date=start_date, end_date=end_date, time_to_exp=[time_to_exp], ticker=[ticker], option_type=[option_type])

        # ticker = "TSLA.O"
        # option_type = "OTM"
        # start_date = "2022-01-03"
        # end_date = dateNow()
        # time_to_exp = 0.07692
        # generate_hedging(start_date=start_date, end_date=end_date, time_to_exp=[time_to_exp], ticker=[ticker], option_type=[option_type])
        # pass

# *********************** Filling up the Null values **************************
def shift5_numba(arr, num, fill_value=np.nan):
    # This function is used for shifting the numpy array
    result = np.empty_like(arr)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result[:] = arr
    return result

def get_bot_backtest_data_date_list(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, option_type=None, uno=False, ucdc=False, classic=False, null_filler=False):
    start_date, end_date = check_start_end_date(start_date, end_date)
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    else:
        table_name = get_bot_classic_backtest_table_name()
    
    query = f"select distinct spot_date from {table_name} where spot_date >= '{start_date}' and spot_date <= '{end_date}' "
    query += f" "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    if(type(time_to_exp) != type(None)):
        query += f"and time_to_exp in {tuple_data(time_to_exp)} "
    if(option_type):
        query += f"and option_type in {tuple_data(option_type)} "
    if(null_filler):
        query += f"and event is null "
    data = read_query(query, table_name, cpu_counts=True)
    return data.spot_date.unique()

def get_bot_backtest_data(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, option_type=None, uno=False, ucdc=False, classic=False, null_filler=False, not_null=False):
    start_date, end_date = check_start_end_date(start_date, end_date)
    if(uno):
        table_name = get_bot_uno_backtest_table_name()
    elif(ucdc):
        table_name = get_bot_ucdc_backtest_table_name()
    elif(classic):
        table_name = get_bot_classic_backtest_table_name()
    else:
        table_name = get_bot_uno_backtest_table_name()
    
    query = f"select * from {table_name} where spot_date >= '{start_date}' and spot_date <= '{end_date}' "
    query += f" "
    check = check_ticker_currency_code_query(ticker=ticker, currency_code=currency_code)
    if(check != ""):
        query += f"and " + check
    if(type(time_to_exp) != type(None)):
        query += f"and time_to_exp in {tuple_data(time_to_exp)} "
    if(null_filler):
        query += f"and event is null "
    if(option_type):
        query += f"and option_type in {tuple_data(option_type)} "
    if(not_null):
        query += f"and event is not null "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def generate_hedging(start_date=None, end_date=None, time_to_exp=None, ticker=None, option_type=None, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    start_date, end_date = check_start_end_date(start_date, end_date)
    # This function is used for filling the nulls in executive options database. Checks whether the options are expired
    # or knocked out or not triggered at all.
    tqdm.pandas()
    dates_df_unique = get_bot_backtest_data_date_list(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, option_type=option_type, uno=True, null_filler=False)
    # Dividing the dates to sections for running manually
    dates_df_unique = np.sort(dates_df_unique)
    divison_length = round(len(dates_df_unique) / total_no_of_runs)
    dates_per_run = [dates_df_unique[x:x + divison_length] for x in range(0, len(dates_df_unique), divison_length)]
    # **************************************************
    date_min = min(dates_per_run[run_number])
    date_max = max(dates_per_run[run_number])
    # Downloading the options that are not triggered from the executive database.
    null_df = get_bot_backtest_data(start_date=date_min, end_date=date_max, time_to_exp=time_to_exp, ticker=ticker, option_type=option_type, uno=True, null_filler=False)
    start_date = null_df.spot_date.min()
    #tac_data = tac_data_download_null_filler(start_date, args)
    tac_data = get_master_tac_price(start_date=date_min, end_date=date_max, ticker=ticker)
    tac_data = tac_data.sort_values(by=["currency_code", "ticker", "trading_day"], ascending=True)
    interest_rate_data = get_interest_rate_data()
    dividends_data = get_dividends_data()

    # ********************************************************************************************
    # ********************************************************************************************
    prices_df = tac_data.pivot_table(index=tac_data.trading_day, columns="ticker", values="tri_adj_close",aggfunc="first", dropna=False)
    prices_df = prices_df.ffill()
    prices_df = prices_df.bfill()

    dates_df = prices_df.index.to_series()
    dates_df = dates_df.reset_index(drop=True)
    dates_df = pd.DataFrame(dates_df)
    dates_df["spot_date_index"] = dates_df.index
    dates_df["expiry_date_index"] = dates_df.index
    dates_df = dates_df.rename(columns={"trading_day": "spot_date"})
    null_df = null_df.merge(dates_df[["spot_date", "spot_date_index"]], on=["spot_date"], how="left")
    dates_df = dates_df.rename(columns={"spot_date": "expiry_date"})
    null_df = null_df.merge(dates_df[["expiry_date", "expiry_date_index"]], on=["expiry_date"], how="left")
    null_df["expiry_date_index"].fillna(-2, inplace=True)

    prices_ind_df = pd.DataFrame(prices_df.columns)
    prices_ind_df["ticker_index"] = prices_ind_df.index
    null_df = null_df.merge(prices_ind_df[["ticker", "ticker_index"]], on=["ticker"])
    null_df = null_df.sort_values(by="expiry_date", ascending=False)

    prices_np = prices_df.values
    dates_np = dates_df.values
    del prices_df, tac_data
    gc.collect()
    table_name = get_bot_uno_backtest_table_name()

    def exec_fill_fun(row, prices_np, dates_np, null_df):
        result = row.to_frame().reset_index(inplace=False)
        column_name = result.columns.values.tolist()
        result = pd.DataFrame(columns=result[column_name[0]].to_list(), data=[result[column_name[1]].to_list()])
        result_columns = result.columns.values.tolist()
        # Calculate the desired quantities row by row.
        # Everything is converted to numpy array for faster runtime.
        if row.expiry_date_index == -2:
            row.expiry_date_index = prices_np.shape[0] - 1
        prices_temp = prices_np[int(row.spot_date_index):int(row.expiry_date_index+1), int(row.ticker_index)]
        if len(prices_temp) == 0:
            return row
        dates_temp = dates_np[int(row.spot_date_index):int(row.expiry_date_index+1), 0]
        t = np.full((len(prices_temp)), ((row["expiry_date"] - dates_temp).astype("timedelta64[D]")) / np.timedelta64(1, "D"))
        strike = np.full((len(prices_temp)), row["strike"])
        barrier = np.full((len(prices_temp)), row["barrier"])

        cond = (null_df.ticker == row.ticker) & (null_df.spot_date >= row.spot_date) &\
               (null_df.spot_date <= row.expiry_date) & (null_df.time_to_exp == row.time_to_exp) &\
               (null_df.option_type == row.option_type)

        dates_df2 = pd.DataFrame(dates_temp, columns=["spot_date"])
        null_df_temp = null_df[cond]
        null_df_temp = null_df_temp.drop_duplicates(subset="spot_date", keep="last")
        null_df_temp = dates_df2.merge(null_df_temp, on="spot_date", how="left")
        null_df_temp = null_df_temp.bfill().ffill()

        atmv_0 = null_df_temp["atm_volatility_spot"].values
        atmv_1Y = null_df_temp["atm_volatility_one_year"].values
        atmv_inf = null_df_temp["atm_volatility_infinity"].values
        skew_1m = null_df_temp["slope"].values
        skew_inf = null_df_temp["slope_inf"].values
        curv_1m = null_df_temp["deriv"].values
        curv_inf = null_df_temp["deriv_inf"].values

        days_to_expiry = np.full((len(prices_temp)),((row["expiry_date"] - dates_temp).astype("timedelta64[D]")) / np.timedelta64(1, "D"))

        r = cal_interest_rate(interest_rate_data[interest_rate_data.currency_code == row.currency_code], days_to_expiry)
        q = cal_q(row, dividends_data[dividends_data.ticker == row.ticker], dates_temp, prices_temp)
        v1 = find_vol(np.divide(strike, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)
        v2 = find_vol(np.divide(barrier, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)
        stock_balance = deltaUnOC(prices_temp, strike, barrier, (barrier - strike), t, r, q, v1, v2)
        stock_balance = np.nan_to_num(stock_balance)
        last_hedge = np.copy(stock_balance)
        last_hedge = shift5_numba(last_hedge, 1)
        last_hedge = np.nan_to_num(last_hedge)
        hedge = 0.05

        # Condition
        condition1A = prices_temp > strike
        condition2A = np.abs(last_hedge - stock_balance) <= 0.05
        conditionA = condition1A & condition2A
        condition1B = prices_temp <= strike
        condition2B = np.abs(last_hedge - stock_balance) <= 0.01
        conditionB = condition1B & condition2B
        condition = conditionA | conditionB
        stock_balance[condition] = 2
        stock_balance = fill_zeros_with_last(stock_balance)
        barrier_indices = np.argmax((prices_temp >= row.barrier))
        stock_balance2 = np.copy(stock_balance)
        stock_balance2 = shift5_numba(stock_balance2, 1)
        stock_balance2 = np.nan_to_num(stock_balance2)
        if barrier_indices != 0:
            # barrier(knockout) is triggered.
            indices = barrier_indices + 1
            stock_balance_dataframe = pd.DataFrame(columns=["stock_balance"], data=stock_balance).reset_index(inplace=False)[:indices]
            stock_price_dataframe = pd.DataFrame(columns=["stock_price"], data=prices_temp).reset_index(inplace=False)[:indices]
            now_price_dataframe = pd.DataFrame(columns=["now_price"], data=prices_temp).reset_index(inplace=False)[:indices]
            now_date_dataframe = pd.DataFrame(columns=["now_date"], data=dates_temp).reset_index(inplace=False)[:indices]
            r_dataframe = pd.DataFrame(columns=["r"], data=r).reset_index(inplace=False)[:indices]
            q_dataframe = pd.DataFrame(columns=["q"], data=q).reset_index(inplace=False)[:indices]
            v1_dataframe = pd.DataFrame(columns=["v1"], data=v1).reset_index(inplace=False)[:indices]
            v2_dataframe = pd.DataFrame(columns=["v2"], data=v2).reset_index(inplace=False)[:indices]
            t_dataframe = pd.DataFrame(columns=["t"], data=t).reset_index(inplace=False)[:indices]

            data = stock_balance_dataframe.merge(stock_price_dataframe, how="left", on=["index"])
            data = data.merge(now_price_dataframe, how="left", on=["index"])
            data = data.merge(now_date_dataframe, how="left", on=["index"])
            data = data.merge(r_dataframe, how="left", on=["index"])
            data = data.merge(q_dataframe, how="left", on=["index"])
            data = data.merge(v1_dataframe, how="left", on=["index"])
            data = data.merge(v2_dataframe, how="left", on=["index"])
            data = data.merge(t_dataframe, how="left", on=["index"])

            event = "KO"
            expiry_index = len(data) - 1
            event_price = prices_temp[barrier_indices]
            expiry_price = prices_temp[-1]
            event_date = dates_temp[barrier_indices]           
            expiry_payoff = row["barrier"] - row["strike"]
            expiry_return = (prices_temp[-1] / prices_temp[0]) - 1
            drawdown_return  = np.amin(prices_temp) / prices_temp[0] - 1
            duration = (row["event_date"] - row["spot_date"]).days / 365
            stock_balance[barrier_indices] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            pnl = pnl[:barrier_indices+1]
            churn = churn[:barrier_indices+1]
            pnl = np.nansum(pnl)
            delta_churn = np.abs(churn)
            delta_churn = np.nansum(delta_churn)
            bot_return = pnl / prices_temp[0]

            num_hedges = stock_balance2 - stock_balance
            num_hedges_dataframe = pd.DataFrame(columns=["num_hedges"], data=num_hedges).reset_index(inplace=False)[:indices]
            data = data.merge(num_hedges_dataframe, how="left", on=["index"])
            data = data.drop(columns=["index"])

            result = result.drop(columns=data.columns.values.tolist())
            result = result.drop(columns=["event_price", "expiry_price", "event_date", "expiry_payoff", "expiry_return", "drawdown_return",
            "duration", "pnl", "delta_churn", "bot_return", "event"])
            column_name = result.columns.values.tolist()
            result = pd.concat([data, result], axis=1)
            for name in column_name:
                result[name] = result[name].bfill().ffill()
            result["days_to_expiry"] = result["t"]
            result.loc[expiry_index, "event_price"] = event_price
            result.loc[expiry_index, "expiry_price"] = expiry_price
            result.loc[expiry_index, "event_date"] = event_date
            result.loc[expiry_index, "expiry_payoff"] = expiry_payoff
            result.loc[expiry_index, "expiry_return"] = expiry_return
            result.loc[expiry_index, "drawdown_return"] = drawdown_return
            result.loc[expiry_index, "duration"] = duration
            result.loc[expiry_index, "pnl"] = pnl
            result.loc[expiry_index, "delta_churn"] = delta_churn
            result.loc[expiry_index, "bot_return"] = bot_return
            result.loc[expiry_index, "event"] = event
            uid = result.loc[0, "uid"]
            result["uid"] = (result["uid"] + result["now_date"].astype(str)).str.replace("-", "", regex=True)
            result[result_columns].to_csv(f"Hedging_{uid}.csv")
        elif dates_temp[-1] == row["expiry_date"]:
            # Expiry is triggered.
            stock_balance_dataframe = pd.DataFrame(columns=["stock_balance"], data=stock_balance).reset_index(inplace=False)
            stock_price_dataframe = pd.DataFrame(columns=["stock_price"], data=prices_temp).reset_index(inplace=False)
            now_price_dataframe = pd.DataFrame(columns=["now_price"], data=prices_temp).reset_index(inplace=False)
            now_date_dataframe = pd.DataFrame(columns=["now_date"], data=dates_temp).reset_index(inplace=False)
            r_dataframe = pd.DataFrame(columns=["r"], data=r).reset_index(inplace=False)
            q_dataframe = pd.DataFrame(columns=["q"], data=q).reset_index(inplace=False)
            v1_dataframe = pd.DataFrame(columns=["v1"], data=v1).reset_index(inplace=False)
            v2_dataframe = pd.DataFrame(columns=["v2"], data=v2).reset_index(inplace=False)
            t_dataframe = pd.DataFrame(columns=["t"], data=t).reset_index(inplace=False)

            data = stock_balance_dataframe.merge(stock_price_dataframe, how="left", on=["index"])
            data = data.merge(now_price_dataframe, how="left", on=["index"])
            data = data.merge(now_date_dataframe, how="left", on=["index"])
            data = data.merge(r_dataframe, how="left", on=["index"])
            data = data.merge(q_dataframe, how="left", on=["index"])
            data = data.merge(v1_dataframe, how="left", on=["index"])
            data = data.merge(v2_dataframe, how="left", on=["index"])
            data = data.merge(t_dataframe, how="left", on=["index"])

            event = "expire"
            expiry_index = len(data) - 1
            event_price = prices_temp[-1]
            expiry_price = prices_temp[-1]
            event_date = dates_temp[-1]            
            expiry_payoff = max(row["now_price"] - row["strike"], 0)
            expiry_return = (prices_temp[-1] / prices_temp[0]) - 1
            drawdown_return  = np.amin(prices_temp) / prices_temp[0] - 1
            duration = (row["event_date"] - row["spot_date"]).days / 365
            stock_balance[-1] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            pnl = np.nansum(pnl)
            delta_churn = np.abs(churn)
            delta_churn = np.nansum(delta_churn)
            bot_return = pnl / prices_temp[0]

            num_hedges = stock_balance2 - stock_balance
            num_hedges_dataframe = pd.DataFrame(columns=["num_hedges"], data=num_hedges).reset_index(inplace=False)
            data = data.merge(num_hedges_dataframe, how="left", on=["index"])
            data = data.drop(columns=["index"])

            result = result.drop(columns=data.columns.values.tolist())
            result = result.drop(columns=["event_price", "expiry_price", "event_date", "expiry_payoff", "expiry_return", "drawdown_return",
            "duration", "pnl", "delta_churn", "bot_return", "event"])
            column_name = result.columns.values.tolist()
            result = pd.concat([data, result], axis=1)
            for name in column_name:
                result[name] = result[name].bfill().ffill()
            result["days_to_expiry"] = result["t"]
            result.loc[expiry_index, "event_price"] = event_price
            result.loc[expiry_index, "expiry_price"] = expiry_price
            result.loc[expiry_index, "event_date"] = event_date
            result.loc[expiry_index, "expiry_payoff"] = expiry_payoff
            result.loc[expiry_index, "expiry_return"] = expiry_return
            result.loc[expiry_index, "drawdown_return"] = drawdown_return
            result.loc[expiry_index, "duration"] = duration
            result.loc[expiry_index, "pnl"] = pnl
            result.loc[expiry_index, "delta_churn"] = delta_churn
            result.loc[expiry_index, "bot_return"] = bot_return
            result.loc[expiry_index, "event"] = event
            uid = result.loc[0, "uid"]
            result["uid"] = (result["uid"] + result["now_date"].astype(str)).str.replace("-", "", regex=True)
            result[result_columns].to_csv(f"Hedging_{uid}.csv")
        else:
            stock_balance_dataframe = pd.DataFrame(columns=["stock_balance"], data=stock_balance).reset_index(inplace=False)
            stock_price_dataframe = pd.DataFrame(columns=["stock_price"], data=prices_temp).reset_index(inplace=False)
            now_price_dataframe = pd.DataFrame(columns=["now_price"], data=prices_temp).reset_index(inplace=False)
            now_date_dataframe = pd.DataFrame(columns=["now_date"], data=dates_temp).reset_index(inplace=False)
            r_dataframe = pd.DataFrame(columns=["r"], data=r).reset_index(inplace=False)
            q_dataframe = pd.DataFrame(columns=["q"], data=q).reset_index(inplace=False)
            v1_dataframe = pd.DataFrame(columns=["v1"], data=v1).reset_index(inplace=False)
            v2_dataframe = pd.DataFrame(columns=["v2"], data=v2).reset_index(inplace=False)
            t_dataframe = pd.DataFrame(columns=["t"], data=t).reset_index(inplace=False)

            data = stock_balance_dataframe.merge(stock_price_dataframe, how="left", on=["index"])
            data = data.merge(now_price_dataframe, how="left", on=["index"])
            data = data.merge(now_date_dataframe, how="left", on=["index"])
            data = data.merge(r_dataframe, how="left", on=["index"])
            data = data.merge(q_dataframe, how="left", on=["index"])
            data = data.merge(v1_dataframe, how="left", on=["index"])
            data = data.merge(v2_dataframe, how="left", on=["index"])
            data = data.merge(t_dataframe, how="left", on=["index"])

            num_hedges = stock_balance2 - stock_balance
            num_hedges_dataframe = pd.DataFrame(columns=["num_hedges"], data=num_hedges).reset_index(inplace=False)
            data = data.merge(num_hedges_dataframe, how="left", on=["index"])
            data = data.drop(columns=["index"])

            result = result.drop(columns=data.columns.values.tolist())
            result = result.drop(columns=["event_price", "expiry_price", "event_date", "expiry_payoff", "expiry_return", "drawdown_return",
            "duration", "pnl", "delta_churn", "bot_return", "event"])
            column_name = result.columns.values.tolist()
            result = pd.concat([data, result], axis=1)
            for name in column_name:
                result[name] = result[name].bfill().ffill()
            result["days_to_expiry"] = result["t"]
            result["event_price"] = None
            result["expiry_price"] = None
            result["event_date"] = None
            result["expiry_payoff"] = None
            result["expiry_return"] = None
            result["drawdown_return"] = None
            result["duration"] = None
            result["pnl"] = None
            result["delta_churn"] = None
            result["bot_return"] = None
            result["event"] = None
            uid = result.loc[0, "uid"]
            result["uid"] = (result["uid"] + result["now_date"].astype(str)).str.replace("-", "", regex=True)
            result[result_columns].to_csv(f"Hedging_{uid}.csv")
        sys.exit(1)
        return row

    logging.basicConfig(filename="logfilename.log", level=logging.INFO)

    def fill_zeros_with_last(arr):
        prev = np.arange(len(arr))
        prev[arr == 2] = 2
        prev = np.maximum.accumulate(prev)
        if(len(arr) > 2):
            return arr[prev]
        return arr

    def foo(k):
        # try:
            # run the null filler for each section of dates
            date_temp = dates_per_run[run_number][k]
            null_df_small = null_df[null_df.spot_date == date_temp]
            print(f"Filling {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")
            null_df_small = null_df_small.progress_apply(lambda x: exec_fill_fun(x, prices_np, dates_np, null_df), axis=1, raw=False)
            null_df_small.drop(["expiry_date_index", "spot_date_index", "ticker_index"], axis=1, inplace=True)
            null_df_small = null_df_small.infer_objects()
            table_name = get_bot_uno_backtest_table_name()
            # upsert_data_to_database(null_df_small, table_name, "uid", how="update", cpu_count=True, Text=True)
            print(f"Finished {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")

        # except Exception as e:
        #     print(e)
        #     logging.error(e)
        #     logging.error(f"Error for {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")
        #     print(f"Error for {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")

    for i in range(int(len(dates_per_run[run_number]))):
        foo(i)
    print(f"Filling up the nulls is finished.")

