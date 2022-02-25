import gc
import logging
import numpy as np
np.seterr(divide='ignore', invalid='ignore')
import pandas as pd
pd.options.mode.chained_assignment = None
from tqdm import tqdm
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta
from general.sql_output import upsert_data_to_database
from general.date_process import backdate_by_day, dateNow, droid_start_date
from general.table_name import get_bot_uno_backtest_table_name
from bot.data_process import check_start_end_date, check_time_to_exp
from bot.preprocess import cal_interest_rate, cal_q
from bot.black_scholes import (find_vol, Up_Out_Call, deltaUnOC)
from bot.data_download import (
    get_bot_backtest_data, 
    get_bot_backtest_data_date_list, 
    get_calendar_data, 
    get_currency_data, 
    get_master_tac_price, 
    get_vol_surface_data, 
    get_interest_rate_data,
    get_dividends_data)
from global_vars import modified_delta_list, max_vol, min_vol, large_hedge, small_hedge

def populate_bot_uno_backtest(start_date=None, end_date=None, ticker=None, currency_code=None, time_to_exp=None, mod=False, infer=True, history=False, daily=False, new_ticker=False):
    if type(start_date) == type(None):
        start_date = droid_start_date()
    if type(end_date) == type(None):
        end_date = dateNow()

    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    # https://loratechai.atlassian.net/wiki/spaces/ARYA/pages/82379155/Executive+UnO+KO+Barrier+Options+all+python+function+in+black+scholes.py

    holidays_df = get_calendar_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)
    tac_data2 = get_master_tac_price(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)
    tac_data2 = tac_data2.loc[tac_data2["day_status"] == "trading_day"]
    vol_surface_data = get_vol_surface_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, infer=infer)
    currency_data = get_currency_data(currency_code=currency_code)
    interest_rate_data = get_interest_rate_data()
    dividends_data = get_dividends_data()

    tac_data = tac_data2[tac_data2.ticker.isin(vol_surface_data.ticker.unique())]
    options_df = pd.DataFrame()
    options_df["ticker"] = tac_data.ticker
    options_df["currency_code"] = tac_data.currency_code
    options_df["now_date"] = tac_data.trading_day
    options_df["trading_day"] = tac_data.trading_day
    options_df["spot_date"] = tac_data.trading_day
    options_df["now_price"] = tac_data.tri_adj_close
    options_df["spot_price"] = tac_data.tri_adj_close
    options_df = options_df.merge(vol_surface_data,on=["ticker", "trading_day"])
    options_df.drop(["trading_day"], axis=1, inplace=True)

    # if not infer:
    #     options_df.drop(["stock_price", "parameter_set_date", "alpha"], axis=1, inplace=True)
    options_df = options_df.sort_values(by="spot_date", ascending=False)

    # *****************************************************************************************************
    # Adding maturities and expiry date
    options_df_temp = pd.DataFrame(columns=options_df.columns)
    print("Calculating Time Exp")
    for time_exp in time_to_exp:
        options_df2 = options_df.copy()
        options_df2["time_to_exp"] = time_exp

        print("Calculating Expiry Date")
        days = int(round((time_exp * 365), 0))
        options_df2["expiry_date"] = options_df2["spot_date"] + relativedelta(days=(days-1))
        # Code when we use business days
        # days = int(round((time_exp * 256), 0))
        # options_df2["expiry_date"] = options_df2["spot_date"] + BDay(days-1)
        options_df_temp = options_df_temp.append(options_df2)
        options_df_temp.reset_index(drop=True, inplace=True)
        del options_df2
    del options_df
    options_df = options_df_temp.copy()
    del options_df_temp

    # *****************************************************************************************************
    # making sure that expiry date is not holiday or weekend
    options_df["expiry_date"] = pd.to_datetime(options_df["expiry_date"])
    while(True):
        cond = options_df["expiry_date"].apply(lambda x: x.weekday()) > 4
        options_df.loc[cond, "expiry_date"] = options_df.loc[cond, "expiry_date"] - BDay(1)
        if(cond.all() == False):
            break

    while(True):
        cond = options_df["expiry_date"].isin(holidays_df["non_working_day"].to_list())
        options_df.loc[cond, "expiry_date"] = options_df.loc[cond, "expiry_date"] - BDay(1)
        if(cond.all() == False):
            break

    options_df["expiry_date"] = (options_df["expiry_date"]).apply(lambda x: x.date())

    options_df["days_to_expiry"] = (options_df["expiry_date"] - options_df["spot_date"]).apply(lambda x:x.days)
    # *************************************************************************************************
    # get_interest_rate vectorized function (r)

    unique_horizons = pd.DataFrame(options_df["days_to_expiry"].unique())
    unique_horizons["id"] = 2
    unique_currencies = pd.DataFrame(interest_rate_data["currency_code"].unique())
    unique_currencies["id"] = 2

    rates = pd.merge(unique_horizons, unique_currencies, on="id", how="outer")
    rates = rates.rename(columns={"0_y": "currency_code", "0_x": "days_to_expiry"})
    interest_rate_data = interest_rate_data.rename(columns={"days_to_maturity": "days_to_expiry"})
    rates = pd.merge(rates, interest_rate_data, on=["days_to_expiry", "currency_code"], how="outer")

    def funs(df):
        df = df.sort_values(by="days_to_expiry")
        df = df.reset_index()
        nan_index = df["rate"].index[df["rate"].isnull()].to_series().reset_index(drop=True)
        not_nan_index = df["rate"].index[~df["rate"].isnull()].to_series().reset_index(drop=True)
        for a in nan_index:
            temp = not_nan_index.copy()
            temp[len(temp)] = a
            temp = temp.sort_values()
            temp.reset_index(inplace=True, drop=True)
            ind = temp[temp == a].index
            if(ind - 1 < 0):
                ind = 1
            ind1 = temp[ind - 1]

            if (ind + 1 > len(df) - 1):
                ind = len(df) - 2
            ind2 = temp[ind + 1]
            if(type(df.loc[ind1, "rate"]) == np.float64):
                rate_1 = df.loc[ind1, "rate"]
                rate_2 = df.loc[ind2, "rate"]
                dtm_1 = df.loc[ind1, "days_to_expiry"]
                dtm_2 = df.loc[ind2, "days_to_expiry"]
            else:
                rate_1 = df.loc[ind1, "rate"].iloc[0]
                rate_2 = df.loc[ind2, "rate"].iloc[0]
                dtm_1 = df.loc[ind1, "days_to_expiry"].iloc[0]
                dtm_2 = df.loc[ind2, "days_to_expiry"].iloc[0]
            df.loc[a, "rate"] = rate_1 * (dtm_2 - df.loc[a, "days_to_expiry"])/(dtm_2 - dtm_1) + rate_2\
                                * (df.loc[a, "days_to_expiry"] - dtm_1)/(dtm_2 - dtm_1)
        df = df.set_index("index")
        return df

    rates = rates.groupby("currency_code").apply(lambda x: funs(x))
    rates = rates.drop(columns="currency_code")
    rates = rates.reset_index()

    # *************************************************************************************************
    # currency_data = rates.merge(currency_data, on="currency_code")
    options_df = options_df.merge(rates[["days_to_expiry", "rate", "currency_code"]], on=["days_to_expiry","currency_code"])
    options_df = options_df.rename(columns={"rate": "r"})
    # *************************************************************************************************

    options_df2 = options_df.merge(dividends_data[["ticker", "ex_dividend_date", "amount"]], on="ticker")
    options_df2 = options_df2[(options_df2.spot_date <= options_df2.ex_dividend_date) &
                              (options_df2.ex_dividend_date <= options_df2.expiry_date)]
    options_df2 = options_df2.groupby(["ticker", "spot_date"])["amount"].sum().reset_index()
    options_df = options_df.merge(options_df2, on=["ticker", "spot_date"], how="outer")
    options_df["amount"] = options_df["amount"].fillna(0)
    options_df["amount"] = options_df["amount"] / options_df["spot_price"]
    options_df = options_df.rename(columns={"amount": "q"})

    # *************************************************************************************************
    options_df["t"] = options_df["days_to_expiry"] / 365
    # *************************************************************************************************
    # Adding OPTION configurations

    v0 = find_vol(1, options_df["t"], options_df["atm_volatility_spot"], options_df["atm_volatility_one_year"],
                  options_df["atm_volatility_infinity"], 12, options_df["slope"], options_df["slope_inf"],
                  options_df["deriv"], options_df["deriv_inf"], options_df["r"], options_df["q"])

    v0[v0 <= min_vol] = min_vol
    v0[v0 >=max_vol] = max_vol
    options_df["vol_t"] = v0 * np.sqrt(options_df["time_to_exp"])

    options_df6 = options_df.copy()

    options_df["option_type"] = "ITM"
    options_df["strike_type"] = "ITM"
    options_df["barrier_type"] = "ITM"

    options_df6["option_type"] = "OTM"
    options_df6["strike_type"] = "OTM"
    options_df6["barrier_type"] = "OTM"

    options_df = options_df.append(options_df6)
    options_df.reset_index(inplace=True, drop=True)

    del options_df2, options_df6

    options_df.loc[options_df["strike_type"] == "ITM", "strike"] = options_df["spot_price"] * (1 - options_df["vol_t"] * 0.5)
    options_df.loc[options_df["strike_type"] == "OTM", "strike"] = options_df["spot_price"] * (1 + options_df["vol_t"] * 0.5)
    options_df.loc[options_df["barrier_type"] == "ITM", "barrier"] = options_df["spot_price"] * (1 + options_df["vol_t"] * 1.5)
    options_df.loc[options_df["barrier_type"] == "OTM", "barrier"] = options_df["spot_price"] * (1 + options_df["vol_t"] * 2)

    options_df["v1"] = find_vol(options_df["strike"] / options_df["now_price"], options_df["t"],
        options_df["atm_volatility_spot"], options_df["atm_volatility_one_year"],
        options_df["atm_volatility_infinity"], 12, options_df["slope"], options_df["slope_inf"],
        options_df["deriv"], options_df["deriv_inf"], options_df["r"], options_df["q"])
    options_df["v2"] = find_vol(options_df["barrier"] / options_df["now_price"], options_df["t"],
        options_df["atm_volatility_spot"], options_df["atm_volatility_one_year"],
        options_df["atm_volatility_infinity"], 12, options_df["slope"], options_df["slope_inf"],
        options_df["deriv"], options_df["deriv_inf"], options_df["r"], options_df["q"])
    options_df["target_max_loss"] = Up_Out_Call(options_df["now_price"], options_df["strike"], options_df["barrier"],
        (options_df["barrier"] - options_df["strike"]), options_df["t"],
        options_df["r"], options_df["q"], options_df["v1"], options_df["v2"])
    options_df["target_profit"] = options_df["barrier"] - options_df["strike"]
    options_df["stock_balance"] = None
    options_df["stock_price"] = None
    options_df["event_date"] = None
    options_df["event_price"] = None
    options_df["event"] = None
    options_df["pnl"] = None
    options_df["delta_churn"] = None
    options_df["expiry_payoff"] = None
    options_df["expiry_return"] = None
    options_df["expiry_price"] = None
    options_df["drawdown_return"] = None
    options_df["duration"] = None
    options_df["bot_return"] = None
    options_df["bot_id"] = "UNO_" + options_df["option_type"].astype(str) + "_" + options_df["time_to_exp"].astype(str)
    options_df["bot_id"] = options_df["bot_id"].str.replace(".", "", regex=True)
    options_df["total_bot_share_num"] = 1
    options_df["initial_delta"] = deltaUnOC(options_df["now_price"], options_df["strike"], options_df["barrier"],
        (options_df["barrier"] - options_df["strike"]), options_df["t"],
        options_df["r"], options_df["q"], options_df["v1"], options_df["v2"])
    options_df["current_delta"] = None
    options_df["avg_delta"] = None

    if (mod):
        options_df_temp = pd.DataFrame(columns=options_df.columns)
        for mod_temp in modified_delta_list:
            options_df2 = options_df.copy()

            options_df2["modified"] = 1
            options_df2["option_type"] = options_df2["option_type"] + mod_temp
            options_df2["modify_arg"] = mod_temp
            options_df_temp = options_df_temp.append(options_df2)

        options_df = options_df_temp
        del options_df_temp, options_df2
    else:
        options_df["modified"] = 0
        options_df["modify_arg"] = ""

    if infer:
        options_df["inferred"] = 1
    else:
        options_df["inferred"] = 0

    options_df.drop(["strike_type", "barrier_type"], axis=1, inplace=True)

    # Adding UID
    options_df["uid"] = options_df["ticker"] + "_" + options_df["spot_date"].astype(str) + "_" + \
        options_df["option_type"].astype(str) + "_" + options_df["time_to_exp"].astype(str)
    options_df["uid"] = options_df["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True)
    options_df["uid"] = options_df["uid"].str.strip()

    options_df = options_df[options_df.atm_volatility_one_year > 0.1]
    options_df = options_df[options_df.atm_volatility_infinity > 0.1]
    options_df = options_df[options_df.atm_volatility_one_year < 1.25]
    options_df = options_df[options_df.atm_volatility_infinity < 1.25]

    table_name = get_bot_uno_backtest_table_name()
    if history:
        upsert_data_to_database(options_df, table_name, "uid", how="ignore", cpu_count=True, Text=True)
    else:
        upsert_data_to_database(options_df, table_name, "uid", how="ignore", cpu_count=True, Text=True)

def FillMissingDay(data, start, end):
    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start, end, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = result[result["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    result["trading_day"] = result["trading_day"].astype(str)
    result["uid"]=result["trading_day"] + data["ticker"]
    result["uid"]=result["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
    result["uid"]=result["uid"].str.strip()
    result["trading_day"] = pd.to_datetime(result["trading_day"])
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["uid", "ticker", "trading_day"])
    return result

def ForwardBackwardFillNull(data, columns_field):
    data = data.sort_values(by="trading_day", ascending=False)
    data = data.infer_objects()
    result = data[["uid"]]
    data_detail = data[["uid", "ticker", "trading_day", "volume", "currency_code", "day_status"]]
    universe = data["ticker"].drop_duplicates()
    universe =universe.tolist()
    for column in columns_field:
        price = data.pivot_table(index="trading_day", columns="ticker", values=column, aggfunc="first", dropna=False)
        price = price.reindex(columns=universe)
        price = price.ffill().bfill()
        price = pd.DataFrame(price.values, index=price.index, columns=price.columns)
        price["trading_day"] = price.index
        price = price.melt(id_vars="trading_day", var_name="ticker", value_name=column)
        price["trading_day"] = price["trading_day"].astype(str)
        price["uid"]=price["trading_day"] + data["ticker"]
        price["uid"]=price["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        price["uid"]=price["uid"].str.strip()
        price["trading_day"] = pd.to_datetime(price["trading_day"])
        price = price.drop(columns=["trading_day", "ticker"])
        result = result.merge(price, on=["uid"], how="left")
    result = result.merge(data_detail, on=["uid"], how="left")
    return result
    
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

def fill_bot_backtest_uno(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, mod=False, history=False, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    start_date, end_date = check_start_end_date(start_date, end_date)
    # This function is used for filling the nulls in executive options database. Checks whether the options are expired
    # or knocked out or not triggered at all.
    tqdm.pandas()
    dates_df_unique = get_bot_backtest_data_date_list(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, uno=True, mod=mod, null_filler=True)
    # Dividing the dates to sections for running manually
    dates_df_unique = np.sort(dates_df_unique)
    divison_length = round(len(dates_df_unique) / total_no_of_runs)
    dates_per_run = [dates_df_unique[x:x + divison_length] for x in range(0, len(dates_df_unique), divison_length)]
    # **************************************************
    date_min = min(dates_per_run[run_number])
    date_max = max(dates_per_run[run_number])
    # Downloading the options that are not triggered from the executive database.
    null_df = get_bot_backtest_data(start_date=date_min, end_date=date_max, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, uno=True, mod=mod, null_filler=True)
    start_date = null_df.spot_date.min()
    #tac_data = tac_data_download_null_filler(start_date, args)
    tac_data = get_master_tac_price(start_date=date_min, end_date=date_max, ticker=ticker, currency_code=currency_code)
    tac_data = FillMissingDay(tac_data, start_date, backdate_by_day(1))
    tac_data = ForwardBackwardFillNull(tac_data, ["open", "high", "low", "close", "total_return_index"])
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
    if mod:
        table_name += "_mod"
    def exec_fill_fun(row, prices_np, dates_np, null_df):
        # Calculate the desired quantities row by row.
        # Everything is converted to numpy array for faster runtime.
        if row.expiry_date_index == -2:
            row.expiry_date_index = prices_np.shape[0] - 1
        prices_temp = prices_np[int(row.spot_date_index):int(row.expiry_date_index+1), int(row.ticker_index)]
        if len(prices_temp) == 0:
            return row
        dates_temp = dates_np[int(row.spot_date_index):int(row.expiry_date_index+1), 0]
        t = np.full((len(prices_temp)), ((row["expiry_date"] - dates_temp).astype("timedelta64[D]")) / np.timedelta64(1, "D"))
        t = t / 365
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

        # Condition
        condition1A = prices_temp > strike
        condition2A = np.abs(last_hedge - stock_balance) <= large_hedge
        conditionA = condition1A & condition2A
        condition1B = prices_temp <= strike
        condition2B = np.abs(last_hedge - stock_balance) <= small_hedge
        conditionB = condition1B & condition2B
        condition = conditionA | conditionB
        stock_balance[condition] = 2
        while(any(stock_balance == 2)):
            stock_balance = forward_fill(stock_balance)
        barrier_indices = np.argmax((prices_temp >= row.barrier))
        stock_balance2 = np.copy(stock_balance)
        stock_balance2 = shift5_numba(stock_balance2, 1)
        stock_balance2 = np.nan_to_num(stock_balance2)

        if barrier_indices != 0:
            # barrier(knockout) is triggered.
            row.event = "KO"
            row["stock_balance"] = stock_balance[barrier_indices]
            row["stock_price"] = prices_temp[barrier_indices]
            row["now_price"] = prices_temp[barrier_indices]
            row["event_date"] = dates_temp[barrier_indices]
            row["event_price"] = prices_temp[barrier_indices]
            row["expiry_price"] = prices_temp[-1]
            row["now_date"] = dates_temp[barrier_indices]
            row["expiry_payoff"] = row["barrier"] - row["strike"]
            row["expiry_return"] = (prices_temp[-1] / prices_temp[0]) - 1
            row["drawdown_return"] = np.amin(prices_temp) / prices_temp[0] - 1
            row["duration"] = (row["event_date"] - row["spot_date"]).days / 365
            row["r"] = r[barrier_indices]
            row["q"] = q[barrier_indices]
            row["v1"] = v1[barrier_indices]
            row["v2"] = v2[barrier_indices]
            row["t"] = t[barrier_indices]
            stock_balance[barrier_indices] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            pnl = pnl[:barrier_indices+1]
            churn = churn[:barrier_indices+1]
            row["pnl"] = np.nansum(pnl)
            delta_churn = np.abs(churn)
            row["delta_churn"] = np.nansum(delta_churn)
            row["bot_return"] = row["pnl"] / prices_temp[0]
            row["num_hedges"] = np.sum(stock_balance2[:barrier_indices+1] != stock_balance[:barrier_indices+1])
            row["current_delta"] = stock_balance2[barrier_indices]
            row["avg_delta"] = np.nansum(stock_balance[:barrier_indices+1]) / stock_balance[:barrier_indices+1].size
        elif dates_temp[-1] == row["expiry_date"]:
            # Expiry is triggered.
            row.event = "expire"
            row["stock_balance"] = stock_balance[-1]
            row["stock_price"] = prices_temp[-1]
            row["now_price"] = prices_temp[-1]
            row["event_price"] = prices_temp[-1]
            row["expiry_price"] = prices_temp[-1]
            row["event_date"] = dates_temp[-1]
            row["now_date"] = dates_temp[-1]
            row["expiry_payoff"] = max(row["now_price"] - row["strike"], 0)
            row["expiry_return"] = (prices_temp[-1] / prices_temp[0]) - 1
            row["drawdown_return"] = np.amin(prices_temp) / prices_temp[0] - 1
            row["duration"] = (row["event_date"] - row["spot_date"]).days / 365
            row["r"] = r[-1]
            row["q"] = q[-1]
            row["v1"] = v1[-1]
            row["v2"] = v2[-1]
            row["t"] = t[-1]
            stock_balance[-1] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            row["pnl"] = np.nansum(pnl)
            delta_churn = np.abs(churn)
            row["delta_churn"] = np.nansum(delta_churn)
            row["bot_return"] = row["pnl"] / prices_temp[0]
            row["num_hedges"] = np.sum(stock_balance2 != stock_balance)
            row["current_delta"] = stock_balance2[-1]
            row["avg_delta"] = np.nansum(stock_balance) / stock_balance.size
        else:
            # No event is triggered.
            row.event = None
            row["stock_balance"] = 0
            row["stock_price"] = prices_temp[-1]
            row["now_price"] = prices_temp[-1]
            row["event_date"] = None
            row["now_date"] = dates_temp[-1]
            row["expiry_payoff"] = None
            row["v1"] = v1[-1]
            row["v2"] = v2[-1]
            row["r"] = r[-1]
            row["q"] = q[-1]
            row["pnl"] = None
            row["delta_churn"] = None
            row["t"] = t[-1]
            row["num_hedges"] = None
            row["current_delta"] = stock_balance2[-1]
            row["avg_delta"] = np.nansum(stock_balance) / stock_balance.size
        return row

    logging.basicConfig(filename="logfilename.log", level=logging.INFO)

    def forward_fill(arr):
        if((arr.size>=2) and (arr[1] == 2)):
            arr[1] = arr[0]
        if((arr.size>=3) and (arr[2] == 2)):
            arr[2] = arr[1]
        prev = np.arange(len(arr))
        prev[arr == 2] = 2
        prev = np.maximum.accumulate(prev)
        return arr[prev]

    def foo(k):
        try:
            # run the null filler for each section of dates
            date_temp = dates_per_run[run_number][k]
            null_df_small = null_df[null_df.spot_date == date_temp]
            print(f"Filling {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")
            null_df_small = null_df_small.progress_apply(lambda x: exec_fill_fun(x, prices_np, dates_np, null_df), axis=1, raw=False)
            null_df_small.drop(["expiry_date_index", "spot_date_index", "ticker_index"], axis=1, inplace=True)
            null_df_small = null_df_small.infer_objects()
            table_name = get_bot_uno_backtest_table_name()
            if mod:
                table_name += "_mod"
            upsert_data_to_database(null_df_small, table_name, "uid", how="update", cpu_count=True, Text=True)
            print(f"Finished {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")

        except Exception as e:
            print(e)
            logging.error(e)
            logging.error(f"Error for {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")
            print(f"Error for {dates_per_run[run_number][k]}, {k} date from {len(dates_per_run[run_number])} dates.")


    if len(null_df) > len(dates_per_run[run_number]):
        for i in range(int(len(dates_per_run[run_number]))):
            foo(i)
    else:
        if (history):
            null_df["num_hedges"] = None
        null_df = null_df.progress_apply(exec_fill_fun, axis=1, raw=True)
        null_df.drop(["expiry_date_index", "spot_date_index", "ticker_index"], axis=1, inplace=True)
        null_df = null_df.infer_objects()
        if len(null_df) > 0 :
            null_df = null_df.drop_duplicates(subset=["uid"], keep="first", inplace=False)
            upsert_data_to_database(null_df, table_name, "uid", how="update", cpu_count=True, Text=True)
    # ********************************************************************************************
    print(f"Filling up the nulls is finished.")

