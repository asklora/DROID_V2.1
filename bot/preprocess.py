import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
from general.date_process import dateNow, datetimeNow, str_to_date
from general.slack import report_to_slack

from general.sql_output import upsert_data_to_database
from general.data_process import tuple_data, uid_maker
from general.sql_query import (
    get_active_universe, 
    get_data_by_table_name,
     get_data_by_table_name_with_condition, 
     get_latest_price)
from general.table_name import (
    get_data_dividend_daily_table_name, 
    get_data_dividend_table_name, 
    get_data_interest_daily_table_name, 
    get_data_interest_table_name)

from global_vars import null_per, r_days, q_days

def remove_holidays(prices_df):
    # This function will shift the holidays to the top.
    prices_df.loc[prices_df.day_status == 'holiday', 'open'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'high'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'low'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'close'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'total_return_index'] = 'H'
    main_open = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='open', aggfunc='first',dropna=False)
    main_high = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='high', aggfunc='first',dropna=False)
    main_low = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='low', aggfunc='first',dropna=False)
    main_close = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='close', aggfunc='first',dropna=False)
    main_tri = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='total_return_index',aggfunc='first',dropna=False)
    main_open = move_holidays_to_top(main_open)
    main_high = move_holidays_to_top(main_high)
    main_low = move_holidays_to_top(main_low)
    main_close = move_holidays_to_top(main_close)
    main_tri = move_holidays_to_top(main_tri)
    main_tri_shifted = main_tri.shift(periods=1)
    main_tri[main_tri == 'H'] = 1
    main_tri_shifted[main_tri_shifted == 'H'] = 1
    main_tri = main_tri.astype(float)
    main_tri_shifted = main_tri_shifted.astype(float)
    tri_df_np = main_tri.values
    tri_df_shifted_np = main_tri_shifted.values
    close_multiple = np.where(np.isnan(tri_df_np) | np.isnan(tri_df_shifted_np), np.nan, tri_df_np / tri_df_shifted_np)
    main_multiples = pd.DataFrame(close_multiple, index=main_tri.index, columns=main_tri.columns)

    return main_open, main_high, main_low, main_close, main_tri, main_multiples


def remove_holidays_forward(prices_df):
    # This function will remove holidays for forward calculation of volatility.
    prices_df.loc[prices_df.day_status == 'holiday', 'open'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'high'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'low'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'close'] = 'H'
    prices_df.loc[prices_df.day_status == 'holiday', 'total_return_index'] = 'H'

    main_tri = prices_df.pivot_table(index=prices_df.trading_day, columns='ticker', values='total_return_index',
                                     aggfunc='first',
                                     dropna=False)
    main_tri = move_holidays_to_end(main_tri)
    main_tri_shifted = main_tri.shift(periods=1)
    main_tri[main_tri == 'H'] = 1
    main_tri_shifted[main_tri_shifted == 'H'] = 1
    main_tri = main_tri.astype(float)
    main_tri_shifted = main_tri_shifted.astype(float)
    tri_df_np = main_tri.values
    tri_df_shifted_np = main_tri_shifted.values
    close_multiple = np.where(np.isnan(tri_df_np) | np.isnan(tri_df_shifted_np), np.nan, tri_df_np / tri_df_shifted_np)
    main_multiples = pd.DataFrame(close_multiple, index=main_tri.index, columns=main_tri.columns)

    return main_multiples[1:]

def move_holidays_to_top(df):
    # This function will shift the holidays to the top.
    for col in df:
        temp = df[col].copy()
        m = temp == 'H'
        temp1 = temp[m].append(temp[~m]).reset_index(drop=True)
        temp1.index = temp.index
        df[col] = temp1
    return df

def move_holidays_to_end(df):
    # This function will shift the holidays to the end.
    for col in df:
        temp = df[col].copy()
        m = temp == 'H'
        temp1 = temp[~m].append(temp[m]).reset_index(drop=True)
        temp1.index = temp.index
        df[col] = temp1
    return df

def move_holidays_to_end2(df):
    # This function will shift the holidays to the end.
    for col in df:
        temp = df[col].copy()
        # m = temp == 'nan'
        m = temp.isnull().values
        temp1 = temp[~m].append(temp[m]).reset_index(drop=True)
        temp1.index = temp.index
        df[col] = temp1
    return df

def series_to_pandas(df, valid_tickers_list):
    # Ths function makes sure that only the available tickers for the current trading date is used.
    aa = pd.DataFrame(df[df.index.isin(valid_tickers_list)])
    aa.reset_index(inplace=True)
    return aa

def take_the_most_recent_one(df,col, valid_tickers_list):
    temp_df = df.pivot_table(index='q_report_date', columns='ticker', values=col,
                                           aggfunc='first', dropna=False)
    temp_df = temp_df.ffill()
    temp_df = temp_df.iloc[-1, :]
    temp_df = series_to_pandas(temp_df, valid_tickers_list)
    temp_df.rename(columns={temp_df.columns[1]: col}, inplace=True)
    return temp_df


def lookback_creator(df, days, days_back):
    # This function will create lookback batches and removing the batches with high number of nulls.
    lookback_df = df[len(df) - days_back:len(df) - days]
    mask = lookback_df.isna().sum() < len(lookback_df) * null_per / 100
    lookback_df.loc[:, mask] = lookback_df.loc[:, mask].fillna(value=1)
    return lookback_df.astype(float)


def forward_creator(df, days, days_forward):
    # This function will create forward batches and removing the batches with high number of nulls.
    forward_df = df[days:days_forward]
    mask = forward_df.isna().sum() < len(forward_df) * null_per / 100
    forward_df.loc[:, mask] = forward_df.loc[:, mask].fillna(value=1)
    # forward_df = forward_df.fillna(value=1)
    return forward_df


def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))

def rounding_fun(df):
    # This is the rounding function as instructed by stephen.
    # https://loratechai.atlassian.net/wiki/spaces/ARYA/pages/48824321/Executive+DROID
    # ***************************************************************************************************
    df.loc[df['atm_volatility_spot'] < 0, 'atm_volatility_spot'] = 0
    df.loc[df['atm_volatility_spot'] > 0.55, 'atm_volatility_spot'] = 0.65

    cond = (df['atm_volatility_spot'] > 0) & (df['atm_volatility_spot'] < 0.25)
    df.loc[cond, 'atm_volatility_spot'] = round(df.loc[cond, 'atm_volatility_spot'] / 0.025)
    df.loc[cond, 'atm_volatility_spot'] = df.loc[cond, 'atm_volatility_spot'] * 0.025

    cond = (df['atm_volatility_spot'] > 0.25) & (df['atm_volatility_spot'] < 0.55)
    df.loc[cond, 'atm_volatility_spot'] = round(df.loc[cond, 'atm_volatility_spot'] / 0.05)
    df.loc[cond, 'atm_volatility_spot'] = df.loc[cond, 'atm_volatility_spot'] * 0.05
    # ***************************************************************************************************
    df.loc[df['atm_volatility_one_year'] < 0, 'atm_volatility_one_year'] = 0
    df.loc[df['atm_volatility_one_year'] > 0.5, 'atm_volatility_one_year'] = 0.55

    cond = (df['atm_volatility_one_year'] > 0) & (df['atm_volatility_one_year'] < 0.25)
    df.loc[cond, 'atm_volatility_one_year'] = round(df.loc[cond, 'atm_volatility_one_year'] / 0.025)
    df.loc[cond, 'atm_volatility_one_year'] = df.loc[cond, 'atm_volatility_one_year'] * 0.025

    cond = (df['atm_volatility_one_year'] > 0.25) & (df['atm_volatility_one_year'] < 0.5)
    df.loc[cond, 'atm_volatility_one_year'] = round(df.loc[cond, 'atm_volatility_one_year'] / 0.05)
    df.loc[cond, 'atm_volatility_one_year'] = df.loc[cond, 'atm_volatility_one_year'] * 0.05
    # ***************************************************************************************************
    df.loc[df['atm_volatility_infinity'] < 0, 'atm_volatility_infinity'] = 0
    df.loc[df['atm_volatility_infinity'] > 0.45, 'atm_volatility_infinity'] = 0.5

    cond = (df['atm_volatility_infinity'] > 0) & (df['atm_volatility_infinity'] < 0.15)
    df.loc[cond, 'atm_volatility_infinity'] = round(df.loc[cond, 'atm_volatility_infinity'] / 0.025)
    df.loc[cond, 'atm_volatility_infinity'] = df.loc[cond, 'atm_volatility_infinity'] * 0.025

    cond = (df['atm_volatility_infinity'] > 0.15) & (df['atm_volatility_infinity'] < 0.45)
    df.loc[cond, 'atm_volatility_infinity'] = round(df.loc[cond, 'atm_volatility_infinity'] / 0.05)
    df.loc[cond, 'atm_volatility_infinity'] = df.loc[cond, 'atm_volatility_infinity'] * 0.05
    # ***************************************************************************************************
    df.loc[df['slope'] < 0, 'slope'] = 0
    df.loc[df['slope'] > 5, 'slope'] = 5
    df['slope'] = round(df['slope'], 0)
    # ***************************************************************************************************
    df['slope_inf'] = df['slope']
    # ***************************************************************************************************
    cond = (df['deriv'] >= 0.05) & (df['deriv'] < 0.09)
    df.loc[df['deriv'] < 0.05, 'deriv'] = 0.025
    df.loc[cond, 'deriv'] = 0.075
    df.loc[df['deriv'] >= 0.09, 'deriv'] = 0.11
    # ***************************************************************************************************
    df['deriv_inf'] = df['deriv']
    df.loc[df['deriv_inf'] == 0.025, 'deriv_inf'] = 0
    df.loc[df['deriv_inf'] > 0.025, 'deriv_inf'] = 0.06
    # ***************************************************************************************************
    # Needed for classification
    # NOTE!!!: In case of "changing", the inference in lgbm model should change too.
    df.loc[df['deriv'] == 0.11, 'deriv'] = 0
    df.loc[df['deriv'] == 0.075, 'deriv'] = 1
    df.loc[df['deriv'] == 0.025, 'deriv'] = 2
    return df

def cal_interest_rate(interest_rate_data, days_to_expiry):
    unique_horizons = pd.DataFrame(days_to_expiry)
    unique_horizons['id'] = 2
    unique_currencies = pd.DataFrame(interest_rate_data['currency_code'].unique())
    unique_currencies['id'] = 2
    rates = pd.merge(unique_horizons, unique_currencies, on='id', how='outer')
    rates = rates.rename(columns={'0_y': 'currency_code', '0_x': 'days_to_expiry'})
    interest_rate_data = interest_rate_data.rename(columns={'days_to_maturity': 'days_to_expiry'})
    rates = pd.merge(rates, interest_rate_data, on=['days_to_expiry', 'currency_code'], how='outer')
    def funs(df):
        df = df.sort_values(by='days_to_expiry')
        df = df.reset_index()
        nan_index = df['rate'].index[df['rate'].isnull()].to_series().reset_index(drop=True)
        not_nan_index = df['rate'].index[~df['rate'].isnull()].to_series().reset_index(drop=True)
        for a in nan_index:
            temp = not_nan_index.copy()
            temp[len(temp)] = a
            temp = temp.sort_values()
            temp.reset_index(inplace=True, drop=True)
            ind = temp[temp == a].index
            ind1 = temp.iloc[ind - 1]
            ind2 = temp.iloc[ind + 1]
            rate_1 = df.loc[ind1, 'rate'].iloc[0]
            rate_2 = df.loc[ind2, 'rate'].iloc[0]
            dtm_1 = df.loc[ind1, 'days_to_expiry'].iloc[0]
            dtm_2 = df.loc[ind2, 'days_to_expiry'].iloc[0]
            df.loc[a, 'rate'] = rate_1 * (dtm_2 - df.loc[a, 'days_to_expiry'])/(dtm_2 - dtm_1) + rate_2* (df.loc[a, 'days_to_expiry'] - dtm_1)/(dtm_2 - dtm_1)
        df = df.set_index('index')
        return df
    rates = rates.groupby('currency_code').apply(lambda x: funs(x))
    rates = rates.reset_index(drop=True)
    unique_horizons = unique_horizons.rename(columns={0: 'days_to_expiry'})

    unique_horizons = pd.merge(unique_horizons, rates[['days_to_expiry', 'rate']], on='days_to_expiry', how='inner')
    return unique_horizons['rate'].values

def cal_q(row, dividends_data, dates_temp, prices_temp):
    dates_temp = pd.DataFrame(dates_temp)
    dates_temp2 = dates_temp.copy()
    dates_temp = dates_temp2
    dates_temp['id'] = 2
    dividends_data['id'] = 2
    dates_temp = pd.merge(dates_temp, dividends_data, on='id', how='outer')
    dates_temp['expiry_date'] = row.expiry_date
    dates_temp = dates_temp.rename(columns={0: 'spot_date'})
    dates_temp = dates_temp[(dates_temp.spot_date <= dates_temp.ex_dividend_date) &
                              (dates_temp.ex_dividend_date <= dates_temp.expiry_date)]
    dates_temp = dates_temp.groupby(['spot_date'])['amount'].sum().reset_index()
    dates_temp2 = dates_temp2.rename(columns={0: 'spot_date'})
    dates_temp2 = dates_temp2.merge(dates_temp, on='spot_date', how='left').fillna(0)
    dates_temp2['amount'] = dates_temp2['amount'] / prices_temp
    return dates_temp2['amount'].values

def dividend_daily_update():
    print("{} : === Dividens Daily Update ===".format(datetimeNow()))
    dividend_data = get_data_by_table_name(get_data_dividend_table_name())
    universe = get_active_universe()
    #intraday_price = get_data_by_table_name(get_latest_price_table_name())
    intraday_price = get_latest_price()
    dividend_data = dividend_data[dividend_data.ticker.isin(universe.ticker)]
    dividend_data = dividend_data.merge(universe[["ticker", "currency_code"]], on="ticker", how="left")
    dividend_data["ex_dividend_date"] = pd.to_datetime(dividend_data["ex_dividend_date"])
    days_q = pd.DataFrame(range(1, q_days))
    dates_temp = days_q
    dates_temp["spot_date"] = str_to_date(dateNow())
    def calculate_expiry(row):
        return (row["spot_date"] + BDay(row[0]))
    dates_temp["expiry_date"] = dates_temp.apply(calculate_expiry, axis=1)
    dates_temp["spot_date"] = pd.to_datetime(dates_temp["spot_date"])
    dates_temp["expiry_date"] = pd.to_datetime(dates_temp["expiry_date"])
    dates_temp3 = dates_temp.copy()
    result = pd.DataFrame()
    for ticker in dividend_data.ticker.unique():
        temp_q = pd.DataFrame()
        dates_temp = pd.DataFrame(dates_temp3)
        dates_temp2 = dates_temp.copy()
        dates_temp = dates_temp2
        dates_temp["id"] = 2
        dividend_data["id"] = 2
        dates_temp = pd.merge(dates_temp, dividend_data[dividend_data.ticker == ticker], on="id", how="outer")
        dates_temp = dates_temp[(dates_temp.spot_date <= dates_temp.ex_dividend_date) & (dates_temp.ex_dividend_date <= dates_temp.expiry_date)]
        dates_temp = dates_temp.groupby(["expiry_date"])["amount"].sum().reset_index()
        dates_temp2 = dates_temp2.merge(dates_temp, on="expiry_date", how="left").fillna(0)
        temp_q["q"] = dates_temp2["amount"] / intraday_price.loc[intraday_price.ticker == ticker, "close"].values
        temp_q["ticker"] = ticker
        temp_q["spot_date"] = dates_temp2["spot_date"]
        temp_q["expiry_date"] = dates_temp2["expiry_date"]
        temp_q["t"] = days_q[0]
        result = result.append(temp_q)
    result = result.merge(universe[["ticker", "currency_code"]], on="ticker", how="left")
    result = uid_maker(result, uid="uid", ticker="ticker", trading_day="t", date=False)
    print(result)
    #insert_data_to_database(result, get_data_dividend_daily_table_name, how="replace")
    upsert_data_to_database(result, get_data_dividend_daily_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Dividens Daily Updated ===".format(datetimeNow()))
    
def interest_daily_update(currency_code=None):
    def cal_interest_rate(interest_rate_data, days_to_expiry):
        unique_horizons = pd.DataFrame(days_to_expiry)
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
                ind1 = temp.iloc[ind - 1]
                ind2 = temp.iloc[ind + 1]
                rate_1 = df.loc[ind1, "rate"].iloc[0]
                rate_2 = df.loc[ind2, "rate"].iloc[0]
                dtm_1 = df.loc[ind1, "days_to_expiry"].iloc[0]
                dtm_2 = df.loc[ind2, "days_to_expiry"].iloc[0]
                df.loc[a, "rate"] = rate_1 * (dtm_2 - df.loc[a, "days_to_expiry"])/(dtm_2 - dtm_1) + rate_2* (df.loc[a, "days_to_expiry"] - dtm_1)/(dtm_2 - dtm_1)
            df = df.set_index("index")
            return df
        rates = rates.groupby("currency_code").apply(lambda x: funs(x))
        rates = rates.reset_index(drop=True)
        unique_horizons = unique_horizons.rename(columns={0: "days_to_expiry"})
        unique_horizons = pd.merge(unique_horizons, rates[["days_to_expiry", "rate"]], on="days_to_expiry", how="inner")
        return unique_horizons["rate"].values
    
    print("{} : === Interest Daily Update ===".format(datetimeNow()))
    if(type(currency_code) == type(None)):
        interest_rate_data = get_data_by_table_name(get_data_interest_table_name())
    else:
        interest_rate_data = get_data_by_table_name_with_condition(get_data_interest_table_name(), f" currency_code in {tuple_data(currency_code)}")

    days_r = pd.DataFrame(range(1, r_days))
    result = pd.DataFrame()

    for currency in interest_rate_data.currency_code.unique():
        temp_r = pd.DataFrame()
        temp_r["r"] = cal_interest_rate(interest_rate_data[interest_rate_data.currency_code == currency], days_r)
        temp_r["t"] = days_r[0]
        temp_r["currency_code"] = currency
        result = result.append(temp_r)
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="t", date=False)
    print(result)
    # insert_data_to_database(result, get_data_interest_daily_table_name(), how="replace")
    upsert_data_to_database(result, get_data_interest_daily_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Interest Daily Updated ===".format(datetimeNow()))