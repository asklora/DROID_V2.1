import datetime
import gc
import sys
import logging
import future.moves.sys
import global_vars
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

from executive.black_scholes import (
    find_vol, 
    get_interest_rate, 
    Up_Out_Call, 
    deltaUnOC)
from executive.data_download import (
    get_new_tickers_list_detail_from_aws,
    tac_data_download, 
    get_vol_surface_data, 
    get_indices_data, 
    get_interest_rate_data,
    get_dividends_data, 
    download_production_executive_null, 
    download_holidays, 
    get_vol_surface_data_inferred, 
    tac_data_download_null_filler, 
    download_production_executive_null_dates_list)
from executive.data_output import (
    write_to_aws_executive_production, 
    write_to_aws_executive_production_null,
    upsert_data_to_database)
import pandas as pd
import numpy as np
from datetime import datetime as dt
from tqdm import tqdm
from sqlalchemy.types import Date, BIGINT, TEXT
from executive.preprocess import cal_interest_rate, cal_q
from general.slack import report_to_slack



def option_fn(args):
   # print("Start the Hunt")
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    # https://loratechai.atlassian.net/wiki/spaces/ARYA/pages/82379155/Executive+UnO+KO+Barrier+Options+all+python+function+in+black+scholes.py

    holidays_df = download_holidays(args)
    args.holidays = holidays_df['non_working_day']
    tac_data2 = tac_data_download(args)
    vol_surface_data = get_vol_surface_data(args)
    indices_data = get_indices_data(args)
    interest_rate_data = get_interest_rate_data(args)
    interest_rate_data = interest_rate_data.rename(columns={'currency': 'currency_code'})
    dividends_data = get_dividends_data(args)

    tac_data = tac_data2[tac_data2.ticker.isin(vol_surface_data.ticker.unique())]

    options_df = pd.DataFrame()

    options_df['ticker'] = tac_data.ticker
    options_df['index'] = tac_data['index']
    options_df['now_date'] = tac_data.trading_day
    options_df['trading_day'] = tac_data.trading_day
    options_df['spot_date'] = tac_data.trading_day
    options_df['now_price'] = tac_data.tri_adjusted_price
    options_df['spot_price'] = tac_data.tri_adjusted_price

    options_df = options_df.merge(vol_surface_data,on=['ticker', 'trading_day'])
    options_df.drop(['trading_day'], axis=1, inplace=True)

    if not args.add_inferred:
        options_df.drop(['uid', 'stock_price', 'parameter_set_date', 'alpha'], axis=1, inplace=True)

    options_df = options_df.sort_values(by='spot_date', ascending=False)

    # if args.debug_mode:
        # if len(options_df) >1000:
        #     options_df = options_df.sample(1000)

    # Adding maturities and expiry date
    options_df_temp = pd.DataFrame(columns=options_df.columns)
    print("Calculating Month Exp")
    for month_exp in args.month_horizon:
        options_df2 = options_df.copy()
        options_df2['month_to_exp'] = month_exp
        options_df_temp = options_df_temp.append(options_df2)
        options_df_temp.reset_index(drop=True, inplace=True)
        del options_df2

    del options_df
    options_df = options_df_temp.copy()
    del options_df_temp
    print(options_df)

    print("Calculating Expiry Date")
    for month_exp in args.month_horizon:
        if month_exp < 1:
            options_df.loc[options_df['month_to_exp'] == month_exp, 'expiry_date'] = options_df['spot_date'] + relativedelta(weeks=(month_exp * 4))
        else:
            options_df.loc[options_df['month_to_exp'] == month_exp, 'expiry_date'] = options_df['spot_date'] + relativedelta(months=month_exp)

    # making sure that expiry date is not holiday or weekend
    options_df['expiry_date'] = pd.to_datetime(options_df['expiry_date'])
    cond = options_df['expiry_date'].apply(lambda x: x.weekday()) > 4
    options_df.loc[cond, 'expiry_date'] = options_df.loc[cond, 'expiry_date'] - BDay(1)

    for i in range(20):
        cond = options_df['expiry_date'].isin(args.holidays)
        options_df.loc[cond, 'expiry_date'] = options_df.loc[cond, 'expiry_date'] - BDay(1)

    options_df['expiry_date'] = (options_df['expiry_date']).apply(lambda x: x.date())

    options_df['days_to_expiry'] = (options_df['expiry_date'] - options_df['spot_date']).apply(lambda x:x.days)
    # *************************************************************************************************
    # get_interest_rate vectorized function (r)

    unique_horizons = pd.DataFrame(options_df['days_to_expiry'].unique())
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
            if(ind - 1 < 0):
                ind = 1
            ind1 = temp[ind - 1]

            if (ind + 1 > len(df) - 1):
                ind = len(df) - 2
            ind2 = temp[ind + 1]
            if(type(df.loc[ind1, 'rate']) == np.float64):
                rate_1 = df.loc[ind1, 'rate']
                rate_2 = df.loc[ind2, 'rate']
                dtm_1 = df.loc[ind1, 'days_to_expiry']
                dtm_2 = df.loc[ind2, 'days_to_expiry']
            else:
                rate_1 = df.loc[ind1, 'rate'].iloc[0]
                rate_2 = df.loc[ind2, 'rate'].iloc[0]
                dtm_1 = df.loc[ind1, 'days_to_expiry'].iloc[0]
                dtm_2 = df.loc[ind2, 'days_to_expiry'].iloc[0]
            df.loc[a, 'rate'] = rate_1 * (dtm_2 - df.loc[a, 'days_to_expiry'])/(dtm_2 - dtm_1) + rate_2\
                                * (df.loc[a, 'days_to_expiry'] - dtm_1)/(dtm_2 - dtm_1)
        df = df.set_index('index')
        return df

    rates = rates.groupby('currency_code').apply(lambda x: funs(x))
    rates = rates.reset_index(drop=True)

    # *************************************************************************************************
    # *************************************************************************************************
    indices_data = rates.merge(indices_data, on='currency_code')
    options_df = options_df.merge(indices_data[['days_to_expiry', 'index', 'rate', 'currency_code']], on=['days_to_expiry','index'])
    options_df = options_df.rename(columns={'rate': 'r'})
    # *************************************************************************************************
    #  q

    options_df2 = options_df.merge(dividends_data[['ticker', 'ex_dividend_date', 'amount']], on='ticker')
    options_df2 = options_df2[(options_df2.spot_date <= options_df2.ex_dividend_date) &
                              (options_df2.ex_dividend_date <= options_df2.expiry_date)]
    options_df2 = options_df2.groupby(['ticker', 'spot_date'])['amount'].sum().reset_index()
    options_df = options_df.merge(options_df2, on=['ticker', 'spot_date'], how='outer')
    options_df['amount'] = options_df['amount'].fillna(0)
    options_df['amount'] = options_df['amount'] / options_df['spot_price']
    options_df = options_df.rename(columns={'amount': 'q'})

    # *************************************************************************************************
    # *************************************************************************************************
    options_df['t'] = options_df['days_to_expiry'] / 365
    options_df.to_csv('option_df.csv')
    # *************************************************************************************************
    # Adding OPTION configurations

    v0 = find_vol(1, options_df['t'], options_df['atm_volatility_spot'], options_df['atm_volatility_one_year'],
                  options_df['atm_volatility_infinity'], 12, options_df['slope'], options_df['slope_inf'],
                  options_df['deriv'], options_df['deriv_inf'], options_df['r'], options_df['q'])

    v0[v0 <= 0.2] = 0.2
    v0[v0 >= 0.80] = 0.80
    options_df['vol_t'] = v0 * np.sqrt(options_df['month_to_exp'] / 12)

    options_df6 = options_df.copy()

    options_df['option_type'] = 'ITM'
    options_df['strike_type'] = 'ITM'
    options_df['barrier_type'] = 'ITM'

    options_df6['option_type'] = 'OTM'
    options_df6['strike_type'] = 'OTM'
    options_df6['barrier_type'] = 'OTM'

    # options_df7['option_type'] = 'ITM_3_4'
    # options_df7['strike_type'] = 'III'
    # options_df7['barrier_type'] = 'IV'

    options_df = options_df.append(options_df6)
    options_df.reset_index(inplace=True, drop=True)

    # del options_df2, options_df3, options_df4, options_df5, options_df6, options_df7
    del options_df2, options_df6

    options_df.loc[options_df['strike_type'] == 'ITM', 'strike'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 0.5)
    # options_df.loc[options_df['strike_type'] == 'II', 'strike'] = options_df['spot_price']
    options_df.loc[options_df['strike_type'] == 'OTM', 'strike'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 0.5)

    options_df.loc[options_df['barrier_type'] == 'ITM', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 1.5)
    # options_df.loc[options_df['barrier_type'] == 'II', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 1.5)
    options_df.loc[options_df['barrier_type'] == 'OTM', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 2)
    # options_df.loc[options_df['barrier_type'] == 'IV', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 2.5)

    options_df['v1'] = find_vol(options_df['strike'] / options_df['now_price'], options_df['t'],
                                options_df['atm_volatility_spot'], options_df['atm_volatility_one_year'],
                                options_df['atm_volatility_infinity'], 12, options_df['slope'], options_df['slope_inf'],
                                options_df['deriv'], options_df['deriv_inf'], options_df['r'], options_df['q'])

    options_df['v2'] = find_vol(options_df['barrier'] / options_df['now_price'], options_df['t'],
                                options_df['atm_volatility_spot'], options_df['atm_volatility_one_year'],
                                options_df['atm_volatility_infinity'], 12, options_df['slope'], options_df['slope_inf'],
                                options_df['deriv'], options_df['deriv_inf'], options_df['r'], options_df['q'])

    options_df['target_max_loss'] = Up_Out_Call(options_df['now_price'], options_df['strike'], options_df['barrier'],
                                                (options_df['barrier'] - options_df['strike']), options_df['t'],
                                                options_df['r'], options_df['q'], options_df['v1'], options_df['v2'])

    options_df['target_profit'] = options_df['barrier'] - options_df['strike']

    options_df['stock_balance'] = None
    options_df['stock_price'] = None
    options_df['event_date'] = None
    options_df['event_price'] = None
    options_df['event'] = None
    options_df['pnl'] = None
    options_df['delta_churn'] = None
    options_df['expiry_payoff'] = None
    options_df['expiry_return'] = None
    options_df['expiry_price'] = None
    options_df['drawdown_return'] = None
    options_df['duration'] = None
    options_df['return'] = None

    options_df['expiry_return'] = options_df['expiry_return'].astype(float)
    options_df['expiry_payoff'] = options_df['expiry_payoff'].astype(float)
    options_df['drawdown_return'] = options_df['drawdown_return'].astype(float)
    options_df['pnl'] = options_df['pnl'].astype(float)
    options_df['duration'] = options_df['duration'].astype(float)
    options_df['stock_balance'] = options_df['stock_balance'].astype(float)
    options_df['stock_price'] = options_df['stock_price'].astype(float)
    options_df['event_price'] = options_df['event_price'].astype(float)
    options_df['expiry_price'] = options_df['expiry_price'].astype(float)
    options_df['return'] = options_df['return'].astype(float)

    if args.modified:
        options_df_temp = pd.DataFrame(columns=options_df.columns)
        for mod_temp in args.modify_arg:
            options_df2 = options_df.copy()

            options_df2['modified'] = 1
            options_df2['option_type'] = options_df2['option_type'] + mod_temp
            options_df2['modify_arg'] = mod_temp
            options_df_temp = options_df_temp.append(options_df2)

        options_df = options_df_temp
        del options_df_temp, options_df2
    else:
        options_df['modified'] = 0
        options_df['modify_arg'] = ''


    if args.add_inferred:
        options_df['inferred'] = 1
    else:
        options_df['inferred'] = 0

    options_df.drop(['strike_type', 'barrier_type'], axis=1, inplace=True)

    # Adding UID
    options_df['uid'] = options_df['ticker'] + '_' + options_df['month_to_exp'].astype(str) + '_' + \
                       options_df['spot_date'].astype(str) + '_' + options_df['option_type'].astype(str)\
                        + '_' + options_df['inferred'].astype(str)
    options_df['uid'] = options_df['uid'].str.replace("-", "").str.replace(".", "")
    options_df['uid'] = options_df['uid'].str.strip()

    options_df = options_df[options_df.atm_volatility_one_year > 0.1]
    options_df = options_df[options_df.atm_volatility_infinity > 0.1]
    options_df = options_df[options_df.atm_volatility_one_year < 1.25]
    options_df = options_df[options_df.atm_volatility_infinity < 1.25]
    # **************************************
    # TEMPORARY
    # Just for OTMs
    # options_df = options_df[options_df['option_type'] == 'OTM']
    # **************************************
    print("options_df")
    print(options_df)
    # Filtering the results for faster writing to AWS.
    if args.option_maker_history:
        options_df2 = options_df[options_df.spot_date >= args.start_date]
    elif (args.new_ticker):
        print("NEW TICKER")
        temp_db = args.latest_dates_db
        for tick in temp_db.ticker.unique():
            if (len(temp_db.loc[temp_db['ticker'] == tick]) > 0):
                min_date = temp_db.loc[temp_db['ticker'] == tick, ['uno_min_date']].values[0][0]
                max_date = temp_db.loc[temp_db['ticker'] == tick, ['uno_max_date']].values[0][0]
                options_df = options_df.drop(options_df[(options_df.spot_date >= min_date) & (options_df.spot_date <= max_date) & (options_df.ticker == tick)].index)
        options_df2 = options_df
    else:
        # options_df3= options_df.copy()
        options_df = options_df.reset_index(drop=True)
        temp_db = args.latest_dates_db
        temp_db['ticker'] = temp_db.index
        for tick in options_df.ticker.unique():
            if(len(temp_db.loc[temp_db['ticker'] == tick]) > 0):
                max_date = temp_db.loc[temp_db['ticker', ''] == tick, ['spot_date', 'max']].values[0][0]
                options_df = options_df.drop(options_df[(options_df.spot_date <= max_date) & (options_df.ticker == tick)].index)

        options_df2 = options_df

    options_df2 = options_df2.infer_objects()

    print(f'Executive calculation is finished.')
    try:
        options_df2 = options_df2.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        print(options_df2)
        # Writing the calculated events to AWS.
        if len(options_df2) > 0 :
            upsert_data_to_database(args, "uid", TEXT, options_df2, method="ignore")
        #write_to_aws_executive_production(options_df2, args)
    except Exception as e:
        print(e)
        options_df2.to_csv('option_df_file.csv')

    if (args.new_ticker):
        if len(options_df2) > 0 :
            return True
        else:
            return False

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

def fill_nulls(args):
    # This function is used for filling the nulls in executive options database. Checks whether the options are expired
    # or knocked out or not triggered at all.
    tqdm.pandas()

    dates_df_unique = download_production_executive_null_dates_list(args)

    # Dividing the dates to sections for running manually
    dates_df_unique = np.sort(dates_df_unique)

    divison_length = round(len(dates_df_unique) / args.total_no_of_runs)

    dates_per_run = [dates_df_unique[x:x + divison_length] for x in
                     range(0, len(dates_df_unique), divison_length)]
    # **************************************************

    args.date_min = min(dates_per_run[args.run_number])
    args.date_max = max(dates_per_run[args.run_number])

    # Downloading the options that are not triggered from the executive database.
    null_df = download_production_executive_null(args)

    start_date = null_df.spot_date.min()

    tac_data = tac_data_download_null_filler(start_date, args)
    tac_data = tac_data.sort_values(by=["index", "ticker", "trading_day"], ascending=True)
    interest_rate_data = get_interest_rate_data(args)
    interest_rate_data = interest_rate_data.rename(columns={'currency': 'currency_code'})
    indices_data = get_indices_data(args)
    dividends_data = get_dividends_data(args)

    # ********************************************************************************************
    # ********************************************************************************************
    prices_df = tac_data.pivot_table(index=tac_data.trading_day, columns='ticker', values='tri_adjusted_price',
                                     aggfunc='first', dropna=False)
    prices_df = prices_df.ffill()
    prices_df = prices_df.bfill()

    dates_df = prices_df.index.to_series()
    dates_df = dates_df.reset_index(drop=True)
    dates_df = pd.DataFrame(dates_df)
    dates_df['spot_date_index'] = dates_df.index
    dates_df['expiry_date_index'] = dates_df.index
    dates_df = dates_df.rename(columns={'trading_day': 'spot_date'})

    null_df = null_df.merge(dates_df[['spot_date', 'spot_date_index']], on=['spot_date'], how='left')
    dates_df = dates_df.rename(columns={'spot_date': 'expiry_date'})
    null_df = null_df.merge(dates_df[['expiry_date', 'expiry_date_index']], on=['expiry_date'], how='left')
    null_df['expiry_date_index'].fillna(-2, inplace=True)

    prices_ind_df = pd.DataFrame(prices_df.columns)
    prices_ind_df['ticker_index'] = prices_ind_df.index
    null_df = null_df.merge(prices_ind_df[['ticker', 'ticker_index']], on=['ticker'])
    null_df = null_df.sort_values(by='expiry_date', ascending=False)

    # # Dividing the dates to sections for running manually
    # dates_df_unique = null_df.spot_date.unique()
    # dates_df_unique = np.sort(dates_df_unique)
    #
    # divison_length = round(len(dates_df_unique) / args.total_no_of_runs)
    #
    # dates_per_run = [dates_df_unique[x:x + divison_length] for x in
    #                  range(0, len(dates_df_unique), divison_length)]
    # # **************************************************
    #
    # date_min = min(dates_per_run[args.run_number])
    # date_max = max(dates_per_run[args.run_number]) + relativedelta(months=4)
    #
    # null_df = null_df[(null_df.spot_date >= date_min) & (null_df.spot_date <= date_max)]

    prices_np = prices_df.values
    dates_np = dates_df.values

    del prices_df, tac_data
    gc.collect()

    def exec_fill_fun(row, prices_np, dates_np, null_df):
        # Calculate the desired quantities row by row.
        # Everything is converted to numpy array for faster runtime.

        if row.expiry_date_index == -2:
            row.expiry_date_index = prices_np.shape[0] - 1
        prices_temp = prices_np[int(row.spot_date_index):int(row.expiry_date_index+1), int(row.ticker_index)]
        if len(prices_temp) == 0:
            return row

        # prices_temp2 = np.copy(prices_temp)
        # prices_temp2 = shift5_numba(prices_temp2, 1)
        # prices_temp2 = np.nan_to_num(prices_temp2)

        dates_temp = dates_np[int(row.spot_date_index):int(row.expiry_date_index+1), 0]

        t = np.full((len(prices_temp)), ((row['expiry_date'] - dates_temp).astype('timedelta64[D]')) / np.timedelta64(1, 'D') / 365)

        strike = np.full((len(prices_temp)), row['strike'])
        barrier = np.full((len(prices_temp)), row['barrier'])

        cond = (null_df.ticker == row.ticker) & (null_df.spot_date >= row.spot_date) &\
               (null_df.spot_date <= row.expiry_date) & (null_df.month_to_exp == row.month_to_exp) &\
               (null_df.option_type == row.option_type)

        dates_df2 = pd.DataFrame(dates_temp, columns=['spot_date'])
        null_df_temp = null_df[cond]
        null_df_temp = null_df_temp.drop_duplicates(subset='spot_date', keep="last")
        null_df_temp = dates_df2.merge(null_df_temp, on='spot_date', how='left')
        null_df_temp = null_df_temp.bfill().ffill()

        atmv_0 = null_df_temp['atm_volatility_spot'].values
        atmv_1Y = null_df_temp['atm_volatility_one_year'].values
        atmv_inf = null_df_temp['atm_volatility_infinity'].values
        skew_1m = null_df_temp['slope'].values
        skew_inf = null_df_temp['slope_inf'].values
        curv_1m = null_df_temp['deriv'].values
        curv_inf = null_df_temp['deriv_inf'].values

        days_to_expiry = np.full((len(prices_temp)),
                                 ((row['expiry_date'] - dates_temp).astype('timedelta64[D]')) / np.timedelta64(1, 'D'))

        r = cal_interest_rate(interest_rate_data[interest_rate_data.currency_code == row.currency_code],
                              days_to_expiry)

        q = cal_q(row, dividends_data[dividends_data.ticker == row.ticker], dates_temp, prices_temp)


        v1 = find_vol(np.divide(strike, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,
                      12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)

        v2 = find_vol(np.divide(barrier, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,
                      12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)

        stock_balance = deltaUnOC(prices_temp, strike, barrier, (barrier - strike), t, r, q, v1, v2)
        stock_balance = np.nan_to_num(stock_balance)

        last_hedge = np.copy(stock_balance)
        last_hedge = shift5_numba(last_hedge, 1)
        last_hedge = np.nan_to_num(last_hedge)

        #hedge = (v1 + v2) / 15
        hedge = 0.05

        # stock_balance3 = np.copy(stock_balance)
        # for i in range(1, len(stock_balance3)):
        #     if (prices_temp[i] > strike[i]):
        #         if (np.abs(stock_balance3[i-1] - stock_balance3[i]) <= hedge[i]):
        #             stock_balance3[i] = stock_balance3[i - 1]

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

        # condition1 = prices_temp > strike
        # condition2 = np.abs(last_hedge - stock_balance) <= hedge
        # condition = condition1 & condition2
        # stock_balance[condition] = 2
        # stock_balance = fill_zeros_with_last(stock_balance)

        barrier_indices = np.argmax((prices_temp >= row.barrier))

        stock_balance2 = np.copy(stock_balance)
        stock_balance2 = shift5_numba(stock_balance2, 1)
        stock_balance2 = np.nan_to_num(stock_balance2)

        # print("stock_balance2")
        # print(stock_balance2)
        # print("stock_balance")
        # print(stock_balance)
        # print(barrier_indices)
        # print(np.sum(stock_balance2 != stock_balance))
        # print(np.sum(stock_balance2[:barrier_indices + 1] != stock_balance[:barrier_indices + 1]))

        if barrier_indices != 0:
            # barrier(knockout) is triggered.
            row.event = 'KO'
            row['stock_balance'] = stock_balance[barrier_indices]
            row['stock_price'] = prices_temp[barrier_indices]
            row['now_price'] = prices_temp[barrier_indices]
            row['event_date'] = dates_temp[barrier_indices]
            row['event_price'] = prices_temp[barrier_indices]
            row['expiry_price'] = prices_temp[-1]
            row['now_date'] = dates_temp[barrier_indices]
            row['expiry_payoff'] = row['barrier'] - row['strike']
            row['expiry_return'] = (prices_temp[-1] / prices_temp[0]) - 1
            row['drawdown_return'] = np.amin(prices_temp) / prices_temp[0] - 1
            row['duration'] = (row['event_date'] - row['spot_date']).days / 365
            row['r'] = r[barrier_indices]
            row['q'] = q[barrier_indices]
            row['v1'] = v1[barrier_indices]
            row['v2'] = v2[barrier_indices]
            row['t'] = t[barrier_indices]
            stock_balance[barrier_indices] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            pnl = pnl[:barrier_indices+1]
            churn = churn[:barrier_indices+1]
            row['pnl'] = np.nansum(pnl)
            delta_churn = np.abs(churn)
            row['delta_churn'] = np.nansum(delta_churn)
            row['return'] = row['pnl'] / prices_temp[0]
            row["num_hedges"] = np.sum(stock_balance2[:barrier_indices+1] != stock_balance[:barrier_indices+1])

        elif dates_temp[-1] == row['expiry_date']:
            # Expiry is triggered.
            row.event = 'expire'
            row['stock_balance'] = stock_balance[-1]
            row['stock_price'] = prices_temp[-1]
            row['now_price'] = prices_temp[-1]
            row['event_price'] = prices_temp[-1]
            row['expiry_price'] = prices_temp[-1]
            row['event_date'] = dates_temp[-1]
            row['now_date'] = dates_temp[-1]
            row['expiry_payoff'] = max(row['now_price'] - row['strike'], 0)
            row['expiry_return'] = (prices_temp[-1] / prices_temp[0]) - 1
            row['drawdown_return'] = np.amin(prices_temp) / prices_temp[0] - 1
            row['duration'] = (row['event_date'] - row['spot_date']).days / 365
            row['r'] = r[-1]
            row['q'] = q[-1]
            row['v1'] = v1[-1]
            row['v2'] = v2[-1]
            row['t'] = t[-1]
            stock_balance[-1] = 0
            pnl = (stock_balance2 - stock_balance) * prices_temp
            churn = (stock_balance2 - stock_balance)
            row['pnl'] = np.nansum(pnl)
            delta_churn = np.abs(churn)
            row['delta_churn'] = np.nansum(delta_churn)
            row['return'] = row['pnl'] / prices_temp[0]
            row["num_hedges"] = np.sum(stock_balance2 != stock_balance)

        else:
            # No event is triggered.
            row.event = None
            row['stock_balance'] = 0
            row['stock_price'] = prices_temp[-1]
            row['now_price'] = prices_temp[-1]
            row['event_date'] = None
            row['now_date'] = dates_temp[-1]
            row['expiry_payoff'] = None
            row['v1'] = v1[-1]
            row['v2'] = v2[-1]
            row['r'] = r[-1]
            row['q'] = q[-1]
            row['pnl'] = None
            row['delta_churn'] = None
            row['t'] = t[-1]
            row["num_hedges"] = None
        return row

    logging.basicConfig(filename="logfilename.log", level=logging.INFO)
    def fill_zeros_with_last(arr):
        prev = np.arange(len(arr))
        prev[arr == 2] = 2
        prev = np.maximum.accumulate(prev)
        return arr[prev]

    def foo(k):
        try:
            # run the null filler for each section of dates
            date_temp = dates_per_run[args.run_number][k]
            null_df_small = null_df[null_df.spot_date == date_temp]
            print(f"Filling {dates_per_run[args.run_number][k]}, {k} date from {len(dates_per_run[args.run_number])} dates.")
            null_df_small = null_df_small.progress_apply(lambda x: exec_fill_fun(x, prices_np, dates_np, null_df), axis=1, raw=True)
            null_df_small.drop(['expiry_date_index', 'spot_date_index', 'ticker_index'], axis=1, inplace=True)
            null_df_small = null_df_small.infer_objects()
            write_to_aws_executive_production_null(null_df_small, args)
            print(f"Finished {dates_per_run[args.run_number][k]}, {k} date from {len(dates_per_run[args.run_number])} dates.")

        except Exception as e:
            print(e)
            logging.error(e)
            logging.error(f"Error for {dates_per_run[args.run_number][k]}, {k} date from {len(dates_per_run[args.run_number])} dates.")
            print(f"Error for {dates_per_run[args.run_number][k]}, {k} date from {len(dates_per_run[args.run_number])} dates.")


    if len(null_df) > len(dates_per_run[args.run_number]):
        args.small_dataset = False
        for i in range(int(len(dates_per_run[args.run_number]))):
            foo(i)
    else:
        args.small_dataset = True
        if args.option_maker_history:
            null_df["num_hedges"] = None
        null_df = null_df.progress_apply(exec_fill_fun, axis=1, raw=True)

        null_df.drop(['expiry_date_index', 'spot_date_index', 'ticker_index'], axis=1, inplace=True)
        null_df = null_df.infer_objects()
        # null_df_small.to_csv('null_df_executive.csv')
        #write_to_aws_executive_production_null(null_df, args)
        if len(null_df) > 0 :
            null_df = null_df.drop_duplicates(subset=["uid"], keep="first", inplace=False)
            upsert_data_to_database(args, "uid", TEXT, null_df, method="update")

    # ********************************************************************************************
    # ********************************************************************************************
    print(f'Filling up the nulls is finished.')

    # report_to_slack("{} : === Executive, filling up the nulls completed ===".format(str(dt.now())), args)

