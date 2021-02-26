import datetime
import gc
import logging
import multiprocessing

from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

from executive.black_scholes import find_vol, get_interest_rate, Up_Out_Call, deltaUnOC, Rev_Conv, deltaRC
from executive.data_download import tac_data_download, get_vol_surface_data, get_indices_data, get_interest_rate_data, \
    get_dividends_data, download_production_executive_null, download_holidays, get_vol_surface_data_inferred, \
    tac_data_download_null_filler
from executive.data_output import write_to_aws_executive_production, write_to_aws_executive_production_null
import pandas as pd
import numpy as np
import scipy.stats as si
from datetime import datetime as dt
import numba
from tqdm import tqdm

from executive.preprocess import cal_interest_rate, cal_q
from general.slack import report_to_slack
# from multiprocessing import Pool
from pathos.multiprocessing import ProcessingPool as Pool

def option_fn_full(args):
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

    # Adding inferred data to option making.
    # if args.add_inferred:
    #     vol_surface_data_inferred = get_vol_surface_data(args)
    #     vol_surface_data = pd.concat([vol_surface_data, vol_surface_data_inferred], axis=0)

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
    options_df.drop(['trading_day', 'uid', 'stock_price', 'parameter_set_date', 'alpha'], axis=1, inplace=True)
    options_df = options_df.sort_values(by='spot_date', ascending=False)

    if args.debug_mode:
        options_df = options_df.sample(1000)

    # Adding maturities and expiry date
    # options_df2 = options_df.copy()
    options_df['month_to_exp'] = 6
    # options_df2['month_to_exp'] = 3
    # options_df = options_df.append(options_df2)
    options_df.reset_index(inplace=True, drop=True)

    options_df.loc[options_df['month_to_exp'] == 6, 'expiry_date'] = options_df['spot_date'] + relativedelta(months=6)

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
            ind1 = temp[ind - 1]
            ind2 = temp[ind + 1]
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
    v0[v0 >= 0.5] = 0.5
    options_df['vol_t'] = v0 * np.sqrt(options_df['month_to_exp'] / 12)

    # options_df1 = options_df.copy()
    # options_df2 = options_df.copy()
    # options_df3 = options_df.copy()
    # options_df4 = options_df.copy()
    # options_df5 = options_df.copy()
    # options_df6 = options_df.copy()
    options_df7 = options_df.copy()
    options_df8 = options_df.copy()

    # options_df['option_type'] = 'TYPE_1_1'
    # options_df['strike_1_type'] = 'I'
    # options_df['strike_2_type'] = 'I'
    #
    # options_df1['option_type'] = 'TYPE_1_2'
    # options_df1['strike_1_type'] = 'I'
    # options_df1['strike_2_type'] = 'II'
    #
    # options_df2['option_type'] = 'TYPE_1_3'
    # options_df2['strike_1_type'] = 'I'
    # options_df2['strike_2_type'] = 'III'
    #
    # options_df3['option_type'] = 'TYPE_2_2'
    # options_df3['strike_1_type'] = 'II'
    # options_df3['strike_2_type'] = 'II'
    #
    # options_df4['option_type'] = 'TYPE_2_3'
    # options_df4['strike_1_type'] = 'II'
    # options_df4['strike_2_type'] = 'III'
    #
    # options_df5['option_type'] = 'TYPE_2_4'
    # options_df5['strike_1_type'] = 'II'
    # options_df5['strike_2_type'] = 'IV'

    options_df['option_type'] = 'TYPE_4_4'
    options_df['strike_1_type'] = 'IV'
    options_df['strike_2_type'] = 'IV'

    options_df7['option_type'] = 'TYPE_4_5'
    options_df7['strike_1_type'] = 'IV'
    options_df7['strike_2_type'] = 'V'


    # options_df = options_df.append(options_df2)
    # options_df = options_df.append(options_df3)
    # options_df = options_df.append(options_df4)
    # options_df = options_df.append(options_df5)
    # options_df = options_df.append(options_df6)
    options_df = options_df.append(options_df7)
    # options_df = options_df.append(options_df8)
    options_df.reset_index(inplace=True, drop=True)

    # del options_df1, options_df2, options_df3, options_df4, options_df5, options_df6, options_df7, options_df8
    del options_df2, options_df7, options_df8

    options_df.loc[options_df['strike_1_type'] == 'I', 'strike_1'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 0.5)
    options_df.loc[options_df['strike_1_type'] == 'II', 'strike_1'] = options_df['spot_price']
    options_df.loc[options_df['strike_1_type'] == 'III', 'strike_1'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 0.5)
    options_df.loc[options_df['strike_1_type'] == 'IV', 'strike_1'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 1)

    options_df.loc[options_df['strike_2_type'] == 'I', 'strike_2'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 0.5)
    options_df.loc[options_df['strike_2_type'] == 'II', 'strike_2'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 1)
    options_df.loc[options_df['strike_2_type'] == 'III', 'strike_2'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 1.5)
    options_df.loc[options_df['strike_2_type'] == 'IV', 'strike_2'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 2.0)
    options_df.loc[options_df['strike_2_type'] == 'V', 'strike_2'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 2.5)

    options_df['v1'] = find_vol(options_df['strike_1'] / options_df['now_price'], options_df['t'],
                                options_df['atm_volatility_spot'], options_df['atm_volatility_one_year'],
                                options_df['atm_volatility_infinity'], 12, options_df['slope'], options_df['slope_inf'],
                                options_df['deriv'], options_df['deriv_inf'], options_df['r'], options_df['q'])

    options_df['v2'] = find_vol(options_df['strike_2'] / options_df['now_price'], options_df['t'],
                                options_df['atm_volatility_spot'], options_df['atm_volatility_one_year'],
                                options_df['atm_volatility_infinity'], 12, options_df['slope'], options_df['slope_inf'],
                                options_df['deriv'], options_df['deriv_inf'], options_df['r'], options_df['q'])

    options_df['target_profit'] = -Rev_Conv(options_df['spot_price'], options_df['strike_1'], options_df['strike_2'],
                                           options_df['t'], options_df['r'], options_df['q'], options_df['v1'],
                                           options_df['v2'])

    options_df['target_max_loss'] = options_df['strike_1'] - options_df['strike_2']

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

    if args.add_inferred:
        options_df['inferred'] = 1
    else:
        options_df['inferred'] = 0

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

    # options_df.drop(['strike_1_type', 'strike_2_type'], axis=1, inplace=True)

    # Adding UID
    options_df['uid'] = options_df['ticker'] + '_' + options_df['month_to_exp'].astype(str) + '_' + \
                       options_df['spot_date'].astype(str) + '_' + options_df['option_type'].astype(str) \
                        + '_' + options_df['inferred'].astype(str)
    options_df['uid'] = options_df['uid'].str.replace("-", "").str.replace(".", "")
    options_df['uid'] = options_df['uid'].str.strip()

    options_df = options_df[options_df.atm_volatility_one_year > 0.1]
    options_df = options_df[options_df.atm_volatility_infinity > 0.1]
    options_df = options_df[options_df.atm_volatility_one_year < 1.25]
    options_df = options_df[options_df.atm_volatility_infinity < 1.25]

    print(f'Executive calculation is finished.')

    # Filtering the results for faster writing to AWS.
    if args.option_maker_history:
        options_df2 = options_df[options_df.spot_date >= args.start_date - BDay(0)]
    else:
        options_df2 = options_df[options_df.spot_date >= args.start_date - BDay(3)]

    options_df2 = options_df2.infer_objects()

    try:
        # Writing the calculated events to AWS.
        write_to_aws_executive_production(options_df2, args)
        if args.exec_index:
            report_to_slack("{} : === {} Executive UCDC calculation completed ===".format(str(dt.now()),args.exec_index), args)
    except Exception as e:
        print(e)
        options_df2.to_csv('option_df_file.csv')



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

def fill_nulls_ucdc(args):
    # This function is used for filling the nulls in executive options database. Checks whether the options are expired
    # or knocked out or not triggered at all.
    tqdm.pandas()

    # Downloading the options that are not triggered from the executive database.
    null_df = download_production_executive_null(args)
    if args.add_inferred:
        null_df = null_df[null_df.inferred == 1]
    else:
        null_df = null_df[null_df.inferred == 0]

    start_date = null_df.spot_date.min()

    tac_data = tac_data_download_null_filler(start_date, args)
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

    # Dividing the dates to sections for running manually
    dates_df_unique = null_df.spot_date.unique()
    dates_df_unique = np.sort(dates_df_unique)

    divison_length = round(len(dates_df_unique) / args.total_no_of_runs)

    dates_per_run = [dates_df_unique[x:x + divison_length] for x in
                     range(0, len(dates_df_unique), divison_length)]
    # **************************************************

    date_min = min(dates_per_run[args.run_number])
    date_max = max(dates_per_run[args.run_number]) + relativedelta(months=4)

    null_df = null_df[(null_df.spot_date >= date_min) & (null_df.spot_date <= date_max)]

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

        dates_temp = dates_np[int(row.spot_date_index):int(row.expiry_date_index+1), 0]

        t = np.full((len(prices_temp)), ((row['expiry_date'] - dates_temp).astype('timedelta64[D]')) / np.timedelta64(1, 'D') / 365)

        strike_1 = np.full((len(prices_temp)), row['strike_1'])
        strike_2 = np.full((len(prices_temp)), row['strike_2'])

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


        v1 = find_vol(np.divide(strike_1, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,
                      12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)

        v2 = find_vol(np.divide(strike_2, prices_temp), t, atmv_0, atmv_1Y, atmv_inf,
                      12, skew_1m, skew_inf, curv_1m, curv_inf, r, q)

        stock_balance = deltaRC(prices_temp, strike_1, strike_2, t, r, q, v1, v2)
        stock_balance = np.nan_to_num(stock_balance)

        stock_balance2 = shift5_numba(stock_balance, 1)
        stock_balance2 = np.nan_to_num(stock_balance2)

        strike_2_indices = np.argmax((prices_temp >= row.strike_2))

        # if strike_2_indices != 0:
        #     # strike_2(knockout) is triggered.
        #     row.event = 'KO'
        #     row['stock_balance'] = 0
        #     row['stock_price'] = prices_temp[strike_2_indices]
        #     row['now_price'] = prices_temp[strike_2_indices]
        #     row['event_date'] = dates_temp[strike_2_indices]
        #     row['event_price'] = prices_temp[strike_2_indices]
        #     row['expiry_price'] = prices_temp[-1]
        #     row['now_date'] = dates_temp[strike_2_indices]
        #     # row['expiry_payoff'] = row['strike_2'] - row['strike_1']
        #     row['expiry_payoff'] = min(max(row['now_price'] - row['strike_1'], row['strike_2'] - row['strike_1']), 0)
        #     row['expiry_return'] = (prices_temp[-1] / prices_temp[0]) - 1
        #     row['drawdown_return'] = np.amin(prices_temp) / prices_temp[0] - 1
        #     row['duration'] = (row['event_date'] - row['spot_date']).days / 365
        #
        #     row['r'] = r[strike_2_indices]
        #     row['q'] = q[strike_2_indices]
        #     row['v1'] = v1[strike_2_indices]
        #     row['v2'] = v2[strike_2_indices]
        #     row['t'] = t[strike_2_indices]
        #
        #     stock_balance[strike_2_indices] = 0
        #     pnl = (stock_balance2 - stock_balance) * prices_temp
        #     row['pnl'] = np.nansum(pnl[:strike_2_indices+1])
        #     row['return'] = row['pnl'] / prices_temp[0]


        if dates_temp[-1] == row['expiry_date']:
            # Expiry is triggered.
            row.event = 'expire'
            row['stock_balance'] = 0
            row['stock_price'] = prices_temp[-1]
            row['now_price'] = prices_temp[-1]
            row['event_price'] = prices_temp[-1]
            row['expiry_price'] = prices_temp[-1]
            row['event_date'] = dates_temp[-1]
            row['now_date'] = dates_temp[-1]
            row['expiry_payoff'] = min(max(row['now_price'] - row['strike_1'], row['strike_2'] - row['strike_1']), 0)
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
            row['pnl'] = np.nansum(pnl)
            row['return'] = row['pnl'] / prices_temp[0]

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
            row['t'] = t[-1]

        return row

    logging.basicConfig(filename="logfilename.log", level=logging.INFO)

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
        null_df = null_df.progress_apply(exec_fill_fun, axis=1, raw=True)

        null_df.drop(['expiry_date_index', 'spot_date_index', 'ticker_index'], axis=1, inplace=True)
        null_df = null_df.infer_objects()
        # null_df_small.to_csv('null_df_executive.csv')
        write_to_aws_executive_production_null(null_df, args)

    # ********************************************************************************************
    # ********************************************************************************************
    print(f'Filling up the nulls is finished.')

    report_to_slack("{} : === Executive, filling up the nulls completed ===".format(str(dt.now())), args)

