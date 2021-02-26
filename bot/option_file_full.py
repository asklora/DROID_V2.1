import datetime
import gc
import logging
import multiprocessing

from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

from executive.black_scholes import find_vol, get_interest_rate, Up_Out_Call, deltaUnOC
from executive.data_download import tac_data_download, get_vol_surface_data, get_indices_data, get_interest_rate_data, \
    get_dividends_data, download_production_executive_null, download_holidays, get_vol_surface_data_inferred
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
    options_df2 = options_df.copy()
    options_df['month_to_exp'] = 1
    options_df2['month_to_exp'] = 3
    options_df = options_df.append(options_df2)
    options_df.reset_index(inplace=True, drop=True)

    options_df.loc[options_df['month_to_exp'] == 1, 'expiry_date'] = options_df['spot_date'] + relativedelta(months=1)
    options_df.loc[options_df['month_to_exp'] == 3, 'expiry_date'] = options_df['spot_date'] + relativedelta(months=3)

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

    options_df2 = options_df.copy()
    options_df3 = options_df.copy()
    options_df4 = options_df.copy()
    options_df5 = options_df.copy()
    options_df6 = options_df.copy()
    options_df7 = options_df.copy()

    options_df['option_type'] = 'type_1_1'
    options_df['strike_type'] = 'I'
    options_df['barrier_type'] = 'I'

    options_df2['option_type'] = 'type_1_2'
    options_df2['strike_type'] = 'I'
    options_df2['barrier_type'] = 'II'

    options_df3['option_type'] = 'type_2_2'
    options_df3['strike_type'] = 'II'
    options_df3['barrier_type'] = 'II'

    options_df4['option_type'] = 'type_2_3'
    options_df4['strike_type'] = 'II'
    options_df4['barrier_type'] = 'III'

    options_df5['option_type'] = 'type_2_4'
    options_df5['strike_type'] = 'II'
    options_df5['barrier_type'] = 'IV'

    options_df6['option_type'] = 'type_3_3'
    options_df6['strike_type'] = 'III'
    options_df6['barrier_type'] = 'III'

    options_df7['option_type'] = 'type_3_4'
    options_df7['strike_type'] = 'III'
    options_df7['barrier_type'] = 'IV'

    options_df = options_df.append(options_df2)
    options_df = options_df.append(options_df3)
    options_df = options_df.append(options_df4)
    options_df = options_df.append(options_df5)
    options_df = options_df.append(options_df6)
    options_df = options_df.append(options_df7)
    options_df.reset_index(inplace=True, drop=True)

    del options_df2, options_df3, options_df4, options_df5, options_df6, options_df7
    # del options_df2, options_df6

    options_df.loc[options_df['strike_type'] == 'I', 'strike'] = options_df['spot_price'] * (1 - options_df['vol_t'] * 0.5)
    options_df.loc[options_df['strike_type'] == 'II', 'strike'] = options_df['spot_price']
    options_df.loc[options_df['strike_type'] == 'III', 'strike'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 0.5)

    options_df.loc[options_df['barrier_type'] == 'I', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 1)
    options_df.loc[options_df['barrier_type'] == 'II', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 1.5)
    options_df.loc[options_df['barrier_type'] == 'III', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 2)
    options_df.loc[options_df['barrier_type'] == 'IV', 'barrier'] = options_df['spot_price'] * (1 + options_df['vol_t'] * 2.5)

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

    if args.add_inferred:
        options_df['inferred'] = 1
    else:
        options_df['inferred'] = 0

    options_df.drop(['strike_type', 'barrier_type', 'trading_day'], axis=1, inplace=True)

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
            report_to_slack("{} : === {} Executive UNO calculation completed ===".format(str(dt.now()),args.exec_index), args)
    except Exception as e:
        print(e)
        options_df2.to_csv('option_df_file.csv')


