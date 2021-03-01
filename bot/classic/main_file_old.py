import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

import global_vars
from volatility.data_process import get_close_vol
from volatility.data_process import make_multiples, lookback_creator, nearest, move_nans_to_top, Sl_Tp
from volatility.data_transfer import tac_data_download, download_holidays, \
    write_to_aws_sltp_production, download_production_sltp


def main_fn(args):
    # ******************************* Calculating the vols *************************************
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    holidays_df = download_holidays(args)
    args.holidays = holidays_df['non_working_day']

    # Get all the prices for start date -4 years to make sure that we have enough data if the nans are removed.
    tac_data = tac_data_download(args)

    dates_df = tac_data.pivot_table(index='trading_day', columns='ticker', values='day_status', aggfunc='first',
                                    dropna=False)
    trading_day_list = tac_data['trading_day'].unique()
    trading_day_list = np.sort(trading_day_list)

    # The following line change the start_date to the first trading day.
    start_date = nearest(trading_day_list, args.start_date)
    trading_day_list = trading_day_list[trading_day_list >= start_date]

    main_columns_list = ['ticker', 'trading_day', 'c2c_vol_0_502']

    col_list = ['ticker', 'spot_date', 'spot_price', 'classic_vol', 'SL', 'TP', 'vol_period', 'month_to_exp',
                'expiry_date', 'event_date', 'event_price', 'event', 'return', 'uid']

    main_pred = pd.DataFrame(columns=col_list)
    horizons = [21, 63]

    for trading_day in trading_day_list:
        tac_values = tac_data.pivot_table(index=tac_data.trading_day, columns='ticker', values='tri_adjusted_price',
                                          aggfunc='first', dropna=False)
        tac_values = tac_values[tac_values.index <= trading_day]
        tac_values = move_nans_to_top(tac_values)

        valid_tickers = (dates_df.loc[trading_day] == 'trading_day')
        valid_tickers_list = valid_tickers[valid_tickers == True].index
        temp_df = pd.DataFrame(columns=main_columns_list)
        temp_df.ticker = valid_tickers_list
        temp_df.trading_day = trading_day

        prices_df_temp = tac_data[(tac_data['trading_day'] <= trading_day) & (tac_data['trading_day'] > trading_day -
                                                                              BDay(550))]
        main_multiples = make_multiples(prices_df_temp, args)

        # Calculating the volatilities based on returns
        c2c_vol_0_502 = get_close_vol(lookback_creator(main_multiples, 0, 502 + 1, args))

        def series_to_pandas(df):
            # Ths function makes sure that only the available tickers for the current trading date is used.
            aa = pd.DataFrame(df[df.index.isin(valid_tickers_list)])
            aa.reset_index(inplace=True)
            return aa

        tech_temp = series_to_pandas(c2c_vol_0_502)
        temp_df['c2c_vol_0_502'] = tech_temp[0]
        temp_df.rename(columns={'trading_day': 'spot_date'}, inplace=True)

        for horizon in horizons:
            # Looping on different horizons
            stock_list = temp_df.ticker.unique()

            main_pred_temp = pd.DataFrame()
            main_pred_temp['ticker'] = stock_list

            main_pred_temp['spot_date'] = trading_day
            main_pred_temp['spot_price'] = tac_values[stock_list].loc[trading_day].values
            main_pred_temp = main_pred_temp.merge(temp_df, on=['ticker', 'spot_date'], how='left')

            # Calculating the Stop Loss and Take Profit
            main_pred_temp['SL'] = (global_vars.sl_multiplier * main_pred_temp['c2c_vol_0_502'] *
                                    (horizon / 252) ** 0.5 + 1).mul(tac_values[stock_list].loc[trading_day].values,
                                                                    axis=0)
            main_pred_temp['TP'] = (global_vars.tp_multiplier * main_pred_temp['c2c_vol_0_502'] *
                                    (horizon / 252) ** 0.5 + 1).mul(tac_values[stock_list].loc[trading_day].values,
                                                                    axis=0)

            # Preparing the final database for writing to AWS.
            main_pred_temp['vol_period'] = horizon
            main_pred_temp['month_to_exp'] = ""
            main_pred_temp.loc[main_pred_temp['vol_period'] == 21, 'month_to_exp'] = 1
            main_pred_temp.loc[main_pred_temp['vol_period'] == 42, 'month_to_exp'] = 2
            main_pred_temp.loc[main_pred_temp['vol_period'] == 63, 'month_to_exp'] = 3

            main_pred_temp.loc[main_pred_temp['vol_period'] == 21, 'expiry_date'] = main_pred_temp[
                                                                                        'spot_date'] + relativedelta(
                months=1)
            main_pred_temp.loc[main_pred_temp['vol_period'] == 42, 'expiry_date'] = main_pred_temp[
                                                                                        'spot_date'] + relativedelta(
                months=2)
            main_pred_temp.loc[main_pred_temp['vol_period'] == 63, 'expiry_date'] = main_pred_temp[
                                                                                        'spot_date'] + relativedelta(
                months=3)

            for ind, row in main_pred.iterrows():
                while (row['expiry_date'].weekday() > 4) or (sum(args.holidays.isin([row['expiry_date']])) > 0):
                    row['expiry_date'] = (row['expiry_date'] + BDay(1)).date()

            main_pred_temp['event_date'] = None
            main_pred_temp['event_price'] = None
            main_pred_temp['event'] = None
            main_pred_temp['return'] = None

            main_pred_temp.rename(columns={'c2c_vol_0_502': 'classic_vol'}, inplace=True)

            main_pred_temp['uid'] = main_pred_temp['ticker'] + '_' + main_pred_temp['vol_period'].astype(str) + '_' + \
                                    main_pred_temp['spot_date'].astype(str)
            main_pred_temp['uid'] = main_pred_temp['uid'].str.replace("-", "").str.replace(".", "")
            main_pred_temp['uid'] = main_pred_temp['uid'].str.strip()
            main_pred = main_pred.append(main_pred_temp)
        print(f'Volatility calculation: {trading_day} is finished.')

    # Writing the calculated events to AWS.
    write_to_aws_sltp_production(main_pred, args)

    # *********************** Filling up the Null values **************************

    sltp_df = download_production_sltp(args)

    # Downloading the not triggered stocks
    null_df = sltp_df[sltp_df.event.isnull()]

    dates_list = null_df.spot_date.unique()
    dates_list = np.sort(dates_list)
    events = pd.DataFrame(columns=['event_date', 'event', 'ticker', 'event_price', 'return',
                                   'vol_period', 'spot_date'])
    for datee in dates_list:
        for per in null_df.vol_period.unique():
            stock_list = null_df[(null_df.spot_date == datee) & (null_df.vol_period == per)].ticker.unique()

            prices_df = tac_data.pivot_table(index=tac_data.trading_day, columns='ticker', values='tri_adjusted_price',
                                             aggfunc='first', dropna=False)
            prices_df = prices_df[prices_df.index >= datee]
            prices_df = prices_df.ffill()
            prices_df = prices_df.bfill()
            prices_df = prices_df.iloc[0:per]
            TKs = null_df.loc[(null_df.spot_date == datee) & (null_df.vol_period == per), 'ticker']
            TKs.reset_index(drop=True, inplace=True)
            SL = null_df.loc[(null_df.spot_date == datee) & (null_df.vol_period == per), 'SL']
            TP = null_df.loc[(null_df.spot_date == datee) & (null_df.vol_period == per), 'TP']
            SL.reset_index(drop=True, inplace=True)
            TP.reset_index(drop=True, inplace=True)

            # Checking which events are triggered
            events_temp = Sl_Tp(prices_df[stock_list].values, SL.values, TP.values, prices_df[stock_list].index,
                                stock_list)

            events_temp['vol_period'] = per
            events_temp['spot_date'] = datee

            condition = (events_temp['event_date'] != datee + BDay(per - 1)) \
                        & (events_temp['event'] == 'NT') & (events_temp['vol_period'] == per)
            events_temp = events_temp.loc[~condition]

            events = events.append(events_temp)

        print(f"Event calculation: {datee} is finished.")

    events['uid'] = events['ticker'] + '_' + events['vol_period'].astype(str) + '_' + events['spot_date'].astype(str)
    events['uid'] = events['uid'].str.replace("-", "").str.replace(".", "")
    events['uid'] = events['uid'].str.strip()

    events = events.merge(
        null_df[['SL', 'TP', 'uid', 'spot_date', 'spot_price', 'month_to_exp', 'expiry_date', 'classic_vol']])

    # Writing the triggered events to Aws.
    write_to_aws_sltp_production(events, args)
