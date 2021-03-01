import datetime
import sys
import math
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from datetime import datetime as dt
import global_vars
from classic.data_process import get_close_vol
from classic.data_process import make_multiples, lookback_creator, nearest, move_nans_to_top, Sl_Tp
from classic.data_transfer import tac_data_download, download_holidays, \
    write_to_aws_sltp_production, download_production_sltp, download_production_sltp_null, \
    write_to_aws_sltp_production_null, write_to_aws_merge_latest_price, tac_data_download_by_ticker
from general.slack import report_to_slack
from tqdm import tqdm
from ingestion.classic_vol_history import upsert_data_to_database

def classic_vol_update(args):
    
    # ******************************* Calculating the vols *************************************
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    holidays_df = download_holidays(args)
    args.holidays = holidays_df['non_working_day']

    # Get all the prices for start date -4 years to make sure that we have enough data if the nans are removed.
    # It will only download data that  has is_active flag set to true.
    tac_data = tac_data_download(args)

    main_multiples = make_multiples(tac_data, args)

    # ********************************************************************************************
    # Calculating the volatilities based on returns
    log_returns = np.log(main_multiples)
    log_returns_sq = np.square(log_returns)
    returns = log_returns_sq.rolling(512, min_periods=1).mean()
    c2c_vol_0_502 = np.sqrt(returns * 256)
    c2c_vol_0_502[c2c_vol_0_502.isnull()] = 0.2
    c2c_vol_0_502[c2c_vol_0_502 < 0.2] = 0.2
    c2c_vol_0_502[c2c_vol_0_502 > 0.80] = 0.80

    c2c_vol_0_502['spot_date'] = c2c_vol_0_502.index
    main_pred = c2c_vol_0_502.melt(id_vars='spot_date', var_name="ticker", value_name="classic_vol")

    # ********************************************************************************************
    # Updating latest_price_updates classic_vol column.

    vol_df = main_pred[['ticker', 'classic_vol', 'spot_date']]
    vol_df = vol_df.sort_values(by='spot_date', ascending=False)
    vol_df.reset_index(inplace=True, drop=True)
    spot_date = max(vol_df['spot_date'])
    aa = vol_df.groupby(['ticker']).agg({'classic_vol': ['first']})
    aa.reset_index(inplace=True)
    aa.columns = aa.columns.droplevel(1)
    
    write_to_aws_merge_latest_price(aa, args)
    args.db_url_droid_write = global_vars.DB_DROID_URL_WRITE
    aa["spot_date"] = spot_date
    aa['spot_date'] = aa['spot_date'].astype(str)
    aa['uid'] = aa['spot_date'] + aa['ticker']
    aa['uid'] = aa['uid'].str.replace("-", "").str.replace(".", "").str.replace(" ", "")
    aa['uid'] = aa['uid'].str.strip()
    aa['spot_date'] = pd.to_datetime(aa['spot_date'])
    print(aa)
    upsert_data_to_database(aa, args)
    report_to_slack("{} : === Updating latest_price_updates classic_vol completed ===".format(str(dt.now())), args)
    # ********************************************************************************************

def main_fn(args):

    # ******************************* Calculating the vols *************************************
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    holidays_df = download_holidays(args)
    args.holidays = holidays_df['non_working_day']

    # Get all the prices for start date -4 years to make sure that we have enough data if the nans are removed.
    # It will only download data that  has is_active flag set to true.
    tac_data = tac_data_download_by_ticker(args)

    main_multiples = make_multiples(tac_data, args)

    # ********************************************************************************************
    # Calculating the volatilities based on returns
    log_returns = np.log(main_multiples)
    log_returns_sq = np.square(log_returns)
    returns = log_returns_sq.rolling(512, min_periods=1).mean()
    c2c_vol_0_502 = np.sqrt(returns * 256)
    c2c_vol_0_502[c2c_vol_0_502.isnull()] = 0.2
    c2c_vol_0_502[c2c_vol_0_502 < 0.2] = 0.2
    c2c_vol_0_502[c2c_vol_0_502 > 0.80] = 0.80

    c2c_vol_0_502['spot_date'] = c2c_vol_0_502.index
    main_pred = c2c_vol_0_502.melt(id_vars='spot_date', var_name="ticker", value_name="classic_vol")
    main_pred = main_pred.merge(tac_data[['ticker', 'trading_day', 'tri_adjusted_price']],
                                how='inner', left_on=['spot_date', 'ticker'], right_on=['trading_day', 'ticker'])
    del main_pred['trading_day']
    main_pred.rename(columns={'tri_adjusted_price': 'spot_price'}, inplace=True)

    # ********************************************************************************************
    # Adding vol periods to main dataframe.
    main_pred_temp = pd.DataFrame(columns=main_pred.columns)
    print("Insert Vol Period")
    for month_exp in args.month_horizon:
        main_pred2 = main_pred.copy()
        main_pred2['vol_period'] = math.floor(month_exp * global_vars.vol_period)
        main_pred_temp = main_pred_temp.append(main_pred2)
        main_pred_temp.reset_index(drop=True, inplace=True)
        del main_pred2
    del main_pred
    main_pred = main_pred_temp.copy()
    del main_pred_temp
    print(main_pred)
    # ********************************************************************************************
    # Calculating stop loss and take profit levels.

    # main_pred['SL'] = (global_vars.sl_multiplier * main_pred['classic_vol'] *
    #                    (main_pred['vol_period'] / 252) ** 0.5 + 1) * main_pred['spot_price']
    # main_pred['TP'] = (global_vars.tp_multiplier * main_pred['classic_vol'] *
    #                    (main_pred['vol_period'] / 252) ** 0.5 + 1) * main_pred['spot_price']
    print("Calculating Sl & TP")
    for month_exp in args.month_horizon :
        vol_period = math.floor(month_exp * global_vars.vol_period)
        main_pred.loc[main_pred['vol_period'] == vol_period, 'SL'] =\
            (global_vars.sl_multiplier_1m * main_pred.loc[main_pred['vol_period'] == vol_period, 'classic_vol'] *
            (main_pred.loc[main_pred['vol_period'] == vol_period, 'vol_period'] / 252) ** 0.5 + 1) *\
            main_pred.loc[main_pred['vol_period'] == vol_period, 'spot_price']

        main_pred.loc[main_pred['vol_period'] == vol_period, 'TP'] =\
            (global_vars.tp_multiplier_1m * main_pred.loc[main_pred['vol_period'] == vol_period, 'classic_vol'] *
            (main_pred.loc[main_pred['vol_period'] == vol_period, 'vol_period'] / 252) ** 0.5 + 1) *\
            main_pred.loc[main_pred['vol_period'] == vol_period, 'spot_price']

    # ********************************************************************************************
    # Adding expiry date and month_to_exp
    print("Calculating Expiry Date & Month To Expiry")
    for month_exp in args.month_horizon:
        vol_period = math.floor(month_exp * global_vars.vol_period)
        if month_exp < 1:
            main_pred.loc[main_pred['vol_period'] == vol_period, 'month_to_exp'] = month_exp
            main_pred.loc[main_pred['vol_period'] == vol_period, 'expiry_date'] = main_pred['spot_date'] + relativedelta(weeks=(month_exp * 4))
        else:
            main_pred.loc[main_pred['vol_period'] == vol_period, 'month_to_exp'] = month_exp
            main_pred.loc[main_pred['vol_period'] == vol_period, 'expiry_date'] = main_pred['spot_date'] + relativedelta(months=month_exp)

    main_pred = main_pred[main_pred['classic_vol'].notna()]

    # making sure that expiry date is not holiday or weekend
    main_pred['expiry_date'] = pd.to_datetime(main_pred['expiry_date'])
    cond = main_pred['expiry_date'].apply(lambda x: x.weekday()) > 4
    main_pred.loc[cond, 'expiry_date'] = main_pred.loc[cond, 'expiry_date'] - BDay(1)

    for i in range(20):  # check for at LEAST twenty days back for holidays and weekends
        cond = main_pred['expiry_date'].isin(args.holidays)
        main_pred.loc[cond, 'expiry_date'] = main_pred.loc[cond, 'expiry_date'] - BDay(1)
    # ********************************************************************************************

    # The following columns will be filled later.
    main_pred['event_date'] = None
    main_pred['event_price'] = None
    main_pred['expiry_price'] = None
    main_pred['drawdown_return'] = None
    main_pred['event'] = None
    main_pred['return'] = None
    main_pred['duration'] = None
    main_pred['pnl'] = None

    # Adding UID
    main_pred['uid'] = main_pred['ticker'] + '_' + main_pred['vol_period'].astype(str) + '_' + \
                       main_pred['spot_date'].astype(str)
    main_pred['uid'] = main_pred['uid'].str.replace("-", "").str.replace(".", "")
    main_pred['uid'] = main_pred['uid'].str.strip()
    print(f'Volatility calculation is finished.')

    # Filtering the results for faster writing to AWS.
    if args.history:
        main_pred2 = main_pred[main_pred.spot_date >= args.start_date - BDay(0)]
    else:
        temp_db = args.latest_dates_db
        temp_db['ticker'] = temp_db.index
        for tick in main_pred.ticker.unique():
            print(tick)
            try:
                max_date = temp_db.loc[temp_db['ticker', ''] == tick, ['spot_date', 'max']].values[0][0]
            except Exception:
                max_date = (dt.today() - relativedelta(years=3)).date()
            main_pred = main_pred.drop(main_pred[(main_pred.spot_date <= max_date) & (main_pred.ticker == tick)].index)
        main_pred2 = main_pred
        # main_pred2 = main_pred[main_pred.spot_date >= args.start_date - BDay(3)]

    # ********************************************************************************************
    # Updating latest_price_updates classic_vol column.

    vol_df = main_pred2[['ticker', 'classic_vol', 'spot_date']]
    vol_df = vol_df.sort_values(by='spot_date', ascending=False)
    vol_df.reset_index(inplace=True, drop=True)
    
    spot_date = args.end_date
    
    aa = vol_df.groupby(['ticker']).agg({'classic_vol': ['first']})
    aa.reset_index(inplace=True)
    aa.columns = aa.columns.droplevel(1)
    if len(aa) > 0:
        write_to_aws_merge_latest_price(aa, args)
        args.db_url_droid_write = global_vars.DB_DROID_URL_WRITE
        aa["spot_date"] = spot_date
        aa['spot_date'] = aa['spot_date'].astype(str)
        aa['uid'] = aa['spot_date'] + aa['ticker']
        aa['uid'] = aa['uid'].str.replace("-", "").str.replace(".", "").str.replace(" ", "")
        aa['uid'] = aa['uid'].str.strip()
        aa['spot_date'] = pd.to_datetime(aa['spot_date'])
        if not args.history:
            upsert_data_to_database(aa, args)
            report_to_slack("{} : === Updating latest_price_updates classic_vol completed ===".format(str(dt.now())), args)
        # ********************************************************************************************

        # Writing the calculated events to AWS.
        main_pred2 = main_pred2.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        write_to_aws_sltp_production(main_pred2, args)
        report_to_slack("{} : === Classic calculation completed ===".format(str(dt.now())), args)


# *********************** Filling up the Null values **************************
def fill_nulls(args):
    tac_data = tac_data_download(args)
    print("Download Null Classic Backtest")
    null_df = download_production_sltp_null(args)
    # ********************************************************************************************
    # ********************************************************************************************
    prices_df = tac_data.pivot_table(index=tac_data.trading_day, columns='ticker', values='tri_adjusted_price',
                                     aggfunc='first', dropna=False)
    prices_df = prices_df.ffill()
    prices_df = prices_df.bfill()
    def SLTP_fn(row):
        # Calculate the desired quantities row by row.

        prices_temp = prices_df.loc[(prices_df.index >= row.spot_date) & (prices_df.index <= row.expiry_date), row.ticker]
        if len(prices_temp) == 0:
            return row
        # Finding the index that take profit is triggered.
        tp_indices = np.argmax((prices_temp >= row.TP).values)
        if tp_indices == 0:
            tp_indices = -1

        # Finding the index that stop loss is triggered.
        sl_indices = np.argmax((prices_temp <= row.SL).values)
        if sl_indices == 0:
            sl_indices = -1

        temp_date = row.spot_date + relativedelta(months=int(row.month_to_exp))
        if temp_date.weekday() > 4:
            temp_date = temp_date - BDay(1)

        if (sl_indices == -1) & (tp_indices == -1):
            # If none of the events are triggered.
            if (prices_temp.index[-1] < temp_date) & (temp_date > dt.today() - BDay(1)):
                # If the expiry date hasn't arrived yet.
                row.event = None
                row['return'] = None
                row.event_date = None
                row.event_price = None
            else:
                # If none of the events are triggered and expiry date has arrived.
                row.event = 'NT'
                row['return'] = prices_temp[-1] / prices_temp[0] - 1
                row.event_date = prices_temp.index[-1]
                row.event_price = prices_temp[-1]
                row.expiry_price = prices_temp[-1]
                row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                row.pnl = prices_temp[-1] - prices_temp[0]
                row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days / 365
        else:
            # If one of the events is triggered.
            if sl_indices > tp_indices:
                # If stop loss is triggered.
                row.event = 'SL'
                row['return'] = prices_temp[sl_indices] / prices_temp[0] - 1
                row.event_date = prices_temp.index[sl_indices]
                row.event_price = prices_temp[sl_indices]
                row.expiry_price = prices_temp[-1]
                row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days / 365
                row.pnl = prices_temp[sl_indices] - prices_temp[0]

            else:
                # If take profit is triggered.
                row.event = 'TP'
                row['return'] = prices_temp[tp_indices] / prices_temp[0] - 1
                row.event_date = prices_temp.index[tp_indices]
                row.event_price = prices_temp[tp_indices]
                row.expiry_price = prices_temp[-1]
                row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days / 365
                row.pnl = prices_temp[tp_indices] - prices_temp[0]


        if prices_temp.index[-1] < temp_date:
            # If the expiry date hasn't arrived yet.
            row['drawdown_return'] = None
        else:
            # If the expiry date is arrived.
            row['drawdown_return'] = min(prices_temp) / prices_temp[0] - 1

        return row

    tqdm.pandas()
    print(null_df)
    if len(null_df) > 0:
        null_df = null_df.progress_apply(SLTP_fn, axis=1)
        print(null_df)
        # ********************************************************************************************
        # ********************************************************************************************
        print(f'Filling up the nulls is finished.')
        #null_df.to_csv('null_df_classic.csv')
        null_df = null_df.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        write_to_aws_sltp_production_null(null_df, args)
        report_to_slack("{} : === Classic, filling up the nulls completed ===".format(str(dt.now())), args)

