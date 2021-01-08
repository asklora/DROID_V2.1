import sys
import global_vars
import pandas as pd
import numpy as np
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Text
from pangres import upsert
from datetime import datetime
from dateutil.relativedelta import relativedelta
from classic.data_process import make_multiples
from classic.data_transfer import tac_data_download, download_holidays

droid_universe_table = 'droid_universe'
classic_vol_history_table = 'classic_vol_history'


def classic_vol_history(args):
    args.holidays_table_name = global_vars.holidays_table_name
    args.tac_data_table_name = global_vars.tac_data_table_name
    args.start_date = datetime.now().date() - relativedelta(years=4)
    args.end_date = datetime.now().date()
    # ******************************* Calculating the vols *************************************
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    holidays_df = download_holidays(args)
    args.holidays = holidays_df['non_working_day']
    print(holidays_df)

    # Get all the prices for start date -4 years to make sure that we have enough data if the nans are removed.
    # It will only download data that  has is_active flag set to true.
    tac_data = tac_data_download(args)
    print(tac_data)

    main_multiples = make_multiples(tac_data, args)
    print(main_multiples)

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
    vol_df['spot_date'] = vol_df['spot_date'].astype(str)
    vol_df['uid'] = vol_df['spot_date'] + vol_df['ticker']
    vol_df['uid'] = vol_df['uid'].str.replace("-", "").str.replace(".", "").str.replace(" ", "")
    vol_df['uid'] = vol_df['uid'].str.strip()
    vol_df['spot_date'] = pd.to_datetime(vol_df['spot_date'])

    vol_df = vol_df.loc[vol_df["spot_date"] >= "2020-01-01"]
    print(vol_df)

    upsert_data_to_database(vol_df, args)
    # aa = vol_df.groupby(['ticker']).agg({'classic_vol': ['first']})
    # aa.reset_index(inplace=True)
    # aa.columns = aa.columns.droplevel(1)

    # write_to_aws_merge_latest_price(aa, args)
    # report_to_slack("{} : === Updating latest_price_updates classic_vol completed ===".format(str(dt.now())), args)
    # ********************************************************************************************

def upsert_data_to_database(result, args):
    print('=== Insert Classic Vol History to database ===')
    result = result.set_index('uid')
    dtype = {'uid':Text,
             'spot_date' : Date}
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=classic_vol_history_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + classic_vol_history_table)
    engines_droid.dispose()