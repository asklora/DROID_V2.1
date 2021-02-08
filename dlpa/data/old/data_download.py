import os
import platform
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

import global_vars

if platform.system() == 'Linux':
    path = '/home/loratech/PycharmProjects/data/'
    if not os.path.exists(path):
        os.makedirs(path)
else:
    path = 'C:/dlpa_master/'
    Path(path).mkdir(parents=True, exist_ok=True)

os.chdir(path)


def data_download(args, start_year=0, start_week=0):
    # Downloading data from AWS
    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    # Selecting the required columns
    fields = 'SELECT ri.ticker, ri.year, ri.subinterval, changes as candles, week_return, forward_return'

    # Desired tables
    table0 = 'w_ohlcv_candles'
    table2 = 'w_weekly_returns_close'
    table1 = 'W_forward_returns_close'

    # Join conditions
    join_con1 = " ON fr.ticker=ri.ticker AND fr.year=ri.year AND fr.subinterval=ri.subinterval "
    join_con2 = " ON gr.ticker=ri.ticker AND gr.year=ri.year AND gr.subinterval=ri.subinterval "

    # Selecting conditions
    cons = 'where ((ri.year=:year and ri.subinterval>:week) or (ri.year>:year))'

    # Final SQL query
    data_sql = fields + ' FROM ' + table0 + ' AS ri ' + 'LEFT JOIN ' + table1 + ' AS fr ' + join_con1 + ' JOIN ' + table2 + ' AS gr ' + join_con2 + cons

    with engine.connect() as conn:
        data_input = conn.execute(text(data_sql), year=start_year, week=start_week).fetchall()

    # Converting the downloaded data from SQL to pandas dataframe.
    full_df = pd.DataFrame(data_input)
    if full_df.shape[1] == 6:
        full_df.columns = ['ticker', 'year', 'week_num', 'candles', 'week_ret_per', 'forward_returns']

    return full_df


def time_max_finder(df):
    # This function finds the maximum year and its corresponding max week of an input dataframe.
    max_year = df['year'].max()
    max_week = df[df['year'] == max_year]['week_num'].max()
    return max_year, max_week


def year_week(y, w):
    # This function returns the datetime object for a given year nad week number.
    return datetime.strptime(f'{y} {w} 1', '%G %V %u')


def load_data(args):
    # This function downloads a fresh dataset or updates an existing one.
    update_lookback = args.update_lookback
    update = bool(args.update)

    df_file = Path(os.path.join(os.getcwd(), 'full_df.pkl'))
    if df_file.exists():
        if update:
            # Read the pickle file.
            full_df = pd.read_pickle(df_file)

            # Find the max year and week number of the dataframe.
            max_year, max_week = time_max_finder(full_df)

            # Convert the max year and week to datetime object.
            t = year_week(max_year, max_week)

            # set the download start date based on the update lookback.
            d = timedelta(days=7 * update_lookback)
            u = t - d
            start_year, start_week = datetime.date(u).isocalendar()[0:2]

            # Update the dataframe using the given start date
            update_df = data_download(args, start_year, start_week)

            # This if statement drops the dates that need updating and replace them with new ones.
            if update_df.shape[1] == 6:
                full_df.drop(full_df[((full_df['year'] == start_year) & (
                        full_df[full_df['year'] == start_year]['week_num'] > start_week)) | (
                                             full_df['year'] > start_year)].index, inplace=True)
                full_df.reset_index(drop=True, inplace=True)
                full_df = full_df.append(update_df)
                full_df.reset_index(drop=True, inplace=True)
                full_df.to_pickle(os.path.join(os.getcwd(), 'full_df.pkl'))
        else:
            full_df = pd.read_pickle(df_file)
    else:
        # Download the whole data if there is no offline data.
        full_df = data_download(args)
        full_df.to_pickle(os.path.join(os.getcwd(), 'full_df.pkl'))
    return full_df
