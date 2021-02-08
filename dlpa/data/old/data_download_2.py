import os
import platform
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import sqlalchemy as db

import global_vars

if platform.system() == 'Linux':
    path = '/home/loratech/PycharmProjects/data/'
    if not os.path.exists(path):
        os.makedirs(path)
else:
    path = 'C:/dlpa_master/'
    Path(path).mkdir(parents=True, exist_ok=True)

os.chdir(path)


def data_download(args, update_date=None):
    db_url = global_vars.DB_PROD_URL_READ
    engine = db.create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = 'daily_multiple_check'
    # table0 = 'daily_returns_multiply'

    origin = '1950-01-01'
    origin = datetime.strptime(origin, '%Y-%m-%d')

    if update_date is not None:
        datee1 = update_date.date()
    else:
        datee1 = origin.date()

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker, table0.columns.trading_day, table0.columns.open, table0.columns.high,
                           table0.columns.low, table0.columns.close, table0.columns.volume]).where(
            table0.columns.trading_day >= datee1.strftime('%Y-%m-%d'))

        # query = db.select([table0.columns.ticker, table0.columns.trading_day, table0.columns.open_return, table0.columns.high_return,
        #                    table0.columns.low_return, table0.columns.close_return, table0.columns.volume]).where(
        #     table0.columns.trading_day >= datee1.strftime('%Y-%m-%d'))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()

    full_df = pd.DataFrame(ResultSet)
    if full_df.shape[1] == 7:
        full_df.columns = ['ticker', 'trading_day', 'open_multiply', 'high_multiply', 'low_multiply',
                           'close_multiply', 'volume']

    return full_df


def load_data(args):
    update_lookback = args.update_lookback
    update = bool(args.update)

    df_file = Path(os.path.join(os.getcwd(), 'full_df_daily.pkl'))
    if df_file.exists():
        if update:
            full_df = pd.read_pickle(df_file)
            temp = pd.DataFrame()
            temp['time'] = full_df['trading_day'].astype('datetime64[ns]')
            max_date = temp.time.max()
            if args.data_period == 0:
                d = timedelta(weeks=update_lookback)
            else:
                d = timedelta(days=update_lookback)
            update_date = max_date - d
            update_df = data_download(args, update_date)

            if update_df.shape[1] == 7:
                full_df['time'] = full_df['trading_day'].astype('datetime64[ns]')
                full_df.drop(full_df[(full_df['time'] >= update_date)].index, inplace=True)
                full_df.reset_index(drop=True, inplace=True)
                del full_df['time']
                full_df = full_df.append(update_df)
                full_df.reset_index(drop=True, inplace=True)
                full_df.to_pickle(os.path.join(os.getcwd(), 'full_df_daily.pkl'))


        else:
            full_df = pd.read_pickle(df_file)
    else:
        full_df = data_download(args)
        full_df.to_pickle(os.path.join(os.getcwd(), 'full_df_daily.pkl'))
    return full_df
