import os
import platform
import sys
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week

from dlpa import global_vars

if platform.system() == "Linux":
    path = "data/"
    if not os.path.exists(path):
        os.makedirs(path)
else:
    path = "C:/dlpa_master/"
    Path(path).mkdir(parents=True, exist_ok=True)

os.chdir(path)


def data_download(args, update_date=None, main_flag=None):
    # db_url = global_vars.DB_PROD_URL
    # if args.master_daily_df:
    #     table0 = global_vars.main_input_data_table_name
    #     db_url = global_vars.DB_PROD_URL
    #
    # # table0 = "daily_multiple_check"
    if main_flag == "master_daily":
        db_url = global_vars.db_read
        table0 = global_vars.main_input_data_table_name

    elif main_flag == "master_daily_rv":
        db_url = global_vars.db_read
        table0 = "master_daily_rv"

    elif main_flag == "master_daily_tac":
        db_url = global_vars.db_read
        table0 = global_vars.master_ohlcv_table_name

    # if main_flag == "master_daily_beta_adj":
    else:
        db_url = global_vars.db_read
        table0 = "master_daily_beta_adjusted_2"


    engine = db.create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    origin = "2002-01-01"
    origin = datetime.strptime(origin, "%Y-%m-%d")

    if update_date is not None:
        datee1 = update_date.date()
    else:
        datee1 = origin.date()

    with engine.connect() as conn:
        metadata = db.MetaData()
        table01 = db.Table(table0, metadata, autoload=True, autoload_with=conn)
        query = db.select(["*"]).where(table01.columns.trading_day >= datee1.strftime("%Y-%m-%d"))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list
    # if full_df.shape[1] == 7:
    #     full_df.columns = ["ticker", "date", "open_multiple", "high_multiple", "low_multiple",
    #                        "close_multiple", "volume"]
    print(f"Finish downloading from table {table0}")
    return full_df


def indices_download(args, update_date=None):
    db_url = global_vars.db_read
    engine = db.create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = global_vars.latest_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker, table0.columns.currency_code]).where(table0.columns.is_active.is_(True))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

    indices_df = pd.DataFrame(ResultSet)
    indices_df.columns = columns_list
    # if indices_df.shape[1] == 2:
    #     indices_df.columns = ["ticker", "index"]
    #indices_df.rename(columns={"index_id": "index"}, inplace=True)

    return indices_df


def load_data(args):
    update_lookback = args.update_lookback
    update = bool(args.update)

    indices_file = Path("data/indices_df.pkl")

    if args.db_full_update:
        if indices_file.exists():
            os.remove(indices_file)

    def update_df(args, flag, df_file):
        full_df = pd.read_pickle(df_file)
        temp = pd.DataFrame()
        temp["time"] = full_df["trading_day"].astype("datetime64[ns]")
        max_date = temp.time.max()
        if args.data_period == 0:
            d = Week(args.update_lookback)
        else:
            d = BDay(args.update_lookback)
        update_date = max_date - d
        updated_df = data_download(args, main_flag=flag, update_date=update_date)

        if updated_df.shape[1] != 0:
            full_df["time"] = full_df["trading_day"].astype("datetime64[ns]")
            full_df.drop(full_df[(full_df["time"] >= update_date)].index, inplace=True)
            full_df.reset_index(drop=True, inplace=True)
            del full_df["time"]
            full_df = full_df.append(updated_df)
            full_df.reset_index(drop=True, inplace=True)
            full_df.to_pickle(df_file)
            print(f"Finish writing to {df_file}")
        else:
            print(f"We don't have data for {df_file}.")
            sys.exit()

        return full_df

    def load_df(args, flag, file_name):
        df_file_path = Path("data/" + file_name)
        if args.db_full_update:
            if df_file_path.exists():
                os.remove(df_file_path)

        if df_file_path.exists():
            if update:
                full_df = update_df(args, flag, df_file_path)
            else:
                full_df = pd.read_pickle(df_file_path)
                print(f"Finish writing to {file_name}")

        else:
            full_df = data_download(args, main_flag=flag)
            full_df.to_pickle(df_file_path)
            print(f"Finish writing to {file_name}")

        return full_df

    if args.master_daily_df:
        full_df_daily = load_df(args, global_vars.main_input_data_table_name, "full_df_daily.pkl")

    if args.master_daily_rv_df:
        full_df_rv_daily = load_df(args, "master_daily_rv", "full_df_rv_daily.pkl")

    if args.master_daily_beta_adj_df:
        full_df_beta_adj_daily = load_df(args, "master_daily_beta_adj", "full_df_beta_adj_daily.pkl")

    if indices_file.exists():
        if update:
            os.remove(indices_file)
            indices_df = indices_download(args)
            indices_df.to_pickle(indices_file)
        else:
            indices_df = pd.read_pickle(indices_file)
    else:
        indices_df = indices_download(args)
        indices_df.to_pickle(indices_file)

    if args.rv_1 or args.rv_2:
        return full_df_daily, indices_df
    elif args.beta_1 or args.beta_2:
        return full_df_daily, full_df_beta_adj_daily, indices_df
    else:
        return full_df_daily, indices_df
