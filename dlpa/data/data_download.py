from general.table_name import get_master_multiple_table_name, get_master_tac_table_name
import os
import platform
import sys
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week

import global_vars
from general.sql_query import read_query
from general.data_process import tuple_data

if platform.system() == "Linux":
    path = "/home/loratech/PycharmProjects/data/"
    if not os.path.exists(path):
        os.makedirs(path)
else:
    path = "C:/dlpa_master/"
    Path(path).mkdir(parents=True, exist_ok=True)

os.chdir(path)


def get_price_data(update_date=datetime.strptime("2002-01-01", "%Y-%m-%d"), tac=False):
    if tac:
        table_name = get_master_tac_table_name()
    else:
        table_name = get_master_multiple_table_name()
    start_date = update_date.date()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    data = read_query(query, table_name, cpu_counts=True)
    return data

    

def indices_download(args, update_date=None):
    db_url = global_vars.DB_PROD_URL_READ
    engine = db.create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = global_vars.latest_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker, table0.columns.index_id]).where(table0.columns.is_active.is_(True))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

    indices_df = pd.DataFrame(ResultSet)
    indices_df.columns = columns_list
    # if indices_df.shape[1] == 2:
    #     indices_df.columns = ["ticker", "index"]
    indices_df.rename(columns={"index_id": "index"}, inplace=True)

    return indices_df


def load_data(data_period, update_lookback, update, full_update=False, tac=False):
    update_lookback = update_lookback
    update = bool(update)

    indices_file = Path("dlpa/data/indices_df.pkl")

    if full_update:
        if indices_file.exists():
            os.remove(indices_file)

    if master_multiple_df:
        file_name = "full_df_daily.pkl"
        df_file_path = Path(f"dlpa/data/{file_name}")
        if full_update:
            if df_file_path.exists():
                os.remove(df_file_path)

        if df_file_path.exists():
            if update:
                full_df = update_df(df_file_path, data_period, update_lookback, tac=False)
            else:
                full_df = pd.read_pickle(df_file_path)
                print(f"Finish writing to {file_name}")

        else:
            full_df = get_price_data(tac=tac)
            full_df.to_pickle(df_file_path)
            print(f"Finish writing to {file_name}")

        full_df_daily = full_df

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

    return full_df_daily, indices_df

    def update_df(df_file, data_period, update_lookback, tac=False):
        full_df = pd.read_pickle(df_file)
        temp = pd.DataFrame()
        temp["time"] = full_df["trading_day"].astype("datetime64[ns]")
        max_date = temp.time.max()
        if data_period == 0:
            d = Week(update_lookback)
        else:
            d = BDay(update_lookback)
        update_date = max_date - d
        updated_df = get_price_data(update_date=update_date, tac=tac)

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

    def load_df(file_name, full_update=False, tac=False):
        

        return full_df

    
