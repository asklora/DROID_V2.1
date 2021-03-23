import os
import sys
import platform
import pandas as pd
from pathlib import Path
from datetime import datetime
from pandas.tseries.offsets import BDay, Week
from general.sql_query import read_query, get_active_currency
from general.table_name import get_master_multiple_table_name, get_master_tac_table_name

# if platform.system() == "Linux":
#     path = "/home/loratech/PycharmProjects/data/"
#     if not os.path.exists(path):
#         os.makedirs(path)
# else:
#     path = "C:/dlpa_master/"
#     Path(path).mkdir(parents=True, exist_ok=True)

# os.chdir(path)


def get_price_data(update_date=datetime.strptime("2002-01-01", "%Y-%m-%d"), tac=False):
    if tac:
        table_name = get_master_tac_table_name()
    else:
        table_name = get_master_multiple_table_name()
    start_date = update_date.date()
    query = f"select * from {table_name} where trading_day >= '{start_date}' "
    data = read_query(query, table_name, cpu_counts=True)
    return data

def load_data(data_period, update_lookback, pickle_update, full_update=False, master_multiple=False):
    price_df = None
    currency_df = None

    currency_file = Path("dlpa/data/currency_df.pkl")

    if full_update:
        if currency_file.exists():
            os.remove(currency_file)

    if master_multiple:
        file_name = "full_df_daily.pkl"
        df_file_path = Path(f"dlpa/data/{file_name}")
        if full_update:
            if df_file_path.exists():
                os.remove(df_file_path)

        if df_file_path.exists():
            if pickle_update:
                price_df = pd.read_pickle(df_file_path)
                temp = pd.DataFrame()
                temp["time"] = price_df["trading_day"].astype("datetime64[ns]")
                max_date = temp.time.max()
                if data_period == 0:
                    d = Week(update_lookback)
                else:
                    d = BDay(update_lookback)
                update_date = max_date - d
                updated_df = get_price_data(update_date=update_date)

                if updated_df.shape[1] != 0:
                    price_df["time"] = price_df["trading_day"].astype("datetime64[ns]")
                    price_df.drop(price_df[(price_df["time"] >= update_date)].index, inplace=True)
                    price_df.reset_index(drop=True, inplace=True)
                    del price_df["time"]
                    price_df = price_df.append(updated_df)
                    price_df.reset_index(drop=True, inplace=True)
                    price_df.to_pickle(df_file_path)
                    print(f"Finish writing to {df_file_path}")
                else:
                    print(f"We don't have data for {df_file_path}.")
                    sys.exit()
            else:
                price_df = pd.read_pickle(df_file_path)
                print(f"Finish writing to {file_name}")

        else:
            price_df = get_price_data()
            price_df.to_pickle(df_file_path)
            print(f"Finish writing to {file_name}")

    if currency_file.exists():
        if pickle_update:
            os.remove(currency_file)
            currency_df = get_active_currency()
            currency_df = currency_df["ticker", "currency_code"]
            currency_df.to_pickle(currency_file)
        else:
            currency_df = pd.read_pickle(currency_file)
    else:
        currency_df = get_active_currency()
        currency_df = currency_df["ticker", "currency_code"]
        currency_df.to_pickle(currency_file)

    return price_df, currency_df