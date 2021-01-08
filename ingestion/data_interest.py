import os
import pandas as pd
from general.slack import report_to_slack
from general.sql_query import get_data_by_table_name
from general.sql_output import  insert_data_to_database
from general.date_process import datetimeNow
from general.data_process import uid_maker
from general.table_name import get_data_interest_table_name, get_data_interest_daily_table_name

data_interest_table = get_data_interest_table_name()
data_interest_daily_table = get_data_interest_daily_table_name()

def cal_interest_rate(interest_rate_data, days_to_expiry):
    unique_horizons = pd.DataFrame(days_to_expiry)
    unique_horizons["id"] = 2
    unique_currencies = pd.DataFrame(interest_rate_data["currency_code"].unique())
    unique_currencies["id"] = 2
    rates = pd.merge(unique_horizons, unique_currencies, on="id", how="outer")
    rates = rates.rename(columns={"0_y": "currency_code", "0_x": "days_to_expiry"})
    interest_rate_data = interest_rate_data.rename(columns={"days_to_maturity": "days_to_expiry"})
    rates = pd.merge(rates, interest_rate_data, on=["days_to_expiry", "currency_code"], how="outer")
    def funs(df):
        df = df.sort_values(by="days_to_expiry")
        df = df.reset_index()
        nan_index = df["rate"].index[df["rate"].isnull()].to_series().reset_index(drop=True)
        not_nan_index = df["rate"].index[~df["rate"].isnull()].to_series().reset_index(drop=True)
        for a in nan_index:
            temp = not_nan_index.copy()
            temp[len(temp)] = a
            temp = temp.sort_values()
            temp.reset_index(inplace=True, drop=True)
            ind = temp[temp == a].index
            ind1 = temp.iloc[ind - 1]
            ind2 = temp.iloc[ind + 1]
            rate_1 = df.loc[ind1, "rate"].iloc[0]
            rate_2 = df.loc[ind2, "rate"].iloc[0]
            dtm_1 = df.loc[ind1, "days_to_expiry"].iloc[0]
            dtm_2 = df.loc[ind2, "days_to_expiry"].iloc[0]
            df.loc[a, "rate"] = rate_1 * (dtm_2 - df.loc[a, "days_to_expiry"])/(dtm_2 - dtm_1) + rate_2* (df.loc[a, "days_to_expiry"] - dtm_1)/(dtm_2 - dtm_1)
        df = df.set_index("index")
        return df
    rates = rates.groupby("currency_code").apply(lambda x: funs(x))
    rates = rates.reset_index(drop=True)
    unique_horizons = unique_horizons.rename(columns={0: "days_to_expiry"})
    unique_horizons = pd.merge(unique_horizons, rates[["days_to_expiry", "rate"]], on="days_to_expiry", how="inner")
    return unique_horizons["rate"].values

def daily_interest_update(args):
    interest_rate_data = get_data_by_table_name(data_interest_table)
    interest_rate_data = interest_rate_data.rename(columns={"currency": "currency_code"})
    interest_rate_data = interest_rate_data[interest_rate_data["currency_code"] == args.r_currency]

    days_r = pd.DataFrame(range(1, os.getenv("DAILY_R_DAYS")))
    result = pd.DataFrame()

    for currency in interest_rate_data.currency_code.unique():
        temp_r = pd.DataFrame()
        temp_r["r"] = cal_interest_rate(interest_rate_data[interest_rate_data.currency_code == currency], days_r)
        temp_r["t"] = days_r
        temp_r["currency_code"] = currency
        result = result.append(temp_r)
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="t")
    insert_data_to_database(result, data_interest_daily_table, how="replace")
    report_to_slack("{} : === Daily Interest Updated ===".format(datetimeNow()))

