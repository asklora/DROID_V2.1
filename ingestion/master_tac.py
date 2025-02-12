import pandas as pd
from talib import RSI, STOCHF
from general.data_process import uid_maker
from general.date_process import datetimeNow, droid_start_date
from general.slack import report_to_slack
from general.table_name import get_master_tac_table_name
from general.sql_query import get_master_ohlcvtr_data
from general.sql_output import delete_data_on_database, upsert_data_to_database
from es_logging.logger import log2es

def ForwardBackwardFillNull(data, columns_field, columns_deletion=False):
    data = data.sort_values(by="trading_day", ascending=False)
    data = data.infer_objects()
    result = data[["uid"]]
    if(columns_deletion):
        data_detail = data.drop(columns=columns_field)
    else:
        data_detail = data[["uid", "ticker", "trading_day", "volume", "currency_code", "day_status"]]
    universe = data["ticker"].drop_duplicates()
    universe =universe.tolist()
    for column in columns_field:
        price = data.pivot_table(index="trading_day", columns="ticker", values=column, aggfunc="first", dropna=False)
        price = price.reindex(columns=universe)
        price = price.ffill().bfill()
        price = pd.DataFrame(price.values, index=price.index, columns=price.columns)
        price["trading_day"] = price.index
        price = price.melt(id_vars="trading_day", var_name="ticker", value_name=column)
        price = uid_maker(price)
        price = price.drop(columns=["trading_day", "ticker"])
        result = result.merge(price, on=["uid"], how="left")
    result = result.merge(data_detail, on=["uid"], how="left")
    return result

def DeleteHolidayStatus(data):
    data = data.loc[data["day_status"] != "holiday"]
    return data

def rolling_apply(group, field):
    adjusted_price = [group[field].iloc[0]]
    for x in group.tac[:-1]:
        adjusted_price.append(adjusted_price[-1] *  x )
    group[field] = adjusted_price
    return group

def roundPrice(price):
    if price > 10000:
        return round(price, 0)
    else:
        return round(price, 2)

def get_rsi(val):
    return RSI(val.values, timeperiod=13)

def get_stochf(df):
    fastk, fastd = STOCHF(df["tri_adj_high"].values,
                    df["tri_adj_low"].values,
                    df["tri_adj_close"].values,
                    fastk_period=12, fastd_period=3, fastd_matype=0)
    df["fast_k"] =fastk
    df["fast_d"]=fastd
    return df

@log2es("db")
def master_tac_update():
    print("Getting OHLCVTR Data")
    data = get_master_ohlcvtr_data(droid_start_date(), local=True)
    print(data)
    print("OHLCTR Done")
    #data = data.rename(columns={"ticker_id" : "ticker", "currency_code_id" : "currency_code"})
    print("Delete Holiday Status")
    data = DeleteHolidayStatus(data)
    print(data)
    data = data.drop(columns=["datapoint_per_day", "datapoint"])
    print("Fill Null Data Forward & Backward")
    data = ForwardBackwardFillNull(data, ["open", "high", "low", "close", "total_return_index"])
    print(data)
    print("Calculate TAC")
    result = data.copy()
    result = result.drop(columns=["day_status"])
    data = data[["uid", "ticker", "trading_day", "volume", "currency_code", "day_status"]]

    result =  result.rename(columns={"close":"tri_adj_close",
        "low":"tri_adj_low",
        "high":"tri_adj_high",
        "open":"tri_adj_open"})

    result = result.sort_values(by="trading_day", ascending=False)
    result["tac"] =  result.groupby("ticker")["total_return_index"].shift(-1) / result.total_return_index
    print(result)
    result["tri_adj_close"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_close")["tri_adj_close"]
    result["tri_adj_low"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_low")["tri_adj_low"]
    result["tri_adj_high"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_high")["tri_adj_high"]
    result["tri_adj_open"] = result.groupby(result["ticker"]).apply(rolling_apply, "tri_adj_open")["tri_adj_open"]
    print(result)
    print("Round Price")
    result["tri_adj_close"] = result["tri_adj_close"].apply(roundPrice)
    result["tri_adj_low"] = result["tri_adj_low"].apply(roundPrice)
    result["tri_adj_high"] = result["tri_adj_high"].apply(roundPrice)
    result["tri_adj_open"] = result["tri_adj_open"].apply(roundPrice)
    result = result.sort_values(by="trading_day", ascending=True)
    result = result.drop(columns=["trading_day", "ticker", "tac", "volume", "currency_code"])
    result = result.merge(data, on=["uid"], how="inner")
    print(result)
    print("Calculate RSI")
    result["rsi"] = result.groupby("ticker")["tri_adj_close"].transform(get_rsi)
    print(result)
    print("Calculate STOCHATIC")
    result = result.groupby("ticker").apply(get_stochf)
    print(result)
    print("Calculate TAC Done")
    print(result)
    #insert_data_to_database(result, "master_tac", how="replace")
    upsert_data_to_database(result, get_master_tac_table_name(), "uid", how="update", Text=True, local=True)
    delete_data_on_database(get_master_tac_table_name(), f"trading_day < '{droid_start_date()}'", delete_ticker=True, local=True)
    report_to_slack("{} : === Master TAC Update Updated ===".format(datetimeNow()))
    del result