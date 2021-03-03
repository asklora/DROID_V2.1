import numpy as np
import pandas as pd
from sqlalchemy.util.langhelpers import NoneType
from general.sql_query import get_master_ohlcvtr_data
from general.date_process import (
    datetimeNow, 
    dlp_start_date,
    str_to_date,
    dlp_start_date_buffer)
from general.sql_output import delete_data_on_database, upsert_data_to_database
from ingestion.master_tac import ForwardBackwardFillNull
from general.slack import report_to_slack
from general.table_name import get_master_multiple_table_name

def dataframe_to_pivot( data, universe, index, column, values, indexes=None):
    result = data.pivot_table(index=index, columns=column, values=values, aggfunc="first", dropna=False)
    result = result.reindex(columns=universe[column].tolist())
    if type(indexes) != NoneType:
        result = result.reindex(index=indexes)
    #result = result[result.index.dayofweek < 5]
    return result

def pivot_to_dataframe( data, index, column, values, indexes=None, columns=None):
    if type(indexes) != NoneType and type(columns) != NoneType:
        result = pd.DataFrame(data, index=indexes, columns=columns)
        result[index] = result.index
        result = result.melt(id_vars=index, var_name=column, value_name=values)
    else:
        data[index] = data.index
        result = data.melt(id_vars=index, var_name=column, value_name=values)
    return result

def master_multiple_update():
    start_date = dlp_start_date()
    start_date_buffer = str_to_date(dlp_start_date_buffer())

    print("Getting OHLCVTR Data")
    data = get_master_ohlcvtr_data(start_date)

    print("Fill Null Data Forward & Backward")
    data = ForwardBackwardFillNull(data, ["total_return_index"], columns_deletion=True)
    
    print("OHLCTR Done")
    data = data.rename(columns={"total_return_index" : "tri"})
    data = data.drop(columns=["datapoint_per_day", "datapoint", "day_status", "currency_code"])

    print(datetimeNow() + " === Calculating Master Multiple ===")
    universe = data[["ticker"]]
    universe = universe.drop_duplicates(subset=["ticker"], keep="first", inplace=False)

    print("Price Dataframe to Pivot")
    close_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "close")
    open_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "open", indexes=close_price.index)
    high_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "high", indexes=close_price.index)
    low_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "low", indexes=close_price.index)
    volume_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "volume", indexes=close_price.index)
    tri_price = dataframe_to_pivot(data, universe, "trading_day", "ticker", "tri", indexes=close_price.index)
    tri_price_shifted = tri_price.shift(periods=1)

    print("Calculate Price Multiples")
    close_multiple = np.where(np.isnan(tri_price.values) | np.isnan(tri_price_shifted.values), np.nan, tri_price.values / tri_price_shifted.values)
    open_multiple = np.where(np.isnan(close_price.values) | np.isnan(open_price.values) | np.isnan(close_multiple), np.nan, close_multiple * open_price.values / close_price.values)
    high_multiple = np.where(np.isnan(close_price.values) | np.isnan(high_price.values) | np.isnan(close_multiple), np.nan, close_multiple * high_price.values / close_price.values)
    low_multiple = np.where(np.isnan(close_price.values) | np.isnan(low_price.values) | np.isnan(close_multiple), np.nan, close_multiple * low_price.values / close_price.values)
    
    print("Calculate Adjusted Volume")
    turn_over = volume_price * close_price
    volume_mean = turn_over.rolling(250, min_periods=1).mean()
    volume_std = turn_over.rolling(250, min_periods=1).std()
    volume_final = (turn_over - volume_mean) / volume_std
    volume_final = volume_final / 3
    volume_final = volume_final.clip(-1, 1)
    volume_final = volume_final * 5
    volume_final = volume_final.round()
    volume_final = volume_final / 5
    
    print("Pivot Price to Dataframe")
    close_multiple = pivot_to_dataframe(close_multiple, "trading_day", "ticker", "close_multiple", indexes=close_price.index, columns=close_price.columns)
    open_multiple = pivot_to_dataframe(open_multiple, "trading_day", "ticker", "open_multiple", indexes=open_price.index, columns=open_price.columns)
    high_multiple = pivot_to_dataframe(high_multiple, "trading_day", "ticker", "high_multiple", indexes=high_price.index, columns=high_price.columns)
    low_multiple = pivot_to_dataframe(low_multiple, "trading_day", "ticker", "low_multiple", indexes=high_price.index, columns=high_price.columns)
    turn_over = pivot_to_dataframe(turn_over, "trading_day", "ticker", "turn_over")
    volume_adj = pivot_to_dataframe(volume_final, "trading_day", "ticker", "volume_adj")
    
    print("Merge All Data")
    result = data.merge(close_multiple, on=["trading_day", "ticker"], how="left")
    result = result.merge(open_multiple, on=["trading_day", "ticker"], how="left")
    result = result.merge(high_multiple, on=["trading_day", "ticker"], how="left")
    result = result.merge(low_multiple, on=["trading_day", "ticker"], how="left")
    result = result.merge(turn_over, on=["trading_day", "ticker"], how="left")
    result = result.merge(volume_adj, on=["trading_day", "ticker"], how="left")
    result = result[result["trading_day"] >= start_date_buffer]
    result = result.drop(columns=["open", "high", "low", "close", "volume"])
    
    print(datetimeNow() + " === Master Multiple Calculate Done ===")
    # insert_data_to_database(result, "master_multiple", how="replace")
    upsert_data_to_database(result, get_master_multiple_table_name(), "uid", how="update", Text=True)
    delete_data_on_database(get_master_multiple_table_name(), f"trading_day < '{dlp_start_date_buffer()}'", delete_ticker=True)
    report_to_slack("{} : === Master TAC Update Updated ===".format(datetimeNow()))
    del result