from talib import RSI, STOCHF
from general.date_process import droid_start_date
from general.sql_query import get_master_ohlcvtr_data

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

def master_tac_update(self):
    print("Getting OHLCVTR Data")
    data = get_master_ohlcvtr_data(droid_start_date)
    print("OHLCTR Done")
    data = data.rename(columns={"ticker_id" : "ticker", "currency_code_id" : "currency_code"})
    print("Delete Holiday Status")
    data = self.DeleteHolidayStatus(data)
    data = data.drop(columns=["datapoint_per_day", "datapoint", "day_status"])
    print("Fill Null Data Forward & Backward")
    data = self.ForwardBackwardFillNull(data, ["open", "high", "low", "close", "total_return_index"])

    print("Calculate TAC")
    result = data.copy()
    data = data[["uid", "ticker", "trading_day", "volume", "currency_code"]]

    result =  result.rename(columns={"close":"tri_adj_close",
        "low":"tri_adj_low",
        "high":"tri_adj_high",
        "open":"tri_adj_open"})

    result = result.sort_values(by="trading_day", ascending=False)
    result["tac"] =  result.groupby("ticker")["total_return_index"].shift(-1) / result.total_return_index

    result["tri_adj_close"] = result.groupby(result["ticker"]).apply(self.rolling_apply, "tri_adj_close")["tri_adj_close"]
    result["tri_adj_low"] = result.groupby(result["ticker"]).apply(self.rolling_apply, "tri_adj_low")["tri_adj_low"]
    result["tri_adj_high"] = result.groupby(result["ticker"]).apply(self.rolling_apply, "tri_adj_high")["tri_adj_high"]
    result["tri_adj_open"] = result.groupby(result["ticker"]).apply(self.rolling_apply, "tri_adj_open")["tri_adj_open"]

    print("Round Price")
    result["tri_adj_close"] = result["tri_adj_close"].apply(self.roundPrice)
    result["tri_adj_low"] = result["tri_adj_low"].apply(self.roundPrice)
    result["tri_adj_high"] = result["tri_adj_high"].apply(self.roundPrice)
    result["tri_adj_open"] = result["tri_adj_open"].apply(self.roundPrice)
    result = result.sort_values(by="trading_day", ascending=True)
    result = result.drop(columns=["trading_day", "ticker", "tac", "volume", "currency_code"])
    result = result.merge(data, on=["uid"], how="inner")

    print("Calculate RSI")
    result["rsi"] = result.groupby("ticker")["tri_adj_close"].transform(get_rsi)

    print("Calculate STOCHATIC")
    result = result.groupby("ticker").apply(get_stochf)

    print("Calculate TAC Done")
    print(result)