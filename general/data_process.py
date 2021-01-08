import pandas as pd

def uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day"):
    data[trading_day] = data[trading_day].astype(str)
    data[uid]=data[trading_day] + data[ticker]
    data[uid]=data[uid].str.replace("-", "").str.replace(".", "")
    data[uid]=data[uid].str.strip()
    data[trading_day] = pd.to_datetime(data[trading_day])
    return data