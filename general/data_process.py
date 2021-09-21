from general.date_process import dateNow
import pandas as pd
import numpy as np
from pandas.core.series import Series
from datetime import datetime, date
from pandas._libs.tslibs.timestamps import Timestamp

def nonetozero(value):
    if value:
        return value
    return 0

def get_uid(ticker, trading_day=dateNow(), replace=True):
    print(type(trading_day))
    if(type(trading_day) == datetime or type(trading_day) == Timestamp or type(trading_day) == date):
        if(type(trading_day) == datetime or type(trading_day) == Timestamp):
            trading_day = trading_day.date()
        trading_day = trading_day.strftime("%Y-%m-%d")
    uid = f"{trading_day}-{ticker}"
    if(replace):
        uid = uid.replace("-", "").replace(".", "").replace(" ", "")
    return uid

def uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day", date=True, ticker_int=False, replace=True):
    data[trading_day] = data[trading_day].astype(str)
    if(ticker_int):
        data[ticker] = data[ticker].astype(str)
    
    data[uid]=data[trading_day] + data[ticker]
    if(replace):
        data[uid]=data[uid].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        data[uid]=data[uid].str.strip()
    if(date):
        data[trading_day] = pd.to_datetime(data[trading_day])
    return data

def remove_null(data, field):
    data = data.loc[data[field].notnull()]
    data = data.loc[data[field] != "None"]
    data = data.loc[data[field] != "NA"]
    data = data.loc[data[field] != "N/A"]
    return data

def tuple_data(data):
    if(type(data) == Series):
        data = tuple(data.to_list())
    elif(type(data) == np.ndarray):
        data = tuple(data.tolist())
    elif(type(data) == list):
        data = tuple(data)
    elif(type(data) == str):
        data = data.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        data = data.split(",")
        data = tuple(data)
    data = str(data)
    data = data.replace(",)", ")")
    return data


def DivByZero(value1, value2):
    try:
        return value1 / value2
    except ZeroDivisionError:
        return 0

def NoneToZero(value):
    if value:
        return value
    return 0