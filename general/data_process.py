import pandas as pd
from pandas.core.series import Series

def nonetozero(value):
    if value:
        return value
    return 0
    
def uid_maker(data, uid="uid", ticker="ticker", trading_day="trading_day", date=True):
    data[trading_day] = data[trading_day].astype(str)
    data[uid]=data[trading_day] + data[ticker]
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
    elif(type(data) == list):
        data = tuple(data)
    elif(type(data) == str):
        data = data.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        data = data.split(",")
        data = tuple(data)
    data = str(data)
    data = data.replace(",)", ")")
    return data
