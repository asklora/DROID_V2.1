import numpy as np
import pandas as pd
from general.slack import report_to_slack
from general.data_process import uid_maker
from general.sql_process import do_function
from general.date_process import backdate_by_day, dateNow, dlp_start_date, datetimeNow
from general.sql_query import get_master_ohlcvtr_data
from general.sql_output import delete_data_on_database, upsert_data_to_database
from general.table_name import get_master_ohlcvtr_table_name
from ingestion.data_from_dsws import update_currency_code_from_dsws, update_data_dsws_from_dsws
from ingestion.data_from_dss import update_data_dss_from_dss

#New Ticker Categories is When Datapoint Less Than 1000 Datapoint
def FindNewTicker(fulldatapoint):
    new_ticker = fulldatapoint.copy()
    new_ticker = new_ticker.loc[new_ticker["fulldatapoint"] < 20]
    new_ticker = new_ticker["ticker"].to_list()
    print(new_ticker)
    if(len(new_ticker) > 0):
        report_to_slack("{} : === New Ticker Found {} Start Historical Ingestion ===".format(datetimeNow(), new_ticker))
        update_currency_code_from_dsws(ticker=new_ticker)
        update_data_dss_from_dss(ticker=new_ticker, history=True)
        update_data_dsws_from_dsws(ticker=new_ticker, history=True)
    print(new_ticker)
    print(len(new_ticker))
    return new_ticker

def FilterWeekend( data):
    data = data[data["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    return data

def FillMissingDay(data, start, end):
    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start, end, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = uid_maker(result)
    result = FilterWeekend(result)
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["uid", "ticker", "trading_day"])
    result["currency_code"] = result["currency_code"].ffill().bfill()
    return result

def CountDatapoint(data):
    filter_data = data.copy()
    filter_data[["open", "high", "low", "close", "volume"]] = filter_data[["open", "high", "low", "close", "volume"]].notnull().astype("int")
    filter_data["datapoint"] = (filter_data["open"]+filter_data["high"]+filter_data["low"]+filter_data["close"]+filter_data["volume"])

    fulldatapoint_result = filter_data[["ticker", "datapoint"]]
    fulldatapoint_result =  fulldatapoint_result.rename(columns={"datapoint":"fulldatapoint"})
    fulldatapoint_result = fulldatapoint_result.groupby("ticker").sum().reset_index()
    print(fulldatapoint_result)
    #update datapoint
    new_ticker = FindNewTicker(fulldatapoint_result)
    datapoint_per_day_result = filter_data[["trading_day", "currency_code", "datapoint"]]
    datapoint_per_day_result =  datapoint_per_day_result.rename(columns={"datapoint":"datapoint_per_day"})
    datapoint_per_day_result = datapoint_per_day_result.groupby(["currency_code", "trading_day"]).sum().reset_index()

    datapoint_result = filter_data[["ticker", "currency_code"]]
    datapoint_result = datapoint_result.drop_duplicates(subset=["ticker"], keep="first")
    datapoint_result =  datapoint_result.rename(columns={"ticker":"datapoint"})
    datapoint_result[["datapoint"]] = datapoint_result[["datapoint"]].notnull().astype("int")
    datapoint_result = datapoint_result.groupby("currency_code").sum().reset_index()
    datapoint_result["datapoint"] = datapoint_result["datapoint"] * 5

    point_result = filter_data[["uid", "datapoint"]]
    point_result =  point_result.rename(columns={"datapoint":"point"})

    data = data.drop(columns=["datapoint", "datapoint_per_day"])
    result = data.merge(fulldatapoint_result, how="left", on=["ticker"])
    result = result.merge(point_result, how="left", on=["uid"])
    result = result.merge(datapoint_result, how="left", on=["currency_code"])
    result = result.merge(datapoint_per_day_result, how="left", on=["trading_day", "currency_code"])
    return result, new_ticker

def FillDayStatus(data):
    conditions = [
        # label with holiday if datapoint less than 25% and the date is more than start date of ticker
        (data["datapoint_per_day"] <= data["datapoint"] / 4) & (data["point"] < 2),
        # label with missing if datapoint more  than 50% and the value is null
        (data["datapoint_per_day"] > data["datapoint"]/2) & (data["point"] < 2),
        # label with trading if value is notnull
        (data["point"] >= 2)
    ]
    choices = ["holiday", "missing", "trading_day"]
    # standarize holiday/filled day with NaN
    data["day_status"] = np.select(conditions, choices, default="missing")
    return data

def master_ohlctr_update(history=False):
    # do_function("master_ohlcvtr_update")
    print("Get Start Date")
    start_date = dlp_start_date()
    print(f"Calculation Start From {start_date}")
    print("Getting OHLCVTR Data")
    master_ohlcvtr_data = get_master_ohlcvtr_data(start_date)
    print("OHLCTR Done")
    print("Filling All Missing Days")
    master_ohlcvtr_data = FillMissingDay(master_ohlcvtr_data, start_date, dateNow())
    print("Calculate Datapoint")
    master_ohlcvtr_data, new_ticker = CountDatapoint(master_ohlcvtr_data)
    if(len(new_ticker) > 0):
        upsert_date = dlp_start_date()
        print("Restart Master OHLCVTR Update")
        do_function("master_ohlcvtr_update")
        start_date = dlp_start_date()
        master_ohlcvtr_data = get_master_ohlcvtr_data(start_date)
        master_ohlcvtr_data = FillMissingDay(master_ohlcvtr_data, start_date, dateNow())
        master_ohlcvtr_data, new_tickers = CountDatapoint(master_ohlcvtr_data)
    elif(history):
        upsert_date = dlp_start_date()
    else:
        upsert_date = backdate_by_day(15)
    print("Fill Day Status")
    master_ohlcvtr_data = FillDayStatus(master_ohlcvtr_data)
    # print("Fill Null Data Forward & Backward")
    # master_ohlcvtr_data = ForwardBackwardFillNull(master_ohlcvtr_data, ["close", "total_return_index"])
    master_ohlcvtr_data = master_ohlcvtr_data.drop(columns=["point", "fulldatapoint"])
    print(master_ohlcvtr_data)
    if(len(master_ohlcvtr_data) > 0):
        master_ohlcvtr_data = master_ohlcvtr_data.loc[master_ohlcvtr_data["trading_day"] >= upsert_date] 
        upsert_data_to_database(master_ohlcvtr_data, get_master_ohlcvtr_table_name(), "uid", how="update", Text=True)
        delete_data_on_database(get_master_ohlcvtr_table_name(), f"trading_day < '{dlp_start_date()}'", delete_ticker=True)
        report_to_slack("{} : === Master OHLCVTR Update Updated ===".format(datetimeNow()))
        del master_ohlcvtr_data
        #master_tac_update()

