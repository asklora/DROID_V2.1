import numpy as np
import pandas as pd
from general.data_process import uid_maker
from general.sql_process import do_function
from general.date_process import dateNow
from general.sql_query import get_master_ohlcvtr_data, get_master_ohlcvtr_start_date
from general.sql_output import insert_data_to_database, upsert_data_to_database
from ingestion.master_tac import master_tac_update, ForwardBackwardFillNull

def datapoint_lte_1000(fulldatapoint):
    exclude = list(module.datasource.models.ReportDatapoint.objects.filter(reingested=True).values_list("ticker",flat=True))
    if exclude:
        low_datapoint = fulldatapoint.loc[fulldatapoint["ticker"].isin(exclude)]
    else:
        low_datapoint = fulldatapoint
    if low_datapoint:
        for data in low_datapoint:
            try:
                datapoint_table = module.datasource.models.ReportDatapoint.objects.get(ticker=data.ticker)
                datapoint_table.datapoint = data.fulldatapoint
                datapoint_table.updated = datetime.now().date()
                datapoint_table.save()
            except module.datasource.models.ReportDatapoint.DoesNotExist:
                datapoint_table = module.datasource.models.ReportDatapoint.objects.create(
                    ticker=data.ticker,
                    datapoint=data.fulldatapoint,
                    updated = datetime.now().date()
                    )

def FilterWeekend( data):
    data = data[data["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    return data

def FillMissingDay(data, start, end):
    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start, end, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = uid_maker(result, "uid", "ticker", "trading_day")
    result = FilterWeekend(result)
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["uid", "ticker", "trading_day"])
    result["currency_code"] = result["currency_code"].ffill().bfill()
    return result

def CountDatapoint( data):
    filter_data = data.copy()
    filter_data[["open", "high", "low", "close", "volume"]] = filter_data[["open", "high", "low", "close", "volume"]].notnull().astype("int")
    filter_data["datapoint"] = (filter_data["open"]+filter_data["high"]+filter_data["low"]+filter_data["close"]+filter_data["volume"])

    fulldatapoint_result = filter_data[["ticker", "datapoint"]]
    fulldatapoint_result =  fulldatapoint_result.rename(columns={"datapoint":"fulldatapoint"})
    fulldatapoint_result = fulldatapoint_result.groupby("ticker").sum().reset_index()

    #update datapoint
    datapoint_lte_1000(fulldatapoint_result)
    
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
    return result

def FillDayStatus(self, data):
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

def master_ohlctr_update():
    print("Get Start Date")
    start_date = get_master_ohlcvtr_start_date()
    print(f"Calculation Start From {start_date}")
    do_function("master_ohlcvtr_update")
    print("Getting OHLCVTR Data")
    master_ohlcvtr_data = get_master_ohlcvtr_data(start_date)
    print("OHLCTR Done")
    print("Filling All Missing Days")
    master_ohlcvtr_data = FillMissingDay(master_ohlcvtr_data, start_date, dateNow())
    print("Calculate Datapoint")
    master_ohlcvtr_data = CountDatapoint(master_ohlcvtr_data)
    print("Fill Day Status")
    master_ohlcvtr_data = FillDayStatus(master_ohlcvtr_data)
    print("Fill Null Data Forward & Backward")
    master_ohlcvtr_data = ForwardBackwardFillNull(master_ohlcvtr_data, ["close", "total_return_index"])
    master_ohlcvtr_data = master_ohlcvtr_data.drop(columns=["point", "fulldatapoint"])
    if(len(master_ohlcvtr_data) > 0):
        upsert_data_to_database(master_ohlcvtr_data, "master_ohlcvtr", "uid", how="update", Text=True)
        del master_ohlcvtr_data
        master_tac_update()

