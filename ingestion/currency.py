from general.slack import report_to_slack
from general.date_process import datetimeNow, get_time_by_timezone, string_to_time
from general.table_name import get_currency_table_name
from general.sql_query import get_active_currency, get_active_currency_ric_not_null
from datasource.dss import get_data_from_dss
from general.sql_output import upsert_data_to_database
from global_vars import REPORT_HISTORY, REPORT_INTRADAY

def update_currency_price_from_dss():
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_currency_ric_not_null()
    currencylist = currencylist.drop(columns=["last_date", "last_price"])
    currency = currencylist["ric"]
    jsonFileName = "files/file_json/currency_price.json"
    result = get_data_from_dss("start_date", "end_date", currency, jsonFileName, report=REPORT_INTRADAY)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if(len(result) > 0):
        result = result.rename(columns={
            "RIC": "ric",
            "Universal Bid Ask Date": "last_date",
            "Universal Ask Price": "ask_price",
            "Universal Bid Price": "bid_price",
        })
        result = result.merge(currencylist, how="left", on="ric")
        result["last_price"] = (result["ask_price"] + result["ask_price"]) / 2
        result = result.drop(columns=["ask_price", "bid_price"])
        print(result)
        upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))

# def calculate_minutes_hours_to_time(market_close_time, utc_offset, close_ingestion_offset, classic_offset):
#     market_time = market_close_time.split(":")
#     utc = utc_offset.split(":")
#     close_offset = close_ingestion_offset.split(":")
#     classic = classic_offset.split(":")

#     #Calculate Hours
#     different_hours =int(market_time[0]) + int(utc[0]) + int(close_offset[0])

#     #Calculate Minutes
#     if(int(utc[0]) < 0):
#         different_minutes = int(market_time[1]) - int(utc[1]) + int(close_offset[1]) + int(classic[1])
#     elif(int(close_offset[0]) < 0):
#         different_minutes = int(market_time[1]) + int(utc[1]) - int(close_offset[1]) + int(classic[1])
#     elif(int(utc[0]) < 0 & int(close_offset[0]) < 0):
#         different_minutes = int(market_time[1]) - int(utc[1]) - int(close_offset[1]) + int(classic[1])
#     else:
#         different_minutes = int(market_time[1]) + int(utc[1]) + int(close_offset[1]) + int(classic[1])

#     if(int(classic[1]) < 0):
#         different_minutes -= int(classic[1])
#     else:
#         different_minutes += int(classic[1])

def calculate_minutes_hours_to_time(time1, time2):
    time1 = time1.split(":")
    time2 = time2.split(":")

    different_hours = int(time1[0]) + int(time2[0])

    if(int(time1[0]) < 0 and int(time2[0]) < 0):
        different_minutes = int(time1[1]) + int(time2[1])
    elif(int(time1[0]) < 0):
        different_minutes = int(time1[1]) - int(time2[1])
    elif(int(time2[0]) < 0):
        different_minutes = int(time2[1]) - int(time1[1])
    else:
        different_minutes = int(time1[1]) + int(time2[1])

    return convert_diff_to_time(different_hours, different_minutes)

def convert_diff_to_time(different_hours, different_minutes):
    #Convert Minutes to Hours
    if(different_minutes >= 60):
        different_hours = different_hours + (int(different_minutes / 60))
        different_minutes = different_minutes % 60
    if(different_minutes < 0):
        different_hours = different_hours - 1
        different_minutes = 60 + different_minutes
    if(different_hours < 0):
        different_hours = 24 + different_hours
    if(different_hours >= 24):
        different_hours = different_hours - 24
    result = str(different_hours) + ":" + str(different_minutes) + ":00"
    return string_to_time(result)

def calculate_timezone(data):
    data["utc_offset"] = ""
    for i in range(len(data)):
        utc_timezone_location = data["utc_timezone_location"].loc[i]
        result = get_time_by_timezone(utc_timezone_location)
        if(result % 4 == 0):
            result = str(int(result / 4 * -1)) + ":00" + ":00"
            data["utc_offset"].loc[i] = result
        else:
            result = str(int(result * -1)) + ":" + str(int((result % 4) * 15)) + ":00"
            data["utc_offset"].loc[i] = result
    return data

def update_utc_offset_from_timezone():
    currency = get_active_currency()
    print(currency.columns)
    result = calculate_timezone(currency)
    print(result)
    result["classic_schedule"] = ""
    for index, row in result.iterrows():
        backtest_schedule = calculate_minutes_hours_to_time(str(row["market_close_time"]), str(row["utc_offset"]))
        backtest_schedule = calculate_minutes_hours_to_time(str(backtest_schedule), str(row["close_ingestion_offset"]))
        backtest_schedule = calculate_minutes_hours_to_time(str(backtest_schedule), "00:15:00")
        result.loc[index, "backtest_schedule"] = backtest_schedule

        hedge_schedule = calculate_minutes_hours_to_time(str(row["market_close_time"]), str(row["utc_offset"]))
        hedge_schedule = calculate_minutes_hours_to_time(str(hedge_schedule), str(row["close_ingestion_offset"]))
        hedge_schedule = calculate_minutes_hours_to_time(str(hedge_schedule), "00:15:00")
        result.loc[index, "hedge_schedule"] = hedge_schedule

        top_stock_schedule = calculate_minutes_hours_to_time(str(row["market_close_time"]), str(row["utc_offset"]))
        top_stock_schedule = calculate_minutes_hours_to_time(str(top_stock_schedule), str(row["close_ingestion_offset"]))
        top_stock_schedule = calculate_minutes_hours_to_time(str(top_stock_schedule), "00:15:00")
        result.loc[index, "top_stock_schedule"] = top_stock_schedule

        # result.loc[index, "backtest_schedule"] = calculate_minutes_hours_to_time(str(row["market_close_time"]), str(row["utc_offset"]), 
        #     str(row["close_ingestion_offset"]), "00:15:00")
        # result.loc[index, "backtest_schedule"] = calculate_minutes_hours_to_time(str(row["market_close_time"]), str(row["utc_offset"]), 
        #     str(row["close_ingestion_offset"]), "00:-15:00")
    print(result)
    upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
    report_to_slack("{} : === UTC Offset & Classic Schedule Updated ===".format(datetimeNow()))