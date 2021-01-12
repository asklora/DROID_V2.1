import os
from dotenv import load_dotenv
load_dotenv()

from general.slack import report_to_slack
from general.date_process import datetimeNow, get_time_by_timezone
from general.table_name import get_currency_table_name
from general.sql_query import get_active_currency, get_active_currency_ric_not_null
from datasource.dss import get_data_from_dss
from general.sql_output import upsert_data_to_database

def update_currency_price_from_dss():
    print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
    currencylist = get_active_currency_ric_not_null()
    currencylist = currencylist.drop(columns=["last_date", "ask_price", "bid_price"])
    currency = currencylist["ric"]
    jsonFileName = "files/file_json/currency_price.json"
    result = get_data_from_dss("start_date", "end_date", currency, jsonFileName, report=os.getenv("REPORT_INTRADAY"))
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
        result["last_price"] = (result["ask_price"] + result["bid_price"]) / 2
        print(result)
        upsert_data_to_database(result, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))

def convert_to_utc(market_close_time, utc_offset, close_ingestion_offset, plus_another_minutes):
    market_time = market_close_time.split(':')
    utc = utc_offset.split(':')
    close_offset = close_ingestion_offset.split(':')
    plus_another = plus_another_minutes.split(':')
    different_hours =int(market_time[0]) + int(utc[0]) + int(close_offset[0])
    if(int(utc[0]) < 0):
        different_minutes = int(market_time[1]) - int(utc[1]) + int(close_offset[1]) + int(plus_another[1])
    elif(int(close_offset[0]) < 0):
        different_minutes = int(market_time[1]) + int(utc[1]) - int(close_offset[1]) + int(plus_another[1])
    elif(int(utc[0]) < 0 & int(close_offset[0]) < 0):
        different_minutes = int(market_time[1]) - int(utc[1]) - int(close_offset[1]) + int(plus_another[1])
    else:
        different_minutes = int(market_time[1]) + int(utc[1]) + int(close_offset[1]) + int(plus_another[1])

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
    return result

def calculate_timezone(data):
    data["utc_offset_minutes"] = ""
    data["utc_offset"] = ""
    for i in range(len(data)):
        utc_timezone_location = data["utc_timezone_location"].loc[i]
        result = get_time_by_timezone(utc_timezone_location)
        data["utc_offset_minutes"].loc[i] = result
        if(result % 4 == 0):
            result = str(int(result / 4 * -1)) + ":00" + ":00"
            data["utc_offset"].loc[i] = result
        else:
            result = str(int(result * -1)) + ":" + str(int((result % 4) * 15)) + ":00"
            data["utc_offset"].loc[i] = result
    print(data)
    return data

def update_utc_offset(args):
    currency = get_active_currency()
    timezone_result = calculate_timezone(currency)
    update_utc_offset_to_database(args, result)
    market_close = get_market_close(args)
    plus_another_minutes = "00:15:00"
    market_close["classic_schedule"] = ""
    for i in range(len(market_close)):
        market_close_time = market_close['market_close_time'].loc[i]
        utc_offset = market_close['utc_offset'].loc[i]
        close_ingestion_offset = market_close['close_ingestion_offset'].loc[i]
        market_close["classic_schedule"].loc[i] = convert_to_utc(market_close_time, utc_offset, close_ingestion_offset, plus_another_minutes)
    print(market_close)
    update_classic_schedule_to_database(args, market_close)
    
    report_to_slack("{} : === UTC Offset Updated ===".format(datetimeNow()))