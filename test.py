from datetime import datetime
from ingestion.mongo_migration import (
    mongo_bot_data_update, 
    mongo_create_currency, 
    mongo_universe_rating_update, 
    mongo_universe_update, 
    mongo_currency_update, 
    mongo_latest_price_update, 
    mongo_price_update)
from general.sql_query import read_query
from bot.data_download import get_latest_price
import json
import pandas as pd
from pymongo import MongoClient
#comment
def change_date_to_str(data):
    for col in data.columns:
        print(col + " " + str(data.loc[0, col]) + " " + str(type(data.loc[0, col])))
        if (str(type(data.loc[0, col])) == "<class 'datetime.date'>" or 
            str(type(data.loc[0, col])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or 
            str(type(data.loc[0, col])) == "<class 'datetime.time'>") :
            data[col] = data[col].astype(str)
    return data

def connects():
    mongo_url = "mongodb+srv://postgres:postgres@cluster0.b0com.mongodb.net/test?retryWrites=true&w=majority"
    client =  MongoClient(mongo_url)
    db_connect = client["latest_data"]["latest_price"]
    return db_connect

def insert_to_mongo():
    db_connect = connects()
    ticker = ["MSFT.O"]
    data = get_latest_price(ticker=ticker)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    print("data")
    print(data)
    print("data_dict")
    print(data_dict)
    db_connect.insert_one(data_dict[0])

def update_to_mongo():
    db_connect = connects()
    ticker = ["MSFT.O"]
    data = get_latest_price(ticker=ticker)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    print("data")
    print(data)
    print("data_dict")
    print(data_dict)

    for new_data in data_dict:
        db_connect.delete_one({"ticker": new_data["ticker"]})
        db_connect.insert_one(new_data)

def delete_to_mongo():
    db_connect = connects()
    ticker = ["MSFT.O"]
    data = get_latest_price(ticker=ticker)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    print("data")
    print(data)
    print("data_dict")
    print(data_dict)

    for new_data in data_dict:
        db_connect.delete_one({"ticker": new_data["ticker"]})

if __name__ == "__main__":
    print("Start Process")
    mongo_universe_update(currency_code=["HKD"])
    # insert_to_mongo()
    # mongo_universe_update()
    # mongo_create_currency()
    # mongo_universe_rating_update()
    # mongo_bot_data_update()
    # mongo_statistic_backtest_update()
    # mongo_latest_price_update()
    # mongo_price_update()
