from bot.data_download import get_latest_price
import json
import pandas as pd
from pymongo import MongoClient


mongo_url = "mongodb+srv://postgres:postgres@cluster0.b0com.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client =  MongoClient(mongo_url)

if __name__ == "__main__":
    # Connect to MongoDB
    db = client["latest_data"]
    collection = db["latest_price"]

    ticker = ["MSFT.O", "AAPL.O"]
    data = get_latest_price(ticker=ticker)
    data_dict = data.to_dict("records")
    # Insert collection
    collection.insert_many(data_dict)