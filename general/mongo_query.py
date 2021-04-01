from pymongo import MongoClient
from global_vars import MONGO_URL

def change_date_to_str(data):
    for col in data.columns:
        print(col + " " + str(data.loc[0, col]) + " " + str(type(data.loc[0, col])))
        if (str(type(data.loc[0, col])) == "<class 'datetime.date'>" or 
            str(type(data.loc[0, col])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or 
            str(type(data.loc[0, col])) == "<class 'datetime.time'>") :
            data[col] = data[col].astype(str)
    return data

def connects(table):
    client =  MongoClient(MONGO_URL)
    db_connect = client["latest_data"][table]
    return db_connect

def insert_to_mongo(data, index, table):
    db_connect = connects(table)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    db_connect.insert_many(data_dict)

def update_to_mongo(data, index, table):
    db_connect = connects(table)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    for new_data in data_dict:
        db_connect.delete_one({index: new_data[index]})
        db_connect.insert_one(new_data)

def delete_to_mongo(data, index, table):
    db_connect = connects(table)
    data = change_date_to_str(data)
    data_dict = data.to_dict("records")
    for new_data in data_dict:
        db_connect.delete_one({index: new_data[index]})