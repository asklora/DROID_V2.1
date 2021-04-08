from pymongo import MongoClient
from global_vars import MONGO_URL

def change_date_to_str(data):
    for col in data.columns:
        if (str(type(data.loc[0, col])) == "<class 'datetime.date'>" or 
            str(type(data.loc[0, col])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or 
            str(type(data.loc[0, col])) == "<class 'datetime.time'>") :
            print(f"Change columns {col} to string")
            data[col] = data[col].astype(str)
    return data

def connects(table):
    client =  MongoClient(MONGO_URL)
    db_connect = client["latest_data"][table]
    return db_connect

def create_collection(collection_validator, table):
    client =  MongoClient(MONGO_URL)
    db_connect = client
    db_connect.createCollection(table, collection_validator)
    return True

def insert_to_mongo(data, index, table, dict=False):
    db_connect = connects(table)
    if(dict):
        data_dict = data
    else:
        data = change_date_to_str(data)
        data_dict = data.to_dict("records")
    db_connect.insert_many(data_dict)

def update_to_mongo(data, index, table, dict=False):
    db_connect = connects(table)
    if(dict):
        data_dict = data
    else:
        data = change_date_to_str(data)
        data_dict = data.to_dict("records")
    for new_data in data_dict:
        db_connect.delete_one({index: new_data[index]})
        db_connect.insert_one(new_data)

def update_specific_to_mongo(data, index, table, column, dict=False):
    db_connect = connects(table)
    if(dict):
        data_dict = data
    else:
        data = change_date_to_str(data)
        data_dict = data.to_dict("records")
    for new_data in data_dict:
        condition = { index: new_data[index] }
        for col in column:
            newvalues = { "$set": { col: new_data[col] } }
            db_connect.update_one(condition, newvalues)

def delete_to_mongo(data, index, table, dict=False):
    db_connect = connects(table)
    if(dict):
        data_dict = data
    else:
        data = change_date_to_str(data)
        data_dict = data.to_dict("records")
    for new_data in data_dict:
        db_connect.delete_one({index: new_data[index]})