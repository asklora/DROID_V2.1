import numpy as np
from pymongo import MongoClient
from global_vars import MONGO_URL
import pandas as pd
from firebase_admin import firestore

def change_null_to_zero(data):
    for col in data.columns:
        if(type(data.loc[0, col]) == str):
            data[col] = np.where(data[col].isnull(), None, data[col])
            data[col] = np.where(data[col] == "NA", None, data[col])
        else:
            data[col] = np.where(data[col].isnull(), 0, data[col])
            data[col] = np.where(data[col] == np.NAN, 0, data[col])
    return data

def change_date_to_str(data):
    for col in data.columns:
        if (str(type(data.loc[0, col])) == "<class 'datetime.date'>" or 
            str(type(data.loc[0, col])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or 
            str(type(data.loc[0, col])) == "<class 'datetime.time'>") :
            # print(f"Change DATE columns {col} to string")
            data[col] = data[col].astype(str)
        elif(str(type(data.loc[0, col])) == "<class 'uuid.UUID'>"):
            # print(f"Change UUID columns {col} to string")
            data[col] = data[col].astype(str)
        elif(type(data.loc[0, col]) == str):
            data[col] = np.where(data[col].isnull(), "", data[col])
        else:
            data[col] = np.where(data[col].isnull(), 0, data[col])
    return data

def connects(table):
    client =  MongoClient(MONGO_URL)
    db_connect = client["universe"][table]
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


def get_price_data_firebase(ticker:list) -> pd.DataFrame:
    # firebase have limitation query max 10 list
    # we need to split in here
    db = firestore.client()
    object_list = []
    # here loop numpy split
    split = len(ticker) / min(len(ticker), 9)
    splitting_df = np.array_split(ticker, split)
    for univ in splitting_df:
        univ = univ.tolist()
        doc_ref = db.collection(u"universe").where("ticker","in", univ).get()
        for data in doc_ref:
            format_data = {}
            data = data.to_dict()
            format_data['ticker'] = data.get('ticker')
            format_data['last_date'] = data.get('price',{}).get('last_date',0)
            format_data['latest_price'] = data.get('price',{}).get('latest_price',0)
            object_list.append(format_data)
    result = pd.DataFrame(object_list)
    return result
    
def update_to_mongo(data, index, table, dict=False):
    data = change_date_to_str(data)
    data["indexes"] = data[index]
    data = data.set_index("indexes")
    df = data.to_dict("index")
    del data
    db = firestore.client()
    for key,val in df.items():
        if(table=="universe"):
            doc_ref = db.collection(u"universe").document(f"{key}")
        elif(table=="portfolio"):
            doc_ref = db.collection(u"portfolio").document(f"{key}")
        else:
            doc_ref = db.collection(u"universe").document(f"{key}")
        doc_ref.set(val)
    # db_connect = connects(table)
    # if(dict):
    #     data_dict = data
    # else:
    #     data = change_date_to_str(data)
    #     data_dict = data.to_dict("records")
    # for new_data in data_dict:
    #     db_connect.delete_one({index: new_data[index]})
    #     db_connect.insert_one(new_data)

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