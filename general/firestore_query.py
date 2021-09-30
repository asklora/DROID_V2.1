import math
import numpy as np
import pandas as pd
from firebase_admin import firestore
from core.djangomodule.general import logging
from django.conf import settings
import time



def change_null_to_zero(data):
    for col in data.columns:
        if(type(data.loc[0, col]) == str):
            data[col] = np.where(data[col].isnull(), None, data[col])
            data[col] = np.where(data[col] == "NA", None, data[col])
        else:
            data[col] = np.where(data[col].isnull(), 0, data[col])
            data[col] = np.where(data[col] == np.NAN, 0, data[col])
    return data

def change_date_to_str(data, exception=None):
    for col in data.columns:
        status = True
        if(exception != None):
            if (col == exception or col in exception):
                status = False
        if(status):
            if (str(type(data.loc[0, col])) == "<class 'datetime.date'>" or 
                str(type(data.loc[0, col])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or 
                str(type(data.loc[0, col])) == "<class 'datetime.time'>") :
                data[col] = data[col].astype(str)
            elif(str(type(data.loc[0, col])) == "<class 'uuid.UUID'>"):
                data[col] = data[col].astype(str)
            elif(type(data.loc[0, col]) == str):
                data[col] = np.where(data[col].isnull(), "", data[col])
            elif(type(data.loc[0, col]) == np.float64 or type(data.loc[0, col]) == int or type(data.loc[0, col]) == float):
                data[col] = np.where(data[col].isnull(), 0, data[col])
    return data




def delete_firestore_user(user_id:str):
    db = firestore.client()
    collection =db.collection(settings.FIREBASE_COLLECTION['portfolio']).document(f"{user_id}")
    trying =0
    while collection.get().exists:
            collection.delete()
            time.sleep(2)
            trying += 1
            if trying > 10:
                logging.warning(f"{user_id} cannot delete")
                break
    logging.info(f"{user_id} deleted")
    # user_data =db.collection(settings.FIREBASE_COLLECTION['portfolio']).where("id","==",f"{user_id}")





def get_price_data_firebase(ticker:list) -> pd.DataFrame:
    # firebase have limitation query max 10 list
    # we need to split in here
    db = firestore.client()
    object_list = []
    # here loop numpy split
    split = math.ceil(len(ticker) / min(len(ticker), 9))
    splitting_df = np.array_split(ticker, split)
    for univ in splitting_df:
        univ = univ.tolist()
        doc_ref = db.collection(settings.FIREBASE_COLLECTION['universe']).where("ticker","in", univ).get()
        for data in doc_ref:
            format_data = {}
            data = data.to_dict()
            format_data['ticker'] = data.get('ticker')
            format_data['last_date'] = data.get('price',{}).get('last_date',0)
            format_data['latest_price'] = data.get('price',{}).get('latest_price',0)
            object_list.append(format_data)
    result = pd.DataFrame(object_list)
    return result
    
def update_to_firestore(data, index, table, dict=False):
    data["indexes"] = data[index]
    data = data.set_index("indexes")
    df = data.to_dict("index")
    del data
    db = firestore.client()
    
    # jsonprint(df)
    for key,val in df.items():
        doc_ref = db.collection(f"{table}").document(f"{key}")
        doc_ref.set(val)