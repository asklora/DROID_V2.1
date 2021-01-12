import os
import re
import pandas as pd
import numpy as np
from pydatastream import Datastream
from general.date_process import backdate_by_year, count_date_range_by_month
from tqdm import tqdm


def setDataStream(DSWS=True):
    if(DSWS):
        DS = Datastream(username=os.getenv("DSWS_USERNAME"), password=os.getenv("DSWS_PASSWORD"))
    else:
        DS = Datastream(username=os.getenv("DSWS_USERNAME2"), password=os.getenv("DSWS_PASSWORD2"))
    return DS

def get_data_static_from_dsws(universe, identifier, *field, use_ticker=True, split_number=40):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")

    chunk_data = []
    error_universe = []
    ticker_list = universe[identifier].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/split_number
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ", ".join([str(elem) for elem in universe])
        try:
            result = DS.fetch(universelist, *field, static=True)
            print(result)
            chunk_data.append(result)
        except Exception as e:
            if use_ticker:
                universelist = universelist.replace("<", "").replace(">", "")
                universelist = universelist.strip()
            error_universe.append(universelist)
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        if use_ticker:
            df2["index"] = df2["index"].str.replace("<", "").str.replace(">", "")
            df2["index"] = df2["index"].str.strip()
        data.append(df2)
    if(len(data)) > 0 :
        data = pd.concat(data)
        data = data.drop(columns="level_0")
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

def get_data_history_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    chunk_data = []
    error_universe = []
    ticker_list = universe[identifier].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/split_number
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ", ".join([str(elem) for elem in universe])
        print(universelist)
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date)
            if use_ticker:
                result[identifier] = universelist.replace("<", "").replace(">", "")
            else:
                result[identifier] = universelist
            print(result)
            chunk_data.append(result)
        except Exception as e:
            if use_ticker:
                universelist = universelist.replace("<", "").replace(">", "")
                universelist = universelist.strip()
            error_universe.append(universelist)
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        if use_ticker:
            df2[identifier] = df2[identifier].replace("<", "").replace(">", "")
        data.append(df2)
    if(len(data)) > 0 :
        data = pd.concat(data)
        data = data.drop(columns="level_0")
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

def get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40, monthly=False, quarterly=False, fundamentals=False):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    chunk_data = []
    error_universe = []
    ticker_list = universe[identifier].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/split_number
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ", ".join([str(elem) for elem in universe])
        print(universelist)
        try:
            if(monthly):
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="M")
            elif(quarterly):
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="Q")
            else:
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="D")
            if (fundamentals):
                result[identifier] = universelist
                result = result.groupby("ticker", as_index=False).last()
            if use_ticker:
                result[identifier] = universelist.replace("<", "").replace(">", "")
            print(result)
            chunk_data.append(result)
        except Exception as e:
            if use_ticker:
                universelist = universelist.replace("<", "").replace(">", "")
                universelist = universelist.strip()
            error_universe.append(universelist)
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        if use_ticker:
            df2[identifier] = df2[identifier].replace("<", "").replace(">", "")
        data.append(df2)
    if(len(data)) > 0 :
        data = pd.concat(data)
        data = data.drop(columns="level_0")
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

# def get_data_history_frequently_by_field_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40, monthly=False, quarterly=False, fundamentals=False):
#     DS = setDataStream()
#     print("== Getting Data From DSWS ==")
#     chunk_data = []
#     error_universe = []
#     ticker_list = universe[identifier].tolist()
#     if(monthly):
#         count_date_range_by_month(start_date, end_date, 1)
#     elif(quarterly):
#         count_date_range_by_month(start_date, end_date, 3)
#     if use_ticker:
#         ticker = ["<" + tick + ">" for tick in ticker_list]
#     else:
#         ticker = ticker_list
#     split = len(ticker)/split_number
#     splitting_df = np.array_split(ticker, split)
#     for universe in splitting_df:
#         universelist = ", ".join([str(elem) for elem in universe])
#         print(universelist)
#         try:
#             if(monthly):
#                 result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="M")
#             elif(quarterly):
#                 result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="Q")
#             else:
#                 result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="D")
#             if use_ticker:
#                 result[identifier] = universelist.replace("<", "").replace(">", "")

#             result[name] = universe
#             result.reset_index(inplace=True)
#             if (len(chunck_field) == 0) :
#                 chunck_field = result
#             else:
#                 chunck_field = pd.merge(chunck_field, result, how="inner", on=[identifier, "index"])
#         except Exception as e:
#             print(e)
#             result = pd.DataFrame({i:[],identifier:[]}, index=[])
#             for ranges in date_range:
#                 result = result.append(pd.DataFrame({i:[np.nan], identifier:[universe]}, index=[ranges]))
#             result.reset_index(inplace=True)
#             result["index"] = pd.to_datetime(result["index"])
#             if (len(chunck_field) == 0) :
#                 chunck_field = result
#             else:
#                 chunck_field = pd.merge(chunck_field, result, how="inner",on=[identifier, "index"])

#             print(result)
#             chunk_data.append(result)
#         except Exception as e:
#             if use_ticker:
#                 universelist = universelist.replace("<", "").replace(">", "")
#                 universelist = universelist.strip()
#             error_universe.append(universelist)
#             print(e)
#     data = []
#     for frame in chunk_data:
#         df = frame.reset_index()
#         df2 = df.reset_index()
#         if use_ticker:
#             df2[identifier] = df2[identifier].replace("<", "").replace(">", "")
#         data.append(df2)
#     if(len(data)) > 0 :
#         data = pd.concat(data)
#         data = data.drop(columns="level_0")
#     print("== Getting Data From DSWS Done ==")
#     return data, error_universe

# def get_fundamentals_quarterly_from_dsws_by_field(args, start_date, end_date, universe, name, field, use_ticker=False, **kwargs):
#     DS = setDataStream(args)
#     print("== Getting Data From DSWS ==")
#     chunk_data = []
#     date_range = count_date_range(start_date, end_date)
#     for universe in universe:
#         chunck_field = []
#         for i in field:
#             try:
#                 result = DS.fetch(universe, [i], date_from=start_date, date_to=end_date, freq="Q")
#                 result[name] = universe
#                 result.reset_index(inplace=True)
#                 if (len(chunck_field) == 0) :
#                     chunck_field = result
#                 else:
#                     chunck_field = pd.merge(chunck_field, result, how="inner", on=["ticker", "index"])
#             except Exception as e:
#                 print(e)
#                 result = pd.DataFrame({i:[],"ticker":[]}, index=[])
#                 for ranges in date_range:
#                     result = result.append(pd.DataFrame({i:[np.nan], "ticker":[universe]}, index=[ranges]))
#                 result.reset_index(inplace=True)
#                 result["index"] = pd.to_datetime(result["index"])
#                 if (len(chunck_field) == 0) :
#                     chunck_field = result
#                 else:
#                     chunck_field = pd.merge(chunck_field, result, how="inner",on=["ticker", "index"])
#         print(chunck_field)
#         chunk_data.append(chunck_field)
#     data = []
#     for frame in chunk_data:
#         df = frame.reset_index()
#         df2 = df
#         df2[name] = df2[name].astype(str)
#         df2[name] = df2[name].str.replace("<", "").str.replace(">", "")
#         df2[name] = df2[name].str.strip()
#         data.append(df2)
#     try:
#         data = pd.concat(data)
#     except Exception as e:
#         print(e)
#     print("== Getting Data From DSWS Done ==")
#     print(data)
#     return data

# def get_interest_rate_from_dsws(args, identifier, universe, *field):
#     DS = setDataStream(args)
#     print("== Getting Data From DSWS ==")
#     try:
#         result = DS.fetch(universe, *field, static=True)
#         result = result.reset_index()
#         result = result.rename(columns={"index": identifier})
#         print(result)
#     except Exception as e:
#         print(e)
#     print("== Getting Data From DSWS Done ==")
#     return result