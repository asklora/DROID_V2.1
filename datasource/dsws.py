import pandas as pd
import numpy as np
from pydatastream import Datastream
from general.date_process import count_date_range_by_month
from global_vars import DSWS_PASSWORD, DSWS_PASSWORD2, DSWS_USERNAME, DSWS_USERNAME2

def setDataStream(DSWS=True):
    if(DSWS):
        DS = Datastream(username=DSWS_USERNAME, password=DSWS_PASSWORD)
    else:
        DS = Datastream(username=DSWS_USERNAME2, password=DSWS_PASSWORD2)
    return DS

def get_data_static_with_string_from_dsws(identifier, universe, *field):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    try:
        result = DS.fetch(universe, *field, static=True)
        result = result.reset_index()
        result = result.rename(columns={"index": identifier})
    except Exception as e:
        result = None
        print(e)
    print("== Getting Data From DSWS Done ==")
    return result

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
        universelist = ",".join([str(elem) for elem in universe])
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
        universelist = ",".join([str(elem) for elem in universe])
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date)
            if(split_number == 1):
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
        print(frame)
        df = frame.reset_index()
        print(df)
        if use_ticker:
            if(len(universe) == 1):
                df["level_1"] = df["index"]
                df[identifier] = universe[0].replace("<", "").replace(">", "")
            else:
                df = df.reset_index()
                df[identifier] = df["level_0"]
                df[identifier] = df[identifier].str.replace("<", "").str.replace(">", "")
                df[identifier] = df[identifier].str.strip()
            df = df.drop(columns="index")
        else:
            if(len(universe) == 1):
                df["level_1"] = df["index"]
                df[identifier] = universe[0]
                df = df.drop(columns="index")
            else:
                df[identifier] = df["level_0"]
                df = df.drop(columns="level_0")
        data.append(df)
    if(len(data)) > 0 :
        data = pd.concat(data)
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

def get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40, monthly=False, quarterly=False, fundamentals_score=False):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    chunk_data = []
    error_universe = []
    if(type(universe) == list):
        splitting_df = universe
    else:
        ticker_list = universe[identifier].tolist()
        if use_ticker:
            ticker = ["<" + tick + ">" for tick in ticker_list]
        else:
            ticker = ticker_list
        split = len(ticker)/split_number
        splitting_df = np.array_split(ticker, split)
    for univ in splitting_df:
        if (type(universe) != list):
            universelist = ",".join([str(elem) for elem in univ])
        else:
            universelist = [univ]
        try:
            if(monthly):
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="M")
            elif(quarterly):
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="Q")
            else:
                result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq="D")
            if (fundamentals_score):
                result[identifier] = universelist
                result = result.groupby(identifier, as_index=False).last()
            if use_ticker:
                result[identifier] = universelist.replace("<", "").replace(">", "")
            else:
                if (type(universe) == list):
                    result[identifier] = univ
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
    print(data)
    return data, error_universe

def get_data_history_frequently_by_field_from_dsws(start_date, end_date, universe, identifier, field, use_ticker=True, split_number=40, monthly=False, quarterly=False, fundamentals_score=False, worldscope=False):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    chunk_data = []
    if(monthly):
        date_range = count_date_range_by_month(start_date, end_date, 1, ascending=True)
    elif(quarterly):
        date_range = count_date_range_by_month(start_date, end_date, 3, ascending=True)
    else:
        date_range = count_date_range_by_month(start_date, end_date, 3, ascending=True)
    for ticker in universe:
        chunck_field = []
        for by_field in field:
            try:
                if(monthly):
                    result = DS.fetch("<"+ticker+">", [by_field], date_from=start_date, date_to=end_date, freq="M")
                elif(quarterly):
                    result = DS.fetch("<"+ticker+">", [by_field], date_from=start_date, date_to=end_date, freq="Q")
                else:
                    result = DS.fetch("<"+ticker+">", [by_field], date_from=start_date, date_to=end_date, freq="D")
                result[identifier] = ticker
                result.reset_index(inplace=True)
                if (fundamentals_score):
                    result = result.groupby(identifier, as_index=False).last()
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how="inner", on=[identifier, "index"])
            except Exception as e:
                result = pd.DataFrame({by_field:[],identifier:[]}, index=[])
                for ranges in date_range:
                    result = result.append(pd.DataFrame({by_field:[np.nan], identifier:[universe]}, index=[ranges]))
                result.reset_index(inplace=True)
                result["index"] = pd.to_datetime(result["index"])
                if (fundamentals_score):
                    result[identifier] = ticker
                    result = result.groupby("ticker", as_index=False).last()
                elif (worldscope):
                    result[identifier] = ticker
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how="inner",on=[identifier, "index"])
        chunk_data.append(chunck_field)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        if use_ticker:
            df[identifier] = df[identifier].replace("<", "").replace(">", "")
        data.append(df)
    if(len(data)) > 0 :
        data = pd.concat(data)
        data = data.drop(columns="level_0")
    print("== Getting Data From DSWS Done ==")
    print(data)
    return data

# def get_fundamentals_quarterly_from_dsws_by_field(args, start_date, end_date, universe, name, field, use_ticker=False, **kwargs):
#     DS = setDataStream(args)
#     print("== Getting Data From DSWS ==")
#     chunk_data = []
#     date_range = count_date_range(start_date, end_date)
#     for universe in universe:
#         chunck_field = []
#         for i in field:
#             try:
#                 result = DS.fetch(universe, [i], date_from=start_date, date_to=end_date, freq='Q')
#                 result[name] = universe
#                 result.reset_index(inplace=True)
#                 if (len(chunck_field) == 0) :
#                     chunck_field = result
#                 else:
#                     chunck_field = pd.merge(chunck_field, result, how='inner', on=['ticker', 'index'])
#             except Exception as e:
#                 print(e)
#                 result = pd.DataFrame({i:[],'ticker':[]}, index=[])
#                 for ranges in date_range:
#                     result = result.append(pd.DataFrame({i:[np.nan], 'ticker':[universe]}, index=[ranges]))
#                 result.reset_index(inplace=True)
#                 result['index'] = pd.to_datetime(result['index'])
#                 if (len(chunck_field) == 0) :
#                     chunck_field = result
#                 else:
#                     chunck_field = pd.merge(chunck_field, result, how='inner',on=['ticker', 'index'])
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
#         #data = data.drop(columns='level_0')
#     except Exception as e:
#         print(e)
#     print("== Getting Data From DSWS Done ==")
#     print(data)
#     return data



