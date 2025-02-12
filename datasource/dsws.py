import pandas as pd
import numpy as np
from pydatastream import Datastream
from general.date_process import count_date_range_by_month
from global_vars import DSWS_PASSWORD, DSWS_PASSWORD2, DSWS_USERNAME, DSWS_USERNAME2
from general.sql_output import update_ingestion_count
from retry import retry

def setDataStream(DSWS=True):
    if(DSWS):
        DS = Datastream(username=DSWS_USERNAME, password=DSWS_PASSWORD)
    else:
        DS = Datastream(username=DSWS_USERNAME2, password=DSWS_PASSWORD2)
    return DS

def fetch_data_from_dsws(start_date, end_date, universe, *field, dsws=True):
    ''' obsolete '''
    DS = setDataStream(DSWS=dsws)
    DS.raise_on_error = False
    result = DS.fetch(universe, *field, date_from=start_date, date_to=end_date)
    return result

def fetch_data_static_from_dsws(universe, *field, dsws=True):
    ''' obsolete '''
    DS = setDataStream(DSWS=dsws)
    DS.raise_on_error = False
    result = DS.fetch(universe, *field, static=True)
    return result

@retry(ConnectionError, tries=3, delay=1)
def get_data_static_with_string_from_dsws(identifier, universe, *field, dsws=True):
    ''' Ingest from DSWS where universe = String

    Parameters
    ----------
    identifier :    Str, index columns name for our DB Table
    universe :      Str, ticker/RIC to fetch
    field :         Tuple, list of field to fetch
    dsws :          Boolean, determine which EIKON-DSWS account used for ingestion (default=True)

    '''

    DS = setDataStream(DSWS=dsws)
    print("== Getting Data From DSWS ==")
    try:
        result = DS.fetch(universe, *field, static=True)
        if len(result) > 0:
            update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
        result = result.reset_index()
        result = result.rename(columns={"index": identifier})
    except Exception as e:
        result = None
        print(e)
    print("== Getting Data From DSWS Done ==")
    return result

@retry(ConnectionError, tries=3, delay=1)
def get_data_static_from_dsws(universe, identifier, *field, use_ticker=True, split_number=40, dsws=True):
    DS = setDataStream(DSWS=dsws)
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
            if len(result)>0:
                update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
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

@retry(ConnectionError, tries=3, delay=1)
def get_data_history_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, dividend=False, split_number=40, dsws=True):
    DS = setDataStream(DSWS=dsws)
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
    for univ in splitting_df:
        universelist = ",".join([str(elem) for elem in univ])
        print(universelist)
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date)
            if len(result)>0:
                update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
            if(split_number == 1):
                result[identifier] = str(universelist)
            print(result)
            chunk_data.append(result)
        except Exception as e:
            if use_ticker:
                universelist = universelist.replace("<", "").replace(">", "")
                universelist = universelist.strip()
            if(split_number > 1):
                universelist = universelist.split(",")
            if(type(universelist) == list):
                if(len(universelist) > 1):
                    for tick in universelist:
                        error_universe.append(tick)
                else:
                    error_universe.append(universelist[0])
            else:
                error_universe.append(universelist)
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        print(df)
        if use_ticker:
            if(dividend):
                df[identifier] = df[identifier].str.replace("<", "").str.replace(">", "")
                df[identifier] = df[identifier].str.strip()
            else:
                if(len(universe) == 1):
                    df["level_1"] = df["index"]
                    df[identifier] = df[identifier].str.replace("<", "").str.replace(">", "")
                    df[identifier] = df[identifier].str.strip()
                elif(split_number == 1):
                    df["level_1"] = df["index"]
                    df[identifier] = df[identifier].str.replace("<", "").str.replace(">", "")
                    df[identifier] = df[identifier].str.strip()
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

@retry(ConnectionError, tries=3, delay=1)
def get_data_history_by_field_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, dividend=False, split_number=40, dsws=True):
    DS = setDataStream(DSWS=dsws)
    chunk_data = []
    print(universe)
    error_universe = []
    for ticker in universe:
        chunck_field = []
        for by_field in field:
            try:
                result = DS.fetch("<"+ticker+">", *[by_field], date_from=start_date, date_to=end_date)
                if len(result)>0:
                    update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
                print(result)
                result[identifier] = ticker
                result.reset_index(inplace=True)
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how="inner", on=[identifier, "index"])
            except Exception as e:
                error_universe.append(ticker)
                print(e)
        print(chunck_field)
        if(len(chunck_field) > 0):
            chunk_data.append(chunck_field)
    print(chunk_data)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        if use_ticker:
            df[identifier] = df[identifier].replace("<", "").replace(">", "")
        data.append(df)
    if(len(data)) > 0 :
        data = pd.concat(data)
        data = data.drop(columns="level_0")
    data["level_1"] = data["index"]
    print("== Getting Data From DSWS Done ==")
    print(data)
    return data, error_universe

@retry(ConnectionError, tries=3, delay=1)
def get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40, monthly=False, quarterly=False, fundamentals_score=False, dsws=True):
    DS = setDataStream(DSWS=dsws)
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
            if len(result)>0:
                update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
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

@retry(ConnectionError, tries=3, delay=1)
def get_data_history_frequently_by_field_from_dsws(start_date, end_date, universe, identifier, field, use_ticker=True,
                                                   split_number=40, monthly=False, quarterly=False,
                                                   fundamentals_score=False, worldscope=False, dsws=True):
    DS = setDataStream(DSWS=dsws)
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
                if len(result) > 0:
                    update_ingestion_count(source='dsws', n_ingest=result.fillna(0).count().count(), dsws=dsws)
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
                    result = result.append(pd.DataFrame({by_field:[np.nan], identifier:[ticker]}, index=[ranges]))
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
        print(chunck_field)
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
    # print(data)
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



