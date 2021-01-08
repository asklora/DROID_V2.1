import os
import re
import pandas as pd
import numpy as np
from pydatastream import Datastream
from general.date_process import backdate_by_year, count_date_range, count_date_range_monthly
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
        data = data.drop(columns='level_0')
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

def get_data_history_from_dsws(start_date, end_date, universe, identifier, *field, use_ticker=True, split_number=40):
    DS = setDataStream()
    print("== Getting Data From DSWS ==")
    chunk_data = []
    ticker_list = universe[identifier].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/split_number
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ', '.join([str(elem) for elem in universe])
        print(universelist)
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date)
            if use_ticker:
                result[identifier] = universelist.replace("<", "").replace(">", "")
            print(result)
            chunk_data.append(result)
        except Exception as e:
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
        data = data.drop(columns='level_0')
    print("== Getting Data From DSWS Done ==")
    return data

def get_fundamentals_score_from_dsws(args, start_date, end_date, universe, name, *field, use_ticker=False, **kwargs):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    except_field = []
    ticker_list = universe[kwargs['identifier']].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/1
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ', '.join([str(elem) for elem in universe])
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq='Q')
            result[name] = universelist
            result = result.groupby('ticker', as_index=False).last()
            print(result)
            chunk_data.append(result)
        except Exception as e:
            print(e)
            tick = universelist.replace("<", "").replace(">", "")
            tick = universelist.strip()
            except_field.append(tick)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        if use_ticker:
            df2[name] = df2[name].astype(str)
            df2[name] = df2[name].str.replace("<", "").str.replace(">", "")
            df2[name] = df2[name].str.strip()
        data.append(df2)
    data = pd.concat(data)
    data = data.drop(columns='level_0')
    print("== Getting Data From DSWS Done ==")
    return data, except_field

def get_fundamentals_score_from_dsws_by_field(args, start_date, end_date, universe, name, field, use_ticker=False, **kwargs):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    for universe in universe:
        chunck_field = []
        for i in field:
            try:
                result = DS.fetch(universe, [i], date_from=start_date, date_to=end_date, freq='Q')
                result[name] = universe
                result = result.groupby('ticker', as_index=False).last()
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how='inner', on='ticker')
            except Exception as e:
                print(e)
                result = pd.DataFrame({'ticker':[universe],i:[np.nan]}, index=[0])
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how='inner',on='ticker')
        print(chunck_field)
        chunk_data.append(chunck_field)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        df2[name] = df2[name].astype(str)
        df2[name] = df2[name].str.replace("<", "").str.replace(">", "")
        df2[name] = df2[name].str.strip()
        data.append(df2)
    data = pd.concat(data)
    data = data.drop(columns='level_0')
    print("== Getting Data From DSWS Done ==")
    return data

def get_vix_history_from_dsws(args, start_date, end_date, universe, *field, **kwargs):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    ticker = universe[kwargs['identifier']].tolist()
    split = len(ticker)/1
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ', '.join([str(elem) for elem in universe])
        print(universelist)
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date)
            result[kwargs['identifier']] = universelist
            chunk_data.append(result)
        except Exception as e:
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        df2 = df2.rename(columns={'index': 'trading_day'})
        data.append(df2)
    try:
        data = pd.concat(data)
        data.reset_index(inplace=True)
        data = data.drop(columns={'level_0', 'index'})
    except Exception as e:
        print(e)
    print("== Getting Data From DSWS Done ==")
    return data

def get_fundamentals_quarterly_from_dsws(args, start_date, end_date, universe, name, *field, use_ticker=False, **kwargs):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    except_field = []
    ticker_list = universe[kwargs['identifier']].tolist()
    if use_ticker:
        ticker = ["<" + tick + ">" for tick in ticker_list]
    else:
        ticker = ticker_list
    split = len(ticker)/1
    splitting_df = np.array_split(ticker, split)
    for universe in splitting_df:
        universelist = ', '.join([str(elem) for elem in universe])
        try:
            result = DS.fetch(universelist, *field, date_from=start_date, date_to=end_date, freq='Q')
            result[name] = universelist
            print(result)
            chunk_data.append(result)
        except Exception as e:
            print(e)
            tick = universelist.replace("<", "").replace(">", "")
            tick = universelist.strip()
            except_field.append(tick)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df.reset_index()
        if use_ticker:
            df2[name] = df2[name].astype(str)
            df2[name] = df2[name].str.replace("<", "").str.replace(">", "")
            df2[name] = df2[name].str.strip()
        data.append(df2)
    try:
        data = pd.concat(data)
        data = data.drop(columns='level_0')
    except Exception as e:
        print(e)
    print("== Getting Data From DSWS Done ==")
    return data, except_field

def get_fundamentals_quarterly_from_dsws_by_field(args, start_date, end_date, universe, name, field, use_ticker=False, **kwargs):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    date_range = count_date_range(start_date, end_date)
    for universe in universe:
        chunck_field = []
        for i in field:
            try:
                result = DS.fetch(universe, [i], date_from=start_date, date_to=end_date, freq='Q')
                result[name] = universe
                result.reset_index(inplace=True)
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how='inner', on=['ticker', 'index'])
            except Exception as e:
                print(e)
                result = pd.DataFrame({i:[],'ticker':[]}, index=[])
                for ranges in date_range:
                    result = result.append(pd.DataFrame({i:[np.nan], 'ticker':[universe]}, index=[ranges]))
                result.reset_index(inplace=True)
                result['index'] = pd.to_datetime(result['index'])
                if (len(chunck_field) == 0) :
                    chunck_field = result
                else:
                    chunck_field = pd.merge(chunck_field, result, how='inner',on=['ticker', 'index'])
        print(chunck_field)
        chunk_data.append(chunck_field)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df2 = df
        df2[name] = df2[name].astype(str)
        df2[name] = df2[name].str.replace("<", "").str.replace(">", "")
        df2[name] = df2[name].str.strip()
        data.append(df2)
    try:
        data = pd.concat(data)
        #data = data.drop(columns='level_0')
    except Exception as e:
        print(e)
    print("== Getting Data From DSWS Done ==")
    print(data)
    return data

def get_interest_rate_from_dsws(args, identifier, universe, *field):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    try:
        result = DS.fetch(universe, *field, static=True)
        result = result.reset_index()
        result = result.rename(columns={'index': identifier})
        print(result)
    except Exception as e:
        print(e)
    print("== Getting Data From DSWS Done ==")
    return result

def get_company_des_from_dsws(args, identifier, universe, *field):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    ticker = ["<" + tick + ">" for tick in universe]
    for universe in ticker:
        universelist = ', '.join([str(elem) for elem in universe])
        try:
            result = DS.fetch(universelist, *field, static=True)
            print(result)
            chunk_data.append(result)
        except Exception as e:
            print(e)
    data = []
    for frame in chunk_data:
        df = frame.reset_index()
        df = df.rename(columns={'index': identifier})
        print(df)
        df[identifier] = df[identifier].str.replace("<", "").str.replace(">", "")
        df[identifier] = df[identifier].str.strip()
        data.append(df)
    if(len(data)) > 0 :
        data = pd.concat(data)
    print(data)
    print("== Getting Data From DSWS Done ==")
    return data

def check_ticker_available_in_DSWS(args, field, start_date, end_date):
    DS = setDataStream(args)
    stocks = DroidUniverse(args)
    stocks = stocks['ticker'].tolist()
    chunk = []
    for tick in tqdm(stocks):
        try:
            res = DS.fetch(["<" + tick + ">"], fields=field, date_from=start_date, date_to=end_date, freq='Q')
        except Exception as e:
            m = re.findall(r"\<([A-Za-z0-9_.]+)\>", str(e))
            m = m[0]
            chunk.append(str(m))
    df = pd.DataFrame(chunk,columns =['ticker'])
    #df['updated'] = datetime.now().date().strftime("%Y-%m-%d")
    return df

def TestDSWSField(args, ticker, field, start_date, end_date):
    DS = setDataStream(args)
    print("== Getting Data From DSWS ==")
    chunk_data = []
    for tick in tqdm(ticker):
        try:
            result = DS.fetch([tick], fields=field, date_from=start_date, date_to=end_date, freq='Q')
            print(result)
            chunk_data.append(result)
        except Exception as e:
            print(e)
    print(chunk_data)