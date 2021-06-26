import gc
import pandas as pd
import numpy as np
from datetime import datetime
from datasource.dsws import setDataStream
from general.sql_query import get_active_universe_by_entity_type
from general.date_process import dateNow
from general.sql_output import upsert_data_to_database
from general.data_process import uid_maker

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
            print(result)
            if(split_number == 1):
                result[identifier] = universelist
            print(universelist)
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
            df[identifier] = df[identifier].str.replace("<", "", regex=True).str.replace(">", "", regex=True)
            df[identifier] = df[identifier].str.strip()
        print(df)
        data.append(df)
    if(len(data)) > 0 :
        data = pd.concat(data)
    print("== Getting Data From DSWS Done ==")
    return data, error_universe

def worldscope(universe, start_date, end_date, filter_field, identifier):
    
    ticker = universe[["ticker"]]
    result, error_universe = get_data_history_from_dsws(start_date, end_date, ticker, identifier, filter_field, use_ticker=True, split_number=1)
    print(result)
    result = result.dropna()
    print(result)
    print(error_universe)
    if(len(result)) > 0 :
        if(filter_field == ["WC05905A"]):
            result["WC05905A"] = result["WC05905A"].astype(str)
            result["WC05905A"] = result["WC05905A"].str.slice(6, 16)
            result["WC05905A"] = np.where(result["WC05905A"] == "nan", np.nan, result["WC05905A"])
            result["WC05905A"] = np.where(result["WC05905A"] == "NA", np.nan, result["WC05905A"])
            result["WC05905A"] = np.where(result["WC05905A"] == "None", np.nan, result["WC05905A"])
            result["WC05905A"] = np.where(result["WC05905A"] == "", np.nan, result["WC05905A"])
            result["WC05905A"] = np.where(result["WC05905A"] == "NaN", np.nan, result["WC05905A"])
            result["WC05905A"] = np.where(result["WC05905A"] == "NaT", np.nan, result["WC05905A"])
            result["WC05905A"] = result["WC05905A"].astype(float)
            result = result.dropna(subset=["WC05905A"], inplace=False)
            result["WC05905A"] = result["WC05905A"].astype(int)
            result["report_date"] = datetime.strptime(dateNow(), "%Y-%m-%d")
            for index, row in result.iterrows():
                WC05905A = row["WC05905A"]
                report_date = datetime.fromtimestamp(WC05905A)
                result.loc[index, "report_date"] = report_date
            result["report_date"] = pd.to_datetime(result["report_date"])
        result = result.reset_index()
        result = result.rename(columns={
            #"WC06035": "identifier",
            #     "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
            "WC05192A": "fn_5192",
            "WC18271A": "fn_18271",
            "WC02999A": "fn_2999",
            "WC03255A": "fn_3255",
            "WC03501A" : "fn_3501",
            "WC18313A" : "fn_18313",
            "WC18312A": "fn_18312",
            #     "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
            "WC18310A": "fn_18310",
            "WC18311A" : "fn_18311",
            "WC18309A" : "fn_18309",
            "WC18308A": "fn_18308",
            "WC18269A": "fn_18269",
            "WC18304A" : "fn_18304",
            "WC18266A" : "fn_18266",
            #     "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
            "WC18267A": "fn_18267",
            "WC18265A": "fn_18265",
            "WC18264A" : "fn_18264",
            "WC18263A" : "fn_18263",
            "WC18262A": "fn_18262",
            "WC18199A": "fn_18199",
            "WC18158A" : "fn_18158",
            #     "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
            "WC18100A": "fn_18100",
            "WC08001A": "fn_8001",
            "WC05085A" : "fn_5085",
            "WC03101A" : "fn_3101",
            "WC02501A": "fn_2501",
            "WC02201A": "fn_2201",
            "WC02101A" : "fn_2101",
            #     "WC02001A"]
            "WC02001A" : "fn_2001",
            "index" : "period_end"
        })
        print(result)
        result["year"] = pd.DatetimeIndex(result["period_end"]).year
        result["month"] = pd.DatetimeIndex(result["period_end"]).month
        result["day"] = pd.DatetimeIndex(result["period_end"]).day
        print(result)
        for index, row in result.iterrows():
            if (result.loc[index, "month"] <= 3) and (result.loc[index, "day"] <= 31) :
                result.loc[index, "month"] = 3
                result.loc[index, "frequency_number"] = int(1)
                result.loc[index, "year"] = int(result.loc[index, "year"]) - 1
            elif (result.loc[index, "month"] <= 6) and (result.loc[index, "day"] <= 31) :
                result.loc[index, "month"] = 6
                result.loc[index, "frequency_number"] = int(2)
            elif (result.loc[index, "month"] <= 9) and (result.loc[index, "day"] <= 31) :
                result.loc[index, "month"] = 9
                result.loc[index, "frequency_number"] = int(3)
            else:
                result.loc[index, "month"] = 12
                result.loc[index, "frequency_number"] = int(4)

            result.loc[index, "period_end"] = datetime(result.loc[index, "year"], result.loc[index, "month"], 1)
        print(result)
        result["period_end"] = result["period_end"].dt.to_period("M").dt.to_timestamp("M")
        result["period_end"] = pd.to_datetime(result["period_end"])
        print(result)
        result = uid_maker(result, trading_day="period_end")
        print(result)
        result["fiscal_quarter_end"] = result["period_end"].astype(str)
        result["fiscal_quarter_end"] = result["fiscal_quarter_end"].str.replace("-", "", regex=True)
        result = result.drop(columns=["month", "day"])
        identifier = universe[["ticker", "worldscope_identifier"]]
        result = result.merge(identifier, how="left", on="ticker")
        result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        print(result)
        upsert_data_to_database(result, "data_worldscope_summary_test", "uid", how="update", Text=True)

if __name__ == '__main__':
    currency_code = ["SGD"]
    # currency_code = ["KRW"]
    # currency_code = ["CNY"]
    # currency_code = ["JPY"]
    # currency_code = ["USD"]
    # currency_code = ["GBP"]
    # currency_code = ["EUR"]
    # currency_code = ["HKD"]
    # currency_code = ["TWD"]
    universe = get_active_universe_by_entity_type(currency_code=currency_code)
    end_date = dateNow()
    start_date = "2000-01-01"
    filter_field = ["WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
        "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A"]
    # filter_field = ["WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
    #     "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A"]
    filter_field = ["WC02001A"]
    identifier="ticker"
    for field in filter_field:
        worldscope(universe, start_date, end_date, [field], identifier)
        gc.collect()