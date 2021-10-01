import gc
import pandas as pd
import numpy as np
from datetime import datetime
from datasource.dsws import setDataStream
from general.sql_query import get_active_universe_by_entity_type
from general.date_process import dateNow
from general.sql_output import upsert_data_to_database
from general.data_process import uid_maker

# def update_worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None, history=False):
#     filter_field = [
#         "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
#         "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
#         "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
#         "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
#         "WC02001A", "WC05575A", "WC01451A", "WC18810A", "WC02401A", "WC18274A", "WC03040A"]
#     for field in filter_field:
#         worldscope_quarter_summary_from_dsws(ticker=ticker, currency_code=currency_code, filter_field=[field], history=history)
#     report_to_slack("{} : === Quarter Summary Data Updated ===".format(datetimeNow()))

# def worldscope_quarter_report_date_from_dsws(ticker = None, currency_code=None, history=False):
#     identifier="ticker"
#     filter_field = ["WC05905A"]
#     universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
#     ticker = universe[["ticker"]]
#     end_date = forwarddate_by_month(9)
#     start_date = backdate_by_month(6)
#     if(history):
#         start_date = "2000-03-31"
#     period_end_list = get_period_end_list(start_date=start_date, end_date=end_date)
#     data = []
#     for period_end in period_end_list:
#         try:
#             result, error_ticker = get_data_history_from_dsws(period_end, period_end, ticker, identifier, filter_field, use_ticker=True, split_number=min(len(universe), 20), dsws=False)
#             print(result)
#             print(error_ticker)
#             if len(error_ticker) == 0 :
#                 second_result = []
#             else:
#                 second_result, error_ticker = get_data_history_by_field_from_dsws(period_end, period_end, error_ticker, identifier, filter_field, use_ticker=True, split_number=1, dsws=False)
#             try:
#                 if(len(result) == 0):
#                     result = second_result
#                 elif(len(second_result) == 0):
#                     result = result
#                 else :
#                     result = result.append(second_result)
#             except Exception as e:
#                 result = second_result
#             print(result)
#             data = result.copy()
#             data = data.rename(columns = {"level_1" : "period_end"})
#             data = data[["ticker", "period_end", "WC05905A"]]
#             print(data)
#             data["WC05905A"] = data["WC05905A"].astype(str)
#             data["WC05905A"] = data["WC05905A"].str.slice(6, 16)
#             data["WC05905A"] = np.where(data["WC05905A"] == "nan", np.nan, data["WC05905A"])
#             data["WC05905A"] = np.where(data["WC05905A"] == "NA", np.nan, data["WC05905A"])
#             data["WC05905A"] = np.where(data["WC05905A"] == "None", np.nan, data["WC05905A"])
#             data["WC05905A"] = np.where(data["WC05905A"] == "", np.nan, data["WC05905A"])
#             data["WC05905A"] = np.where(data["WC05905A"] == "NaN", np.nan, data["WC05905A"])
#             data["WC05905A"] = np.where(data["WC05905A"] == "NaT", np.nan, data["WC05905A"])
#             data["WC05905A"] = data["WC05905A"].astype(float)
#             data = data.dropna(subset=["WC05905A"], inplace=False)
#             if(len(data) > 0):
#                 data["WC05905A"] = data["WC05905A"].astype(int)
#                 data["WC05905A"] = np.where(data["WC05905A"] > 9000000000, data["WC05905A"]/10, data["WC05905A"])
#                 data["report_date"] = datetime.strptime(dateNow(), "%Y-%m-%d")
#                 data = data.reset_index(inplace=False, drop=True)
#                 for index, row in data.iterrows():
#                     WC05905A = row["WC05905A"]
#                     report_date = datetime.fromtimestamp(WC05905A)
#                     data.loc[index, "report_date"] = report_date
#                 data["report_date"] = pd.to_datetime(data["report_date"])
#                 data["period_end"] = pd.to_datetime(data["period_end"])
#                 data["year"] = pd.DatetimeIndex(data["period_end"]).year
#                 data["month"] = pd.DatetimeIndex(data["period_end"]).month
#                 data["day"] = pd.DatetimeIndex(data["period_end"]).day
#                 # print(data)
#                 for index, row in data.iterrows():
#                     if (data.loc[index, "month"] <= 3) and (data.loc[index, "day"] <= 31) :
#                         data.loc[index, "month"] = 3
#                         data.loc[index, "frequency_number"] = int(1)
#                     elif (data.loc[index, "month"] <= 6) and (data.loc[index, "day"] <= 31) :
#                         data.loc[index, "month"] = 6
#                         data.loc[index, "frequency_number"] = int(2)
#                     elif (data.loc[index, "month"] <= 9) and (data.loc[index, "day"] <= 31) :
#                         data.loc[index, "month"] = 9
#                         data.loc[index, "frequency_number"] = int(3)
#                     else:
#                         data.loc[index, "month"] = 12
#                         data.loc[index, "frequency_number"] = int(4)

#                     data.loc[index, "period_end"] = datetime(data.loc[index, "year"], data.loc[index, "month"], 1)
#                 data["period_end"] = data["period_end"].dt.to_period("M").dt.to_timestamp("M")
#                 data["period_end"] = pd.to_datetime(data["period_end"])

#                 data = uid_maker(data, trading_day="period_end")
#                 data = data.drop(columns=["WC05905A", "year", "month", "day", "frequency_number"])
#                 data = data.drop_duplicates(subset=["uid"], keep="first", inplace=False)
#                 print(data)
#                 upsert_data_to_database(data, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)
#         except Exception as e:
#             print("{} : === ERROR === : {}".format(dateNow(), e))

# def worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None, filter_field=None, history=False):
#     identifier="ticker"
#     universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
#     ticker = universe[["ticker"]]
#     end_date = forwarddate_by_month(9)
#     start_date = backdate_by_month(6)
#     if(history):
#         start_date = "2000-03-31"
#     period_end_list = get_period_end_list(start_date=start_date, end_date=end_date)
#     data = []
#     for period_end in period_end_list:
#         try:
#             result, error_ticker = get_data_history_from_dsws(period_end, period_end, ticker, identifier, filter_field, use_ticker=True, split_number=min(len(universe), 20), dsws=False)
#             print(result)
#             print(error_ticker)
#             if len(error_ticker) == 0 :
#                 second_result = []
#             else:
#                 second_result, error_ticker = get_data_history_by_field_from_dsws(period_end, period_end, error_ticker, identifier, filter_field, use_ticker=True, split_number=1, dsws=False)
#             try:
#                 if(len(result) == 0):
#                     result = second_result
#                 elif(len(second_result) == 0):
#                     result = result
#                 else :
#                     result = result.append(second_result)
#             except Exception as e:
#                 result = second_result
#             print(result)
#             data = result.copy()
#             result = result.rename(columns = {"level_1" : "period_end"})
#             result = result[["ticker", "period_end", filter_field[0]]]
#             print(result)
#             result[filter_field[0]] = result[filter_field[0]].astype(str)
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "nan", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "NA", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "None", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "NaN", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = np.where(result[filter_field[0]] == "NaT", np.nan, result[filter_field[0]])
#             result[filter_field[0]] = result[filter_field[0]].astype(float)
#             result = result.dropna(subset=[filter_field[0]], inplace=False)
#             if(len(result) > 0):
#                 result = result.rename(columns={
#                     #"WC06035": "identifier",
#                     #     "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
#                     "WC05192A": "fn_5192",
#                     "WC18271A": "fn_18271",
#                     "WC02999A": "fn_2999",
#                     "WC03255A": "fn_3255",
#                     "WC03501A" : "fn_3501",
#                     "WC18313A" : "fn_18313",
#                     "WC18312A": "fn_18312",
#                     #     "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
#                     "WC18310A": "fn_18310",
#                     "WC18311A" : "fn_18311",
#                     "WC18309A" : "fn_18309",
#                     "WC18308A": "fn_18308",
#                     "WC18269A": "fn_18269",
#                     "WC18304A" : "fn_18304",
#                     "WC18266A" : "fn_18266",
#                     #     "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
#                     "WC18267A": "fn_18267",
#                     "WC18265A": "fn_18265",
#                     "WC18264A" : "fn_18264",
#                     "WC18263A" : "fn_18263",
#                     "WC18262A": "fn_18262",
#                     "WC18199A": "fn_18199",
#                     "WC18158A" : "fn_18158",
#                     #     "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
#                     "WC18100A": "fn_18100",
#                     "WC08001A": "fn_8001",
#                     "WC05085A" : "fn_5085",
#                     "WC03101A" : "fn_3101",
#                     "WC02501A": "fn_2501",
#                     "WC02201A": "fn_2201",
#                     "WC02101A" : "fn_2101",
#                     #     "WC02001A", "WC05575A"]
#                     "WC02001A" : "fn_2001",
#                     "WC05575A" : "fn_5575",
#                     "index" : "period_end",
#                     # "WC01451A", "WC18810A", "WC02401A", "WC18274A", "WC03040A"
#                     "WC01451A" : "fn_1451",
#                     "WC18810A" : "fn_18810",
#                     "WC02401A" : "fn_2401",
#                     "WC18274A" : "fn_18274",
#                     "WC03040A" : "fn_3040"
#                 })
#                 result = result.reset_index(inplace=False, drop=True)
#                 result["period_end"] = pd.to_datetime(result["period_end"])
#                 result["year"] = pd.DatetimeIndex(result["period_end"]).year
#                 result["month"] = pd.DatetimeIndex(result["period_end"]).month
#                 result["day"] = pd.DatetimeIndex(result["period_end"]).day
#                 for index, row in result.iterrows():
#                     if (result.loc[index, "month"] <= 3) and (result.loc[index, "day"] <= 31) :
#                         result.loc[index, "month"] = 3
#                         result.loc[index, "frequency_number"] = int(1)
#                     elif (result.loc[index, "month"] <= 6) and (result.loc[index, "day"] <= 31) :
#                         result.loc[index, "month"] = 6
#                         result.loc[index, "frequency_number"] = int(2)
#                     elif (result.loc[index, "month"] <= 9) and (result.loc[index, "day"] <= 31) :
#                         result.loc[index, "month"] = 9
#                         result.loc[index, "frequency_number"] = int(3)
#                     else:
#                         result.loc[index, "month"] = 12
#                         result.loc[index, "frequency_number"] = int(4)
#                     result.loc[index, "period_end"] = datetime(result.loc[index, "year"], result.loc[index, "month"], 1)
#                 result["period_end"] = result["period_end"].dt.to_period("M").dt.to_timestamp("M")
#                 result["period_end"] = pd.to_datetime(result["period_end"])

#                 result = uid_maker(result, trading_day="period_end")
#                 result["fiscal_quarter_end"] = result["period_end"].astype(str)
#                 result["fiscal_quarter_end"] = result["fiscal_quarter_end"].str.replace("-", "", regex=True)
#                 result = result.drop(columns=["WC05905A", "month", "day"])
#                 worldscope_identifier = universe[["ticker", "worldscope_identifier"]]
#                 result = result.merge(worldscope_identifier, how="left", on="ticker")
#                 result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)
#                 print(result)
#                 upsert_data_to_database(result, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)
#         except Exception as e:
#             print("{} : === ERROR === : {}".format(dateNow(), e))

# def worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None, filter_field=None, history=False):
#     universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
#     if(len(universe) < 1):
#         return False
#     end_date = dateNow()
#     start_date = backdate_by_month(6)
#     if(history):
#         start_date = "2000-03-31"
#     identifier="ticker"
#     ticker = universe[["ticker"]]
#     ticker = ticker["ticker"].tolist()
#     result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, ticker, identifier, filter_field, use_ticker=True, split_number=1, monthly=True, worldscope=True)
#     # print(result)
#     result = result.dropna()
#     print(result)
#     if(len(result)) > 0 :
#         result = result.rename(columns={
#             #"WC06035": "identifier",
#             #     "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
#             "WC05192A": "fn_5192",
#             "WC18271A": "fn_18271",
#             "WC02999A": "fn_2999",
#             "WC03255A": "fn_3255",
#             "WC03501A" : "fn_3501",
#             "WC18313A" : "fn_18313",
#             "WC18312A": "fn_18312",
#             #     "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
#             "WC18310A": "fn_18310",
#             "WC18311A" : "fn_18311",
#             "WC18309A" : "fn_18309",
#             "WC18308A": "fn_18308",
#             "WC18269A": "fn_18269",
#             "WC18304A" : "fn_18304",
#             "WC18266A" : "fn_18266",
#             #     "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
#             "WC18267A": "fn_18267",
#             "WC18265A": "fn_18265",
#             "WC18264A" : "fn_18264",
#             "WC18263A" : "fn_18263",
#             "WC18262A": "fn_18262",
#             "WC18199A": "fn_18199",
#             "WC18158A" : "fn_18158",
#             #     "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
#             "WC18100A": "fn_18100",
#             "WC08001A": "fn_8001",
#             "WC05085A" : "fn_5085",
#             "WC03101A" : "fn_3101",
#             "WC02501A": "fn_2501",
#             "WC02201A": "fn_2201",
#             "WC02101A" : "fn_2101",
#             #     "WC02001A", "WC05575A"]
#             "WC02001A" : "fn_2001",
#             "WC05575A" : "fn_5575",
#             "index" : "period_end",
#             # "WC01451A", "WC18810A", "WC02401A", "WC18274A", "WC03040A"
#             "WC01451A" : "fn_1451",
#             "WC18810A" : "fn_18810",
#             "WC02401A" : "fn_2401",
#             "WC18274A" : "fn_18274",
#             "WC03040A" : "fn_3040"
#         })
#         result = result.reset_index(inplace=False)
#         # print(result)
#         result["period_end"] = pd.to_datetime(result["period_end"])
#         result["year"] = pd.DatetimeIndex(result["period_end"]).year
#         result["month"] = pd.DatetimeIndex(result["period_end"]).month
#         result["day"] = pd.DatetimeIndex(result["period_end"]).day
#         # print(result)
#         for index, row in result.iterrows():
#             if (result.loc[index, "month"] <= 3) and (result.loc[index, "day"] <= 31) :
#                 result.loc[index, "month"] = 3
#                 result.loc[index, "frequency_number"] = int(1)
#             elif (result.loc[index, "month"] <= 6) and (result.loc[index, "day"] <= 31) :
#                 result.loc[index, "month"] = 6
#                 result.loc[index, "frequency_number"] = int(2)
#             elif (result.loc[index, "month"] <= 9) and (result.loc[index, "day"] <= 31) :
#                 result.loc[index, "month"] = 9
#                 result.loc[index, "frequency_number"] = int(3)
#             else:
#                 result.loc[index, "month"] = 12
#                 result.loc[index, "frequency_number"] = int(4)

#             result.loc[index, "period_end"] = datetime(result.loc[index, "year"], result.loc[index, "month"], 1)
#         result["period_end"] = result["period_end"].dt.to_period("M").dt.to_timestamp("M")
#         result["period_end"] = pd.to_datetime(result["period_end"])
        
#         result = uid_maker(result, trading_day="period_end")
#         result["fiscal_quarter_end"] = result["period_end"].astype(str)
#         result["fiscal_quarter_end"] = result["fiscal_quarter_end"].str.replace("-", "", regex=True)
#         result = result.drop(columns=["month", "day", "index"])
#         identifier = universe[["ticker", "worldscope_identifier"]]
#         result = result.merge(identifier, how="left", on="ticker")
#         result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)
#         print(result)
#         upsert_data_to_database(result, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)


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
    if (len(result)) > 0:
        if "WC05905A" in filter_field:
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
            "WC03501A": "fn_3501",
            "WC18313A": "fn_18313",
            "WC18312A": "fn_18312",
            #     "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
            "WC18310A": "fn_18310",
            "WC18311A": "fn_18311",
            "WC18309A": "fn_18309",
            "WC18308A": "fn_18308",
            "WC18269A": "fn_18269",
            "WC18304A": "fn_18304",
            "WC18266A": "fn_18266",
            #     "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
            "WC18267A": "fn_18267",
            "WC18265A": "fn_18265",
            "WC18264A": "fn_18264",
            "WC18263A": "fn_18263",
            "WC18262A": "fn_18262",
            "WC18199A": "fn_18199",
            "WC18158A": "fn_18158",
            #     "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
            "WC18100A": "fn_18100",
            "WC08001A": "fn_8001",
            "WC05085A": "fn_5085",
            "WC03101A": "fn_3101",
            "WC02501A": "fn_2501",
            "WC02201A": "fn_2201",
            "WC02101A": "fn_2101",
            #     "WC02001A"]
            "WC02001A": "fn_2001",
            "index": "period_end"
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
        result = result.drop(columns=["month", "day", "level_0"])
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
    # filter_field = ["WC02001A", "WC05905A"]
    identifier="ticker"
    for field in filter_field:
        worldscope(universe, start_date, end_date, [field], identifier)
        gc.collect()