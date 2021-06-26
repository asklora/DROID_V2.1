from datasource.fred import read_fred_csv
import pandas as pd
import numpy as np
from datetime import datetime
from general.slack import report_to_slack
from general.date_process import backdate_by_year, dateNow, backdate_by_month, datetimeNow
from general.data_process import uid_maker
from datasource.dsws import get_data_history_frequently_by_field_from_dsws, get_data_history_frequently_from_dsws
from general.table_name import get_data_ibes_monthly_table_name, get_data_ibes_table_name, get_data_macro_monthly_table_name, get_data_macro_table_name, get_data_worldscope_summary_table_name
from general.sql_output import upsert_data_to_database
from general.sql_query import get_active_universe_by_entity_type, get_data_ibes_monthly, get_data_macro_monthly, get_ibes_new_ticker

def populate_ibes_table():
    table_name = get_data_ibes_table_name()
    start_date = backdate_by_month(30)
    ibes_data = get_data_ibes_monthly(start_date)
    ibes_data = ibes_data.drop(columns=["trading_day"])
    upsert_data_to_database(ibes_data, table_name, "uid", how="update", Text=True)
    report_to_slack("{} : === Data IBES Update Updated ===".format(datetimeNow()))


def update_ibes_data_monthly_from_dsws(ticker=None, currency_code=None, history=False):
    end_date = dateNow()
    start_date = backdate_by_month(1)
    filter_field = ["EPS1TR12", "EPS1FD12", "EBD1FD12", "CAP1FD12", "I0EPS", "EPSI1MD"]

    if(history):
        universe = get_ibes_new_ticker()
        start_date = backdate_by_year(4)
        filter_field = ["EPS1TR12", "EPS1FD12"]
    
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    print(universe)
    if len(universe) > 0:
        result, except_field = get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1, monthly=True)
    print("Error Ticker = " + str(except_field))
    if len(except_field) == 0 :
        second_result = []
    else:
        second_result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, except_field, identifier, filter_field, use_ticker=True, split_number=1, monthly=True)
    try:
        if(len(result) == 0):
            result = second_result
        elif(len(second_result) == 0):
            result = result
        else :
            result = result.append(second_result)
    except Exception as e:
        result = second_result
    if(len(result)) > 0 :
        result = result.reset_index()
        result = result.drop(columns=["level_0"])
        
        result = result.rename(columns={"EBD1FD12": "ebd1fd12",
            "CAP1FD12": "cap1fd12",
            "EPS1FD12": "eps1fd12",
            "EPS1TR12": "eps1tr12",
            "EPSI1MD": "epsi1md",
            "I0EPS" : "i0eps",
            "index" : "trading_day"
        })
        result = uid_maker(result)

        result["year"] = pd.DatetimeIndex(result["trading_day"]).year
        result["month"] = pd.DatetimeIndex(result["trading_day"]).month

        result["year"] = np.where(result["month"].isin([1, 2]), result["year"] - 1, result["year"])
        result.loc[result["month"].isin([12, 1, 2]), "period_end"] = result["year"].astype(str) + "-" + "12-31"
        result.loc[result["month"].isin([3, 4, 5]), "period_end"] = result["year"].astype(str) + "-" + "03-31"
        result.loc[result["month"].isin([6, 7, 8]), "period_end"] = result["year"].astype(str) + "-" + "06-30"
        result.loc[result["month"].isin([9, 10, 11]), "period_end"] = result["year"].astype(str) + "-" + "09-30"
        result["year"] = np.where(result["month"].isin([1, 2]), result["year"] + 1, result["year"])
        result = result.drop(columns=["month", "year"])
        print(result)
        upsert_data_to_database(result, get_data_ibes_monthly_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === Data IBES Monthly Update Updated ===".format(datetimeNow()))
        populate_ibes_table()

def get_fred_csv_monthly():
    end_date = dateNow()
    start_date = backdate_by_month(6)
    print(f"=== Read Fred Data ===")
    try :
        data = read_fred_csv(start_date, end_date)
        data["data"] = np.where(data["data"]== ".", 0, data["data"])
        data["trading_day"] = pd.to_datetime(data["trading_day"])
        data["data"] = data["data"].astype(float)
        data = data.loc[data["data"] > 0]
        data["year"] = pd.DatetimeIndex(data["trading_day"]).year
        data["month"] = pd.DatetimeIndex(data["trading_day"]).month
        data = data.sort_values(by=["trading_day"], ascending=True)
        data = data.drop_duplicates(subset=["year", "month"], keep="last", inplace=False)
        data = data.drop(columns=["trading_day"])
        return data
    except Exception as ex:
        print("error: ", ex)
        return []

def populate_macro_table():
    table_name = get_data_macro_table_name()
    end_date = dateNow()
    start_date = backdate_by_month(30)
    macro_data = get_data_macro_monthly(start_date)
    macro_data = macro_data.drop(columns=["trading_day"])
    macro_data = macro_data.dropna(subset=["period_end"])
    upsert_data_to_database(macro_data, table_name, "period_end", how="update", Text=True)
    report_to_slack("{} : === Data MACRO Update Updated ===".format(datetimeNow()))

def update_macro_data_monthly_from_dsws():
    print("Get Macro Data From DSWS")
    end_date = dateNow()
    start_date_month = backdate_by_month(3)
    start_date_quarter = backdate_by_month(6)
    ticker_quarterly_field = ["CHGDP...C", "JPGDP...D", "USGDP...D", "EMGDP...D"]
    ticker_monthly_field = ["USINTER3", "USGBILL3", "EMIBOR3.", "JPMSHORT", "EMGBOND.", "CHGBOND."]
    ticker_field = ["EMIBOR3.", "USGBILL3", "CHGDP...C", "EMGDP...D", "USGDP...D", "USINTER3", "EMGBOND.", "JPMSHORT", "CHGBOND.", "JPGDP...D"]
    filter_field = ["ESA"]
    identifier="ticker"
    result_monthly, except_field = get_data_history_frequently_from_dsws(start_date_month, end_date, ticker_monthly_field, identifier, filter_field, use_ticker=False, split_number=1, monthly=True)
    result_quarterly, except_field = get_data_history_frequently_from_dsws(start_date_quarter, end_date, ticker_quarterly_field, identifier, filter_field, use_ticker=False, split_number=1, quarterly=True)
    print(result_monthly)
    print(result_quarterly)
    if(len(result_monthly)) > 0 :
        result_monthly = result_monthly.reset_index()
        result_monthly = result_monthly.drop(columns=["level_0"])
        result_monthly = result_monthly.rename(columns={"index" : "trading_day"})
        result_monthly["year"] = pd.DatetimeIndex(result_monthly["trading_day"]).year
        result_monthly["month"] = pd.DatetimeIndex(result_monthly["trading_day"]).month
        data_monthly = result_monthly.loc[result_monthly["ticker"] == "USINTER3"][["trading_day"]]
        data_monthly["year"] = pd.DatetimeIndex(data_monthly["trading_day"]).year
        data_monthly["month"] = pd.DatetimeIndex(data_monthly["trading_day"]).month
        data_monthly = data_monthly.drop_duplicates(subset=["month"], keep="first", inplace=False)
        data_monthly = data_monthly.set_index("month")
        for index, row in result_monthly.iterrows():
            ticker_field = row["ticker"]
            month = row["month"]
            if (row["ticker"] == "USGBILL3") :
                ticker_field = "usgbill3"
            elif (row["ticker"] == "USINTER3") :
                ticker_field = "usinter3"
            elif (row["ticker"] == "JPMSHORT") :
                ticker_field = "jpmshort"
            elif (row["ticker"] == "EMGBOND.") :
                ticker_field = "emgbond"
            elif (row["ticker"] == "CHGBOND.") :
                ticker_field = "chgbond"
            elif (row["ticker"] == "EMIBOR3.") :
                ticker_field = "emibor3"
            data_field = row["ESA"]
            data_monthly.loc[month, ticker_field] = data_field
        data_monthly = data_monthly.reset_index(inplace=False)
        data_monthly = data_monthly.sort_values(by="trading_day", ascending=False)
        fred_data = get_fred_csv_monthly()
        data_monthly = data_monthly.merge(fred_data, how="left", on=["year", "month"])
        data_monthly.loc[data_monthly["month"].isin([12, 1, 2]), "quarter"] = 1
        data_monthly.loc[data_monthly["month"].isin([3, 4, 5]), "quarter"] = 2
        data_monthly.loc[data_monthly["month"].isin([6, 7, 8]), "quarter"] = 3
        data_monthly.loc[data_monthly["month"].isin([9, 10, 11]), "quarter"] = 4
        data_monthly["year"] = np.where(data_monthly["month"] == 12, data_monthly["year"] + 1, data_monthly["year"])
        print(data_monthly)
    if(len(result_quarterly)) > 0 :
        result_quarterly = result_quarterly.rename(columns={"index": "trading_day"})
        data_quarterly = result_quarterly.loc[result_quarterly["ticker"] == "CHGDP...C"][["trading_day"]]
        result_quarterly["year"] = pd.DatetimeIndex(result_quarterly["trading_day"]).year
        result_quarterly["month"] = pd.DatetimeIndex(result_quarterly["trading_day"]).month
        data_quarterly["year"] = pd.DatetimeIndex(data_quarterly["trading_day"]).year
        data_quarterly["month"] = pd.DatetimeIndex(data_quarterly["trading_day"]).month
        data_quarterly = data_quarterly.set_index("month")
        for index, row in result_quarterly.iterrows():
            ticker_field = row["ticker"]
            if (row["ticker"] == "CHGDP...C") :
                ticker_field = "chgdp"
            elif (row["ticker"] == "JPGDP...D") :
                ticker_field = "jpgdp"
            elif (row["ticker"] == "USGDP...D") :
                ticker_field = "usgdp"
            elif (row["ticker"] == "EMGDP...D") :
                ticker_field = "emgdp"
            month = row["month"]
            data_field = row["ESA"]
            data_quarterly.loc[month, ticker_field] = data_field
            #data_quarterly.loc[month, "period_end"] = datetime.strptime("", "%Y-%m-%d")
        data_quarterly = data_quarterly.reset_index(inplace=False)
        data_quarterly.loc[data_quarterly["month"].isin([12, 1, 2]), "quarter"] = 1
        data_quarterly.loc[data_quarterly["month"].isin([3, 4, 5]), "quarter"] = 2
        data_quarterly.loc[data_quarterly["month"].isin([6, 7, 8]), "quarter"] = 3
        data_quarterly.loc[data_quarterly["month"].isin([9, 10, 11]), "quarter"] = 4
        for index, row in data_quarterly.iterrows():
            if (row["quarter"] == 1):
                if(row["month"] == 12):
                    period_end = str(row["year"]) + "-" + "12-31"
                else:
                    period_end = str(row["year"] - 1) + "-" + "12-31"
            elif (row["quarter"] == 2):
                period_end = str(row["year"]) + "-" + "03-31"
            elif (row["quarter"] == 3):
                period_end = str(row["year"]) + "-" + "06-30"
            else:
                period_end = str(row["year"]) + "-" + "09-30"
            data_quarterly.loc[index, "period_end"] = datetime.strptime(period_end, "%Y-%m-%d")
        data_quarterly["year"] = np.where(data_quarterly["month"] == 12, data_quarterly["year"] + 1, data_quarterly["year"])
        data_quarterly = data_quarterly.drop(columns=["month", "trading_day"])
        print(data_quarterly)
    result = data_monthly.merge(data_quarterly, how="left", on=["year", "quarter"])
    result = result.drop(columns=["month", "year", "quarter"])
    print(result)
    result = result.dropna(subset=["period_end"])
    upsert_data_to_database(result, get_data_macro_monthly_table_name(), "trading_day", how="update", Text=True)
    report_to_slack("{} : === Data MACRO Monthly Update Updated ===".format(datetimeNow()))
    populate_macro_table()


def update_worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None):
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    if(len(universe) < 1):
        return False
    end_date = dateNow()
    start_date = backdate_by_month(12)
    filter_field = [
        "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
        "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
        "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
        "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
        "WC02001A", "WC05905A"]
    identifier="ticker"
    ticker = universe[["ticker"]]
    ticker = ticker["ticker"].tolist()
    # ticker = ["<" + tick + ">" for tick in ticker]
    # result = get_data_quarterly_from_dsws_by_field(args, start_date, end_date, ticker, identifier, filter_field, use_ticker=True, identifier = identifier)
    result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, ticker, identifier, filter_field, use_ticker=True, split_number=1, quarterly=True, worldscope=True)
    print(result)
    if(len(result)) > 0 :
        result["WC05905A"] = result["WC05905A"].astype(str)
        result = result.reset_index()
        result = result.drop(columns=["level_0"])
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
        result = result.drop(columns=["level_0"])
        for field in filter_field:
            if field != "WC06035" and field!="WC05905A":
                result[field] = np.where(result[field] == "NA", np.nan, result[field])
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
            "WC02001A" : "fn_2001"
        })
        result["period_end"] = result["report_date"]
        result["year"] = pd.DatetimeIndex(result["period_end"]).year
        result["month"] = pd.DatetimeIndex(result["period_end"]).month
        result["day"] = pd.DatetimeIndex(result["period_end"]).day

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
        result["period_end"] = result["period_end"].dt.to_period("M").dt.to_timestamp("M")
        result["period_end"] = pd.to_datetime(result["period_end"])
        
        result = uid_maker(result, trading_day="period_end")
        result["fiscal_quarter_end"] = result["period_end"].astype(str)
        result["fiscal_quarter_end"] = result["fiscal_quarter_end"].str.replace("-", "", regex=True)
        result = result.drop(columns=["month", "index", "day", "WC05905A"])
        identifier = universe[["ticker", "worldscope_identifier"]]
        result = result.merge(identifier, how="left", on="ticker")
        result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        print(result)
        upsert_data_to_database(result, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === Quarter Summary Data Updated ===".format(datetimeNow()))