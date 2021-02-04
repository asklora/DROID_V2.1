import os
import sys
from dotenv import load_dotenv
load_dotenv()
from general.date_process import (
    relativedelta,
    datetimeNow, 
    backdate_by_day, 
    dlp_start_date, 
    backdate_by_month,
    droid_start_date,
    forwarddate_by_day,
    str_to_date,
    dateNow)
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
from sklearn.preprocessing import robust_scale, minmax_scale, MinMaxScaler
from general.slack import report_to_slack
from general.sql_process import do_function
from general.data_process import tuple_data, uid_maker, remove_null
from general.sql_query import (
    get_data_by_table_name, 
    get_data_by_table_name_with_condition,
    get_latest_price,
    get_vix, 
    get_fundamentals_score,
    get_active_universe,
    get_last_close_industry_code,
    get_active_universe_by_quandl_symbol,
    get_active_universe_by_entity_type, 
    get_universe_rating,
    get_max_last_ingestion_from_universe)
from general.sql_output import delete_old_dividends_on_database, upsert_data_to_database, insert_data_to_database
from datasource.dsws import (
    get_data_static_from_dsws, 
    get_data_history_from_dsws, 
    get_data_history_frequently_from_dsws, 
    get_data_static_with_string_from_dsws,
    get_data_history_frequently_by_field_from_dsws)
from datasource.dss import get_data_from_dss
from datasource.fred import read_fred_csv
from datasource.quandl import read_quandl_csv
from general.table_name import (
    get_data_vix_table_name, 
    get_universe_rating_table_name,
    get_quandl_table_name,
    get_fred_table_name,
    get_fundamental_score_table_name, 
    get_data_dss_table_name, 
    get_data_dsws_table_name,
    get_data_dividend_table_name, 
    get_data_dividend_daily_table_name, 
    get_data_interest_table_name, 
    get_data_interest_daily_table_name,
    get_latest_price_table_name)

# data_dividend
# data_dividend_daily_rates
# data_fundamental_score
# data_interest
# data_interest_daily_rates
# data_split
# data_vol_surface_inferred
def update_data_dss_from_dss(ticker=None, currency_code=None, history=False, manual=False):
    print("{} : === DSS Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    
    if(history):
        start_date = dlp_start_date()
    elif(manual):
        start_date = backdate_by_day(1)
    else:
        start_date = get_max_last_ingestion_from_universe(ticker=ticker, currency_code=currency_code)
        if(start_date=="None"):
            start_date = backdate_by_day(3)
    
    print(f"Ingestion Start From {start_date}")
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    jsonFileName = "files/file_json/historyAPI.json"
    result = get_data_from_dss(start_date, end_date, universe["ticker"].to_list(), jsonFileName, report=os.getenv("REPORT_HISTORY"))
    print(result)
    result = result.drop(columns=["IdentifierType", "Identifier"])
    print(result)
    if (len(result) > 0 ):
        result = result.rename(columns={
            "RIC": "ticker",
            "Trade Date": "trading_day",
            "Open Price": "open",
            "High Price": "high",
            "Low Price": "low",
            "Universal Close Price": "close",
            "Accumulated Volume Unscaled": "volume"
        })
        result = uid_maker(result, uid="dss_id", ticker="ticker", trading_day="trading_day")
        print(result)
        upsert_data_to_database(result, get_data_dss_table_name(), "dss_id", how="update", Text=True)
        report_to_slack("{} : === DSS Updated ===".format(datetimeNow()))

def update_data_dsws_from_dsws(ticker=None, currency_code=None, history=False, manual=False):
    print("{} : === DSWS Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    
    if(history):
        start_date = dlp_start_date()
    elif(manual):
        start_date = backdate_by_day(1)
    else:
        start_date = get_max_last_ingestion_from_universe(ticker=ticker, currency_code=currency_code)
        if(start_date=="None"):
            start_date = backdate_by_day(3)
    
    print(f"Ingestion Start From {start_date}")
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe[["ticker"]]
    identifier="ticker"
    filter_field = ["RI"]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"RI": "total_return_index", "level_1" : "trading_day"})
        result = uid_maker(result, uid="dsws_id", ticker="ticker", trading_day="trading_day")
        print(result)
        result = result[["dsws_id", "ticker", "trading_day", "total_return_index"]]
        upsert_data_to_database(result, get_data_dsws_table_name(), "dsws_id", how="update", Text=True)
        report_to_slack("{} : === DSWS Updated ===".format(datetimeNow()))

def update_vix_from_dsws(vix_id=None, history=False):
    print("{} : === Vix Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_day(3)
    if(history):
        start_date = droid_start_date()
    universe = get_vix(vix_id=vix_id)
    universe = universe[["vix_id"]]
    identifier="vix_id"
    filter_field = ["PI"]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=False, split_number=min(len(universe), 40))
    if(len(result)) > 0 :
        result = result.rename(columns={"PI": "vix_value", "level_1" : "trading_day"})
        result = uid_maker(result, uid="uid", ticker="vix_id", trading_day="trading_day")
        result["vix_id"] = result["vix_id"] 
        print(result)
        # upsert_data_to_database(result, get_data_vix_table_name(), "uid", how="update", Text=True)
        # report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_fred_data_from_fred():
    print("{} : === Vix Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    result = read_fred_csv(start_date, end_date)
    result["data"] = np.where(result["data"]== ".", 0, result["data"])
    result["data"] = result["data"].astype(float)
    if(len(result)) > 0 :
        upsert_data_to_database(result, get_fred_table_name(), "uid", how="update", Text=True)
        #insert_data_to_database(result, get_fred_table_name(), how="replace")
        report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_quandl_orats_from_quandl(ticker=None, quandl_symbol=None):
    print("{} : === Quandl Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    print("=== Getting data from Quandl ===")
    result = pd.DataFrame({"date":[],"stockpx":[],
        "iv30":[],"iv60":[],"iv90":[],
        "m1atmiv":[],"m1dtex":[],
        "m2atmiv":[],"m2dtex":[],
        "m3atmiv":[],"m3dtex":[],
        "m4atmiv":[],"m4dtex":[],
        "slope":[],"deriv":[],
        "slope_inf":[],"deriv_inf":[],
        "10dclsHV":[],"20dclsHV":[],"60dclsHV":[],"120dclsHV":[],"252dclsHV":[],
        "10dORHV":[],"20dORHV":[],"60dORHV":[],"120dORHV":[],"252dORHV":[]}, index=[])
    quandl_symbol_list = get_active_universe_by_quandl_symbol(ticker=ticker, quandl_symbol=quandl_symbol)
    print(quandl_symbol_list)
    for index, row in quandl_symbol_list.iterrows():
        data_from_quandl = read_quandl_csv(row["ticker"], row["quandl_symbol"], start_date, end_date)
        if (len(data_from_quandl) > 0):
            result = result.append(data_from_quandl)
    print("=== Getting data from Quandl DONE ===")
    result = result[["uid", "ticker", "trading_day","stockpx",
        "iv30","iv60","iv90",
        "m1atmiv", "m1dtex","m2atmiv","m2dtex",
        "m3atmiv","m3dtex","m4atmiv","m4dtex",
        "slope","deriv","slope_inf", "deriv_inf"]]
    if(len(result)) > 0 :
        print(result)
        # if type(ticker) != type(None) or type(quandl_symbol) != type(None):
        #     upsert_data_to_database(result, get_quandl_table_name(), "uid", how="update", Text=True)
        # else:
        #     upsert_data_to_database(result, get_quandl_table_name(), "uid", how="update", Text=True)
        #     #insert_data_to_database(result, get_quandl_table_name(), how="replace")
        upsert_data_to_database(result, get_quandl_table_name(), "uid", how="update", Text=True)
        do_function("data_vol_surface_update")
        report_to_slack("{} : === Quandl Updated ===".format(datetimeNow()))
    # do_function("calculate_latest_vol_updates_us")

    # report_to_slack("{} : === Quandl Ingested ===".format(str(datetime.now())), args)
    # print("=== Quandl Orats DONE ===")
    # try:
    #     r = requests.get(f"{args.urlAPIAsklora}/api-helper/calc_latest_bot/?index=US")
    #     if r.status_code == 200:
    #         report_to_slack("{} : === LATEST BOT UPDATES QUANDL SUCCESS === : ".format(str(datetime.now())), args)
    # except Exception as e:
    #     report_to_slack("{} : === LATEST BOT UPDATES QUANDL ERROR === : {}".format(str(datetime.now()), e), args)
    #     print(e)

def update_fundamentals_score_from_dsws(ticker=None, currency_code=None):
    print("{} : === Fundamentals Score Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_month(12)
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    print(universe)
    filter_field = ["EPS1TR12","WC05480","WC18100A","WC18262A","WC08005",
        "WC18309A","WC18311A","WC18199A","WC08372","WC05510","WC08636A",
        "BPS1FD12","EBD1FD12","EVT1FD12","EPS1FD12","SAL1FD12","CAP1FD12"]
    
    column_name = {"EPS1TR12": "eps", "WC05480": "bps", "WC18100A": "ev",
        "WC18262A": "ttm_rev","WC08005": "mkt_cap","WC18309A": "ttm_ebitda",
        "WC18311A": "ttm_capex","WC18199A": "net_debt","WC08372": "roe",
        "WC05510": "cfps","WC08636A": "peg","BPS1FD12" : "bps1fd12",
        "EBD1FD12" : "ebd1fd12","EVT1FD12" : "evt1fd12","EPS1FD12" : "eps1fd12",
        "SAL1FD12" : "sal1fd12","CAP1FD12" : "cap1fd12"}
    result, except_field = get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1, quarterly=True, fundamentals_score=True)
    print("Error Ticker = " + str(except_field))
    if len(except_field) == 0 :
        second_result = []
    else:
        second_result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, except_field, identifier, filter_field, use_ticker=True, split_number=1, quarterly=True, fundamentals_score=True)
    try:
        if(len(result) == 0):
            result = second_result
        elif(len(second_result) == 0):
            result = result
        else :
            result = result.append(second_result)
    except Exception as e:
        result = second_result
    print(result)
    result = result.rename(columns=column_name)
    result.reset_index(inplace = True)
    result = result.drop(columns={"index", "level_0"})
    print(result)
    if(len(universe)) > 0 :
        upsert_data_to_database(result, get_fundamental_score_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Fundamentals Score Updated ===".format(datetimeNow()))

#get_fundamentals_score(ticker=None, currency_code=None)
#get_last_close_industry_code(ticker=None, currency_code=None)

def update_fundamentals_quality_value(ticker=None, currency_code=None):
    print("{} : === Fundamentals Quality & Value Start Calculate ===".format(datetimeNow()))
    universe_rating = get_universe_rating(ticker=ticker, currency_code=currency_code)
    universe_rating = universe_rating.drop(columns=["fundamentals_value", "fundamentals_quality", "updated"])
    print("=== Calculating Fundamentals Value & Fundamentals Quality ===")
    calculate_column = ["earnings_yield", "book_to_price", "ebitda_to_ev", "sales_to_price", "roic", "roe", "cf_to_price", "eps_growth", 
                        "fwd_bps","fwd_ebitda_to_ev", "fwd_ey", "fwd_sales_to_price", "fwd_roic"]
    fundamentals_score = get_fundamentals_score(ticker=ticker, currency_code=currency_code)
    print(fundamentals_score)
    close_price = get_last_close_industry_code(ticker=ticker, currency_code=currency_code)
    print(close_price)
    fundamentals_score = close_price.merge(fundamentals_score, how="left", on="ticker")
    fundamentals_score["earnings_yield"] = fundamentals_score["eps"] / fundamentals_score["close"]
    fundamentals_score["book_to_price"] = fundamentals_score["bps"] / fundamentals_score["close"]
    fundamentals_score["ebitda_to_ev"] = fundamentals_score["ttm_ebitda"] / fundamentals_score["ev"]
    fundamentals_score["sales_to_price"] = fundamentals_score["ttm_rev"] / fundamentals_score["mkt_cap"]
    fundamentals_score["roic"] = (fundamentals_score["ttm_ebitda"] - fundamentals_score["ttm_capex"]) / (fundamentals_score["mkt_cap"] + fundamentals_score["net_debt"])
    fundamentals_score["roe"] = fundamentals_score["roe"]
    fundamentals_score["cf_to_price"] = fundamentals_score["cfps"] / fundamentals_score["close"]
    fundamentals_score["eps_growth"] = fundamentals_score["peg"]
    fundamentals_score["fwd_bps"] = fundamentals_score["bps1fd12"]  / fundamentals_score["close"]
    fundamentals_score["fwd_ebitda_to_ev"] = fundamentals_score["ebd1fd12"]  / fundamentals_score["evt1fd12"]
    fundamentals_score["fwd_ey"] = fundamentals_score["eps1fd12"]  / fundamentals_score["close"]
    fundamentals_score["fwd_sales_to_price"] = fundamentals_score["sal1fd12"]  / fundamentals_score["mkt_cap"]
    fundamentals_score["fwd_roic"] = (fundamentals_score["ebd1fd12"] - fundamentals_score["cap1fd12"]) / (fundamentals_score["mkt_cap"] + fundamentals_score["net_debt"])
    fundamentals = fundamentals_score[["earnings_yield", "book_to_price", "ebitda_to_ev", "sales_to_price", 
        "roic", "roe", "cf_to_price", "eps_growth", "currency_code", "ticker", "industry_code","fwd_bps",
        "fwd_ebitda_to_ev","fwd_ey", "fwd_sales_to_price", "fwd_roic"]]
    print(fundamentals)

    calculate_column_score = []
    for column in calculate_column:
        column_score = column + "_score"
        mean = np.nanmean(fundamentals[column])
        std = np.nanstd(fundamentals[column])
        upper = mean + (std * 2)
        lower = mean - (std * 2)
        fundamentals[column_score] = np.where(fundamentals[column] > upper, upper, fundamentals[column])
        fundamentals[column_score] = np.where(fundamentals[column_score] < lower, lower, fundamentals[column_score])
        calculate_column_score.append(column_score)
    print(calculate_column_score)
    calculate_column_robust_score = []
    for column in calculate_column:
        column_score = column + "_score"
        column_robust_score = column + "_robust_score"
        fundamentals[column_robust_score] = robust_scale(fundamentals[column_score])
        calculate_column_robust_score.append(column_robust_score)
    # fundamentals_robust_score_calc = fundamentals[calculate_column_robust_score]
    # scaler = MinMaxScaler().fit(fundamentals_robust_score_calc)
    for column in calculate_column:
        column_score = column + "_score"
        column_robust_score = column + "_robust_score"
        column_minmax_currency_code = column + "_minmax_currency_code"
        column_minmax_industry = column + "_minmax_industry"
        df_currency_code = fundamentals[["currency_code", column_robust_score]]
        df_currency_code = df_currency_code.rename(columns = {column_robust_score : "score"})
        print(df_currency_code)
        df_industry = fundamentals[["industry_code", column_robust_score]]
        df_industry = df_industry.rename(columns = {column_robust_score : "score"})
        print(df_industry)
        fundamentals[column_minmax_currency_code] = df_currency_code.groupby("currency_code").score.transform(lambda x: minmax_scale(x.astype(float)))
        fundamentals[column_minmax_industry] = df_industry.groupby("industry_code").score.transform(lambda x: minmax_scale(x.astype(float)))

        fundamentals[column_minmax_currency_code] = np.where(fundamentals[column_minmax_currency_code].isnull(), 0.4, fundamentals[column_minmax_currency_code])

        fundamentals[column_minmax_industry] = np.where(fundamentals[column_minmax_industry].isnull(), 0.4, fundamentals[column_minmax_industry])

    #TWELVE points - everthing average yields 0.5 X 12 = 6.0 score
    fundamentals["fundamentals_value"] = ((fundamentals["earnings_yield_minmax_currency_code"]) + 
                                    fundamentals["earnings_yield_minmax_industry"] + 
                                    fundamentals["book_to_price_minmax_currency_code"] + 
                                    fundamentals["book_to_price_minmax_industry"] + 
                                    fundamentals["ebitda_to_ev_minmax_currency_code"] + 
                                    fundamentals["ebitda_to_ev_minmax_industry"] +
                                    fundamentals["fwd_bps_minmax_industry"] + 
                                    fundamentals["fwd_ebitda_to_ev_minmax_currency_code"] + 
                                    fundamentals["fwd_ebitda_to_ev_minmax_industry"] + 
                                    fundamentals["fwd_ey_minmax_currency_code"]+ 
                                    fundamentals["roe_minmax_industry"]+ 
                                    fundamentals["cf_to_price_minmax_currency_code"]).round(1)
    fundamentals["fundamentals_quality"] = ((fundamentals["roic_minmax_currency_code"]) + 
                                      fundamentals["roic_minmax_industry"]+
                                      fundamentals["cf_to_price_minmax_industry"]+
                                      fundamentals["eps_growth_minmax_currency_code"] + 
                                      fundamentals["eps_growth_minmax_industry"] + 
                                      (fundamentals["fwd_ey_minmax_industry"] *2) + 
                                      fundamentals["fwd_sales_to_price_minmax_industry"]+ 
                                      (fundamentals["fwd_roic_minmax_industry"] *2) +
                                      fundamentals["earnings_yield_minmax_industry"]).round(1)

    print("=== Calculate Fundamentals Value & Fundamentals Quality DONE ===")
    if(len(fundamentals)) > 0 :
        print(fundamentals)
        result = universe_rating.merge(fundamentals[["ticker", "fundamentals_value", "fundamentals_quality"]], how="left", on="ticker")
        result["updated"] = dateNow()
        print(result)
        upsert_data_to_database(result, get_universe_rating_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Universe Fundamentals Quality & Value Updated ===".format(datetimeNow()))

def dividend_updated(ticker=None, currency_code=None):
    print("{} : === Dividens Update ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    identifier="ticker"
    filter_field = ["UDDE"]
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe[["ticker"]]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1)
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"UDDE": "amount", "level_1":"ex_dividend_date"})
        result = result.dropna(subset=["amount"])
        result = remove_null(result, "amount")
        result = uid_maker(result, uid="uid", ticker="ticker", trading_day="ex_dividend_date")
        print(result)
        delete_old_dividends_on_database()
        upsert_data_to_database(result, get_data_dividend_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === Dividens Updated ===".format(datetimeNow()))

def dividend_daily_update():
    print("{} : === Dividens Daily Update ===".format(datetimeNow()))
    dividend_data = get_data_by_table_name(get_data_dividend_table_name())
    universe = get_active_universe()
    #intraday_price = get_data_by_table_name(get_latest_price_table_name())
    intraday_price = get_latest_price()
    dividend_data = dividend_data[dividend_data.ticker.isin(universe.ticker)]
    dividend_data = dividend_data.merge(universe[["ticker", "currency_code"]], on="ticker", how="left")
    dividend_data["ex_dividend_date"] = pd.to_datetime(dividend_data["ex_dividend_date"])
    days_q = pd.DataFrame(range(1, int(os.getenv("DAILY_Q_DAYS"))))
    dates_temp = days_q
    dates_temp["spot_date"] = str_to_date(dateNow())
    def calculate_expiry(row):
        return (row["spot_date"] + BDay(row[0]))
    dates_temp["expiry_date"] = dates_temp.apply(calculate_expiry, axis=1)
    dates_temp["spot_date"] = pd.to_datetime(dates_temp["spot_date"])
    dates_temp["expiry_date"] = pd.to_datetime(dates_temp["expiry_date"])
    dates_temp3 = dates_temp.copy()
    result = pd.DataFrame()
    for ticker in dividend_data.ticker.unique():
        temp_q = pd.DataFrame()
        dates_temp = pd.DataFrame(dates_temp3)
        dates_temp2 = dates_temp.copy()
        dates_temp = dates_temp2
        dates_temp["id"] = 2
        dividend_data["id"] = 2
        dates_temp = pd.merge(dates_temp, dividend_data[dividend_data.ticker == ticker], on="id", how="outer")
        dates_temp = dates_temp[(dates_temp.spot_date <= dates_temp.ex_dividend_date) & (dates_temp.ex_dividend_date <= dates_temp.expiry_date)]
        dates_temp = dates_temp.groupby(["expiry_date"])["amount"].sum().reset_index()
        dates_temp2 = dates_temp2.merge(dates_temp, on="expiry_date", how="left").fillna(0)
        temp_q["q"] = dates_temp2["amount"] / intraday_price.loc[intraday_price.ticker == ticker, "close"].values
        temp_q["ticker"] = ticker
        temp_q["spot_date"] = dates_temp2["spot_date"]
        temp_q["expiry_date"] = dates_temp2["expiry_date"]
        temp_q["t"] = days_q[0]
        result = result.append(temp_q)
    result = result.merge(universe[["ticker", "currency_code"]], on="ticker", how="left")
    result = uid_maker(result, uid="uid", ticker="ticker", trading_day="t", date=False)
    print(result)
    #insert_data_to_database(result, get_data_dividend_daily_table_name, how="replace")
    upsert_data_to_database(result, get_data_dividend_daily_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Dividens Daily Updated ===".format(datetimeNow()))

def interest_update():
    print("{} : === Interest Update ===".format(datetimeNow()))
    universe = get_data_by_table_name(get_data_interest_table_name())
    print(universe)
    data = pd.DataFrame({"ticker_interest":[],"raw_data":[],
                    "currency_code":[],"days_to_maturity":[],"ingestion_field":[],
                    "maturity":[],"updated":[], "rate":[]}, index=[])
    identifier = "ticker_interest"
    print("== Calculating Interest Rate ==")
    for index, row in universe.iterrows():
        ticker_interest = row["ticker_interest"]
        currency_code = row["currency_code"]
        ingestion_field = row["ingestion_field"]
        days_to_maturity = row["days_to_maturity"]
        filter_field = ingestion_field.split(",")
        #result = get_data_static_from_dsws(ticker_interest, identifier, filter_field, use_ticker=False, split_number=1)
        result = get_data_static_with_string_from_dsws(identifier, ticker_interest, filter_field)
        print(result)
        try:
            if (result.loc[0, "RY"] != "NA"):
                result["raw_data"] = result.loc[0, "RY"]
                result = result.drop(columns="RY")
            else:
                result["raw_data"] = 0
                result = result.drop(columns="RY")
        except Exception as e:
            print(e)

        try:
            if (result.loc[0, "IR"] != "NA"):
                result["raw_data"] = result.loc[0, "IR"]
                result = result.drop(columns={"IR", "IB", "IO"})
            elif (result.loc[0, "IB"] != "NA") and (result.loc[0, "IO"] != "NA"):
                result["raw_data"] = (result.loc[0, "IB"] + result.loc[0, "IO"]) / 2
                result = result.drop(columns={"IR", "IB", "IO"})
            elif (result.loc[0, "IB"] != "NA"):
                result["raw_data"] = result.loc[0, "IB"]
                result = result.drop(columns={"IR", "IB", "IO"})
            else:
                result["raw_data"] = result.loc[0, "IO"]
                result = result.drop(columns={"IR", "IB", "IO"})
        except Exception as e:
            print(e)
        result["currency_code"] = currency_code
        result["days_to_maturity"] = days_to_maturity
        result["ingestion_field"] = ingestion_field
        result["maturity"] = forwarddate_by_day(days_to_maturity)
        result["updated"] = dateNow()
        result["rate"] = result["raw_data"] / 100
        data = data.append(result)
    data.reset_index(inplace=True)
    data = data.drop(columns={"index"})
    print(data)
    print("== Interest Rate Calculated ==")
    upsert_data_to_database(data, get_data_interest_table_name(), "ticker_interest", how="update", Text=True)
    report_to_slack("{} : === Interest Updated ===".format(datetimeNow()))

def interest_daily_update(currency_code=None):
    def cal_interest_rate(interest_rate_data, days_to_expiry):
        unique_horizons = pd.DataFrame(days_to_expiry)
        unique_horizons["id"] = 2
        unique_currencies = pd.DataFrame(interest_rate_data["currency_code"].unique())
        unique_currencies["id"] = 2
        rates = pd.merge(unique_horizons, unique_currencies, on="id", how="outer")
        rates = rates.rename(columns={"0_y": "currency_code", "0_x": "days_to_expiry"})
        interest_rate_data = interest_rate_data.rename(columns={"days_to_maturity": "days_to_expiry"})
        rates = pd.merge(rates, interest_rate_data, on=["days_to_expiry", "currency_code"], how="outer")
        def funs(df):
            df = df.sort_values(by="days_to_expiry")
            df = df.reset_index()
            nan_index = df["rate"].index[df["rate"].isnull()].to_series().reset_index(drop=True)
            not_nan_index = df["rate"].index[~df["rate"].isnull()].to_series().reset_index(drop=True)
            for a in nan_index:
                temp = not_nan_index.copy()
                temp[len(temp)] = a
                temp = temp.sort_values()
                temp.reset_index(inplace=True, drop=True)
                ind = temp[temp == a].index
                ind1 = temp.iloc[ind - 1]
                ind2 = temp.iloc[ind + 1]
                rate_1 = df.loc[ind1, "rate"].iloc[0]
                rate_2 = df.loc[ind2, "rate"].iloc[0]
                dtm_1 = df.loc[ind1, "days_to_expiry"].iloc[0]
                dtm_2 = df.loc[ind2, "days_to_expiry"].iloc[0]
                df.loc[a, "rate"] = rate_1 * (dtm_2 - df.loc[a, "days_to_expiry"])/(dtm_2 - dtm_1) + rate_2* (df.loc[a, "days_to_expiry"] - dtm_1)/(dtm_2 - dtm_1)
            df = df.set_index("index")
            return df
        rates = rates.groupby("currency_code").apply(lambda x: funs(x))
        rates = rates.reset_index(drop=True)
        unique_horizons = unique_horizons.rename(columns={0: "days_to_expiry"})
        unique_horizons = pd.merge(unique_horizons, rates[["days_to_expiry", "rate"]], on="days_to_expiry", how="inner")
        return unique_horizons["rate"].values
    
    print("{} : === Interest Daily Update ===".format(datetimeNow()))
    if(type(currency_code) == type(None)):
        interest_rate_data = get_data_by_table_name(get_data_interest_table_name())
    else:
        interest_rate_data = get_data_by_table_name_with_condition(get_data_interest_table_name(), f" currency_code in {tuple_data(currency_code)}")

    days_r = pd.DataFrame(range(1, int(os.getenv("DAILY_R_DAYS"))))
    result = pd.DataFrame()

    for currency in interest_rate_data.currency_code.unique():
        temp_r = pd.DataFrame()
        temp_r["r"] = cal_interest_rate(interest_rate_data[interest_rate_data.currency_code == currency], days_r)
        temp_r["t"] = days_r[0]
        temp_r["currency_code"] = currency
        result = result.append(temp_r)
    result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="t", date=False)
    print(result)
    # insert_data_to_database(result, get_data_interest_daily_table_name(), how="replace")
    upsert_data_to_database(result, get_data_interest_daily_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Interest Daily Updated ===".format(datetimeNow()))