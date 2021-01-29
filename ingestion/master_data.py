import sys
from dotenv import load_dotenv
load_dotenv()
from general.date_process import (
    datetimeNow, 
    backdate_by_day, 
    dlp_start_date, 
    backdate_by_month,
    droid_start_date,
    dateNow)
import pandas as pd
import numpy as np
from sklearn.preprocessing import robust_scale, minmax_scale, MinMaxScaler
from general.slack import report_to_slack
from general.sql_process import do_function
from general.data_process import uid_maker
from general.sql_query import (
    get_vix, 
    get_fundamentals_score,
    get_last_close_industry_code,
    get_active_universe_by_quandl_symbol,
    get_active_universe_by_entity_type, 
    get_universe_rating)
from general.sql_output import upsert_data_to_database, insert_data_to_database
from datasource.dsws import get_data_static_from_dsws, get_data_history_from_dsws, get_data_history_frequently_from_dsws, get_data_history_frequently_by_field_from_dsws
from datasource.dss import get_data_from_dss
from datasource.fred import read_fred_csv
from datasource.quandl import read_quandl_csv
from general.table_name import (
    get_vix_table_name, 
    get_universe_rating_table_name,
    get_quandl_table_name,
    get_fred_table_name,
    get_fundamental_score_table_name)

# data_dividend
# data_dividend_daily_rates
# data_dss
# data_dsws
# data_fundamental_score
# data_interest
# data_interest_daily_rates
# data_split
# data_vol_surface_inferred
# subinterval
# top_stock_performance
# top_stock_weekly

def update_vix_from_dsws(vix_id=None, history=False):
    print("{} : === Vix Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_day(3)
    if(history):
        start_date = droid_start_date()
    universe = get_vix()
    universe = universe[["vix_index"]]
    identifier="vix_index"
    filter_field = ["PI"]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=False, split_number=min(len(universe), 40))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"PI": "vix_value", "index" : "trading_day"})
        result = uid_maker(result, uid="uid", ticker="vix_index", trading_day="trading_day")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_vix_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_fred_data_from_fred():
    print("{} : === Vix Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    result = read_fred_csv(start_date, end_date)
    result["data"] = np.where(result["data"]== ".", 0, result["data"])
    result["data"] = result["data"].astype(float)
    if(len(result)) > 0 :
        insert_data_to_database(result, get_fred_table_name(), how="replace")
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
        if type(ticker) != type(None) or type(quandl_symbol) != type(None):
            upsert_data_to_database(result, get_quandl_table_name(), "uid", how="update", Text=True)
        else:
            insert_data_to_database(result, get_quandl_table_name(), how="replace")
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
    filter_field = ["WC05255","WC05480","WC18100A","WC18262A","WC08005",
        "WC18309A","WC18311A","WC18199A","WC08372","WC05510","WC08636A",
        "BPS1FD12","EBD1FD12","EVT1FD12","EPS1FD12","SAL1FD12","CAP1FD12"]
    
    column_name = {"WC05255": "eps", "WC05480": "bps", "WC18100A": "ev",
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
    print(fundamentals)
    if(len(fundamentals)) > 0 :
        upsert_data_to_database(fundamentals, get_universe_rating_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Universe Fundamentals Quality & Value Updated ===".format(datetimeNow()))
    