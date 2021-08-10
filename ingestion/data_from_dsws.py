import numpy as np
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import robust_scale, minmax_scale, quantile_transform
from general.data_process import remove_null, uid_maker
from general.sql_process import do_function
from general.slack import report_to_slack
from general.sql_output import (
    delete_data_on_database, 
    delete_old_dividends_on_database, 
    fill_null_company_desc_with_ticker_name, 
    upsert_data_to_database)
from general.table_name import (
    get_data_dividend_table_name, 
    get_data_dsws_table_name,
    get_data_fred_table_name,
    get_data_ibes_monthly_table_name,
    get_data_ibes_table_name, 
    get_data_interest_table_name,
    get_data_macro_monthly_table_name,
    get_data_macro_table_name, 
    get_data_vix_table_name,
    get_data_worldscope_summary_table_name, 
    get_fundamental_score_table_name, 
    get_industry_table_name, 
    get_industry_worldscope_table_name, 
    get_universe_consolidated_table_name, 
    get_universe_rating_detail_history_table_name, 
    get_universe_rating_history_table_name, 
    get_universe_rating_table_name, 
    get_universe_table_name)
from datasource.dsws import (
    get_data_history_frequently_by_field_from_dsws, 
    get_data_history_frequently_from_dsws, 
    get_data_history_from_dsws, 
    get_data_static_from_dsws, 
    get_data_static_with_string_from_dsws)
from general.sql_query import (
    get_active_universe, 
    get_active_universe_by_entity_type, 
    get_active_universe_company_description_null, 
    get_active_universe_consolidated_by_field, 
    get_all_universe, get_data_by_table_name,
    get_data_ibes_monthly,
    get_data_macro_monthly, 
    get_fundamentals_score,
    get_ibes_new_ticker, 
    get_last_close_industry_code, 
    get_max_last_ingestion_from_universe, 
    get_pred_mean, 
    get_specific_tri, 
    get_universe_rating, 
    get_vix)
from general.date_process import (
    backdate_by_day, 
    backdate_by_month,
    backdate_by_year, 
    dateNow, 
    datetimeNow, 
    dlp_start_date, 
    droid_start_date, 
    forwarddate_by_day)
from datasource.fred import read_fred_csv

def populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=None):
    print("{} : === Ticker ISIN Start Ingestion ===".format(datetimeNow()))
    manual_universe = get_active_universe_consolidated_by_field(manual=True, ticker=ticker)
    manual_universe = manual_universe.loc[manual_universe["use_manual"] == True]
    manual_universe["consolidated_ticker"] = manual_universe["origin_ticker"]

    universe = get_active_universe_consolidated_by_field(isin=True, ticker=ticker)
    universe = universe.loc[universe["use_manual"] == False]
    if(len(universe) > 0):
        universe = universe.drop(columns=["isin", "consolidated_ticker", "sedol", "is_active", "cusip", "permid", "updated"])
        print(universe)
        identifier="origin_ticker"
        filter_field = ["ISIN", "SECD", "WC06004", "IPID"]
        result, error_ticker = get_data_static_from_dsws(universe[["origin_ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 20))
        result = result.rename(columns={"ISIN": "isin", "index":"origin_ticker", "SECD": "sedol", "WC06004": "cusip", "IPID": "permid"})
        
        print(result)

        isin_list = result[["isin"]]
        isin_list = isin_list.drop_duplicates(keep="first", inplace=False)
        result2, error_ticker = get_data_static_from_dsws(isin_list, "isin", ["RIC", "SECD"], use_ticker=False, split_number=min(len(isin_list), 20))
        result2 = result2.rename(columns={"RIC": "consolidated_ticker", "index":"isin", "SECD": "sedol"})
        print(result2)
        result = result.merge(result2, how="left", on=["isin", "sedol"])
        result = result.loc[result["consolidated_ticker"] != "FGBSM2^1"]
        null_result = result.loc[result["isin"] == "NA"]
        null_result["use_manual"] = True
        null_result["isin"] = None
        null_result["sedol"] = None
        null_result["cusip"] = None
        null_result["permid"] = None
        null_result["is_active"] = True
        null_result["use_isin"] = False
        null_result["consolidated_ticker"] = null_result["origin_ticker"]
        print(null_result)
        result = result.loc[result["consolidated_ticker"] != "NA"]
        print(result)
        if(len(result)) > 0 :
            consolidated_ticker = result.loc[result["consolidated_ticker"].notnull()]
            consolidated_ticker["is_active"] = True
            null_consolidated_ticker = result.loc[result["consolidated_ticker"].isnull()]
            #null_consolidated_ticker = remove_null(null_consolidated_ticker)
            null_consolidated_ticker["is_active"] = False
            for index, row in null_consolidated_ticker.iterrows():
                origin_ticker = row["origin_ticker"]
                isin = row["isin"]
                sedol = row["sedol"]
                #find the same isin
                same_isin = consolidated_ticker.loc[consolidated_ticker["isin"] == isin]
                if(len(same_isin) > 0 and isin != "NA"):
                    #find the same sedol
                    same_sedol = same_isin.loc[same_isin["sedol"] == sedol]
                    if(len(same_sedol) == 0 and isin != "NA"):
                        null_consolidated_ticker.loc[index, "consolidated_ticker"] = origin_ticker
                        null_consolidated_ticker.loc[index, "is_active"] = True
            result = consolidated_ticker.append(null_consolidated_ticker)
            result = result.merge(universe, how="left", on=["origin_ticker"])
            
            if(len(manual_universe) > 0):
                result = result.append(manual_universe)
            
            if(len(null_result) > 0):
                result = result.append(null_result)
    else:
        result = manual_universe
    result["updated"] = dateNow()
    print(result)
    upsert_data_to_database(result, get_universe_consolidated_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Ticker ISIN Updated ===".format(datetimeNow()))

def update_ticker_name_from_dsws(ticker=None, currency_code=None):
    print("{} : === Ticker Name Start Ingestion ===".format(datetimeNow()))
    universe = get_all_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["ticker_name", "ticker_fullname"])
    print(universe)
    filter_field = ["WC06003", "NAME"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"WC06003": "ticker_name", "NAME" : "ticker_fullname", "index":"ticker"})
        result["ticker_name"]=result["ticker_name"].str.replace("'", "", regex=True)
        result["ticker_fullname"]=result["ticker_fullname"].str.replace("'", "", regex=True)
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        do_function("universe_populate")
        report_to_slack("{} : === Ticker Name Updated ===".format(datetimeNow()))

def update_entity_type_from_dsws(ticker=None, currency_code=None):
    print("{} : === Entity Type Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["entity_type"])
    filter_field = ["WC06100"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    result = result.rename(columns={"WC06100": "entity_type", "index":"ticker"})
    result = remove_null(result, "entity_type")
    print(result)
    if(len(result)) > 0 :
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Entity Type Updated ===".format(datetimeNow()))

def update_company_desc_from_dsws(ticker=None, currency_code=None):
    print("{} : === Company Description Ingestion ===".format(datetimeNow()))
    universe = get_active_universe_company_description_null(ticker=ticker)
    universe = universe.drop(columns=["company_description"])
    if(len(universe) > 0):
        identifier = "ticker"
        filter_field = ["WC06092"]
        result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=1)
        if(len(result) > 0):
            result = result.rename(columns={"WC06092": "company_description", "index" : "ticker"})
            result = remove_null(result, "company_description")
            result = universe.merge(result, how="left", on=["ticker"])
            print(result)
            upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
            fill_null_company_desc_with_ticker_name()
            report_to_slack("{} : === Company Description Updated ===".format(datetimeNow()))

def update_industry_from_dsws(ticker=None, currency_code=None):
    print("{} : === Country & Industry Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["industry_code", "wc_industry_code"])
    identifier="ticker"
    filter_field = ["WC07040", "WC06011"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    print(result)
    if(len(result)>0):
        result = result.rename(columns={
            "index":"ticker",
            "WC07040":"industry_code", 
            "WC06011":"wc_industry_code"})
        print(result)

        industry = result[["industry_code"]]
        industry = industry.drop_duplicates(keep="first")
        industry["industry_name"] = "NA"
        upsert_data_to_database(industry, get_industry_table_name(), "industry_code", how="ignore", Text=True)

        wc_industry = result[["wc_industry_code"]]
        wc_industry = wc_industry.drop_duplicates(keep="first")
        wc_industry["wc_industry_name"] = "NA"
        upsert_data_to_database(wc_industry, get_industry_worldscope_table_name(), "wc_industry_code", how="ignore", Text=True)
        
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Country & Industry Updated ===".format(datetimeNow()))
    
def update_worldscope_identifier_from_dsws(ticker=None, currency_code=None):
    print("{} : === Worldscope Identifier Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["worldscope_identifier", "icb_code", "fiscal_year_end"])
    identifier="ticker"
    filter_field = ["WC06035", "WC07040", "WC05352"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 10))
    print(result)
    if (len(result) > 0):
        result = result.rename(columns={"WC06035": "worldscope_identifier", "WC07040": "icb_code", "WC05352": "fiscal_year_end", "index" : "ticker"})
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Worldscope Identifier Updated ===".format(datetimeNow()))
        
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
            start_date = dlp_start_date()
    
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
        upsert_data_to_database(result, get_data_vix_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === VIX Updated ===".format(datetimeNow()))

def update_fundamentals_score_from_dsws(ticker=None, currency_code=None):
    print("{} : === Fundamentals Score Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_month(12)
    start_date2 = backdate_by_month(24)
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    print(universe)
    filter_field = ["EPS1TR12","WC05480","WC18100A","WC18262A","WC08005",
        "WC18309A","WC18311A","WC18199A","WC08372","WC05510","WC08636A",
        "BPS1FD12","EBD1FD12","EVT1FD12","EPS1FD12","SAL1FD12","CAP1FD12"]
    static_field = ["ENSCORE","SOSCORE","CGSCORE"]
    column_name = {"EPS1TR12": "eps", "WC05480": "bps", "WC18100A": "ev",
        "WC18262A": "ttm_rev","WC08005": "mkt_cap","WC18309A": "ttm_ebitda",
        "WC18311A": "ttm_capex","WC18199A": "net_debt","WC08372": "roe",
        "WC05510": "cfps","WC08636A": "peg","BPS1FD12" : "bps1fd12",
        "EBD1FD12" : "ebd1fd12","EVT1FD12" : "evt1fd12","EPS1FD12" : "eps1fd12",
        "SAL1FD12" : "sal1fd12","CAP1FD12" : "cap1fd12",
        "ENSCORE" : "environment","SOSCORE" : "social",
        "CGSCORE" : "goverment"}
    
    result, except_field = get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1, quarterly=True, fundamentals_score=True)
    result2, except_field2 = get_data_history_frequently_from_dsws(start_date2, end_date, universe, identifier, static_field, use_ticker=True, split_number=1, quarterly=True, fundamentals_score=True)
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
    result2 = result2.drop(columns=["index"])
    result = result.merge(result2, how="left", on="ticker")
    print(result)
    if(len(universe)) > 0 :
        upsert_data_to_database(result, get_fundamental_score_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Fundamentals Score Updated ===".format(datetimeNow()))

def check_trading_day(days = 0):
    today = datetime.now().date()
    count = 0
    today2 = datetime.now().date()
    count2 = 0
    while (today.weekday() != days):
        today =  today - relativedelta(days=1)
        count = count + 1
    while (today2.weekday() != days):
        today2 =  today2 + relativedelta(days=1)
        count2 = count2 + 1
    if(count > count2):
        return today2.strftime("%Y-%m-%d")
    else:
        return today.strftime("%Y-%m-%d")

    
def update_fundamentals_quality_value(ticker=None, currency_code=None):
    print("{} : === Fundamentals Quality & Value Start Calculate ===".format(datetimeNow()))
    universe_rating = get_universe_rating(ticker=ticker, currency_code=currency_code)
    universe_rating = universe_rating[["ticker", "wts_rating", "dlp_1m", "dlp_3m", "wts_rating2", "classic_vol"]]
    print("=== Calculating Fundamentals Value & Fundamentals Quality ===")
    calculate_column = ["earnings_yield", "book_to_price", "ebitda_to_ev", "sales_to_price", "roic", "roe", "cf_to_price", "eps_growth", 
                        "fwd_bps","fwd_ebitda_to_ev", "fwd_ey", "fwd_sales_to_price", "fwd_roic", "earnings_pred", 
                        "environment", "social", "goverment"]
    fundamentals_score = get_fundamentals_score(ticker=ticker, currency_code=currency_code)
    print(fundamentals_score)

    close_price = get_last_close_industry_code(ticker=ticker, currency_code=currency_code)
    print(close_price)

    pred_mean = get_pred_mean()
    print(pred_mean)

    tri_2m = get_specific_tri(backdate_by_month(2), tri_name="tri_2m")
    tri_6m = get_specific_tri(backdate_by_month(6), tri_name="tri_6m")
    tri_2m = tri_2m.merge(tri_6m, how="left", on="ticker")
    tri_2m["tri"] = tri_2m["tri_2m"] / tri_2m["tri_6m"]
    print(tri_2m)
    
    fundamentals_score = close_price.merge(fundamentals_score, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(pred_mean, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(tri_2m[["ticker", "tri"]], how="left", on="ticker")
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
    fundamentals_score["earnings_pred"] = ((1 + fundamentals_score["pred_mean"]) * fundamentals_score["eps"] - fundamentals_score["eps1fd12"]) / fundamentals_score["close"]
    fundamentals = fundamentals_score[["earnings_yield", "book_to_price", "ebitda_to_ev", "sales_to_price", 
        "roic", "roe", "cf_to_price", "eps_growth", "currency_code", "ticker", "industry_code","fwd_bps",
        "fwd_ebitda_to_ev","fwd_ey", "fwd_sales_to_price", "fwd_roic", "earnings_pred", 
        "environment", "social", "goverment", "tri"]]
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
        
    minmax_column = ["uid", "ticker", "trading_day"]
    for column in calculate_column:
        column_robust_score = column + "_robust_score"
        column_minmax_currency_code = column + "_minmax_currency_code"
        column_minmax_industry = column + "_minmax_industry"
        df_currency_code = fundamentals[["currency_code", column_robust_score]]
        df_currency_code = df_currency_code.rename(columns = {column_robust_score : "score"})
        df_industry = fundamentals[["industry_code", column_robust_score]]
        df_industry = df_industry.rename(columns = {column_robust_score : "score"})
        fundamentals[column_minmax_currency_code] = df_currency_code.groupby("currency_code").score.transform(lambda x: minmax_scale(x.astype(float)))
        fundamentals[column_minmax_industry] = df_industry.groupby("industry_code").score.transform(lambda x: minmax_scale(x.astype(float)))
        if(column == "earnings_pred"):
            fundamentals[column_minmax_currency_code] = np.where(fundamentals[column_minmax_currency_code].isnull(), 0, fundamentals[column_minmax_currency_code])
            fundamentals[column_minmax_industry] = np.where(fundamentals[column_minmax_industry].isnull(), 0, fundamentals[column_minmax_industry])
        else:
            fundamentals[column_minmax_currency_code] = np.where(fundamentals[column_minmax_currency_code].isnull(), 0.4, fundamentals[column_minmax_currency_code])
            fundamentals[column_minmax_industry] = np.where(fundamentals[column_minmax_industry].isnull(), 0.4, fundamentals[column_minmax_industry])
        minmax_column.append(column_minmax_currency_code)
        minmax_column.append(column_minmax_industry)
    
    fundamentals["trading_day"] = check_trading_day(days=6)
    fundamentals = uid_maker(fundamentals, uid="uid", ticker="ticker", trading_day="trading_day")

    print("Calculate Fundamentals Value")
    fundamentals["fundamentals_value"] = ((fundamentals["earnings_yield_minmax_currency_code"]) + 
        fundamentals["earnings_yield_minmax_industry"] + 
        fundamentals["book_to_price_minmax_currency_code"] + 
        fundamentals["book_to_price_minmax_industry"] + 
        fundamentals["ebitda_to_ev_minmax_currency_code"] + 
        fundamentals["ebitda_to_ev_minmax_industry"] +
        fundamentals["fwd_bps_minmax_currency_code"] +
        fundamentals["fwd_bps_minmax_industry"] +
        fundamentals["fwd_ebitda_to_ev_minmax_currency_code"] +
        fundamentals["fwd_ebitda_to_ev_minmax_industry"] + 
        fundamentals["roe_minmax_currency_code"]+
        fundamentals["roe_minmax_industry"]).round(1)

    print("Calculate Fundamentals Quality")
    fundamentals["fundamentals_quality"] = ((fundamentals["roic_minmax_currency_code"]) + 
        fundamentals["roic_minmax_industry"] +
        fundamentals["cf_to_price_minmax_currency_code"] +
        fundamentals["cf_to_price_minmax_industry"] +
        fundamentals["eps_growth_minmax_currency_code"] + 
        fundamentals["eps_growth_minmax_industry"] + 
        fundamentals["fwd_ey_minmax_industry"] +
        fundamentals["fwd_ey_minmax_currency_code"] +
        fundamentals["fwd_sales_to_price_minmax_industry"]+ 
        (fundamentals["fwd_roic_minmax_industry"]) +
        fundamentals["earnings_pred_minmax_industry"] +
        fundamentals["earnings_pred_minmax_currency_code"]).round(1)

    print("Calculate Momentum Value")
    quantile = pd.DataFrame({"ticker" : [], "tri" : [], "tri_quantile":[]}, index=[])
    for currency in fundamentals["currency_code"].unique():
        data = fundamentals.loc[fundamentals["currency_code"] == currency]
        data = data[["ticker", "tri"]]
        quantile_data = data["tri"].to_numpy().reshape((len(data),1))
        data["tri_quantile"] = quantile_transform(quantile_data, n_quantiles=4)
        quantile = quantile.append(data)
    fundamentals = fundamentals.merge(quantile[["ticker", "tri_quantile"]], how="left", on="ticker")
    minmax_column.append("tri_quantile")
    fundamentals["momentum"] = fundamentals["tri_quantile"] * 10

    print("Calculate ESG Value")
    fundamentals["esg"] = (fundamentals["environment_minmax_currency_code"] + fundamentals["environment_minmax_industry"] + \
        fundamentals["social_minmax_currency_code"] + fundamentals["social_minmax_industry"] + \
        fundamentals["goverment_minmax_currency_code"] + fundamentals["goverment_minmax_industry"]) / 6

    print("Calculate Technical Value")
    fundamentals = fundamentals.merge(universe_rating, how="left", on="ticker")
    fundamentals["technical"] = (fundamentals["wts_rating"] + fundamentals["dlp_1m"]+ fundamentals["momentum"]) / 3

    print("Calculate AI Score")
    fundamentals["ai_score"] = (fundamentals["fundamentals_value"] + fundamentals["fundamentals_quality"] + \
        fundamentals["technical"] + fundamentals["technical"]) / 4

    print("Calculate AI Score 2")
    fundamentals["ai_score2"] = (fundamentals["fundamentals_value"] + fundamentals["fundamentals_quality"] + fundamentals["technical"] + fundamentals["esg"]) / 4
    
    universe_rating_history = fundamentals[["uid", "ticker", "trading_day", "fundamentals_value", 
    "fundamentals_quality", "ai_score", "ai_score2", "esg", 
    "momentum", "technical", "wts_rating", "dlp_1m", "dlp_3m", "wts_rating2", "classic_vol"]]

    universe_rating_detail_history = fundamentals[minmax_column]
    
    print("=== Calculate Fundamentals Value & Fundamentals Quality DONE ===")
    
    if(len(fundamentals)) > 0 :
        print(fundamentals)
        result = fundamentals[["ticker", "fundamentals_value", "fundamentals_quality"]].merge(universe_rating, how="left", on="ticker")
        result["updated"] = dateNow()
        print(result)
        print(universe_rating_history)
        print(universe_rating_detail_history)
        
        upsert_data_to_database(result, get_universe_rating_table_name(), "ticker", how="update", Text=True)
        upsert_data_to_database(universe_rating_history, get_universe_rating_history_table_name(), "uid", how="update", Text=True)
        upsert_data_to_database(universe_rating_detail_history, get_universe_rating_detail_history_table_name(), "uid", how="update", Text=True)

        delete_data_on_database(get_universe_rating_table_name(), f"ticker is not null", delete_ticker=True)
        delete_data_on_database(get_universe_rating_history_table_name(), f"ticker is not null", delete_ticker=True)
        delete_data_on_database(get_universe_rating_detail_history_table_name(), f"ticker is not null", delete_ticker=True)
        report_to_slack("{} : === Universe Fundamentals Quality & Value Updated ===".format(datetimeNow()))

def dividend_updated_from_dsws(ticker=None, currency_code=None):
    print("{} : === Dividens Update ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    identifier="ticker"
    filter_field = ["UDDE"]
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe[["ticker"]]
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, dividend=True, split_number=1)
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

def interest_update_from_dsws():
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

def update_fred_data_from_fred():
    print("{} : === Fred Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    result = read_fred_csv(start_date, end_date)
    result["data"] = np.where(result["data"]== ".", 0, result["data"])
    result["data"] = result["data"].astype(float)
    if(len(result)) > 0 :
        upsert_data_to_database(result, get_data_fred_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === Fred Updated ===".format(datetimeNow()))

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
    filter_field = [
        "WC05192A", "WC18271A", "WC02999A", "WC03255A", "WC03501A", "WC18313A", "WC18312A",
        "WC18310A", "WC18311A", "WC18309A", "WC18308A", "WC18269A", "WC18304A", "WC18266A",
        "WC18267A", "WC18265A", "WC18264A", "WC18263A", "WC18262A", "WC18199A", "WC18158A",
        "WC18100A", "WC08001A", "WC05085A", "WC03101A", "WC02501A", "WC02201A", "WC02101A",
        "WC02001A", "WC05575A"]
    for field in filter_field:
        worldscope_quarter_summary_from_dsws(ticker=ticker, currency_code=currency_code, filter_field=[field])
    report_to_slack("{} : === Quarter Summary Data Updated ===".format(datetimeNow()))

def worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None, filter_field=None):

    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    if(len(universe) < 1):
        return False
    end_date = dateNow()
    start_date = backdate_by_month(6)
    identifier="ticker"
    ticker = universe[["ticker"]]
    ticker = ticker["ticker"].tolist()
    result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, ticker, identifier, filter_field, use_ticker=True, split_number=1, monthly=True, worldscope=True)
    # print(result)
    result = result.dropna()
    print(result)
    if(len(result)) > 0 :
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
            "WC05575A" : "fn_5575",
            "index" : "period_end"
        })
        result = result.reset_index(inplace=False)
        # print(result)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["year"] = pd.DatetimeIndex(result["period_end"]).year
        result["month"] = pd.DatetimeIndex(result["period_end"]).month
        result["day"] = pd.DatetimeIndex(result["period_end"]).day
        # print(result)
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
        result = result.drop(columns=["month", "day", "index"])
        identifier = universe[["ticker", "worldscope_identifier"]]
        result = result.merge(identifier, how="left", on="ticker")
        result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)
        print(result)
        upsert_data_to_database(result, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)
        