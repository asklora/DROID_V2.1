import numpy as np
from scipy.stats import skew
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import robust_scale, minmax_scale, QuantileTransformer, PowerTransformer, MinMaxScaler
from general.data_process import remove_null, uid_maker
from general.sql_process import do_function
from general.slack import report_to_slack
from general.sql_output import (
    delete_data_on_database, 
    delete_old_dividends_on_database, 
    fill_null_company_desc_with_ticker_name,
    update_universe_where_currency_code_null, 
    upsert_data_to_database,
    upsert_data_to_database_ali,
    replace_table_datebase_ali)
from general.table_name import (
    get_currency_price_history_table_name,
    get_currency_table_name,
    get_data_dividend_table_name, 
    get_data_dsws_table_name,
    get_data_fred_table_name,
    get_data_fundamental_score_history_table_name,
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
    fetch_data_from_dsws,
    get_data_history_by_field_from_dsws,
    get_data_history_frequently_by_field_from_dsws,
    get_data_history_frequently_from_dsws, 
    get_data_history_from_dsws, 
    get_data_static_from_dsws, 
    get_data_static_with_string_from_dsws)
from general.sql_query import (
    get_active_currency_ric_not_null,
    get_active_universe, 
    get_active_universe_by_entity_type, 
    get_active_universe_company_description_null, 
    get_active_universe_consolidated_by_field, 
    get_all_universe, get_data_by_table_name,
    get_data_ibes_monthly,
    get_data_macro_monthly,
    get_factor_calculation_formula,
    get_factor_rank,
    get_fundamentals_score,
    get_last_close_industry_code,
    get_currency_fx_rate_dict,
    get_max_last_ingestion_from_universe, 
    get_ai_value_pred_final,
    get_ai_score_testing_history,
    get_specific_tri_avg,
    get_specific_volume_avg,
    get_universe_rating, 
    get_vix,
    get_vix_since,
    get_master_ohlcvtr_data,
    get_ingestion_name_source,
    get_currency_code_ibes_ws,
    get_iso_currency_code_map,
    get_missing_field_ticker_list,
    get_ingestion_name_macro_source
)
from general.date_process import (
    backdate_by_day, 
    backdate_by_month,
    dateNow, 
    datetimeNow, 
    dlp_start_date, 
    droid_start_date,
    find_nearest_specific_days,
    forwarddate_by_day,
    forwarddate_by_month,
    get_period_end_list,
    str_to_date)
from datasource.fred import read_fred_csv

def populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=None, manual_universe=None, universe=None):
    print("{} : === Ticker ISIN Start Ingestion ===".format(datetimeNow()))
    if(manual_universe is None):
        manual_universe = get_active_universe_consolidated_by_field(manual=True, ticker=ticker)
    manual_universe = manual_universe.loc[manual_universe["use_manual"] == True]
    manual_universe["consolidated_ticker"] = manual_universe["origin_ticker"]

    if(universe is None):
        universe = get_active_universe_consolidated_by_field(isin=True, ticker=ticker)
    universe = universe.loc[universe["use_manual"] == False]
    if(len(universe) > 0):
        universe = universe.drop(columns=["isin", "consolidated_ticker", "sedol", "is_active", "cusip", "permid", "updated"])
        print(universe)
        identifier="origin_ticker"
        filter_field = ["ISIN", "SECD", "WC06004", "IPID"]
        result, error_ticker = get_data_static_from_dsws(universe[["origin_ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
        result = result.rename(columns={"ISIN": "isin", "index":"origin_ticker", "SECD": "sedol", "WC06004": "cusip", "IPID": "permid"})
        
        print(result)

        isin_list = result[["isin"]]
        isin_list = isin_list.drop_duplicates(keep="first", inplace=False)
        result2, error_ticker = get_data_static_from_dsws(isin_list, "isin", ["RIC", "SECD"], use_ticker=False, split_number=min(len(isin_list), 1))
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
    return result

def update_ticker_name_from_dsws(ticker=None, currency_code=None):
    print("{} : === Ticker Name Start Ingestion ===".format(datetimeNow()))
    universe = get_all_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["ticker_name", "ticker_fullname"])
    print(universe)
    filter_field = ["WC06003", "NAME"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
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
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
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
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
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
    universe["icb_code"] = universe["industry_code"]
    identifier="ticker"
    filter_field = ["WC06035", "WC05352"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
    print(result)
    if (len(result) > 0):
        result = result.rename(columns={"WC06035": "worldscope_identifier", "WC05352": "fiscal_year_end", "index" : "ticker"})
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
    result, error_ticker = get_data_history_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=min(len(universe), 20))
    print(result)
    print(error_ticker)
    if len(error_ticker) == 0 :
        second_result = []
    else:
        second_result, error_ticker = get_data_history_by_field_from_dsws(start_date, end_date, error_ticker, identifier, filter_field, use_ticker=True, split_number=1)
        if(len(error_ticker) > 0):
            report_to_slack("{} : === DSWS TICKER ERROR {} ===".format(datetimeNow(), error_ticker))
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
    if(len(result)) > 0 :
        result = result.rename(columns={"RI": "total_return_index", "level_1" : "trading_day"})
        result = result.dropna(subset=["ticker"])
        result = result.dropna(subset=["trading_day"])
        result = uid_maker(result, uid="dsws_id", ticker="ticker", trading_day="trading_day")
        result = result[["dsws_id", "ticker", "trading_day", "total_return_index"]]
        print(result)
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
    ''' weekly update data_fundamental_score '''
    get_ingestion_name_source()

    print("{} : === Fundamentals Score Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_month(12)
    start_date2 = backdate_by_month(24)
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    print(universe)
    if(len(universe)):
        filter_field = ["EPS1TR12","WC05480","WC18100A","WC18262A","WC08005",
            "WC18309A","WC18311A","WC18199A","WC08372","WC05510","WC08636A",
            "BPS1FD12","EBD1FD12","EVT1FD12","EPS1FD12","SAL1FD12","CAP1FD12",
            "WC02999", "WC02001", "WC03101", "WC03501", "WC18312A", "WC02101", 
            "WC18264", "WC18267", "WC01451", "WC18810", "WC02401", "WC18274", 
            "WC07211", "i0eps"]

        static_field = ["ENSCORE","SOSCORE","CGSCORE"]
        column_name = {"EPS1TR12": "eps", "WC05480": "bps", "WC18100A": "ev",
            "WC18262A": "ttm_rev","WC08005": "mkt_cap","WC18309A": "ttm_ebitda",
            "WC18311A": "ttm_capex","WC18199A": "net_debt","WC08372": "roe",
            "WC05510": "cfps","WC08636A": "peg","BPS1FD12" : "bps1fd12",
            "EBD1FD12" : "ebd1fd12","EVT1FD12" : "evt1fd12","EPS1FD12" : "eps1fd12",
            "SAL1FD12" : "sal1fd12","CAP1FD12" : "cap1fd12",
            "ENSCORE" : "environment","SOSCORE" : "social",
            "CGSCORE" : "goverment", 
            "WC02999" : "total_asset", "WC02001" : "cash", 
            "WC03101" : "current_asset", "WC03501" : "equity", 
            "WC18312A" : "ttm_cogs", "WC02101" : "inventory", 
            "WC18264" : "ttm_eps", "WC18267" : "ttm_gm", 
            "WC01451" : "income_tax", "WC18810" : "pension_exp", 
            "WC02401" : "ppe_depreciation", "WC18274" : "ppe_impairment", 
            "WC07211" : "mkt_cap_usd", "i0eps" : "eps_lastq"}

        result, except_field = get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier,
                                                                     filter_field, use_ticker=True, split_number=1,
                                                                     quarterly=True, fundamentals_score=True)
        result2, except_field2 = get_data_history_frequently_from_dsws(start_date2, end_date, universe, identifier,
                                                                       static_field, use_ticker=True, split_number=1,
                                                                       quarterly=True, fundamentals_score=True)
        print("Error Ticker = " + str(except_field))
        if len(except_field) == 0 :
            second_result = []
        else:
            second_result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, except_field, identifier,
                                                                           filter_field, use_ticker=True, split_number=1,
                                                                           quarterly=True, fundamentals_score=True)
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

            result["trading_day"] = find_nearest_specific_days(days=0)
            result = uid_maker(result, uid="uid", ticker="ticker", trading_day="trading_day", date=True)
            upsert_data_to_database(result, get_data_fundamental_score_history_table_name(), "uid", how="update", Text=True)
            report_to_slack("{} : === Fundamentals Score Updated ===".format(datetimeNow()))

def update_daily_fundamentals_score_from_dsws(ticker=None, currency_code=None):
    print("{} : === Fundamentals Daily Score Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = backdate_by_month(12)
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    print(universe)
    if(len(universe)):
        filter_field = ["WC08005"]
        column_name = {"WC08005": "mkt_cap"}
        result, except_field = get_data_history_frequently_from_dsws(start_date, end_date, universe, identifier, filter_field, use_ticker=True, split_number=1, quarterly=True, fundamentals_score=True)
        print(result)
        result = result.rename(columns=column_name)
        result = result.drop(columns=["index"])
        print(result)
        upsert_data_to_database(result, get_fundamental_score_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Fundamentals Score Daily Updated ===".format(datetimeNow()))

def score_update_vol_rs(list_of_start_end, days_in_year=256):
    """ Calculate roger satchell volatility:
        daily = average over period from start to end: Log(High/Open)*Log(High/Close)+Log(Low/Open)*Log(Open/Close)
        annualized = sqrt(daily*256)
    """

    # download past prices since 1 months before the earliest volitility calculation month i.e. end
    tri = get_master_ohlcvtr_data(trading_day=backdate_by_month(list_of_start_end[-1][-1]+1))
    tri["trading_day"] = pd.to_datetime(tri["trading_day"])
    tri = tri.sort_values(by=["ticker", "trading_day"], ascending=[True, False]).reset_index(drop=True)
    open_data, high_data, low_data, close_data = tri["open"].values, tri["high"].values, tri["low"].values, tri[
        "close"].values

    # Calculate daily volatility
    hc_ratio = np.divide(high_data, close_data)
    log_hc_ratio = np.log(hc_ratio.astype(float))
    ho_ratio = np.divide(high_data, open_data)
    log_ho_ratio = np.log(ho_ratio.astype(float))
    lo_ratio = np.divide(low_data, open_data)
    log_lo_ratio = np.log(lo_ratio.astype(float))
    lc_ratio = np.divide(low_data, close_data)
    log_lc_ratio = np.log(lc_ratio.astype(float))

    input1 = np.multiply(log_hc_ratio, log_ho_ratio)
    input2 = np.multiply(log_lo_ratio, log_lc_ratio)
    sum_ = np.add(input1, input2)

    # Calculate annualize volatility
    vol_col = []
    for l in list_of_start_end:
        start, end = l[0]*30, l[1]*30
        name_col = f"vol_{start}_{end}"
        vol_col.append(name_col)
        tri[name_col] = sum_
        tri[name_col] = tri.groupby("ticker")[name_col].rolling(end - start, min_periods=1).mean().reset_index(drop=1)
        tri[name_col] = tri[name_col].apply(lambda x: np.sqrt(x * days_in_year))
        tri[name_col] = tri[name_col].shift(start)

    # return tri on the most recent trading_day
    final_tri = tri[["ticker"]+vol_col].dropna(how="any").groupby(["ticker"]).last().reset_index()

    return final_tri

def score_update_stock_return(list_of_start_end):
    """ Calculate specific period stock return (months) """

    df = pd.DataFrame(get_active_universe()["ticker"])

    for l in list_of_start_end:
        name_col = f"stock_return_{l[0]}_{l[1]}"
        tri_start = get_specific_tri_avg(backdate_by_month(l[0]), avg_days=7, tri_name=f"tri_{l[0]}m")
        tri_end = get_specific_tri_avg(backdate_by_month(l[1]), avg_days=7, tri_name=f"tri_{l[1]}m")
        tri = tri_start.merge(tri_end, how="left", on="ticker")
        tri[name_col] = tri[f"tri_{l[0]}m"] / tri[f"tri_{l[1]}m"]
        df = df.merge(tri[["ticker", name_col]], how="left", on="ticker")
        print(df)

    return df

def score_update_fx_conversion(df):
    """ Convert all columns to USD for factor calculation (DSS, WORLDSCOPE, IBES using different currency) """

    org_cols = df.columns.to_list()     # record original columns for columns to return

    curr_code = get_currency_code_ibes_ws()     # map ibes/ws currency for each ticker
    df = df.merge(curr_code, on='ticker', how='left')
    # df = df.dropna(subset=['currency_code_ibes', 'currency_code_ws', 'currency_code'], how='any')   # remove ETF / index / some B-share -> tickers will not be recommended

    # map fx rate for conversion for each ticker
    fx = get_currency_fx_rate_dict()
    df['fx_dss'] = df['currency_code'].map(fx)
    df['fx_ibes'] = df['currency_code_ibes'].map(fx)
    df['fx_ws'] = df['currency_code_ws'].map(fx)

    ingestion_source = get_ingestion_name_source().dropna(subset=['score_name'])
    ingestion_source = ingestion_source.loc[ingestion_source['non_ratio']]     # no fx conversion for ratio items

    for name, g in ingestion_source.groupby(['source']):        # convert for ibes / ws
        cols = g['score_name'].to_list()
        df[cols] = df[cols].div(df[f'fx_{name}'], axis="index")

    df['close'] = df['close']/df['fx_dss']  # convert close price

    return df[org_cols]

def score_update_factor_ratios(df):
    """ Calculate all factor used referring to DB ratio table """

    formula = get_factor_calculation_formula()
    print(df.columns)

    df = score_update_fx_conversion(df)

    # Prepare for field requires add/minus
    add_minus_fields = formula[["field_num", "field_denom"]].dropna(how="any").to_numpy().flatten()
    add_minus_fields = [i for i in list(set(add_minus_fields)) if any(["-" in i, "+" in i, "*" in i])]

    for i in add_minus_fields:
        x = [op.strip() for op in i.split()]
        if x[0] in "*+-": raise Exception("Invalid formula")
        temp = df[x[0]].copy()
        n = 1
        while n < len(x):
            if x[n] == "+":
                temp += df[x[n + 1]].replace(np.nan, 0)
            elif x[n] == "-":
                temp -= df[x[n + 1]].replace(np.nan, 0)
            elif x[n] == "*":
                temp *= df[x[n + 1]]
            else:
                raise Exception(f"Unexpected operand/operator: {x[n]}")
            n += 2
        df[i] = temp

    # a) Keep original values
    keep_original_mask = formula["field_denom"].isnull() & formula["field_num"].notnull()
    new_name = formula.loc[keep_original_mask, "name"].to_list()
    old_name = formula.loc[keep_original_mask, "field_num"].to_list()
    df[new_name] = df[old_name]

    # c) Divide ratios
    print(f"      ------------------------> Calculate dividing ratios ")
    for r in formula.loc[(formula["field_denom"].notnull())&
                         (formula["field_num"]!= formula["field_denom"])].to_dict(orient="records"):  # minus calculation for ratios
        df[r["name"]] = df[r["field_num"]] / df[r["field_denom"]]

    return df, formula.set_index(["name"])

def score_update_scale(fundamentals, calculate_column, universe_currency_code, factor_rank):

    fundamentals['dummy'] = True
    groupby_col = ['currency_code']  # or 'dummy'

    def transform_trim_outlier(x):
        s = skew(x)
        if (s < -5) or (s > 5):
            x = np.log(x + 1 - np.min(x))
        m = np.median(x)
        # clip_x = np.clip(x, np.percentile(x, 0.01), np.percentile(x, 0.99))
        std = np.nanstd(x)
        return np.clip(x, m - 2 * std, m + 2 * std)

    # Scale 1: log transformation for high skewness & trim outlier to +/- 2 std
    calculate_column_score = []
    for column in calculate_column:
        column_score = column + "_score"
        fundamentals[column_score] = fundamentals.dropna(subset=[column]).groupby(groupby_col)[column].transform(
            transform_trim_outlier)
        calculate_column_score.append(column_score)
    print(calculate_column_score)

    # x1 = fundamentals.groupby("currency_code")[[x+'_score' for x in calculate_column]].skew()
    # y1 = fundamentals.groupby("currency_code")[[x+'_score' for x in calculate_column]].apply(pd.DataFrame.kurtosis)

    # Scale 2: Reverse value for long_large = False (i.e. recommend short larger value)
    append_df = factor_rank.loc[factor_rank['keep']]
    append_df.loc[append_df['factor_name']=='earnings_pred', 'long_large'] = factor_rank.loc[factor_rank['factor_name']=='fwd_ey', 'long_large'].values[0]
    for group, g in factor_rank.groupby(['group']):
        neg_factor = [x+'_score' for x in g.loc[(g['long_large'] == False), 'factor_name'].to_list()]
        if not factor_rank.loc[factor_rank['factor_name']=='fwd_ey', 'long_large'].values[0]:
            neg_factor += ['earnings_pred_score']

        fundamentals.loc[(fundamentals['currency_code'] == group), neg_factor] *= -1

        # for non calculating socre -> we add same for each one
        append_df["group"] = group
        factor_rank = factor_rank.append(append_df, ignore_index=True)

    # Scale 3: apply robust scaler
    calculate_column_robust_score = []
    for column in calculate_column:
        try:
            column_score = column + "_score"
            column_robust_score = column + "_robust_score"
            fundamentals[column_robust_score] = fundamentals.dropna(subset=[column_score]).groupby(groupby_col)[
                column_score].transform(lambda x: robust_scale(x))
            calculate_column_robust_score.append(column_robust_score)
        except Exception as e:
            print(e)
    print(calculate_column_robust_score)

    # Scale 4a: apply maxmin scaler on Currency / Industry
    minmax_column = ["uid", "ticker", "trading_day"]
    for column in calculate_column:
        column_robust_score = column + "_robust_score"
        column_minmax_currency_code = column + "_minmax_currency_code"
        df_currency_code = fundamentals[["currency_code", column_robust_score]]
        df_currency_code = df_currency_code.rename(columns={column_robust_score: "score"})
        fundamentals[column_minmax_currency_code] = df_currency_code.dropna(subset=["currency_code", "score"]).groupby(
            groupby_col).score.transform(
            lambda x: minmax_scale(x.astype(float)) if x.notnull().sum() else np.full_like(x, np.nan))
        fundamentals[column_minmax_currency_code] = np.where(fundamentals[column_minmax_currency_code].isnull(),
                                                             fundamentals[column_minmax_currency_code].mean() * 0.9,
                                                             fundamentals[column_minmax_currency_code]) * 10
        minmax_column.append(column_minmax_currency_code)

        if column in ["environment", "social", "goverment"]:  # for ESG scores also do industry partition
            column_minmax_industry = column + "_minmax_industry"
            df_industry = fundamentals[["industry_code", column_robust_score]]
            df_industry = df_industry.rename(columns={column_robust_score: "score"})
            fundamentals[column_minmax_industry] = df_industry.dropna(subset=["industry_code", "score"]).groupby(
                "industry_code").score.transform(
                lambda x: minmax_scale(x.astype(float)) if x.notnull().sum() else np.full_like(x, np.nan))
            fundamentals[column_minmax_industry] = np.where(fundamentals[column_minmax_industry].isnull(),
                                                            fundamentals[column_minmax_industry].mean() * 0.9,
                                                            fundamentals[column_minmax_industry]) * 10
            minmax_column.append(column_minmax_industry)

    print(minmax_column)

    # Scale 4b: apply quantile transformation on before scaling scores
    # tmp = fundamentals.melt(["ticker", "currency_code", "industry_code"], calculate_column)
    # tmp["quantile_transformed"] = tmp.dropna(subset=["value"]).groupby(groupby_col + ["variable"])["value"].transform(
    #     lambda x: QuantileTransformer(n_quantiles=4).fit_transform(
    #         x.values.reshape(-1, 1)).flatten() if x.notnull().sum() else np.full_like(x, np.nan))
    # tmp = tmp[["ticker", "variable", "quantile_transformed"]]
    # tmp["variable"] = tmp["variable"] + "_quantile_currency_code"
    # tmp = tmp.pivot(["ticker"], ["variable"]).droplevel(0, axis=1)
    # fundamentals = fundamentals.merge(tmp, how="left", on="ticker")

    # add column for 3 pillar score
    fundamentals[[f"fundamentals_{name}" for name in factor_rank['pillar'].unique()]] = np.nan
    # fundamentals[['dlp_1m','wts_rating']] = fundamentals[['dlp_1m','wts_rating']]/10    # adjust dlp score to 0 ~ 1 (originally 0 ~ 10)

    # dataframe for details checking
    fundamentals_details = {}
    fundamentals_details_column_names = {}
    for i in universe_currency_code:
        if i:
            fundamentals_details[i] = {}
            fundamentals_details_column_names[i] = {}

    # Scale 5a: calculate ai_score by each currency_code (i.e. group) for each of [Quality, Value, Momentum]
    for (group, pillar_name), g in factor_rank.groupby(["group", "pillar"]):
        print(f"Calculate Fundamentals [{pillar_name}] in group [{group}]")
        sub_g = g.loc[(g["factor_weight"] == 2) | (g["factor_weight"].isnull())]  # use all rank=2 (best class)
        if len(sub_g.dropna(
                subset=["pred_z"])) == 0:  # if no factor rank=2, use the highest ranking one & DLPA/ai_value scores
            sub_g = g.loc[g.nlargest(1, columns=["pred_z"]).index.union(g.loc[g["factor_weight"].isnull()].index)]

        fundamentals_details_column_names[group][pillar_name] = ','.join(sub_g['factor_name'])
        score_col = [f"{x}_{y}_currency_code" for x, y in
                     sub_g.loc[sub_g["scaler"].notnull(), ["factor_name", "scaler"]].to_numpy()]
        score_col += [x for x in sub_g.loc[sub_g["scaler"].isnull(), "factor_name"]]
        fundamentals.loc[fundamentals["currency_code"] == group, f"fundamentals_{pillar_name}"] = fundamentals[
            score_col].mean(axis=1)

        # save used columns to pillars
        score_col_detail = ['ticker', f"fundamentals_{pillar_name}"] + sub_g.loc[
            sub_g["scaler"].notnull(), 'factor_name'].to_list() + score_col
        fundamentals_details[group][pillar_name] = fundamentals.loc[
            fundamentals['currency_code'] == group, score_col_detail].sort_values(by=[f"fundamentals_{pillar_name}"])

    # Scale 5b: calculate ai_score by each currency_code (i.e. group) for [Extra]
    for group, g in factor_rank.groupby("group"):
        print(f"Calculate Fundamentals [extra] in group [{group}]")
        sub_g = g.loc[(g["factor_weight"] == 2) | (g["factor_weight"].isnull())]  # use all rank=2 (best class)
        sub_g = sub_g.loc[(g["pred_z"] >= 1) | (
            g["pred_z"].isnull())]  # use all rank=2 (best class) and predicted factor premiums with z-value >= 1

        if len(sub_g.dropna(subset=["pred_z"])) > 0:  # if no factor rank=2, don"t add any factor into extra pillar
            score_col = [f"{x}_{y}_currency_code" for x, y in
                         sub_g.loc[sub_g["scaler"].notnull(), ["factor_name", "scaler"]].to_numpy()]
            score_col_detail = sub_g.loc[sub_g["scaler"].notnull(), 'factor_name'].to_list() + score_col
            fundamentals.loc[fundamentals["currency_code"] == group, f"fundamentals_extra"] = fundamentals[
                score_col].mean(axis=1)
            fundamentals_details_column_names[group]['extra'] = ','.join(sub_g['factor_name'])
        else:
            score_col_detail = []
            fundamentals.loc[fundamentals["currency_code"] == group, f"fundamentals_extra"] = \
                fundamentals.loc[fundamentals["currency_code"] == group].filter(regex="^fundamentals_").mean().mean()
            fundamentals_details_column_names[group]['extra'] = ''

        # save used columns to pillars
        # fundamentals_details[group]['extra'] = fundamentals.loc[fundamentals['currency_code']==group,
        #        ['ticker', "fundamentals_extra"] + score_col_detail].sort_values(by=[ f"fundamentals_extra"])

    replace_table_datebase_ali(pd.DataFrame(fundamentals_details_column_names).transpose().reset_index(),
                               f"test_fundamental_score_current_names")

    # manual score check output to alibaba DB
    for group, v in fundamentals_details.items():
        pillar_df = []
        for pillar, df in v.items():
            pillar_df.append(df.set_index(['ticker']))
        pillar_df = pd.concat(pillar_df, axis=1)
        pillar_df.index = pillar_df.index.set_names(['index'])
        replace_table_datebase_ali(pillar_df.reset_index(), f"test_fundamental_score_details_{group}")

    fundamentals_factors_scores_col = fundamentals.filter(regex="^fundamentals_").columns

    print("Calculate ESG Value")
    esg_cols = ["environment_minmax_currency_code", "environment_minmax_industry", "social_minmax_currency_code",
                "social_minmax_industry", "goverment_minmax_currency_code", "goverment_minmax_industry"]
    fundamentals["esg"] = fundamentals[esg_cols].mean(1)

    print("Calculate AI Score")
    ai_score_cols = ["fundamentals_value", "fundamentals_quality", "fundamentals_momentum", "fundamentals_extra"]
    fundamentals["ai_score"] = fundamentals[ai_score_cols].mean(1)

    print("Calculate AI Score 2")
    ai_score_cols2 = ["fundamentals_value", "fundamentals_quality", "fundamentals_momentum", "esg"]
    fundamentals["ai_score2"] = fundamentals[ai_score_cols2].mean(1)

    print(fundamentals[["fundamentals_value", "fundamentals_quality", "fundamentals_momentum", "fundamentals_extra",
                        'esg']].describe())

    # Scale 6: scale ai_score with history min / max
    # print(fundamentals.groupby(['currency_code'])[["ai_score", "ai_score2"]].agg(['min','mean','median','max']).transpose()[['HKD','USD','CNY','EUR']])
    fundamentals[["ai_score_unscaled", "ai_score2_unscaled"]] = fundamentals[["ai_score", "ai_score2"]]
    score_history = get_ai_score_testing_history(backyear=1)
    for cur, g in fundamentals.groupby(['currency_code']):
        try:
            # raise Exception('Scaling with current score')
            score_history_cur = score_history.loc[score_history['currency_code'] == cur]
            print(f'{cur} History Min/Max: ',
                  score_history_cur[["ai_score_unscaled", "ai_score2_unscaled"]].min().values,
                  score_history_cur[["ai_score_unscaled", "ai_score2_unscaled"]].max().values)
            print(f'{cur} Current Min/Max: ', g[["ai_score", "ai_score2"]].min().values,
                  g[["ai_score", "ai_score2"]].max().values)
            m1 = MinMaxScaler(feature_range=(0, 10)).fit(score_history_cur[["ai_score_unscaled", "ai_score2_unscaled"]])
            fundamentals.loc[g.index, ["ai_score", "ai_score2"]] = m1.transform(g[["ai_score", "ai_score2"]])
        except Exception as e:
            print(e)
            print(f'{cur} Current Min/Max: ', g[["ai_score", "ai_score2"]].min().values,
                  g[["ai_score", "ai_score2"]].max().values)
            fundamentals.loc[g.index, ["ai_score", "ai_score2"]] = MinMaxScaler(feature_range=(0, 10)).fit_transform(
                g[["ai_score", "ai_score2"]])

    return fundamentals, minmax_column, fundamentals_factors_scores_col

def update_fundamentals_quality_value(ticker=None, currency_code=None):
    ''' Update: '''
    # Ingest 1: DLPA & Universe
    print("{} : === Fundamentals Quality & Value Start Calculate ===".format(datetimeNow()))
    universe_rating = get_universe_rating(ticker=ticker, currency_code=currency_code)
    universe_rating = universe_rating[["ticker", "wts_rating", "dlp_1m", "dlp_3m", "wts_rating2", "classic_vol"]]

    # if DLPA results has problem will not using DLPA
    for col in ['dlp_1m', 'wts_rating']:
        if any(universe_rating[[col]].value_counts()/len(universe_rating) > .95):
            universe_rating[[col]] = np.nan

    # Ingest 2: fundamental score
    print("=== Calculating Fundamentals Value & Fundamentals Quality ===")
    fundamentals_score = get_fundamentals_score(ticker=ticker, currency_code=currency_code)
    print(fundamentals_score)

    # get last trading price for factor calculation
    close_price = get_last_close_industry_code(ticker=ticker, currency_code=currency_code)
    print(close_price)

    # get volatility
    vol = score_update_vol_rs(list_of_start_end=[[0,1]])     # calculate RS volatility -> list_of_start_end in ascending sequence (start_month, end_month)
    print(vol)

    pred_mean = get_ai_value_pred_final()
    print(pred_mean)

    # get different period stock return
    tri = score_update_stock_return(list_of_start_end=[[0,1],[2,6],[6,12]])
    print(tri)

    # get last week average volume
    volume1 = get_specific_volume_avg(backdate_by_month(0), avg_days=7).set_index('ticker')
    volume2 = get_specific_volume_avg(backdate_by_month(0), avg_days=91).set_index('ticker')
    volume = (volume1/volume2).reset_index()
    print(volume)

    # merge scores used for calculation
    fundamentals_score = close_price.merge(fundamentals_score, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(vol, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(pred_mean, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(tri, how="left", on="ticker")
    fundamentals_score = fundamentals_score.merge(volume, how="left", on="ticker")

    # calculate ratios refering to table X
    fundamentals_score, factor_formula = score_update_factor_ratios(fundamentals_score)

    factor_rank = get_factor_rank()

    # for currency not predicted by Factor Model -> Use factor of USD
    universe_currency_code = get_active_universe()['currency_code'].unique()
    for i in set(universe_currency_code) - set(factor_rank['group'].unique()):
        replace_rank = factor_rank.loc[factor_rank['group']=='USD'].copy()
        replace_rank['group'] = i
        factor_rank = factor_rank.append(replace_rank, ignore_index=True)

    factor_rank = factor_rank.merge(factor_formula, left_on=['factor_name'], right_index=True, how='outer')
    factor_rank['long_large'] = factor_rank['long_large'].fillna(True)
    factor_rank = factor_rank.dropna(subset=['pillar'])

    calculate_column = list(factor_formula.loc[factor_formula["scaler"].notnull()].index)
    calculate_column = sorted(set(calculate_column))
    calculate_column += ["environment", "social", "goverment"]

    fundamentals = fundamentals_score[["ticker", "currency_code", "industry_code"] + calculate_column]
    fundamentals = fundamentals.replace([np.inf, -np.inf], np.nan).copy()

    # add DLPA scores
    fundamentals = fundamentals.merge(universe_rating, on="ticker", how="left")
    print(fundamentals)

    # Scale original fundamental score
    fundamentals, minmax_column, fundamentals_factors_scores_col = score_update_scale(fundamentals, calculate_column,
                                                                                      universe_currency_code, factor_rank)

    # print(fundamentals.groupby(['currency_code'])[["ai_score", "ai_score2"]].agg(['min','mean','median','max']).transpose()[['HKD','USD','CNY','EUR']])

    fundamentals[['ai_score','ai_score2']] = fundamentals[['ai_score','ai_score2']].clip(0, 10)
    fundamentals[['ai_score','ai_score2',"esg"]] = fundamentals[['ai_score','ai_score2',"esg"]].round(1)
    fundamentals[fundamentals_factors_scores_col] = fundamentals[fundamentals_factors_scores_col].round(1)

    fundamentals["trading_day"] = dateNow()
    fundamentals = uid_maker(fundamentals, uid="uid", ticker="ticker", trading_day="trading_day")

    universe_rating_history = fundamentals[["uid", "ticker", "trading_day", "fundamentals_value", "fundamentals_quality",
                                            "fundamentals_momentum", "esg", "ai_score", "ai_score2", "wts_rating", "dlp_1m",
                                            "dlp_3m", "wts_rating2", "classic_vol", "fundamentals_extra",
                                            "ai_score_unscaled", "ai_score2_unscaled"]]

    universe_rating_detail_history = fundamentals[minmax_column]

    print("=== Calculate Fundamentals Value & Fundamentals Quality DONE ===")
    if(len(fundamentals)) > 0 :
        print(fundamentals)
        result = fundamentals[["ticker", "fundamentals_value", "fundamentals_quality", "fundamentals_momentum", "fundamentals_extra", "esg",
                               "ai_score", "ai_score2"]].merge(universe_rating, how="left", on="ticker")
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
        result = result.rename(columns={"UDDE": "amount", "index":"ex_dividend_date"})
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
        upsert_data_to_database(result, get_data_fred_table_name(), "trading_day", how="update", Date=True)
        report_to_slack("{} : === Fred Updated ===".format(datetimeNow()))

def populate_ibes_table():
    ''' (Update) obsolete '''
    table_name = get_data_ibes_table_name()
    start_date = backdate_by_month(30)
    ibes_data = get_data_ibes_monthly(start_date)
    ibes_data = ibes_data.drop(columns=["trading_day"])
    upsert_data_to_database(ibes_data, table_name, "uid", how="update", Text=True)
    report_to_slack("{} : === Data IBES Update Updated ===".format(datetimeNow()))

def update_ibes_data_monthly_from_dsws(ticker=None, currency_code=None, history=False):
    ''' (Update) weekly ingestion of IBES data '''

    # Prep 1. field: get data_ibes_monthly ingestion field from Table ingesion_name
    df = get_ingestion_name_source()
    filter_field = df.loc[df['ibes_monthly'], ['dsws_name', 'our_name']].values.tolist()

    # Prep 2. ticker: make sure input ticker list is always active ticker
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    tickers = universe["ticker"].to_list()
    print(universe)

    # Prep 3. period_end: for company without Dec year_end, there could be available field in future
    end_date = dateNow()
    start_date = dateNow()
    if(history):
        start_date = "2000-01-01"

    # start ingestion
    data = []
    for field_dsws, field_rename in filter_field:
        result = get_data_history_frequently_by_field_from_dsws(start_date, end_date, tickers, identifier, [field_dsws],
                                                                       use_ticker=True, split_number=1, monthly=True)
        if(len(result)) > 0:
            result = result.rename(columns={"index": "trading_day", field_dsws: field_rename})
            data.append(result.set_index(["ticker", "trading_day"]))
            print(result)
            # result["year"] = pd.DatetimeIndex(result["trading_day"]).year
            # result["month"] = pd.DatetimeIndex(result["trading_day"]).month
            # result.loc[result["month"].isin([1, 2, 3]), "period_end"] = result["year"].astype(str) + "-" + "03-31"
            # result.loc[result["month"].isin([4, 5, 6]), "period_end"] = result["year"].astype(str) + "-" + "06-30"
            # result.loc[result["month"].isin([7, 8, 9]), "period_end"] = result["year"].astype(str) + "-" + "09-30"
            # result.loc[result["month"].isin([10, 11, 12]), "period_end"] = result["year"].astype(str) + "-" + "12-31"
            # result["year"] = np.where(result["month"].isin([1, 2]), result["year"] - 1, result["year"])
            # result.loc[result["month"].isin([12, 1, 2]), "period_end"] = result["year"].astype(str) + "-" + "12-31"
            # result.loc[result["month"].isin([3, 4, 5]), "period_end"] = result["year"].astype(str) + "-" + "03-31"
            # result.loc[result["month"].isin([6, 7, 8]), "period_end"] = result["year"].astype(str) + "-" + "06-30"
            # result.loc[result["month"].isin([9, 10, 11]), "period_end"] = result["year"].astype(str) + "-" + "09-30"
            # result["year"] = np.where(result["month"].isin([1, 2]), result["year"] + 1, result["year"])
            # result = result.drop(columns=["month", "year"])

    result = pd.concat(data, axis=1)
    result = result.reset_index()
    result = uid_maker(result)
    upsert_data_to_database(result, get_data_ibes_monthly_table_name(), "uid", how="update", Text=True)
    report_to_slack("{} : === Data IBES Monthly Update Updated ===".format(datetimeNow()))
    # populate_ibes_table()

def get_fred_csv_monthly(start_date = backdate_by_month(6), end_date = dateNow()):
    print(f"=== Read Fred Data ===")
    try :
        data = read_fred_csv(start_date, end_date)
        data = data.rename(columns={"data" : "fred_data"})
        data["fred_data"] = np.where(data["fred_data"]== ".", 0, data["fred_data"])
        data["trading_day"] = pd.to_datetime(data["trading_day"])
        data["fred_data"] = data["fred_data"].astype(float)
        data = data.loc[data["fred_data"] > 0]
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
    ''' obsolete '''
    table_name = get_data_macro_table_name()
    end_date = dateNow()
    start_date = backdate_by_month(30)
    macro_data = get_data_macro_monthly(start_date)
    macro_data = macro_data.drop(columns=["trading_day"])
    macro_data = macro_data.dropna(subset=["period_end"])
    upsert_data_to_database(macro_data, table_name, "period_end", how="update", Text=True)
    report_to_slack("{} : === Data MACRO Update Updated ===".format(datetimeNow()))

def update_macro_data_monthly_from_dsws():
    ''' (Update) Weekly ingestion of data_macro_monthly '''

    print("Get Macro Data From DSWS")

    # Prep 1. field: get data_macro_monthly ingestion field from Table ingesion_name_macro
    df = get_ingestion_name_macro_source()
    ticker_quarterly_field = df.loc[df['quarterly'], ['dsws_name', 'our_name']].set_index('dsws_name')['our_name'].to_dict()
    ticker_monthly_field = df.loc[df['monthly'], ['dsws_name', 'our_name']].set_index('dsws_name')['our_name'].to_dict()

    # Prep 2. ticker: make sure input ticker list is always active ticker
    end_date = dateNow()
    start_date_month = backdate_by_month(3)
    start_date_quarter = backdate_by_month(6)

    result_vix = get_vix_since(date=start_date_quarter).drop(columns=["uid"])
    result_vix = result_vix.set_index(["trading_day","vix_id"])["vix_value"].unstack()
    result_vix.columns = [x.lower() for x in result_vix.columns.to_list()]
    result_vix = result_vix.resample('D').ffill().reset_index()
    # ticker_vix = ["CBOEVIX", "VHSIVOL", "VSTOXXI", "VKOSPIX"]
    # filter_field_vix = ["PI"]
    # result_monthly_vix, except_field = get_data_history_frequently_from_dsws(start_date_month, end_date, ticker_vix, identifier, filter_field_vix, use_ticker=False, split_number=1, monthly=True)
    # print(result_monthly_vix)
    # if(len(result_monthly_vix)) > 0 :
    #     result_monthly_vix = result_monthly_vix.reset_index(drop=True)
    #     result_monthly_vix = result_monthly_vix.rename(columns={"index" : "trading_day"})
    #     result_monthly_vix["year"] = pd.DatetimeIndex(result_monthly_vix["trading_day"]).year
    #     result_monthly_vix["month"] = pd.DatetimeIndex(result_monthly_vix["trading_day"]).month
    #     result_monthly_vix["year_month"] = result_monthly_vix["month"].astype(str) + "-" + result_monthly_vix["year"].astype(str)
    #     data_monthly_vix = pd.DataFrame(result_monthly_vix["year_month"].unique(), columns =["year_month"])
    #     data_monthly_vix = data_monthly_vix.set_index("year_month")
    #     for index, row in result_monthly_vix.iterrows():
    #         ticker_field = row["ticker"]
    #         year_month = row["year_month"]
    #         if (row["ticker"] == "CBOEVIX") :
    #                 ticker_field = "cboevix"
    #         elif (row["ticker"] == "VHSIVOL") :
    #                 ticker_field = "vhsivol"
    #         elif (row["ticker"] == "VSTOXXI") :
    #                 ticker_field = "vstoxxi"
    #         elif (row["ticker"] == "VKOSPIX") :
    #                 ticker_field = "vkospix"
    #         data_field = row["PI"]
    #         data_monthly_vix.loc[year_month, ticker_field] = data_field
    #     data_monthly_vix = data_monthly_vix.reset_index(inplace=False)
    #     print(data_monthly_vix)

    filter_field = ["ESA"]
    identifier="ticker"
    result_monthly, except_field = get_data_history_frequently_from_dsws(start_date_month, end_date, list(ticker_monthly_field.keys()), identifier, filter_field, use_ticker=False, split_number=1, monthly=True)
    result_quarterly, except_field = get_data_history_frequently_from_dsws(start_date_quarter, end_date, list(ticker_quarterly_field.keys()), identifier, filter_field, use_ticker=False, split_number=1, quarterly=True)
    print(result_monthly)
    print(result_quarterly)

    result = pd.concat([result_monthly, result_quarterly], axis=0).dropna(how='any')      # combine monthly, weekly ingestion
    if(len(result)) > 0 :
        result = result.rename(columns={"index" : "trading_day"})
        result["trading_day"] = pd.to_datetime(result["trading_day"])
        result['ticker'] = result['ticker'].replace(ticker_monthly_field)
        result['ticker'] = result['ticker'].replace(ticker_quarterly_field)
        print(result)
        result = pd.pivot_table(data=result, index=["trading_day"], columns=["ticker"], values="ESA").reset_index()
        result = result.merge(result_vix, on=["trading_day"], how='left')
        # result["year"] = pd.DatetimeIndex(result["trading_day"]).year
        # result["month"] = pd.DatetimeIndex(result["trading_day"]).month
        # result["year_month"] = result["month"].astype(str) + "-" + result["year"].astype(str)
        # data_monthly = pd.DataFrame(result["year_month"].unique(), columns =["year_month"])
        # data_monthly = data_monthly.set_index("year_month")
        # for index, row in result.iterrows():
        #     ticker_field = row["ticker"]
        #     year_month = row["year_month"]
        #     month = row["month"]
        #     year = row["year"]
        #     trading_day = row["trading_day"]
        #     if (row["ticker"] == "USGBILL3") :
        #         ticker_field = "usgbill3"
        #     elif (row["ticker"] == "USINTER3") :
        #         ticker_field = "usinter3"
        #     elif (row["ticker"] == "JPMSHORT") :
        #         ticker_field = "jpmshort"
        #     elif (row["ticker"] == "EMGBOND.") :
        #         ticker_field = "emgbond"
        #     elif (row["ticker"] == "CHGBOND.") :
        #         ticker_field = "chgbond"
        #     elif (row["ticker"] == "EMIBOR3.") :
        #         ticker_field = "emibor3"
        #     data_field = row["ESA"]
        #     data_monthly.loc[year_month, ticker_field] = data_field
        #     data_monthly.loc[year_month, "month"] = month
        #     data_monthly.loc[year_month, "year"] = year
        #     data_monthly.loc[year_month, "trading_day"] = trading_day.replace(day=15)
        # data_monthly = data_monthly.reset_index(inplace=False)
        # data_monthly = data_monthly.sort_values(by="trading_day", ascending=False)
        # fred_data = get_fred_csv_monthly(start_date = start_date_month, end_date = end_date)
        # data_monthly = data_monthly.merge(fred_data, how="left", on=["year", "month"])
        # data_monthly.loc[data_monthly["month"].isin([1, 2, 3]), "quarter"] = 1
        # data_monthly.loc[data_monthly["month"].isin([4, 5, 6]), "quarter"] = 2
        # data_monthly.loc[data_monthly["month"].isin([7, 8, 9]), "quarter"] = 3
        # data_monthly.loc[data_monthly["month"].isin([10, 11, 12]), "quarter"] = 4
        # print(data_monthly)
    # if(len(result_quarterly)) > 0 :
    #     result_quarterly = result_quarterly.rename(columns={"index": "trading_day"})
    #     data_quarterly = result_quarterly.loc[result_quarterly["ticker"] == "CHGDP...C"][["trading_day"]]
    #     result_quarterly["year"] = pd.DatetimeIndex(result_quarterly["trading_day"]).year
    #     result_quarterly["month"] = pd.DatetimeIndex(result_quarterly["trading_day"]).month
    #     data_quarterly["year"] = pd.DatetimeIndex(data_quarterly["trading_day"]).year
    #     data_quarterly["month"] = pd.DatetimeIndex(data_quarterly["trading_day"]).month
    #     data_quarterly = data_quarterly.set_index("month")
    #     for index, row in result_quarterly.iterrows():
    #         ticker_field = row["ticker"]
    #         if (row["ticker"] == "CHGDP...C") :
    #             ticker_field = "chgdp"
    #         elif (row["ticker"] == "JPGDP...D") :
    #             ticker_field = "jpgdp"
    #         elif (row["ticker"] == "USGDP...D") :
    #             ticker_field = "usgdp"
    #         elif (row["ticker"] == "EMGDP...D") :
    #             ticker_field = "emgdp"
    #         month = row["month"]
    #         data_field = row["ESA"]
    #         data_quarterly.loc[month, ticker_field] = data_field
    #         #data_quarterly.loc[month, "period_end"] = datetime.strptime("", "%Y-%m-%d")
    #     data_quarterly = data_quarterly.reset_index(inplace=False)
    #     data_quarterly.loc[data_quarterly["month"].isin([1, 2, 3]), "quarter"] = 1
    #     data_quarterly.loc[data_quarterly["month"].isin([4, 5, 6]), "quarter"] = 2
    #     data_quarterly.loc[data_quarterly["month"].isin([7, 8, 9]), "quarter"] = 3
    #     data_quarterly.loc[data_quarterly["month"].isin([10, 11, 12]), "quarter"] = 4
    #     for index, row in data_quarterly.iterrows():
    #         if (row["quarter"] == 1):
    #             period_end = str(row["year"]) + "-" + "03-31"
    #         elif (row["quarter"] == 2):
    #             period_end = str(row["year"]) + "-" + "06-30"
    #         elif (row["quarter"] == 3):
    #             period_end = str(row["year"]) + "-" + "09-30"
    #         else:
    #             period_end = str(row["year"]) + "-" + "12-31"
    #         data_quarterly.loc[index, "period_end"] = datetime.strptime(period_end, "%Y-%m-%d")
    #     # data_quarterly["year"] = np.where(data_quarterly["month"] == 12, data_quarterly["year"] + 1, data_quarterly["year"])
    #     data_quarterly = data_quarterly.drop(columns=["month", "trading_day"])
    #     print(data_quarterly)
    # result = data_monthly.merge(data_quarterly, how="left", on=["year", "quarter"])
    # result = result.merge(data_monthly_vix, how="left", on=["year_month"])
        # result = result.drop(columns=["month", "year", "quarter", "year_month"])
        # result = result.dropna(subset=["period_end"])
        upsert_data_to_database(result, get_data_macro_monthly_table_name(), "trading_day", how="update", Text=True)
        report_to_slack("{} : === Data MACRO Monthly Update Updated ===".format(datetimeNow()))
        # populate_macro_table()

def worldscope_report_date_format_change(data):
    ''' change format for report_date fetch from DSWS '''

    data["report_date"] = data["report_date"].astype(str)
    data["report_date"] = data["report_date"].str.slice(6, 16)
    data["report_date"] = pd.to_numeric(data["report_date"], errors='raise')
    data = data.dropna(subset=["report_date"], inplace=False)

    data["report_date"] = data["report_date"].astype(int)
    data["report_date"] = np.where(data["report_date"] > 9000000000, data["report_date"] / 10, data["report_date"])
    data["report_date"] = pd.to_datetime(data["report_date"], unit='s')
    
    return data

# def worldscope_quarter_report_date_from_dsws(ticker = None, currency_code=None, history=False):
#     ''' (update) ingestion "report_date" quarterly worldscope fields for missing fields only '''
#
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
#             result, error_ticker = get_data_history_from_dsws(period_end, period_end, ticker, identifier, filter_field,
#                                                               use_ticker=True, split_number=min(len(universe), 1), dsws=False)
#             if(len(result) == 0):
#                 result = ticker
#                 result[field_rename] = np.nan
#                 result["level_1"] = str_to_date(period_end)
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
#
#                     data.loc[index, "period_end"] = datetime(data.loc[index, "year"], data.loc[index, "month"], 1)
#                 data["period_end"] = data["period_end"].dt.to_period("M").dt.to_timestamp("M")
#                 data["period_end"] = pd.to_datetime(data["period_end"])
#
#                 data = uid_maker(data, trading_day="period_end")
#                 data = data.drop(columns=["WC05905A", "year", "month", "day", "frequency_number"])
#                 data = data.drop_duplicates(subset=["uid"], keep="first", inplace=False)
#                 print(data)
#                 upsert_data_to_database(data, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)
#         except Exception as e:
#             print("{} : === ERROR === : {}".format(dateNow(), e))

def update_worldscope_quarter_summary_from_dsws(ticker = None, currency_code=None, history=False):
    ''' (updated) ingestion quarterly worldscope fields for missing fields only '''

    # Prep 1. field: get data_worldscope_summary ingestion field from Table ingesion_name
    df = get_ingestion_name_source()
    filter_field = df.loc[df['worldscope_summary'], ['dsws_name','our_name']].values.tolist()
    filter_field.append(["WC05905A", "report_date"])

    # Prep 2. ticker: make sure input ticker list is always active ticker
    identifier = "ticker"
    universe = get_active_universe_by_entity_type(ticker=ticker, currency_code=currency_code)
    tickers = universe["ticker"].to_list()

    # Prep 3. period_end: for company without Dec year_end, there could be available field in future
    end_date = forwarddate_by_month(9)
    start_date = backdate_by_month(6)  # ingest weekly, every time lookback to 6m ago
    if (history):  # if re-ingest all
        start_date = "2000-03-31"
    period_end_list = get_period_end_list(start_date=start_date, end_date=end_date)

    # start ingestion
    for field_dsws, field_rename in filter_field:
        data_missing_data = []
        data_ingest = []
        for period_end in period_end_list:
            print(field_dsws, period_end)

            # only fetch missing field ticker
            missing_data = get_missing_field_ticker_list(table_name=get_data_worldscope_summary_table_name(),
                                                         field=field_rename, tickers=tickers, period_end=period_end)
            missing_tickers_universe = universe.loc[universe['ticker'].isin(missing_data['ticker'].to_list()), ['ticker']]
            if len(missing_tickers_universe)==0:    # if no missing continue with next period/field
                continue
            data_missing_data.append(missing_data)

            # missing_tickers_universe = universe[["ticker"]]

            # fetch from dsws
            result, error_ticker = get_data_history_from_dsws(period_end, period_end, missing_tickers_universe, identifier,
                                                              [field_dsws], use_ticker=True, split_number=1, dsws=False)
            if(len(result)==0):
                continue            # if no return for (period, field) -> next period_end
            data_ingest.append(result)

        if len(data_missing_data)==0:
            continue                # if no missing_field for (field) -> next field
        else:
            missing_data = pd.concat(data_missing_data, axis=0)

        # concat single field ingested data for each ticker
        if len(data_ingest)==0:
            continue                # if no return for (field) -> next field
        else:
            result = pd.concat(data_ingest, axis=0)

        result = result.rename(columns = {"level_1" : "period_end", field_dsws: field_rename})  # rename
        result = result[["ticker", "period_end", field_rename]]
        print(result)

        if field_rename == "report_date":       # for report_date -> extra format_change
            result = worldscope_report_date_format_change(result)
        else:
            result[field_rename] = result[field_rename].astype(str)     # all missing -> NaN
            result[field_rename] = np.where(result[field_rename] == "nan", np.nan, result[field_rename])
            result[field_rename] = np.where(result[field_rename] == "NA", np.nan, result[field_rename])
            result[field_rename] = np.where(result[field_rename] == "None", np.nan, result[field_rename])
            result[field_rename] = np.where(result[field_rename] == "", np.nan, result[field_rename])
            result[field_rename] = np.where(result[field_rename] == "NaN", np.nan, result[field_rename])
            result[field_rename] = np.where(result[field_rename] == "NaT", np.nan, result[field_rename])
            result[field_rename] = result[field_rename].astype(float)
            result = result.dropna(subset=[field_rename], inplace=False)

        # update ingested results to missing_data DataFrame
        missing_data['period_end'] = pd.to_datetime(missing_data['period_end'])
        missing_data = missing_data.set_index(["ticker", "period_end"])
        result = result.set_index(["ticker", "period_end"])
        missing_data[field_rename] = result[field_rename]
        result = missing_data.reset_index(drop=False)

        # add Date reference columns
        result["period_end"] = pd.to_datetime(result["period_end"])
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
        # result["fiscal_quarter_end"] = result["period_end"].astype(str)
        # result["fiscal_quarter_end"] = result["fiscal_quarter_end"].str.replace("-", "", regex=True)
        result = result.drop(columns=["month", "day"])
        worldscope_identifier = universe[["ticker", "worldscope_identifier"]].set_index(['ticker'])['worldscope_identifier'].to_dict()
        result['worldscope_identifier'] = result['ticker'].map(worldscope_identifier)
        # result = result.merge(worldscope_identifier, how="left", on="ticker")
        result = result.drop_duplicates(subset=["uid"], keep="first", inplace=False)

        # upsert to database for each field ingested
        upsert_data_to_database(result, get_data_worldscope_summary_table_name(), "uid", how="update", Text=True)

    report_to_slack("{} : === Quarter Summary Data Updated ===".format(datetimeNow()))

def update_rec_buy_sell_from_dsws(ticker=None, currency_code=None):
    print("{} : === RECSELL RECBUY Start Ingestion ===".format(datetimeNow()))
    universe = get_all_universe(ticker=ticker, currency_code=currency_code)
    print(universe)
    filter_field = ["RECSELL", "RECBUY"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], "ticker", filter_field, use_ticker=True, split_number=min(len(universe), 1))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"RECSELL": "recsell", "RECBUY" : "recbuy", "index":"ticker"})
        result["trading_day"] = find_nearest_specific_days(days=6)
        result = uid_maker(result, uid="uid", ticker="ticker", trading_day="trading_day")
        result1 = result.loc[result["recsell"] != "NA"][["uid", "trading_day", "ticker", "recsell"]]
        result2 = result.loc[result["recsell"] != "NA"][["uid", "trading_day", "ticker", "recbuy"]]
        print(result1)
        print(result2)
        upsert_data_to_database(result1, get_universe_rating_history_table_name(), "uid", how="update", Text=True)
        upsert_data_to_database(result2, get_universe_rating_history_table_name(), "uid", how="update", Text=True)
        report_to_slack("{} : === RECSELL RECBUY Updated ===".format(datetimeNow()))

def update_currency_code_from_dsws(ticker=None, currency_code=None):
    print("{} : === CURRENCY CODE Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["currency_code"])
    filter_field = ["NPCUR"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], "ticker", filter_field, use_ticker=True, split_number=min(len(universe), 1))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"NPCUR": "currency_code", "index":"ticker"})
        result = remove_null(result, "currency_code")
        result = universe.merge(result, how="left", on=["ticker"])
        result["currency_code"] = result["currency_code"].str.upper()
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Currency Code Updated ===".format(datetimeNow()))
        update_universe_where_currency_code_null()

def update_lot_size_from_dsws(ticker=None, currency_code=None):
    print("{} : === LOT SIZE Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["lot_size"])
    filter_field = ["LSZ"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], "ticker", filter_field, use_ticker=True, split_number=min(len(universe), 1))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"LSZ": "lot_size", "index":"ticker"})
        result = remove_null(result, "lot_size")
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === Lot Size Updated ===".format(datetimeNow()))

def update_mic_from_dsws(ticker=None, currency_code=None):
    print("{} : === MIC Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["mic"])
    filter_field = ["SEGM"]
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], "ticker", filter_field, use_ticker=True, split_number=min(len(universe), 1))
    print(result)
    if(len(result)) > 0 :
        result = result.rename(columns={"SEGM": "mic", "index":"ticker"})
        result = remove_null(result, "mic")
        result["mic"] = np.where(result["mic"] == "XETB", "XETA", result["mic"])
        result["mic"] = np.where(result["mic"] == "XXXX", "XNAS", result["mic"])
        result["mic"] = np.where(result["mic"] == "MTAA", "XMIL", result["mic"])
        result["mic"] = np.where(result["mic"] == "WBAH", "XEUR", result["mic"])
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), "ticker", how="update", Text=True)
        report_to_slack("{} : === MIC Updated ===".format(datetimeNow()))

def update_ibes_currency_from_dsws(ticker=None, currency_code=None):
    print("{} : === IBES Currency Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["currency_code_ibes"])
    filter_field = ["IBCUR"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
    result = result.rename(columns={"IBCUR": "currency_code_ibes", "index":"ticker"})
    result = remove_null(result, "currency_code_ibes")
    print(result)
    if(len(result)) > 0 :
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === IBES Currency Updated ===".format(datetimeNow()))
    
    update_worldscope_currency_from_dsws(ticker=ticker, currency_code=currency_code)

def update_worldscope_currency_from_dsws(ticker=None, currency_code=None):
    ''' get currency code used by worldscope: currency for the primary listing country - get nation_code from WC06027'''
    print("{} : === Worldscope Currency Start Ingestion ===".format(datetimeNow()))
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    nation_code_to_curr_dict = get_iso_currency_code_map()
    universe = universe.drop(columns=["currency_code_ws"])
    filter_field = ["WC06027"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 1))
    result = result.rename(columns={"WC06027": "currency_code_ws", "index":"ticker"})
    result["currency_code_ws"] = result["currency_code_ws"].map(nation_code_to_curr_dict)
    result = remove_null(result, "currency_code_ws")
    print(result)
    if(len(result)) > 0 :
        result = universe.merge(result, how="left", on=["ticker"])
        print(result)
        upsert_data_to_database(result, get_universe_table_name(), identifier, how="update", Text=True)
        report_to_slack("{} : === Worldscope Currency Updated ===".format(datetimeNow()))

def update_currency_price_from_dsws(currency_code=None):
    print("{} : === Currency Price Start Ingestion ===".format(datetimeNow()))
    currency = get_active_currency_ric_not_null(currency_code=currency_code)
    currency = currency.drop(columns=["last_date", "last_price"])
    ticker = list(set(get_currency_code_ibes_ws().iloc[:, 1:].values.flatten()))
    ticker = ["<USD"+x+"FIXM=WM>" for x in ticker if x and (x not in ["KHR", "USD"])]
    start_date = dateNow()
    end_date = dateNow()
    filter_field = ["ER"]
    result = fetch_data_from_dsws(start_date, end_date, ticker, filter_field, dsws=True)
    result = result.reset_index(inplace=False)
    result = result.rename(columns={"ER": "last_price", "level_0":"currency_code", "level_1" : "last_date"})
    result["currency_code"] = result["currency_code"].str[4:7]
    result["last_price"] = np.where(result["currency_code"] == "EUR", 1/result["last_price"], result["last_price"])
    result["last_price"] = np.where(result["currency_code"] == "GBP", 1/result["last_price"], result["last_price"])
    result["last_price"] = np.where(result["currency_code"] == "AUD", 1/result["last_price"], result["last_price"])
    result["last_price"] = np.where(result["currency_code"] == "USD", 1, result["last_price"])
    result["last_price"] = np.where(result["currency_code"] == "KHR", 4070, result["last_price"])
    print(result)
    if(len(result)) > 0 :
        currency = currency.merge(result, how="left", on=["currency_code"])
        print(currency)
        upsert_data_to_database(currency, get_currency_table_name(), "currency_code", how="update", Text=True)
        report_to_slack("{} : === Currency Price Updated ===".format(datetimeNow()))
        result = uid_maker(result, uid="uid", ticker="currency_code", trading_day="last_date")
        upsert_data_to_database(result, get_currency_price_history_table_name(), "uid", how="update", Text=True)