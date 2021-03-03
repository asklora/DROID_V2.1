from general.data_process import uid_maker
from general.table_name import get_bot_data_table_name
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
from bot.data_download import (
    get_currency_data, 
    get_data_vix_price, 
    get_data_vol_surface_ticker, 
    get_latest_price, 
    get_master_tac_price)
from bot.preprocess import remove_holidays, lookback_creator, remove_holidays_forward
from bot.vol_calculations import get_close_vol, get_kurt, get_rogers_satchell, get_total_return
from general.sql_output import truncate_table, upsert_data_to_database
from general.sql_query import get_active_universe
from general.date_process import dateNow, droid_start_date
from global_vars import period, index_to_etf_file

def populate_bot_data(start_date=None, end_date=None, ticker=None, currency_code=None, daily=False, new_ticker=False, history=False):
    if type(start_date) == type(None):
        start_date = droid_start_date()
    if type(end_date) == type(None):
        end_date = dateNow()
    # Get all the prices for the above dates +-4 weeks to make sure all the nans are covered by back filling
    prices_df = get_master_tac_price(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)

    #Adding Latest Price to Master TAC
    if (daily):
        intraday_price = get_latest_price(ticker=ticker, currency_code=currency_code)
        last_date = intraday_price.last_date.max()
        last_price = intraday_price[["open", "high", "low", "close", "ticker"]]
        if prices_df.trading_day.max() <= last_date:
            prices_df = prices_df.drop(prices_df[prices_df.trading_day == last_date].index)
            last_price = last_price.assign(day_status="trading_day")
            last_price = last_price.assign(trading_day=last_date)
        prices_df = pd.concat([prices_df, last_price], axis=0, join="outer")

    #Get Vol Surface Parameter Ticker That Not Infer
    outputs_df = get_data_vol_surface_ticker(ticker=ticker, currency_code=currency_code)

    #Prepare All Data for Calculation
    dates_df = prices_df.pivot_table(index="trading_day", columns="ticker", values="day_status", aggfunc="first",dropna=False)
    cond = (prices_df["trading_day"] >= start_date) & (prices_df["trading_day"] <= end_date)
    trading_day_list = prices_df.loc[cond, "trading_day"].unique()
    trading_day_list = np.sort(trading_day_list)
    vix_data = get_data_vix_price()
    currency_data = get_currency_data(currency_code=currency_code)
    vix_data = vix_data.merge(currency_data[["currency_code", "vix_id"]], on="vix_id", how="left")
    prices_df = prices_df.merge(vix_data[["vix_value", "currency_code", "trading_day"]], on=["currency_code", "trading_day"],
                                how="left")

    main_columns_list = ["ticker", "trading_day", "c2c_vol_0_21", "c2c_vol_21_42", "c2c_vol_42_63", "c2c_vol_63_126",
                         "c2c_vol_126_252", "c2c_vol_252_504", "kurt_0_504", "rs_vol_0_21", "rs_vol_21_42",
                         "rs_vol_42_63", "rs_vol_63_126", "rs_vol_126_252", "rs_vol_252_504", "total_returns_0_1",
                         "total_returns_0_21", "total_returns_0_63", "total_returns_21_126", "total_returns_21_231"]
    main_df = pd.DataFrame(columns=main_columns_list)
    # *********************************************************************************
    for trading_day in trading_day_list:
        # For each day technicals are calculated separately and at the end all are appended together.
        valid_tickers = (dates_df.loc[trading_day] == "trading_day")
        valid_tickers_list = valid_tickers[valid_tickers == True].index

        temp_df = pd.DataFrame(columns=main_columns_list)
        temp_df.ticker = valid_tickers_list
        temp_df.trading_day = trading_day
        prices_df_temp = prices_df[(prices_df["trading_day"] <= trading_day) & (prices_df["trading_day"] > trading_day - BDay(period * 48))]
        prices_df_temp_forward = prices_df[(prices_df["trading_day"] >= trading_day - BDay(1)) &
                                           (prices_df["trading_day"] <= trading_day + BDay(125 * 2))]

        main_open, main_high, main_low, main_close, main_tri, main_multiples = remove_holidays(prices_df_temp)
        main_multiples_f = remove_holidays_forward(prices_df_temp_forward)

        main_open = main_open.bfill().ffill()
        main_high = main_high.bfill().ffill()
        main_low = main_low.bfill().ffill()
        main_close = main_close.bfill().ffill()
        main_tri = main_tri.bfill().ffill()
        main_multiples = main_multiples.bfill().ffill()
        main_multiples_f = main_multiples_f.bfill().ffill()

        main_open = main_open.replace(to_replace=0, method="ffill")
        main_high = main_high.replace(to_replace=0, method="ffill")
        main_low = main_low.replace(to_replace=0, method="ffill")
        main_close = main_close.replace(to_replace=0, method="ffill")
        main_tri = main_tri.replace(to_replace=0, method="ffill")
        main_multiples = main_multiples.replace(to_replace=0, method="ffill")
        main_multiples_f = main_multiples_f.replace(to_replace=0, method="ffill")

        # **********************************************************************************************
        # In case any stock did not exist for a period of time, and all the values for that period are just H.

        main_close = main_close.loc[:, ~main_close.fillna("H").eq("H").all()]
        main_open = main_open[main_close.columns]
        main_high = main_high[main_close.columns]
        main_low = main_low[main_close.columns]
        main_tri = main_tri[main_close.columns]
        main_multiples = main_multiples[main_close.columns]
        main_multiples_f = main_multiples_f[main_close.columns]
        # **********************************************************************************************
        c2c_vol_0_21 = get_close_vol(lookback_creator(main_multiples, 0, period))
        c2c_vol_21_42 = get_close_vol(lookback_creator(main_multiples, period, 2 * period))
        c2c_vol_42_63 = get_close_vol(lookback_creator(main_multiples, 2 * period, 3 * period))
        c2c_vol_63_126 = get_close_vol(lookback_creator(main_multiples, 3 * period, 6 * period))
        c2c_vol_126_252 = get_close_vol(lookback_creator(main_multiples, 6 * period, 12 * period))
        c2c_vol_252_504 = get_close_vol(lookback_creator(main_multiples, 12 * period, 24 * period))

        # c2c_vol_forward_0_21 = get_close_vol(forward_creator(main_multiples_f, 0, period))
        # c2c_vol_forward_0_63 = get_close_vol(forward_creator(main_multiples_f, 0, 3 * period))
        # c2c_vol_forward_0_126 = get_close_vol(forward_creator(main_multiples_f, 0, 6 * period))

        kurt_0_504 = get_kurt(lookback_creator(main_multiples, 0, 24 * period))
        rs_vol_0_21 = get_rogers_satchell(lookback_creator(main_open, 0, period),
                                          lookback_creator(main_high, 0, period),
                                          lookback_creator(main_low, 0, period),
                                          lookback_creator(main_close, 0, period))
        rs_vol_21_42 = get_rogers_satchell(lookback_creator(main_open, period, 2 * period),
                                           lookback_creator(main_high, period, 2 * period),
                                           lookback_creator(main_low, period, 2 * period),
                                           lookback_creator(main_close, period, 2 * period))
        rs_vol_42_63 = get_rogers_satchell(lookback_creator(main_open, 2 * period, 3 * period),
                                           lookback_creator(main_high, 2 * period, 3 * period),
                                           lookback_creator(main_low, 2 * period, 3 * period),
                                           lookback_creator(main_close, 2 * period, 3 * period))
        rs_vol_63_126 = get_rogers_satchell(lookback_creator(main_open, 3 * period, 6 * period),
                                            lookback_creator(main_high, 3 * period, 6 * period),
                                            lookback_creator(main_low, 3 * period, 6 * period),
                                            lookback_creator(main_close, 3 * period, 6 * period))
        rs_vol_126_252 = get_rogers_satchell(lookback_creator(main_open, 6 * period, 12 * period),
                                             lookback_creator(main_high, 6 * period, 12 * period),
                                             lookback_creator(main_low, 6 * period, 12 * period),
                                             lookback_creator(main_close, 6 * period, 12 * period))
        rs_vol_252_504 = get_rogers_satchell(lookback_creator(main_open, 12 * period, 24 * period),
                                             lookback_creator(main_high, 12 * period, 24 * period),
                                             lookback_creator(main_low, 12 * period, 24 * period),
                                             lookback_creator(main_close, 12 * period, 24 * period))

        total_returns_0_1 = get_total_return(lookback_creator(main_tri, 0, 1 + 1))
        total_returns_0_21 = get_total_return(lookback_creator(main_tri, 0, period + 1))
        total_returns_0_63 = get_total_return(lookback_creator(main_tri, 0, 3 * period + 1))
        total_returns_21_126 = get_total_return(lookback_creator(main_tri, period, 6 * period + 1))
        total_returns_21_231 = get_total_return(lookback_creator(main_tri, period, 231 + 1))

        technicals_list = [c2c_vol_0_21, c2c_vol_21_42, c2c_vol_42_63, c2c_vol_63_126,
                           c2c_vol_126_252, c2c_vol_252_504, kurt_0_504, rs_vol_0_21, rs_vol_21_42,
                           rs_vol_42_63, rs_vol_63_126, rs_vol_126_252, rs_vol_252_504, total_returns_0_1,
                           total_returns_0_21, total_returns_0_63, total_returns_21_126, total_returns_21_231]

        technicals_names_list = ["c2c_vol_0_21", "c2c_vol_21_42", "c2c_vol_42_63", "c2c_vol_63_126",
                                 "c2c_vol_126_252", "c2c_vol_252_504", "kurt_0_504", "rs_vol_0_21", "rs_vol_21_42",
                                 "rs_vol_42_63", "rs_vol_63_126", "rs_vol_126_252", "rs_vol_252_504",
                                 "total_returns_0_1", "total_returns_0_21", "total_returns_0_63",
                                 "total_returns_21_126",
                                 "total_returns_21_231"]

        def series_to_pandas(df):
            aa = pd.DataFrame(df[df.index.isin(valid_tickers_list)])
            aa.reset_index(inplace=True)
            return aa

        for i in range(len(technicals_list)):
            tech_temp = series_to_pandas(technicals_list[i])
            temp_df[technicals_names_list[i]] = np.where(tech_temp["ticker"] == temp_df["ticker"], tech_temp[0],
                                                         temp_df[technicals_names_list[i]])
        main_df = main_df.append(temp_df)
        print(f"{trading_day} is finished.")
    main_df = main_df.merge(prices_df[["vix_value", "ticker", "trading_day"]], on=["ticker", "trading_day"], how="left")
    main_df = main_df.merge(outputs_df, on=["ticker", "trading_day"], how="left")
    Y_columns_temp = ["slope", "atm_volatility_spot", "atm_volatility_one_year", "atm_volatility_infinity", "deriv_inf",
                      "deriv", "slope_inf", "ticker", "trading_day"]

    index_to_etf = pd.read_csv(index_to_etf_file, names=["index", "etf"])
    droid_universe_df = get_active_universe()
    droid_universe_df = droid_universe_df.merge(index_to_etf, on="index")
    etf_list = index_to_etf.etf.unique()

    main_df2 = main_df.copy()
    main_df = main_df2.copy()

    # Adding index vols to the main dataframe
    main_df = main_df.merge(droid_universe_df[["etf", "ticker"]], on="ticker", how="inner")
    etf_df = main_df[main_df.ticker.isin(etf_list)].copy()

    # main_df = main_df[~main_df.ticker.isin(etf_list)]
    added_list = ["total_returns_0_63", "total_returns_21_126", "total_returns_0_21", "total_returns_21_231",
                  "c2c_vol_0_21", "c2c_vol_21_42", "c2c_vol_42_63", "c2c_vol_63_126","c2c_vol_126_252",
                  "c2c_vol_252_504"]

    Y_columns_temp.extend(added_list)
    etf_df = etf_df[Y_columns_temp]
    etf_df.rename(columns={"slope": "slope_x", "atm_volatility_spot": "atm_volatility_spot_x",
                           "atm_volatility_one_year": "atm_volatility_one_year_x",
                           "atm_volatility_infinity": "atm_volatility_infinity_x", "ticker": "etf",
                           "total_returns_0_63": "total_returns_0_63_x", "total_returns_21_126": "total_returns_21_126_x",
                           "total_returns_0_21": "total_returns_0_21_x",
                           "total_returns_21_231": "total_returns_21_231_x", "c2c_vol_0_21": "c2c_vol_0_21_x",
                           "c2c_vol_21_42": "c2c_vol_21_42_x",
                           "c2c_vol_42_63": "c2c_vol_42_63_x", "c2c_vol_63_126": "c2c_vol_63_126_x",
                           "c2c_vol_126_252": "c2c_vol_126_252_x",
                           "c2c_vol_252_504": "c2c_vol_252_504_x", "deriv": "deriv_x", "slope_inf": "slope_inf_x",
                           "deriv_inf": "deriv_inf_x",
                           }, inplace=True)

    main_df = main_df.merge(etf_df, on=["etf", "trading_day"], how="left")
    main_df.drop(["uid", "stock_price", "parameter_set_date", "alpha", "slope_x", "slope_inf_x", "deriv_x","deriv_inf_x", "etf"], axis=1, inplace=True)
    
    # temp = get_active_universe()
    # temp = temp[["ticker", "industry_code"]]
    # temp[temp.industry_code == "NA"] = 0
    # main_df = main_df.merge(temp, on=["ticker"], how="left")

    main_df["uid"] = uid_maker(main_df, uid="uid", ticker="ticker", trading_day="trading_day")

    table_name = get_bot_data_table_name()
    if(daily):
        upsert_data_to_database(main_df, table_name, "uid", how="update", cpu_count=True, Text=True)
    elif(new_ticker):
        latest_main_df = main_df[main_df.trading_day == main_df.trading_day.max()]
        upsert_data_to_database(latest_main_df, table_name, "uid", how="update", cpu_count=True, Text=True)
    else:
        main_df.to_csv("main_df_executive.csv")
        truncate_table(table_name)
        upsert_data_to_database(main_df, table_name, "uid", how="update", cpu_count=True, Text=True)