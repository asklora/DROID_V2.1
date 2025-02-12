import time
import numpy as np
import pandas as pd
import lightgbm as lgb
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from bot.preprocess import rounding_fun
from bot.data_download import get_executive_data_download
from general.date_process import dateNow, timeNow, timestampNow
from general.data_process import uid_maker
from general.sql_query import get_active_universe
from general.sql_output import upsert_data_to_database
from general.table_name import get_data_vol_surface_inferred_table_name

from global_vars import random_state, saved_model_path, model_filename, X_columns, Y_columns, time_to_expiry, bots_list, max_vol, min_vol

def populate_vol_infer(start_date, end_date, ticker=None, currency_code=None, train_model=False, daily=False, history=False):
    '''

    Parameters
    ----------
    start_date : Date, bot_data sample start date = 4yr ago
    end_date : Date, bot_data sample end date = now
    ticker / currency_code : List, default = None
    train_model : production will sets as True
    daily : production will sets as False
    history : production will sets as False

    '''
    cols_temp_1 = X_columns.copy()
    cols_temp_1.extend(Y_columns[0])
    cols_temp_1.extend(Y_columns[1])

    cols_temp_2 = X_columns.copy()

    # get training sample from AWS TABLE bot_date (4yr ago - Now)
    main_df = get_executive_data_download(start_date, end_date, ticker=ticker, currency_code=currency_code, local=True)
    temp_y_rf = []
    
    # *****************************************************************************************
    droid_universe_df = get_active_universe()
    main_df = main_df.merge(droid_universe_df[["ticker", "currency_code"]], on="ticker", how="left")

    main_df_columns = cols_temp_1.copy()
    main_df_columns.extend(["uid", "ticker", "trading_day"])
    # Picking the final columns after research.
    main_df_copy_no_fund = main_df[main_df_columns].copy()
    
    # ******************************************** Best Model *******************************************
    start_date = main_df["trading_day"].min()
    end_date = main_df["trading_day"].max()

    main_df_copy_no_fund = main_df_copy_no_fund.infer_objects()
    main_train = main_df_copy_no_fund[~main_df_copy_no_fund.atm_volatility_spot.isna()].copy()
    not_inferred_tickers = main_train.ticker.unique()
    cond = (main_df_copy_no_fund.atm_volatility_spot.isna()) & (~main_df_copy_no_fund.ticker.isin(not_inferred_tickers))
    main_infer = main_df_copy_no_fund[cond].copy()
    main_train = main_train.reset_index()
    main_infer = main_infer.reset_index()

    # Filling nans by mean values grouped by ticker
    for col in cols_temp_1:
        try:
            main_train[col] = main_train[col].fillna(main_train.groupby("ticker")[col].transform("mean"))
        except:
            main_train = main_train.fillna(0)

    for col in cols_temp_2:
        try:
            main_infer[col] = main_infer[col].fillna(main_infer.groupby("ticker")[col].transform("mean"))
        except:
            main_infer = main_infer.fillna(0)

    trading_day_list = main_infer.trading_day
    ticker_list = main_infer.ticker

    X_col_list = X_columns
    state_ = ["not rounded", "rounded"]

    #Loop for Model Filename
    for i in range(len(model_filename)):
        Y_columns_list = Y_columns[i]

        # ***************************************************************************************************
        # ***************************************  PREPROCESSING  *******************************************

        if state_[i] == "rounded":
            main_train = rounding_fun(main_train)
        else:
            pass
        # ***************************************************************************************************
        if (i==0):
            main_infer_copy = main_infer.copy()
            main_train_copy = main_train.copy()
            # Remove infinite row
            for col in main_train_copy.columns:
                main_train_copy = main_train_copy.loc[main_train_copy[col] != np.inf]
            for col in main_infer_copy.columns:
                main_infer_copy = main_infer_copy.loc[main_infer_copy[col] != np.inf]

            X_train = main_train_copy[X_col_list]
            Y_train = main_train_copy[Y_columns_list]

            X_infer = main_infer_copy[X_col_list]

            # ERASE any data with Nones or NA--------------------------------------------
            # If None or NA in any, ERASE IN ALL THREE X_train, Y_train, X_infer
            # We didn"t have vol data for some etfs so I add the following lines so the model can work.
            X_train = X_train.bfill().ffill()
            Y_train = Y_train.bfill().ffill()
            X_infer = X_infer.bfill().ffill()

            X_train = X_train.fillna(0)
            Y_train = Y_train.fillna(0)
            X_infer = X_infer.fillna(0)

            if train_model:
                reg = RandomForestRegressor(n_estimators=200, max_depth=100, n_jobs=-1, verbose=100, random_state=random_state)
                reg.fit(X_train, Y_train)
                dump(reg, model_filename[i])

            if history or daily:
                reg = load(model_filename[i])
                y_rf = reg.predict(X_infer)
                temp_y_rf.append(pd.DataFrame(y_rf, columns=Y_columns_list))
        else:
            #Loop for slope & deriv
            for k in ["slope", "deriv"]:
                y_col = [k]

                X_train = main_train[X_col_list]
                Y_train = main_train[y_col]
                Y_train = Y_train.astype(int)
                X_infer = main_infer[X_col_list]

                # ERASE any data with Nones or NA--------------------------------------------
                # If None or NA in any, ERASE IN ALL THREE X_train, Y_train, X_infer
                # We didn"t have vol data for some etfs so I add this line so the model can work.
                X_train = X_train.bfill().ffill()
                Y_train = Y_train.bfill().ffill()
                X_infer = X_infer.bfill().ffill()

                lgb_train = lgb.Dataset(X_train, label=Y_train)

                params = {}
                params["learning_rate"] = 0.1
                params["boosting_type"] = "gbdt"  # GradientBoostingDecisionTree
                params["objective"] = "multiclass"  # Multi-class target feature
                params["metric"] = "multi_logloss"  # metric for multi-class
                params["max_depth"] = -1
                params["num_leaves"] = 50

                if (k=="slope"):
                    params["num_class"] = 6
                else:
                    params["num_class"] = 3

                if train_model:
                    gbm = lgb.train(params, lgb_train)
                    gbm.save_model(saved_model_path + y_col[0] + model_filename[i])

                if history or daily:
                    # load the model from disk
                    gbm = lgb.Booster(model_file=saved_model_path + y_col[0] + model_filename[i])
                    y_rf = gbm.predict(X_infer)
                    y_rf = [np.argmax(line) for line in y_rf]

                    if (k == "slope"):
                        temp_df_infer = pd.DataFrame(y_rf, columns=y_col)
                        temp_df_infer["slope_inf"] = temp_df_infer["slope"]
                    else:
                        temp_df_infer = pd.DataFrame(y_rf, columns=y_col)
                        temp_df_infer["deriv_inf"] = temp_df_infer["deriv"]
                        # This should be the reverse of what happens in the rounding function in process.py
                        temp_df_infer.loc[temp_df_infer["deriv"] == 0, "deriv"] = 0.11
                        temp_df_infer.loc[temp_df_infer["deriv"] == 1, "deriv"] = 0.075
                        temp_df_infer.loc[temp_df_infer["deriv"] == 2, "deriv"] = 0.025

                        temp_df_infer.loc[temp_df_infer["deriv_inf"] == 0, "deriv_inf"] = 0.06
                        temp_df_infer.loc[temp_df_infer["deriv_inf"] == 1, "deriv_inf"] = 0.06
                        temp_df_infer.loc[temp_df_infer["deriv_inf"] == 2, "deriv_inf"] = 0
                    temp_y_rf.append(temp_df_infer)

    if history or daily:
        # Preparing final inferred dataframe and writing it to aws.
        final_inferred = pd.concat([temp_y_rf[0], temp_y_rf[1], temp_y_rf[2]], axis=1)
        final_inferred["trading_day"] = trading_day_list
        final_inferred["ticker"] = ticker_list

        # Adding UID
        final_inferred = uid_maker(final_inferred, uid="uid", ticker="ticker", trading_day="trading_day")
        final_inferred = final_inferred.infer_objects()

        final_inferred = final_inferred[final_inferred.atm_volatility_one_year > min_vol * 0.65]
        final_inferred = final_inferred[final_inferred.atm_volatility_infinity > min_vol * 0.65]
        final_inferred = final_inferred[final_inferred.atm_volatility_one_year < max_vol * 1.25]
        final_inferred = final_inferred[final_inferred.atm_volatility_infinity < max_vol * 1.25]

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        print("Finished inferring. Writing to AWS.")
        table_name = get_data_vol_surface_inferred_table_name()
        if(daily):
            upsert_data_to_database(final_inferred, table_name, "uid", how="update", cpu_count=True, Text=True)
        else:
            final_inferred.to_csv("vol_history_infered.csv")
            # truncate_table(table_name)
            upsert_data_to_database(final_inferred, table_name, "uid", how="update", cpu_count=True, Text=True)
    # do_function("calculate_latest_vol_updates_not_us")
    # do_function("calculate_latest_vol_updates_us")
    finish = timeNow()
    print(finish)
    print("Finished!")

def model_trainer(train_df, infer_df, model_type, Y_columns=Y_columns[0], just_train=False):
    # This function will train the model either for history or for future inference.
    final_report = pd.DataFrame()
    for col in Y_columns:
        train_df_copy = train_df.copy()
        for cols in train_df_copy.columns:
            train_df_copy = train_df_copy.loc[train_df_copy[cols] != np.inf]

        infer_df_copy=None
        if(type(infer_df) != type(None)):
            infer_df_copy = infer_df.copy()
            for cols in infer_df_copy.columns:
                infer_df_copy = infer_df_copy.loc[infer_df_copy[cols] != np.inf]
        # Remove infinite row
        X_train = train_df_copy[X_columns]
        Y_train = train_df_copy[Y_columns]
        if not just_train:
            X_infer = infer_df_copy[X_columns]
            # Y_infer = infer_df[args.Y_columns]

            final_report["ticker"] = infer_df_copy.loc[:, "ticker"]
            final_report["spot_date"] = infer_df_copy.loc[:, "spot_date"]
            final_report["spot_price"] = infer_df_copy.loc[:, "spot_price"]

            for col2 in Y_columns:
                col22 = col2[:-6]
                final_report[col22] = infer_df_copy.loc[:, col22]

            final_report[col + "_original"] = infer_df_copy[col]

        X_train = X_train[Y_train[col].notna()]
        Y_train = Y_train[Y_train[col].notna()]
        # X_infer = X_infer[Y_infer[col].notna()]
        # Y_infer = Y_infer[Y_infer[col].notna()]

        X_train = X_train.bfill().ffill()
        Y_train = Y_train.bfill().ffill()

        if not just_train:
            X_infer = X_infer.bfill().ffill()
            X_infer = X_infer.fillna(0)

        # Y_infer = Y_infer.bfill().ffill()

        X_train = X_train.fillna(0)
        Y_train = Y_train.fillna(0)
        # Y_infer = Y_infer.fillna(0)

        reg = []
        if model_type == "rf":
            reg = RandomForestClassifier(n_estimators=200, max_depth=100, n_jobs=-1, random_state=random_state)
            reg.fit(X_train, Y_train[col])
            if just_train:
                dump(reg, saved_model_path + f"{col}_rf.joblib")

        if model_type == "lgbm":
            reg = lgb.LGBMClassifier(n_estimators=200, silent=True, random_state=random_state)
            reg.fit(X_train, Y_train[col])
            if just_train:
                dump(reg, saved_model_path + f"{col}_lgbm.joblib")

        # if model_type == "xgb":
        #     reg = xgb.XGBClassifier(n_estimators=200, random_state=random_state)
        #     reg.fit(X_train, Y_train[col])
        #     if just_train:
        #         dump(reg, saved_model_path + f"{col}_xgb.joblib")

        if not just_train:
            if len(X_infer) > 0:
                final_report[col + "_prob"] = (reg.predict_proba(X_infer))[:, 1]
            final_report["model_type"] = model_type
            final_report["when_created"] = timestampNow()
    if not just_train:
        return final_report


def find_rank(data, time_to_exp):
    # This function finds the rank for each bot.
    result = pd.DataFrame({"ticker":[], "spot_date":[], "bot_type":[], "bot_option_type":[], "time_to_exp_str":[], 
    "ranking":[], "bot_id":[]}, index=[])
    for index, row in data.iterrows():
        ticker = row["ticker"]
        spot_date = row["spot_date"]
        for cols in data.columns:
            column = cols.split("_")
            if(column[0] == "rank"):
                ranking = column[1]
                time_to_exp_str = column[2]
                bot = row[cols].split("_")
                bot_type = bot[0].upper()
                bot_option_type = bot[1]
                bot_id = bot_type + "_" + bot_option_type + "_" + time_to_exp_str
                data_temp = pd.DataFrame({"ticker":[ticker], "spot_date":[spot_date], "bot_type":[bot_type], 
                "bot_option_type":[bot_option_type], "time_to_exp_str":[time_to_exp_str], 
                "ranking":[ranking], "bot_id":[bot_id]}, index=[0])
                result = result.append(data_temp)
    result["time_to_exp"] = 0
    for time_exp in time_to_exp:
        time_exp_str = str(time_exp).replace(".", "")
        result["time_to_exp"] = np.where(result["time_to_exp_str"] == time_exp_str, time_exp, result["time_to_exp"])
    return result

def sort_to_rank(row, rank_columns, time_to_exp):
    # This function finds the rank for each bot.
    for time_exp in time_to_exp:
        time_exp_str = str(time_exp).replace(".", "")
        rank_columns_temp = []
        for cols in rank_columns:
            if(cols.split("_")[2] == time_exp_str):
                rank_columns_temp.append(cols)
        row2 = row[rank_columns_temp].sort_values(ascending=False)
        for i in range(len(rank_columns_temp)):
            row[f"rank_{i + 1}_{time_exp_str}"] = row2.index[i].replace("_pnl_class_prob", "")
    return row

def bot_infer(infer_df, model_type, rank_columns, Y_columns, time_to_exp=time_to_expiry, bots_list=bots_list):
    # This function is used for bot ranking daily and live.
    final_report = pd.DataFrame()
    for cols in infer_df.columns:
        infer_df = infer_df.loc[infer_df[cols] != np.inf]

    for col in Y_columns:
        X_infer = infer_df[X_columns]

        final_report["ticker"] = infer_df.loc[:, "ticker"]
        final_report["spot_date"] = infer_df.loc[:, "spot_date"]

        X_infer = X_infer.bfill().ffill()
        X_infer = X_infer.fillna(0)

        reg = []
        if model_type == "rf":
            reg = load(saved_model_path + f"{col}_rf.joblib")

        if model_type == "lgbm":
            reg = load(saved_model_path + f"{col}_lgbm.joblib")

        # if model_type == "xgb":
        #     reg = load(saved_model_path + f"{col}_xgb.joblib")

        final_report[col + "_prob"] = (reg.predict_proba(X_infer))[:, 1]
        final_report["model_type"] = model_type
        final_report["created"] = dateNow()
    final_report = final_report.apply(lambda x: sort_to_rank(x, rank_columns, time_to_exp), axis=1)
    final_report = final_report.sort_values(by='spot_date', ascending=False)
    latest_df = final_report.copy()
    latest_df = latest_df.drop_duplicates(subset=["ticker"], keep='first', inplace=False)
    latest_df = latest_df.reset_index(drop=True)
    latest_df = find_rank(latest_df, time_to_exp)
    latest_df = latest_df.reset_index(drop=True)
    print(latest_df)
    return final_report, latest_df

