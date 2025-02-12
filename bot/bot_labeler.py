from general.data_process import uid_maker
from general.table_name import get_bot_latest_ranking_table_name, get_bot_ranking_table_name
from general.sql_output import upsert_data_to_database
import numpy as np
import pandas as pd
from tqdm import tqdm
from bot.data_download import (
    get_bot_backtest_data, 
    get_bot_ranking_data, 
    get_executive_data_download, 
    get_master_tac_price)

from bot.final_model import model_trainer, bot_infer
from global_vars import X_columns, bots_list, labeler_model_type, bot_labeler_threshold, time_to_expiry, bot_slippage

def populate_bot_labeler(start_date=None, end_date=None, model_type=labeler_model_type, ticker=None, currency_code=None, time_to_exp=time_to_expiry, mod=False, bots_list=bots_list, bot_labeler_train = False, history=False):
    # ************************************************************************
    # *********************** Data download **********************************
    main_df = get_executive_data_download(start_date, end_date, ticker=ticker, currency_code=currency_code)
    # output_tickers = get_data_vol_surface_ticker(ticker=ticker, currency_code=currency_code)
    # # Just taking the rows that we have output for them.
    # main_df = main_df[main_df.ticker.isin(output_tickers["ticker"])]
    # ***************************************************************************************************
    # ******************************************** Data preprocessing ***********************************
    # ***************************************************************************************************

    cols_temp = X_columns.copy()
    cols_temp.extend(["uid", "ticker", "trading_day"])

    main_df_copy_no_fund = main_df[cols_temp].copy()
    del main_df

    main_df_copy_no_fund = main_df_copy_no_fund.infer_objects()

    for col in X_columns:
        try:
            main_df_copy_no_fund[col] = main_df_copy_no_fund[col].fillna(main_df_copy_no_fund.groupby("ticker")[col].transform("mean"))
        except:
            main_df_copy_no_fund = main_df_copy_no_fund.fillna(0)

    tac_df = get_master_tac_price(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)

    main_df_copy_no_fund.rename(columns={"trading_day": "spot_date"}, inplace=True)
    tac_df.rename(columns={"trading_day": "spot_date", "tri_adj_close": "spot_price"}, inplace=True)
    final_df = main_df_copy_no_fund

    Y_columns = []
    rank_columns = []
    for bot in bots_list:
        print(bot)
        # Download and preprocess the output data for each bot.
        if(bot == "uno"):
            df = get_bot_backtest_data(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, uno=True, mod=mod)
        elif(bot == "ucdc"):
            df = get_bot_backtest_data(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, ucdc=True, mod=mod)
        else:
            df = get_bot_backtest_data(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, classic=True, mod=mod)
        
        df["bot_return"] = df["bot_return"].astype(float)
        print(df)
        if bot == "classic":
            df["option_type"] = "classic"
            df["ret"] = df["bot_return"] - (bot_slippage *  2) #slippage/comms X 2 (in/out)
        else:
            df["delta_churn"] = df["delta_churn"].astype(float)
            df["ret"] = df["bot_return"] - df["delta_churn"] * bot_slippage #ALL delta trading X slippage/comms
        df = df[["ticker", "pnl", "ret", "option_type", "time_to_exp", "spot_date", "spot_price"]]
        df.loc[df.ret >= bot_labeler_threshold, "pnl_class"] = 1 #greater than threshold to deem "profitable"
        df.loc[df.ret < bot_labeler_threshold, "pnl_class"] = 0 #greater than threshold to deem "profitable"
        option_type_list = df.option_type.unique()
        for opt_type in option_type_list:
            print(opt_type)
            for time_exp in time_to_exp:
                time_exp_str = str(time_exp).replace(".", "")
                #if not (opt_type == "classic" and month_exp == 6):
                Y_columns.extend([f"{bot}_{opt_type}_{time_exp_str}_pnl_class"])
                rank_columns.extend([f"{bot}_{opt_type}_{time_exp_str}_pnl_class_prob"])
                df2 = df.loc[(df.option_type == opt_type) & (df.time_to_exp == time_exp), :]
                df2 = df2[df2["pnl"].notna()]
                df2.rename(columns={"pnl": f"{bot}_{opt_type}_{time_exp_str}_pnl",
                    "pnl_class": f"{bot}_{opt_type}_{time_exp_str}_pnl_class"}, inplace=True)
                df2 = df2.drop_duplicates(["ticker", "spot_date"], keep="last")
                df2["spot_date"] = pd.to_datetime(df2["spot_date"])
                final_df["spot_date"] = pd.to_datetime(final_df["spot_date"])
                final_df = final_df.merge(df2[["ticker", "spot_date", f"{bot}_{opt_type}_{time_exp_str}_pnl_class",
                    f"{bot}_{opt_type}_{time_exp_str}_pnl"]], on=["ticker", "spot_date"], how="left")
    tac_df["spot_date"] = pd.to_datetime(tac_df["spot_date"])
    final_df = final_df.merge(tac_df[["ticker", "spot_date", "spot_price"]], on=["ticker", "spot_date"], how="left")
    Y_columns = Y_columns
    rank_columns = rank_columns
    tac_df = tac_df[["ticker", "currency_code"]]
    tac_df = tac_df.drop_duplicates(subset=["ticker"], keep="first")
    if bot_labeler_train:
        model_trainer(final_df, None, model_type, Y_columns=Y_columns, just_train=True)
    else:
        infer_df, latest_df = bot_infer(final_df, model_type, rank_columns, Y_columns, time_to_exp=time_to_exp, bots_list=bots_list)
        infer_df = infer_df.merge(tac_df, on=["ticker"], how="left")
        infer_df = uid_maker(infer_df, uid="uid", ticker="ticker", trading_day="spot_date")
        latest_df["uid"]=latest_df["ticker"] + "_" + latest_df["bot_id"]
        print(infer_df)
        print(latest_df)
        upsert_data_to_database(infer_df, get_bot_ranking_table_name(), "uid", how="update", cpu_count=True, Text=True)
        upsert_data_to_database(latest_df, get_bot_latest_ranking_table_name(), "uid", how="update", cpu_count=True, Text=True)

def bot_stats_report():
    tqdm.pandas()
    full_df = get_bot_ranking_data()
    full_df = full_df[full_df.model_type == "rf"]

    # **************************************************************************************

    report = pd.DataFrame(index=range(len(full_df.model_type.unique())))
    for j in range(len(full_df.model_type.unique())):
        model_type_name = full_df.model_type.unique()[j]
        # report.loc[j, "model_type"] = full_df.model_type.unique()[j]

        for i in range(len(full_df.rank_1.unique())):
            report.loc[j * len(full_df.rank_1.unique()) + i, "model_type"] = full_df.model_type.unique()[j]
            report.loc[j * len(full_df.rank_1.unique()) + i, "rank"] = i + 1
            report.loc[j * len(full_df.rank_1.unique()) + i, f"pnl_spot_avg"] = np.nanmean(
                full_df.loc[full_df.model_type == model_type_name, f"pnl_avg_rank_{i + 1}"].values)
            report.loc[j * len(full_df.rank_1.unique()) + i, f"pnl_spot_std"] = np.nanstd(
                full_df.loc[full_df.model_type == model_type_name, f"pnl_avg_rank_{i + 1}"].values)
            report.loc[j * len(full_df.rank_1.unique()) + i, f"acc_avg"] = np.nanmean(
                full_df.loc[full_df.model_type == model_type_name, f"acc_rank_{i + 1}"].values)

    report.to_csv("report_bot_statistics.csv")

    report_2 = pd.DataFrame()
    for i in range(len(full_df.rank_1.unique())):
        report_2[f"rank_{i + 1}_classes"] = full_df[f"rank_{i + 1}"].value_counts(normalize=True) * 100

    report_2.to_csv("report_bot_classes_statistics.csv")

    print("Finished creating report for bot labeler!")


