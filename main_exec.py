import argparse
import datetime
import os
import platform
import sys
import time

from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import pandas as pd
from pandas.tseries.offsets import BDay

import global_vars
from executive.bot_labeler import main_fun, bot_stats_report
from executive.data_download import (
    get_new_tickers_list_detail_from_aws,
    get_latest_date,
    get_tickers_list_from_aws,
    get_new_tickers_list_from_aws,
    executive_data_download,
    download_production_executive_max_date,
    get_active_tickers_list,
    get_outputs_tickers,
    option_stats_func)
from executive.data_output import gpu_mac_address
from executive.final_model import final_model_fun
from executive.main_file import main_fn
from executive.option_file import option_fn, fill_nulls
from executive.option_file_full import option_fn_full
from executive.option_file_UCDC import option_fn_full as option_fn_ucdc, fill_nulls_ucdc
from executive.option_file_full_UCDC import option_fn_full as option_fn_full_ucdc
from executive.preprocess import cal_q_daily, cal_r_daily
from executive.statistcs_uno import bench_fn as bench_fn_uno
from executive.statistcs_ucdc import bench_fn as bench_fn_ucdc
from general.general import timeNow
from general.slack import report_to_slack

parser = argparse.ArgumentParser()
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************

parser.add_argument("--start_date")
parser.add_argument("--end_date")

parser.add_argument("--period", type=int, default=21, help="period for predicting volatility")
parser.add_argument("--null_per", type=int, default=5, help="Minimum number of nulls for a batch to be skipped")

parser.add_argument("--random_state", type=int, default=8)


parser.add_argument("--live", dest="live", action="store_true")
parser.add_argument("--no_live", dest="live", action="store_false")
parser.set_defaults(live=False)  # Will go into live mode.

parser.add_argument("--daily", dest="daily", action="store_true")
parser.add_argument("--no_daily", dest="daily", action="store_false")
parser.set_defaults(daily=False)  # Will go into live mode.

parser.add_argument("--data_prep_live", dest="data_prep_live", action="store_true")
parser.add_argument("--no_data_prep_live", dest="data_prep_live", action="store_false")
parser.set_defaults(data_prep_live=False)  # When true will update the executive data for only the most recent date.

parser.add_argument("--data_prep_daily", dest="data_prep_daily", action="store_true")
parser.add_argument("--no_data_prep_daily", dest="data_prep_daily", action="store_false")
parser.set_defaults(data_prep_daily=False)  # When true will update the executive data for the most recent uncalculated dates.

parser.add_argument("--data_prep_daily_new_ticker", dest="data_prep_daily_new_ticker", action="store_true")
parser.add_argument("--no_data_prep_daily_new_ticker", dest="data_prep_daily_new_ticker", action="store_false")
parser.set_defaults(data_prep_daily_new_ticker=False)  # When true will update the executive data for the most recent uncalculated dates.

parser.add_argument("--data_prep_history", dest="data_prep_history", action="store_true")
parser.add_argument("--no_data_prep_history", dest="data_prep_history", action="store_false")
parser.set_defaults(data_prep_history=False)  # When true will update the executive data for the whole history.

parser.add_argument("--infer_live", dest="infer_live", action="store_true")
parser.add_argument("--no_infer_live", dest="infer_live", action="store_false")
parser.set_defaults(infer_live=False)  # When True will go into the inference live(only the most recent date) mode.

parser.add_argument("--infer_daily", dest="infer_daily", action="store_true")
parser.add_argument("--no_infer_daily", dest="infer_daily", action="store_false")
parser.set_defaults(infer_daily=False)  # When True will go into the inference daily mode.

parser.add_argument("--infer_history", dest="infer_history", action="store_true")
parser.add_argument("--no_infer_history", dest="infer_history", action="store_false")
parser.set_defaults(infer_history=False)  # When True will go into the inference history mode.

parser.add_argument("--train_model", dest="train_model", action="store_true")
parser.add_argument("--no_train_model", dest="train_model", action="store_false")
parser.set_defaults(train_model=False)  # When true will go into the model training mode.

parser.add_argument("--load_new_model", dest="load_new_model", action="store_true")
parser.add_argument("--no_load_new_model", dest="load_new_model", action="store_false")
parser.set_defaults(load_new_model=False)  # When true will load the new model to replace the old model.

parser.add_argument("--hyper_tune", dest="hyper_tune", action="store_true")
parser.add_argument("--no_hyper_tune", dest="hyper_tune", action="store_false")
parser.set_defaults(hyper_tune=False)  # When true will go into the model training _ hyper tuning mode.

parser.add_argument("--option_maker_live", dest="option_maker_live", action="store_true")
parser.add_argument("--no_option_maker_live", dest="option_maker_live", action="store_false")
parser.set_defaults(option_maker_live=False)  # When true will go into the option maker live mode.

parser.add_argument("--option_maker_live_ucdc", dest="option_maker_live_ucdc", action="store_true")
parser.add_argument("--no_option_maker_live_ucdc", dest="option_maker_live_ucdc", action="store_false")
parser.set_defaults(option_maker_live_ucdc=False)  # When true will go into the option maker live mode.

parser.add_argument("--option_maker_daily", dest="option_maker_daily", action="store_true")
parser.add_argument("--no_option_maker_daily", dest="option_maker_daily", action="store_false")
parser.set_defaults(option_maker_daily=False)  # When true will go into the option maker daily mode.

parser.add_argument("--option_maker_daily_ucdc", dest="option_maker_daily_ucdc", action="store_true")
parser.add_argument("--no_option_maker_daily_ucdc", dest="option_maker_daily_ucdc", action="store_false")
parser.set_defaults(option_maker_daily_ucdc=False)  # When true will go into the option maker daily mode.

parser.add_argument("--option_maker_history", dest="option_maker_history", action="store_true")
parser.add_argument("--no_option_maker_history", dest="option_maker_history", action="store_false")
parser.set_defaults(option_maker_history=False)  # When true will go into the option maker history mode.

parser.add_argument("--option_maker_history_ucdc", dest="option_maker_history_ucdc", action="store_true")
parser.add_argument("--no_option_maker_history_ucdc", dest="option_maker_history_ucdc", action="store_false")
parser.set_defaults(option_maker_history_ucdc=False)  # When true will go into the option maker history mode.

parser.add_argument("--option_maker_history_full", dest="option_maker_history_full", action="store_true")
parser.add_argument("--no_option_maker_history_full", dest="option_maker_history_full", action="store_false")
parser.set_defaults(option_maker_history_full=False)  # When true will go into the option maker history mode.

parser.add_argument("--option_maker_history_full_ucdc", dest="option_maker_history_full_ucdc", action="store_true")
parser.add_argument("--no_option_maker_history_full_ucdc", dest="option_maker_history_full_ucdc", action="store_false")
parser.set_defaults(option_maker_history_full_ucdc=False)  # When true will go into the option maker history mode.

parser.add_argument("--debug_mode", dest="debug_mode", action="store_true")
parser.add_argument("--no_debug_mode", dest="debug_mode", action="store_false")
parser.set_defaults(debug_mode=False)  # When true will go into the debug mode.

parser.add_argument("--r_daily", dest="r_daily", action="store_true")
parser.add_argument("--no_r_daily", dest="r_daily", action="store_false")
parser.set_defaults(r_daily=False)  # When true will create table daily_interest_rates.

parser.add_argument("--q_daily", dest="q_daily", action="store_true")
parser.add_argument("--no_q_daily", dest="q_daily", action="store_false")
parser.set_defaults(q_daily=False)  # When true will create table daily_div_rates.

parser.add_argument("--add_inferred", dest="add_inferred", action="store_true")
parser.add_argument("--no_add_inferred", dest="add_inferred", action="store_false")
parser.set_defaults(add_inferred=False)  # When true will add inferred vol surface parameters to the option making.

parser.add_argument("--option_maker", dest="option_maker", action="store_true")
parser.add_argument("--no_option_maker", dest="option_maker", action="store_false")
parser.set_defaults(option_maker=False)  # When true will only run option maker.

parser.add_argument("--null_filler", dest="null_filler", action="store_true")
parser.add_argument("--no_null_filler", dest="null_filler", action="store_false")
parser.set_defaults(null_filler=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler", dest="bot_labeler", action="store_true")
parser.add_argument("--no_bot_labeler", dest="bot_labeler ", action="store_false")
parser.set_defaults(bot_labeler=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_stats", dest="bot_labeler_stats", action="store_true")
parser.add_argument("--no_bot_labeler_stats", dest="bot_labeler_stats ", action="store_false")
parser.set_defaults(bot_labeler_stats=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_train", dest="bot_labeler_train", action="store_true")
parser.add_argument("--no_bot_labeler_train", dest="bot_labeler_train ", action="store_false")
parser.set_defaults(bot_labeler_train=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_infer_history", dest="bot_labeler_infer_history", action="store_true")
parser.add_argument("--no_bot_labeler_infer_history", dest="bot_labeler_infer_history ", action="store_false")
parser.set_defaults(bot_labeler_infer_history=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_infer_daily", dest="bot_labeler_infer_daily", action="store_true")
parser.add_argument("--no_bot_labeler_infer_daily", dest="bot_labeler_infer_daily ", action="store_false")
parser.set_defaults(bot_labeler_infer_daily=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_infer_live", dest="bot_labeler_infer_live", action="store_true")
parser.add_argument("--no_bot_labeler_infer_live", dest="bot_labeler_infer_live ", action="store_false")
parser.set_defaults(bot_labeler_infer_live=False)  # When true will  only run null filler.

parser.add_argument("--bot_labeler_performance_history", dest="bot_labeler_performance_history", action="store_true")
parser.add_argument("--no_bot_labeler_performance_history", dest="bot_labeler_performance_history ", action="store_false")
parser.set_defaults(bot_labeler_performance_history=False)  # When true will  only run null filler.

parser.add_argument("--daily_r_days", type=int, default=200)
parser.add_argument("--daily_q_days", type=int, default=200)
parser.add_argument("--no_splits", type=int, default=300)
parser.add_argument("--q_index", type=str, default="all")
parser.add_argument("--r_currency", type=str, default="all")


parser.add_argument("--benchmark", dest="benchmark", action="store_true")
parser.add_argument("--no_benchmark", dest="benchmark", action="store_false")
parser.set_defaults(benchmark=False)  # When true will create a production history.

parser.add_argument("--check", dest="check", action="store_true")
parser.add_argument("--no_check", dest="check", action="store_false")
parser.set_defaults(check=False)  # When true will create a production history.

parser.add_argument("--benchmark_ucdc", dest="benchmark_ucdc", action="store_true")
parser.add_argument("--no_benchmark_ucdc", dest="benchmark_ucdc", action="store_false")
parser.set_defaults(benchmark_ucdc=False)  # When true will create a production history.

parser.add_argument("--monthly", dest="monthly", action="store_true")
parser.add_argument("--no_monthly", dest="monthly", action="store_false")
parser.set_defaults(monthly=False)  # When true will create a production history.

parser.add_argument("--modified", dest="modified", action="store_true")
parser.add_argument("--no_modified", dest="modified", action="store_false")
parser.set_defaults(modified=False)  # For modified options.
parser.add_argument("--modify_arg", type=str, default=global_vars.modified_delta_list)  # For modified options.
# https://loratechai.atlassian.net/wiki/spaces/ARYA/pages/142573634/modified+delta

parser.add_argument("--lookback_horizon", type=int, default=1)
parser.add_argument("--monthly_horizon", type=int)
parser.add_argument("--history_num_years", type=int, default=3)
parser.add_argument("--bot_labeler_training_num_years", type=int, default=2)
parser.add_argument("--total_no_of_runs", type=int, default=1)
parser.add_argument("--run_number", type=int, default=0)
parser.add_argument("--model_type", type=str, default="rf")

parser.add_argument("--exec_index", nargs="+")
parser.add_argument("--exec_ticker", nargs="+")
parser.add_argument("--bot_type", nargs="+")
parser.add_argument("--month_exp", nargs="+")
parser.add_argument("--month_horizon", nargs="+")

parser.add_argument("--slack_api", type=str, default=global_vars.slack_api)


# ******************************************************************************
args = parser.parse_args()

if __name__ == "__main__":
    if args.month_horizon:
        args.month_horizon = [float(value) for value in args.month_horizon]
    args.run_time_min = time.time()
    args.active_tickers_list = get_active_tickers_list(args)
    pd.options.mode.chained_assignment = None
    gpu_mac_address(args)

    # Prepare the technical data daily from the last calculated date
    args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
    temp = get_tickers_list_from_aws()

    index_to_etf = pd.read_csv("executive/index_to_etf.csv", names=["index", "etf"])
    etf_list = index_to_etf.etf.unique().tolist()

    if args.exec_index is not None:
        args.tickers_list = (temp[temp["index"].isin(args.exec_index)].ticker).tolist()

        # args.tickers_list.extend(etf_list)
    if args.exec_ticker is not None:
        args.tickers_list = args.exec_ticker
        # args.tickers_list.extend(etf_list)

    # ******************************* PATH defining *******************************************
    if platform.system() == "Linux":
        args.model_path = "/home/loratech/PycharmProjects/DROID/saved_models/"
        # args.production_model_path = "/home/loratech/PycharmProjects/DROID/production_saved_models/"
        if not os.path.exists(args.model_path):
            os.makedirs(args.model_path)
        # if not os.path.exists(args.production_model_path):
        #     os.makedirs(args.production_model_path)
    # *****************************************************************************************

    if args.infer_live and args.infer_daily and args.infer_history:
        print("Please only set one of the args.infer_live or args.infer_history to True!")
        sys.exit()

    if args.data_prep_live and args.data_prep_history:
        print("Please only set one of the args.data_prep_live or args.data_prep_history to True!")
        sys.exit()

    if args.option_maker_live and args.option_maker_history:
        print("Please only set one of the args.option_maker_live or args.option_maker_history to True!")
        sys.exit()
    # *****************************************************************************************
    # ***************************** Daily and LIVE pipelines **********************************
    # *****************************************************************************************
    if args.daily:
        args.data_prep_daily = True
        args.infer_daily = True
        args.option_maker_daily = True
    # *****************************************************************************************
    # ***************************** Updating the input database *******************************
    # *****************************************************************************************

    # if args.infer_live:
    #     args.data_prep_live = True
    #
    # if args.infer_live or args.infer_daily or args.infer_history:
    #     args.data_prep_history = False
    #     args.data_prep_live = False
    #     args.train_model = False

    if args.data_prep_live or args.data_prep_history or args.data_prep_daily:

        # *************************************** History *****************************************
        def diff(first, second):
            second = set(second)
            return [item for item in first if item not in second]

        #
        # current_stocks_list = get_current_stocks_list(args)
        # vol_surface_stocks_list = get_vol_surface_tickers_list_from_aws(args).tolist()
        #
        # newly_added_stocks = diff(vol_surface_stocks_list, current_stocks_list)
        # newly_added_stocks = set(current_stocks_list).symmetric_difference(set(vol_surface_stocks_list))
        #
        # if len(newly_added_stocks) > 0:
        #     args.data_prep_history = True

        if args.data_prep_history:
            print("Data preparation history started!")
            if args.start_date is None:
                args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

            args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
            main_fn(args)

        # *************************************** Daily *****************************************
        if args.data_prep_daily:
            if args.exec_index is None:
                print("Please input the desired index!")
                sys.exit()

            args.latest_date = get_latest_date(args)
            print("Data preparation daily started!")
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

            # # Prepare the technical data daily from the last calculated date
            # args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
            # temp = get_tickers_list_from_aws()
            #
            # index_to_etf = pd.read_csv("/home/loratech/PycharmProjects/DROID/executive/index_to_etf.csv",
            #                            names=["index", "etf"])
            # etf_list = index_to_etf.etf.unique().tolist()
            #
            # if args.data_index is not None:
            #     args.tickers_list = (temp[temp["index"].isin(args.data_index)].ticker).tolist()
            #
            #     # args.tickers_list.extend(etf_list)
            # if args.data_ticker is not None:
            #     args.tickers_list = args.data_ticker
            #     # args.tickers_list.extend(etf_list)

            main_fn(args)

            #Check New Ticker
            args.start_date = dt.now().date() - relativedelta(years=args.history_num_years)
            args.start_date2 = dt.now().date() - relativedelta(years=args.history_num_years - 1)
            args.end_date = dt.now().date()
            print(f"The start date is set as: {args.start_date}")
            print(f"The end date is set as: {args.end_date}")
            date_identifier = "trading_day"
            table_name = args.executive_data_table_name
            new_ticker = get_new_tickers_list_from_aws(args, date_identifier, table_name)
            try:
                if (len(new_ticker) > 0):
                    print(new_ticker)
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker["ticker"].tolist()
                    args.data_prep_daily = False
                    args.data_prep_daily_new_ticker = True
                    main_fn(args)
            except Exception as e:
                print(e)
            report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(str(dt.now()), args.exec_index), args)

        # *************************************** Live *****************************************
        if args.data_prep_live:
            print("Data preparation live started!")
            if args.start_date is None:
                args.start_date = datetime.date.today()
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

            # Prepare the technical data for only the most recent date using intraday prices
            # args.latest_date = get_latest_date(args)
            # if (args.live_index is not None) or (args.live_ticker is not None):
            #     args.start_date = args.latest_date
            # else:
            #     args.start_date = (args.latest_date - BDay(4)).date()
                # args.start_date = args.latest_date

            # args.end_date = datetime.date.today()
            # args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
            # temp = get_tickers_list_from_aws()
            #
            # index_to_etf = pd.read_csv("/home/loratech/PycharmProjects/DROID/executive/index_to_etf.csv",
            #                            names=["index", "etf"])
            # etf_list = index_to_etf.etf.unique().tolist()
            #
            # if args.live_index is not None:
            #     args.tickers_list = (temp[temp["index"].isin(args.live_index)].ticker).tolist()
            #     # args.tickers_list.extend(etf_list)
            # if args.live_ticker is not None:
            #     args.tickers_list = args.live_ticker
            #     # args.tickers_list.extend(etf_list)

            main_fn(args)

        # if args.data_prep_history:
        print("Data preparation is finished")
        #sys.exit()


    # *****************************************************************************************
    # ********************** running the model and the inference file *************************
    # *****************************************************************************************

    # *****************************************************************************************
    # ************************************ Trainig ********************************************

    # if args.infer_live or args.infer_history:
    # args.model_filename = [args.model_path + "not_rounded_finalized_model.sav", args.model_path + "rounded_finalized_model.sav"]
    args.model_filename = [args.model_path + "vols_finalized_model.joblib", "_finalized_model.txt"]
    args.s3_model_filename = ["vols_finalized_model.joblib", "_finalized_model.txt"]
    # args.model_filename = "executive_nn_model.hdf5"

    # if (not os.path.isfile(args.model_filename[0])) or args.train_model:
    if args.train_model:

        # if (not os.path.isfile(args.model_filename[0])):
        #     print("There are no saved models!")
        print("Started training!")
        args.train_model = True
        start = timeNow()
        print(start)

        if args.start_date is None:
            args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
        print(f"The start date is set as: {args.start_date}")

        if args.end_date is None:
            args.end_date = datetime.date.today()
        print(f"The end date is set as: {args.end_date}")

        if type(args.start_date) == str:
            args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

        if type(args.end_date) == str:
            args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

        args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()

        main_df = executive_data_download(args)
        output_tickers = get_outputs_tickers(args)
        # Just taking the rows that we have output for them.
        main_df = main_df[main_df.ticker.isin(output_tickers)]

        # ***************************************** research models ********************************************
        # nrf(main_df, args)
        # rf_model_grouped(main_df, args)
        # rf_model_mega(main_df, args)
        # run_model(main_df, args)
        # run_model_clustering(main_df, args)
        # rf_model_mega_no_loop(main_df, args)
        # *******************************************************************************************************

        final_model_fun(main_df, args)
        print("Finished training!")

    # *******************************************************************************************
    # ************************************ Inference ********************************************

    # *************************************** History *****************************************
    if args.infer_history:
        print("Inference history started!")
        if args.start_date is None:
            args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
        print(f"The start date is set as: {args.start_date}")

        if args.end_date is None:
            args.end_date = datetime.date.today()
        print(f"The end date is set as: {args.end_date}")

        if type(args.start_date) == str:
            args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

        if type(args.end_date) == str:
            args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

        args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
        main_df = executive_data_download(args)
        final_model_fun(main_df, args)

    # *************************************** Daily *****************************************
    if args.infer_daily:
        if args.exec_index is None:
            print("Please input the desired index!")
            sys.exit()

        args.latest_date = get_latest_date(args)
        print(f"{args.exec_index} Inference daily started!")
        if args.start_date is None:
            args.start_date = args.latest_date
        print(f"The start date is set as: {args.start_date}")

        if args.end_date is None:
            args.end_date = datetime.date.today()
        print(f"The end date is set as: {args.end_date}")

        if type(args.start_date) == str:
            args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

        if type(args.end_date) == str:
            args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

        # # Prepare the technical data daily from the last calculated date
        # args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
        # temp = get_tickers_list_from_aws()
        #
        # index_to_etf = pd.read_csv("/home/loratech/PycharmProjects/DROID/executive/index_to_etf.csv",
        #                            names=["index", "etf"])
        # etf_list = index_to_etf.etf.unique().tolist()
        #
        # if args.infer_index is not None:
        #     args.tickers_list = (temp[temp["index"].isin(args.infer_index)].ticker).tolist()
        #
        #     # args.tickers_list.extend(etf_list)
        # if args.infer_ticker is not None:
        #     args.tickers_list = args.infer_ticker
        #     # args.tickers_list.extend(etf_list)

        main_df = executive_data_download(args)
        print(main_df)
        final_model_fun(main_df, args)

    # *************************************** Live *****************************************
    if args.infer_live:
        print("Inference live started!")
        if args.start_date is None:
            args.start_date = datetime.date.today()
        print(f"The start date is set as: {args.start_date}")

        if args.end_date is None:
            args.end_date = datetime.date.today()
        print(f"The end date is set as: {args.end_date}")

        if type(args.start_date) == str:
            args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

        if type(args.end_date) == str:
            args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

        args.tickers_list = get_tickers_list_from_aws()["ticker"].tolist()
        temp = get_tickers_list_from_aws()

        index_to_etf = pd.read_csv("/home/loratech/PycharmProjects/DROID/executive/index_to_etf.csv",
                                   names=["index", "etf"])
        etf_list = index_to_etf.etf.unique().tolist()

        if args.live_index is not None:
            args.tickers_list = (temp[temp["index"].isin(args.live_index)].ticker).tolist()
            # args.tickers_list.extend(etf_list)
        if args.live_ticker is not None:
            args.tickers_list = args.live_ticker
            # args.tickers_list.extend(etf_list)

        main_df = executive_data_download(args)
        final_model_fun(main_df, args)

    # *****************************************************************************************
    # ***************************** Creating options  *****************************************
    # *****************************************************************************************

    if args.option_maker_live or args.option_maker_history or args.option_maker_history_full or args.option_maker_daily\
            or args.option_maker_history_full_ucdc or args.option_maker_history_ucdc or args.option_maker_live_ucdc\
            or args.option_maker_daily_ucdc:
        # *************************************** History *****************************************
        if args.option_maker_history or args.option_maker_history_full or args.option_maker_history_full_ucdc or args.option_maker_history_ucdc:
            print("Option making history started!")
            if args.start_date is None:
                args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

            # *************************************** Daily *****************************************
        elif args.option_maker_daily or args.option_maker_daily_ucdc:
            args.option_maker = True
            args.null_filler = True

            if args.exec_index is None:
                print("Please input the desired index!")
                sys.exit()

            # The following lines finds the last date of executive data and will use that as the start of the live mode.
            # This helps if some days are skipped in the live mode.
            args.latest_dates_db = download_production_executive_max_date(args)
            # args.start_date = temp_df.spot_date.iloc[0]
            args.start_date = args.latest_dates_db["spot_date", "max"].min()

            args.latest_date = get_latest_date(args)
            print("Option making daily started!")
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            # *************************************** Live *****************************************
            print("Option making live started!")
            if args.start_date is None:
                args.start_date = datetime.date.today()
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()


        if args.option_maker_history_full:
            args.new_ticker = False
            if args.option_maker:
                option_fn_full(args)
                print("Option creation full options is finished")

            if args.null_filler:
                fill_nulls(args)
        elif args.option_maker_history_full_ucdc:
            args.new_ticker = False
            if args.option_maker:
                option_fn_full_ucdc(args)
                print("Option creation full options is finished")

            if args.null_filler:
                fill_nulls_ucdc(args)
        elif args.option_maker_history_ucdc:
            if not args.month_horizon:
                args.month_horizon = [6]
            args.new_ticker = False
            if args.option_maker:
                option_fn_ucdc(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls_ucdc(args)
        elif args.option_maker_daily or args.option_maker_live or args.option_maker_history:  # UnO daily and live and history
            if not args.month_horizon:
                args.month_horizon = [1, 3]
            args.new_ticker = False
            if args.option_maker:
                option_fn(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls(args)

            if args.option_maker_daily :
                args.start_date = dt.now().date() - relativedelta(years=3)
                table_name = args.executive_production_table_name
                new_ticker = get_new_tickers_list_detail_from_aws(args, args.start_date, table_name)
                print(new_ticker)
                if (len(new_ticker) > 0):
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker["ticker"].tolist()
                    args.start_date = dt.now().date() - relativedelta(years=3)
                    args.end_date = dt.now().date()
                    args.latest_dates_db = new_ticker
                    args.new_ticker = True
                    print(args.tickers_list)
                    print(args.start_date)
                    print(args.end_date)
                    print(args.latest_dates_db)
                    status = option_fn(args)
                    if(status):
                        fill_nulls(args)

            if args.exec_index:
                report_to_slack("{} : === {} Executive UNO Create Option Completed ===".format(str(dt.now()),args.exec_index), args)
        else:
            if not args.month_horizon:
                args.month_horizon = [6]
            args.new_ticker = False
            if args.option_maker:
                option_fn_ucdc(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls_ucdc(args)
            
            if args.option_maker_daily_ucdc :
                args.start_date = dt.now().date() - relativedelta(years=3)
                table_name = args.executive_production_ucdc_table_name
                new_ticker = get_new_tickers_list_detail_from_aws(args, args.start_date, table_name)
                print(new_ticker)
                if (len(new_ticker) > 0):
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker["ticker"].tolist()
                    args.start_date = dt.now().date() - relativedelta(years=3)
                    args.end_date = dt.now().date()
                    args.latest_dates_db = new_ticker
                    args.new_ticker = True
                    print(args.tickers_list)
                    print(args.start_date)
                    print(args.end_date)
                    print(args.latest_dates_db)
                    status = option_fn_ucdc(args)
                    if(status):
                        fill_nulls_ucdc(args)

            if args.exec_index:
                report_to_slack("{} : === {} Executive UCDC Create Option Completed ===".format(str(dt.now()),args.exec_index), args)
        #sys.exit()

    # *****************************************************************************************
    # ***************************** option checking *******************************************
    # *****************************************************************************************

    if args.check:
        latest_dates_db_uno = option_stats_func(args)
        # latest_dates_db_uno["ticker"] = latest_dates_db_uno.index
        # latest_dates_db_uno.reset_index(drop=True, inplace=True)

        args.option_maker_daily_ucdc = True
        latest_dates_db_ucdc = option_stats_func(args)
        # latest_dates_db_ucdc["ticker"] = latest_dates_db_ucdc.index
        # latest_dates_db_ucdc.reset_index(drop=True, inplace=True)