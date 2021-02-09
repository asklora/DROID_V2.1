import calendar
import datetime
import time
from argparse import Namespace
from datetime import datetime
from multiprocessing import cpu_count

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine

from dlpa import global_vars


def write_to_sql(predicted_values, indices_df, args):
    # This function is used for outputting the data for test and final production datasets. The difference between
    # this function and write_to_sql_model_data is that this one outputs for each stock separately, and also just
    # have the essential data as seen in the columns list.
    columns = ["uid", "data_period", "created", "forward_date", "spot_date", "year", "week", "day_of_week",
               "num_periods_to_predict",
               "number_of_quantiles", "currency_code", "ticker",
               "predicted_quantile_1", "signal_strength_1", "model_filename", "pc_number"]

    if args.data_period == 0:
        data_period = "weekly"
    else:
        data_period = "daily"

    # We are predicting the period after the end_date.
    if args.data_period == 0:
        d = Week(1)
    else:
        d = BDay(1)
    production_date = args.forward_date
    year, week, day_of_week = datetime.date(production_date).isocalendar()
    if args.go_live:
        stock_list = pd.DataFrame(args.stocks_list)
    else:
        stock_list = pd.DataFrame(args.stocks_list[args.test_mask])

    to_aws_df = pd.DataFrame(index=range(stock_list.shape[0]), columns=columns)
    list1 = ["predicted_quantile_1"]
    # list2 = ["real_quantile_1", "real_quantile_2", "real_quantile_3", "real_quantile_4", "real_quantile_5", ]
    list3 = ["signal_strength_1"]
    # Setting the values from args to aws dataframe.

    to_aws_df["data_period"] = data_period
    to_aws_df["when_created"] = datetime.fromtimestamp(time.time())
    to_aws_df["forward_date"] = production_date.strftime("%Y-%m-%d")
    if args.tomorrow:
        to_aws_df["forward_date"] = (production_date + BDay(1)).strftime("%Y-%m-%d")
    if args.data_period == 0:
        to_aws_df["spot_date"] = (production_date - Week(1)).strftime("%Y-%m-%d")
    else:
        to_aws_df["spot_date"] = (production_date - BDay(1)).strftime("%Y-%m-%d")

    to_aws_df["year"] = year
    to_aws_df["week"] = week
    to_aws_df["day_of_week"] = day_of_week
    to_aws_df["num_periods_to_predict"] = int(args.num_periods_to_predict)
    to_aws_df["number_of_quantiles"] = int(args.num_bins)
    to_aws_df["model_filename"] = args.model_filename
    to_aws_df["pc_number"] = args.pc_number
    datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d")
    for i in range(stock_list.shape[0]):
        if sum(indices_df.ticker == stock_list.iloc[i][0]) > 0:
            to_aws_df.loc[i, "currency_code"] = indices_df[indices_df.ticker == stock_list.iloc[i][0]]["currency_code"].iloc[0]
        to_aws_df.loc[i, "ticker"] = str(stock_list.iloc[i][0])
        # for j in range(args.num_periods_to_predict):
        for j in range(1):
            to_aws_df.loc[i, list1[j]] = int(np.argmax(predicted_values[i, j, :]))
            to_aws_df.loc[i, list3[j]] = float(np.max(predicted_values[i, j, :]))
    to_aws_df = to_aws_df.infer_objects()
    to_aws_df = to_aws_df.round({"signal_strength_1": 4})

    to_aws_df["uid"]=to_aws_df["ticker"] + to_aws_df["model_filename"]
    to_aws_df["uid"]=to_aws_df["uid"].str.replace("-", "").str.replace(".", "").str.replace(" ", "")
    to_aws_df["uid"]=to_aws_df["uid"].str.strip()

    db_url = global_vars.DB_URL_WRITE

    if args.production_output_flag:
        table = global_vars.production_inference_table_name
        engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            to_aws_df.to_sql(con=conn, name=table, if_exists="append", index=False)
        print(f"Saved the data to {table}!")
    if args.test_output_flag:
        table = global_vars.test_inference_table_name
        engine = create_engine(global_vars.DB_TEST_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            to_aws_df.to_sql(con=conn, name=table, if_exists="append", index=False)
        print(f"Saved the data to {table}!")

def write_to_sql_model_data(args):
    # MODEL SUMMARIES
    # This function is used for outputting the model data for each run.
    # The list of outputted values are shown in aws_columns_list.
    args.run_time_min = (time.time() - args.run_time_min) / 60
    args_copy = Namespace(**vars(args))

    output_dict = vars(args_copy)

    rr = pd.DataFrame(output_dict.items())
    rr = rr.transpose()
    rr.columns = rr.iloc[0]
    rr = rr.drop(rr.index[0])
    rr["created"] = datetime.fromtimestamp(time.time())
    rr["forward_dow"] = list(calendar.day_abbr)[int(args.forward_day) - 1]
    rr["train_dow"] = list(calendar.day_abbr)[int(args.dow) - 1]
    rr["forward_week"] = datetime.date(args.forward_date).isocalendar()[1]
    if args.tomorrow:
        rr.forward_date = rr.forward_date + BDay(1)

    rr = rr[args.aws_columns_list]
    rr = rr.infer_objects()
    # datetime.strptime(f"{y} {w} {d}", "%G %V %u")
    # ************************** Cleaning the data to look better. ************************************
    rr = rr.round(
        {"best_train_acc": 4, "best_valid_acc": 4, "test_acc_1": 4, "test_acc_2": 4, "test_acc_3": 4, "test_acc_4": 4,
         "test_acc_5": 4, "run_time_min": 2})
    if rr.model_type.iloc[0] == 0:
        rr.model_type.iloc[0] = "DLPA"
    elif rr.model_type.iloc[0] == 1:
        rr.model_type.iloc[0] = "DLPM"
    else:
        rr.model_type.iloc[0] = "SIMPLE"

    if args.rv_1:
        rr.model_type.iloc[0] = rr.model_type.iloc[0] + "_rv_1"

    if args.rv_2:
        rr.model_type.iloc[0] = rr.model_type.iloc[0] + "_rv_2"

    if args.beta_1:
        rr.model_type.iloc[0] = rr.model_type.iloc[0] + "_beta_1"

    if args.beta_2:
        rr.model_type.iloc[0] = rr.model_type.iloc[0] + "_beta_2"

    if rr.data_period.iloc[0] == 0:
        rr.data_period.iloc[0] = "weekly"
    else:
        rr.data_period.iloc[0] = "daily"

    rr.candle_type_candles.iloc[0] = candle_type_to_str(rr.candle_type_candles.iloc[0])
    rr.candle_type_returnsX.iloc[0] = candle_type_to_str(rr.candle_type_returnsX.iloc[0])
    rr.candle_type_returnsY.iloc[0] = candle_type_to_str(rr.candle_type_returnsY.iloc[0])

    rr.reset_index(drop=True, inplace=True)

    if args.production_output_flag:  # if writing to production_data table, i.e., write out the stocks - keep in "live"
        table_name = global_vars.production_model_data_table_name
        engine = create_engine(global_vars.DB_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            rr.to_sql(con=conn, name=table_name, schema="public", if_exists="append", index=False)

        print(f"Saved the data to {table_name}!")

    if args.test_output_flag:
        table_name = global_vars.test_model_data_table_name  # this is for sample testing,....
        engine = create_engine(global_vars.DB_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            rr.to_sql(con=conn, name=table_name, schema="public", if_exists="append", index=False)

        print(f"Saved the data to {table_name}!")



def candle_type_to_str(a):
    if a == 0:
        return "all"
    if a == 1:
        return "open"
    if a == 2:
        return "high"
    if a == 3:
        return "low"
    if a == 4:
        return "close"
    if a == 5:
        return "volume"
