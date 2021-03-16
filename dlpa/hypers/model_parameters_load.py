from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
# from hypers.hyper_run import gpu_mac_address, jump_period
from pandas.tseries.offsets import Week, BDay
from sqlalchemy import text

import global_vars
from global_vars import production_model_data_table_name, no_top_models


def download_model_data(args):
    # This function is used for downloading the latest available models data for the desired period.
    # E.g. takes the top 10(no_top_models) model and their properties based on their best_valid_acc.
    if args.data_period == 0:
        period = 'weekly'
    else:
        period = 'daily'
    db_url = global_vars.DB_PROD_URL_READ
    engine = db.create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:

        q = "select * from "
        q = q + production_model_data_table_name
        q = q + " where forward_date = (select max(forward_date) from "
        q = q + production_model_data_table_name
        q = q + " where forward_date <= :test_d ) and data_period = :period"

        ResultProxy = conn.execute(text(q), test_d=str(args.forward_date), period=period)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(text(q), test_d=str(args.forward_date), period=period).keys()

    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list
    # # Getting rid of 'should_use' column
    # full_df = full_df.iloc[:, :-1]
    # if len(full_df) > 0:
    #     full_df.columns = args.aws_columns_list
    full_df = full_df.sort_values(by=['best_valid_acc'], ascending=False)
    full_df = full_df.head(no_top_models)

    return full_df


def load_model_data(args):
    # Takes the loaded model data from AWS (which is in args.temp)
    # and save them to each args parameter for going into dataset preparation and models inference.
    temp_dict = vars(args)
    for items in args.temp_data.iteritems():
        temp_dict[items[0]] = items[1]
    args.train_num = 0
    if args.data_period == 'weekly':
        args.data_period = 0
    else:
        args.data_period = 1

    if args.model_type == 'DLPA':
        args.model_type = 0
    elif args.model_type == 'DLPM':
        args.model_type = 1
    else:
        args.model_type = 2

    args.candle_type_candles = candle_type_to_int(args.candle_type_candles)
    args.candle_type_returnsX = candle_type_to_int(args.candle_type_returnsX)
    args.candle_type_returnsY = candle_type_to_int(args.candle_type_returnsY)

    args.production_output_flag = True
    if args.data_period == 0:
        # We need to add 1 business day to the start since the week starts from it. e.g. test -> Fri => start-> Mon
        args.start_date = args.forward_date - Week(args.train_num + args.valid_num + 1) + BDay(1)
        args.end_date = args.forward_date + Week(args.test_num - 1)
    else:
        args.start_date = args.forward_date - BDay(args.train_num + args.valid_num + 1)
        args.end_date = args.forward_date + BDay(args.test_num)

    args.stage = 0


def candle_type_to_int(a):
    if a == 'all':
        return 0
    if a == 'open':
        return 1
    if a == 'high':
        return 2
    if a == 'low':
        return 3
    if a == 'close':
        return 4
    if a == 'volume':
        return 5
