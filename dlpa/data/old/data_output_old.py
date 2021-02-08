import datetime
import time
from argparse import Namespace
from datetime import datetime
from multiprocessing import cpu_count

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import global_vars


def write_to_sql(df1, df2, df_out, model, table_name, args):
    # This function is used for outputting the data for test and final production datasets. The difference between
    # this function and write_to_sql_model_data is that this one outputs for each stock separately, and also just
    # have the essential data as seen in the columns list.
    columns = ['when_created', 'year', 'week_number_start', 'ticker', 'number_of_quantiles', 'num_predicted_weeks',
               'predicted_quantile_1', 'real_quantile_1', 'signal_strength_1', 'predicted_quantile_2',
               'real_quantile_2', 'signal_strength_2', 'predicted_quantile_3', 'real_quantile_3', 'signal_strength_3',
               'predicted_quantile_4', 'real_quantile_4', 'signal_strength_4',
               'predicted_quantile_5', 'real_quantile_5', 'signal_strength_5']
    if df_out is None:
        df_out = np.empty((len(args.stocks_list), args.num_weeks_to_predict))
        df_out[:] = np.nan
    predict_zeros = np.zeros((df1.shape[0], 1))

    # Predicting the test or final production values for AWS.
    if df2 is None:
        predicted_values = model.predict([df1, predict_zeros])
    else:
        predicted_values = model.predict([df2, df1, predict_zeros])

    to_aws_df = pd.DataFrame(columns=columns)
    list1 = ['predicted_quantile_1', 'predicted_quantile_2', 'predicted_quantile_3', 'predicted_quantile_4',
             'predicted_quantile_5', ]
    list2 = ['real_quantile_1', 'real_quantile_2', 'real_quantile_3', 'real_quantile_4', 'real_quantile_5', ]
    list3 = ['signal_strength_1', 'signal_strength_2', 'signal_strength_3', 'signal_strength_4', 'signal_strength_5', ]

    # Setting the values from args to aws dataframe.
    for i in range(df1.shape[0]):
        to_aws_df.loc[i, 'when_created'] = datetime.fromtimestamp(args.timestamp)
        to_aws_df.loc[i, 'year'] = int(args.end_y)
        to_aws_df.loc[i, 'week_number_start'] = int(args.end_w)
        to_aws_df.loc[i, 'ticker'] = str(args.stocks_list[i])
        to_aws_df.loc[i, 'number_of_quantiles'] = int(args.num_bins)
        to_aws_df.loc[i, 'num_predicted_weeks'] = int(args.num_weeks_to_predict)
        for j in range(args.num_weeks_to_predict):
            to_aws_df.loc[i, list1[j]] = int(np.argmax(predicted_values[i, j, :]))
            to_aws_df.loc[i, list2[j]] = float(df_out[i, j])
            to_aws_df.loc[i, list3[j]] = float(np.max(predicted_values[i, j, :]))

    db_url = global_vars.DB_PROD_URL_WRITE
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        to_aws_df.to_sql(con=conn, name=table_name, if_exists='append', index=False)


def write_to_sql_model_data(args):
    # This function is used for outputting the model data for each run.
    # The list of outputted values are shown in aws_columns_list.
    args.model_run_time = (time.time() - args.model_run_time) / 60
    args_copy = Namespace(**vars(args))
    db_url = global_vars.DB_PROD_URL_WRITE
    table_name = args.model_data_table_name

    output_dict = vars(args_copy)
    # output_dict.pop("mode", None)

    rr = pd.DataFrame(output_dict.items())
    rr = rr.transpose()
    rr.columns = rr.iloc[0]
    rr = rr.drop(rr.index[0])
    rr = rr[args.aws_columns_list]

    # This is done because in AWS the column names are all in lowercase, and if we don't do this it will throw an error.
    rr = rr.rename(columns={'CNN_hidden_size': 'cnn_hidden_size'})

    rr['when_created'] = datetime.fromtimestamp(time.time())
    rr = rr.infer_objects()
    rr.reset_index(drop=True, inplace=True)
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        rr.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
