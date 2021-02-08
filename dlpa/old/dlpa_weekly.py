import argparse
import os
import platform
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import tensorflow as tf
from hyperopt import fmin, tpe, hp
from hyperopt.pyll.base import scope

from data.old.data_download import load_data
from data.old.data_preprocess import create_train_test_valid
from data.old.data_preprocess import dataset_prep
from model.old.model_dlpa_weekly import full_model

# from data.data_preprocess import standardize_on_all_stocks_each_ohlcv,normalize_on_all_stocks_each_ohlcv
# from data.data_preprocess import standardize_on_each_stocks_each_ohlcv,normalize_on_each_stocks_each_ohlcv
# from data.data_preprocess import remove_outliers_on_all_stocks_each_ohlcv,remove_outliers_on_each_stocks_each_ohlcv
# ******************************************************************************
# *******************  PARAMETERS  *********************************************
# ******************************************************************************
parser = argparse.ArgumentParser(description='Loratech')
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************
# parser.add_argument('--start_year', type=int, help='The starting year of the whole dataset.')
# parser.add_argument('--start_week', type=int, default=1, help='The starting week of the whole dataset.')
parser.add_argument('--end_year', type=int, help='The last year of the whole dataset.')
parser.add_argument('--end_week', type=int, default=1, help='The last week of the whole dataset.')
parser.add_argument('--stock_percentage', type=float, default=1.0,
                    help='The percentage of the stocks to use in the whole data.')
# *******************  DATASET SIZES  ******************************************
# ******************************************************************************
parser.add_argument('--train_num', type=int, default=260, help='Number of training weeks.')
parser.add_argument('--valid_num', type=int, default=5, help='Number of validation weeks.')
parser.add_argument('--test_num', type=int, default=5, help='Number of test weeks.')
parser.add_argument('--week_jump', type=int, default=6,
                    help='The start and end year arguments are jumped forward by this amount.')
# *******************  DATA PARAMETERS  ****************************************
# ******************************************************************************
parser.add_argument('--lookback', type=int, default=10, help='Number of week lookbacks for each data input')
parser.add_argument('--num_weeks_to_predict', type=int, default=1, help='Number of weeks to predict.')
parser.add_argument('--num_bins', type=int, default=3, help='Number of bins for output classification.')
parser.add_argument('--num_nans_to_skip', type=int, default=1, help='Number of nans to skip for data preperation.')
parser.add_argument('--accuracy_for_embedding', type=int, default=2, help='e.g. 3 means: 0.0012 -> 1')
parser.add_argument('--use_capping', default=True, help='use capping on accuracy for embedding')
# *******************  MODEL PARAMETERS  ***************************************
# ******************************************************************************
parser.add_argument('--epoch', type=int, default=20, help='Number of epochs.')
parser.add_argument('--batch_size', type=int, default=16, help='batch size')
parser.add_argument('--hidden_size', type=int, default=12, help='Number of GRU layers for encoder and decoder.')
parser.add_argument('--attention_hidden_size', type=int, default=12, help='Hidden size for attention layer.')
parser.add_argument('--CNN_hidden_size', type=int, default=0, help='Number of kernel for cnn layers')
parser.add_argument('--dropout', type=float, default=0.3, help='dropout value')
parser.add_argument('--learning_rate', type=float, default=1e-3, help='learning rate')
parser.add_argument('--embedding_flag', default=True, help='False means no embedding')
# *******************  TO AWS PARAMETERS  **************************************
# ******************************************************************************
parser.add_argument('--test_output_flag', default=False,
                    help='If set true, the test results for each stock will be written to AWS.')
parser.add_argument('--save_plots', default=False, help='If set true, the plots will be saved to hard drive.')
parser.add_argument('--production_output_flag', default=False,
                    help='If set true, the production results for each stock will be written to AWS.')
parser.add_argument('--test_table_name', type=str, default='DLPA_forward_weekly', help='The test table name in AWS.')
parser.add_argument('--production_table_name', type=str, default='DLPA_production_weekly',
                    help='The production table name in AWS.')
parser.add_argument('--model_data_table_name', type=str, default='DLPA_model_data_weekly',
                    help='The model data table name in AWS.')
# *******************  HYPEROPT PARAMETERS  ************************************
# ******************************************************************************
parser.add_argument('--Hyperopt_runs', type=int, default=10, help='number of runs in hyperopt loop.')
# ******************************************************************************
# *******************  PROGRAM PARAMETERS  *************************************
# ******************************************************************************

# *******************  PATHS  **************************************************
# ******************************************************************************
parser.add_argument('--db_url', type=str,
                    default="postgres://postgres:LORA2018#@seoul-prod2.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres",
                    help='Database URL')
parser.add_argument('--model_path', type=str)
parser.add_argument('--plot_path', type=str)

parser.add_argument('--update_lookback', type=int, default=2,
                    help="You can set the number of recent weeks for updating as load_data's argument.")
parser.add_argument('--update', default=False,
                    help="'Update = false' means that if there is a local database it won't connect to AWS anymore.")
parser.add_argument('--seed', type=int, default=123, help='Random seed')
parser.add_argument("--mode", default='client')
parser.add_argument("--port", default=64891)

parser.add_argument('--best_train_acc', type=float, default=1e-1)
parser.add_argument('--best_valid_acc', type=float, default=1e-1)
parser.add_argument('--test_acc_1', type=float, default=0)
parser.add_argument('--test_acc_2', type=float, default=0)
parser.add_argument('--test_acc_3', type=float, default=0)
parser.add_argument('--test_acc_4', type=float, default=0)
parser.add_argument('--test_acc_5', type=float, default=0)
parser.add_argument('--lowest_train_loss', type=float, default=1e-1)
parser.add_argument('--lowest_valid_loss', type=float, default=1e-1)
parser.add_argument('--best_valid_epoch', type=int, default=1)
parser.add_argument('--lowest_train_epoch', type=int, default=1)
parser.add_argument('--stocks_list', type=list)
parser.add_argument('--end_y', type=int, default=1)
parser.add_argument('--end_w', type=int, default=1)
parser.add_argument('--unique_num_of_returns', type=int, default=1)
parser.add_argument('--unique_num_of_outputs', type=int, default=1)
parser.add_argument('--timestamp', type=float, default=time.time())
parser.add_argument('--model_run_time', type=float)
parser.add_argument('--gpu_number', type=int, default=0)
parser.add_argument('--pc_number', type=str, default='unknown')

args_main = parser.parse_args()
# ******************************************************************************
# ******************************************************************************
# ******************************************************************************
np.random.seed(args_main.seed)


def main(args):
    space = {
        # Range of values that hyperopt is trained on.

        'Num_hidden': hp.choice('Num_hidden', list(np.linspace(1, 7, 4, dtype=int))),
        'Learning_rate': hp.choice('Learning_rate', [1e-3, 1e-4, 1e-5]),
        'Lookback': hp.choice('Lookback', list(np.linspace(5, 20, 4, dtype=int))),
        'Num_hidden_att': hp.choice('Num_hidden_att', list(np.linspace(3, 10, 4, dtype=int))),
        'Dropout': hp.choice('Dropout', list(np.linspace(0.0, 0.5, 3))),
        'batch_size': scope.int(hp.quniform('batch_size', 5, 8, 1)),  # => 2**x
        # 'Stock_usage': hp.choice('Stock_usage', [0.25, 0.5]),

        # 'lr': hp.quniform('lr', 4, 6, .5),  # => 1e-x
        # # 'opt': hp.choice('opt', ['adam', 'rmsprop']),  # winner=adam
        # 'batch_size': scope.int(hp.quniform('batch_size', 8, 11, 1)),  # => 2**x
        # 'layers': hp.choice('layers', [2]),
        # 'gru_nodes': hp.choice('gru_nodes', [4, 8]),
        # 'gru_nodes_mult': hp.choice('gru_nodes_mult', [0, 1]),
        # 'wr_layers': hp.choice('wr_layers', [2]),
        # 'wr_gru_nodes': hp.choice('wr_gru_nodes', [8]),
        # 'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0, 1]),
        # 'gru_drop': hp.quniform('gru_drop', 0, 0.5, .25),
        # 'wr_gru_drop': hp.quniform('wr_gru_drop', 0, 0.5, .25),
    }

    args.aws_columns_list = ['start_year', 'start_week', 'stock_percentage', 'valid_num', 'test_num', 'lookback',
                             'num_weeks_to_predict', 'num_bins', 'num_nans_to_skip', 'accuracy_for_embedding', 'epoch',
                             'batch_size', 'hidden_size', 'attention_hidden_size', 'CNN_hidden_size', 'dropout',
                             'learning_rate', 'seed',
                             'best_train_acc', 'best_valid_acc', 'test_acc_1', 'test_acc_2', 'test_acc_3', 'test_acc_4',
                             'test_acc_5', 'lowest_train_loss', 'lowest_valid_loss',
                             'best_valid_epoch', 'best_train_epoch', 'end_y', 'end_w', 'timestamp', 'model_run_time',
                             'pc_number', 'train_num']
    # You can set the number of recent weeks for updating as load_data's argument.
    # For example load_data(3) means that the function will update the most recent 3 weeks from sql server too.
    # 'Update = false' means that if there is a local database it won't connect to AWS anymore.
    full_df = load_data(args)
    # ? we have to do this because there were duplicates in the main SQL dataframe
    full_df.drop(full_df[full_df['ticker'] == '0151.HK'].index, inplace=True)

    if platform.system() == 'Linux':
        args.model_path = '/home/loratech/PycharmProjects/models/dlpa_weekly/'
        if not os.path.exists(args.model_path):
            os.makedirs(args.model_path)
        args.plot_path = '/home/loratech/PycharmProjects/plots/dlpa_weekly/'
        if not os.path.exists(args.plot_path):
            os.makedirs(args.plot_path)
    else:
        args.model_path = 'C:/dlpa_master/model/dlpa_weekly/'
        Path(args.model_path).mkdir(parents=True, exist_ok=True)
        args.plot_path = 'C:/dlpa_master/plots/dlpa_weekly/'
        Path(args.plot_path).mkdir(parents=True, exist_ok=True)

    def hyper_fn(hypers):
        # Hyperopt function which optimizes the whole process.
        args.learning_rate = float(hypers['Learning_rate'])
        args.hidden_size = int(hypers['Num_hidden'])
        args.attention_hidden_size = int(hypers['Num_hidden_att'])
        args.dropout = float(hypers['Dropout'])
        args.lookback = int(hypers['Lookback'])
        args.batch_size = 2 ** int(hypers['batch_size'])
        candles_X1, week_ret_per_X2, week_ret_Y = dataset_prep(full_df, args)

        input_dataset = week_ret_Y
        output_dataset = week_ret_Y
        trainX, trainY, validX, validY, testX, testY, final_prediction = create_train_test_valid(input_dataset,
                                                                                                 output_dataset,
                                                                                                 args)
        args.model_run_time = time.time()
        best_val = full_model(args, trainX, trainY, validX, validY, testX, testY, final_prediction)
        return best_val

    best = fmin(hyper_fn, space=space, algo=tpe.suggest, max_evals=args.Hyperopt_runs)


# ******************************************************************************
# *** If I want to use generators??(it is much more likely to make mistakes) ***
# ******************************************************************************
# X_train = input_generator(input_dataset[0:train_size], lookback, num_weeks_to_predict)
# Y_train = output_generator(output_dataset[0:train_size], lookback, num_weeks_to_predict)
#
# X_valid = input_generator(input_dataset[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size],
#                           lookback, num_weeks_to_predict)
# Y_valid = output_generator(output_dataset[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size],
#                            lookback, num_weeks_to_predict)
#
# X_test = input_generator(
#     input_dataset[train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size],
#     lookback, num_weeks_to_predict)
# Y_test = output_generator(
#     output_dataset[train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size],
#     lookback, num_weeks_to_predict)

# tr_t = np.asarray(list(next(X_train)))

# ******************************************************************************
# ******************************************************************************

if __name__ == "__main__":

    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
        try:
            # Currently, memory growth needs to be the same across GPUs
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            logical_gpus = tf.config.experimental.list_logical_devices('GPU')
            print(len(gpus), "Physical GPUs,", len(logical_gpus), "Logical GPUs")
        except RuntimeError as e:
            # Memory growth must be set before GPUs have been initialized
            print(e)
    # iman_pc = 0xd861c24963
    # pc#4 = 0xc9d92c510e5
    # pc#3 = 0xac1f6b153eba
    # pc#2 = 0x309c23270a79
    # pc #1 = 0x49226d8eb81

    if str(hex(uuid.getnode())) == '0xd861c24963':
        args_main.pc_number = "Iman's PC"
    if str(hex(uuid.getnode())) == '0xc9d92c510e5':
        args_main.pc_number = "PC#4"
    if str(hex(uuid.getnode())) == '0xac1f6b153eba':
        args_main.pc_number = "PC#3"
    if str(hex(uuid.getnode())) == '0x309c23270a79':
        args_main.pc_number = "PC#2"
    if str(hex(uuid.getnode())) == '0x49226d8eb81':
        args_main.pc_number = "PC#1"

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args_main.gpu_number)


    def year_week(y, w):
        return datetime.strptime(f'{y} {w} 1', '%G %V %u')


    d0 = timedelta(weeks=args_main.train_num + args_main.valid_num + 1)

    t2_1 = year_week(args_main.end_year, args_main.end_week)
    t2_2 = year_week(args_main.end_year, args_main.end_week)

    u0 = t2_1 - d0
    args_main.start_year, args_main.start_week = datetime.date(u0).isocalendar()[0:2]

    if u0 > datetime.fromtimestamp(time.time()):
        sys.exit('The start date is after today!')

    flag = True
    d = timedelta(weeks=0)
    while flag:
        # The loop which jumps forward all data by a number of weeks.

        t1 = year_week(args_main.start_year, args_main.start_week)

        # Needed for making the test date as anchor (only for end_date)
        d2 = timedelta(weeks=args_main.test_num)

        u1 = t1 + d
        t2_1 = t2_1 + d
        # t2_2 = t2_2 + d
        u2_2 = t2_1 + d2

        args_main.start_year, args_main.start_week = datetime.date(u1).isocalendar()[0:2]
        args_main.end_year, args_main.end_week = datetime.date(u2_2).isocalendar()[0:2]

        t2_2 = t2_1
        main(args_main)

        # Needed for week jump
        d = timedelta(weeks=args_main.week_jump)
        if u2_2 > datetime.fromtimestamp(time.time()):
            flag = False
