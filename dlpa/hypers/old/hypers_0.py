import gc
import os
import platform
import time
from pathlib import Path

import numpy as np
import pandas as pd
from hyperopt import fmin, tpe, hp
from hyperopt.pyll.base import scope
from pandas.tseries.offsets import Week, BDay

from data.data_download import load_data
from data.data_preprocess import dataset_p, dataset_prep, create_train_test_valid_ohlcv, \
    standardize_on_all_stocks_each_ohlcv, remove_outliers_on_all_stocks_each_ohlcv
from model.model_dlpa import full_model as dlpa
from model.model_dlpm import full_model as dlpm
from model.model_simple import full_model as simple


# from data.data_preprocess import standardize_on_all_stocks_each_ohlcv,normalize_on_all_stocks_each_ohlcv
# from data.data_preprocess import standardize_on_each_stocks_each_ohlcv,normalize_on_each_stocks_each_ohlcv
# from data.data_preprocess import remove_outliers_on_all_stocks_each_ohlcv,remove_outliers_on_each_stocks_each_ohlcv


def hypers(args):
    global full_df, indices_df
    # Range of values that hyperopt is trained on.
    if args.model_type == 0:
        # DLPA
        space = {
            'batch_size': scope.int(hp.quniform('batch_size', 5, 8, 1)),  # => 2**x - need lower batches
            'learning_rate': hp.choice('lr', [2, 3, 5, 7]),  # => 1e-x
            'lookback': hp.choice('lookback', [15]),

            'num_hidden': hp.choice('num_hidden', [7, 9]),  # more hidden layers
            'num_hidden_att': hp.choice('num_hidden_att', [5]),  # not significant
            'dropout': hp.choice('dropout', [0]),  # no dropout - already underfitting
        }

        if args.use_candles:
            temp_dic = {
                'cnn_kernel_size': hp.choice('cnn_kernel_size', [64, 128, 256, 384])}  # larger number of kernels better
            space.update(temp_dic)
        else:
            temp_dic = {'cnn_kernel_size': hp.choice('cnn_kernel_size', [0])}
            space.update(temp_dic)

    elif args.model_type == 1:
        # DLPM
        space = {
            'batch_size': scope.int(hp.quniform('batch_size', 5, 8, 1)),  # => 2**x
            'learning_rate': hp.choice('lr', [2, 3, 5]),  # => 1e-x
            'lookback': hp.choice('lookback', [15]),

        }

        if args.use_candles:
            temp_dic = {
                'cnn_kernel_size': hp.choice('cnn_kernel_size', [64, 128, 256, 384])}  # larger number of kernels better
            space.update(temp_dic)
        else:
            temp_dic = {'cnn_kernel_size': hp.choice('cnn_kernel_size', [0])}

            space.update(temp_dic)

        if space['cnn_kernel_size'] != 0:
            space_update = {
                'gru_nodes_mult': hp.choice('gru_nodes_mult', [0, 1]),
                'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0, 1]),
                'gru_drop': hp.choice('gru_drop', [0]),  # no dropout - already underfitting
                'wr_gru_drop': hp.choice('wr_gru_drop', [0]),  # no dropout - already underfitting
                'gru_nodes': hp.choice('gru_nodes', [4]),  # not significant - fix at 4
            }
        else:
            space_update = {
                'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0, 1]),
                'wr_gru_drop': hp.choice('wr_gru_drop', [0]),  # no dropout - already underfitting
            }
        space.update(space_update)
    else:
        # SIMPLE
        space = {
            'batch_size': scope.int(hp.quniform('batch_size', 5, 8, 1)),  # => 2**x
            'learning_rate': hp.choice('learning_rate', [3, 4, 5]),  # => 1e-x
            'lookback': hp.choice('lookback', [15]),

            'num_layers': hp.choice('num_layers', [2, 4, 8]),
            'num_nodes': hp.choice('num_nodes', [4, 8]),
            'dropout': hp.choice('dropout', [0]),  # no dropout - already underfitting
        }

        if args.use_candles:
            temp_dic = {'cnn_kernel_size': hp.choice('cnn_kernel_size',
                                                     [64, 128, 256, 384])}  # larger number of kernels better
            space.update(temp_dic)
        else:
            temp_dic = {'cnn_kernel_size': hp.choice('cnn_kernel_size', [0])}

            space.update(temp_dic)

    args.aws_columns_list = ['model_type', 'data_period', 'when_created', 'forward_date', 'forward_week', 'test_dow',
                             'train_dow', 'best_train_acc',
                             'best_valid_acc', 'test_acc_1', 'test_acc_2', 'test_acc_3', 'test_acc_4',
                             'test_acc_5', 'run_time_min', 'train_num', 'cnn_kernel_size',
                             'batch_size', 'learning_rate', 'lookback',
                             'epoch', 'param_name_1', 'param_val_1', 'param_name_2', 'param_val_2', 'param_name_3',
                             'param_val_3', 'param_name_4', 'param_val_4', 'param_name_5', 'param_val_5',
                             'num_bins', 'num_nans_to_skip', 'accuracy_for_embedding',
                             'candle_type_returnsX', 'candle_type_returnsY', 'candle_type_candles', 'seed',
                             'best_valid_epoch', 'best_train_epoch', 'model_filename',
                             'pc_number', 'stock_percentage', 'valid_num', 'test_num']

    # You can set the number of recent weeks for updating as load_data's argument.
    # For example load_data(3) means that the function will update the most recent 3 weeks from sql server too.
    # 'Update = false' means that if there is a local database it won't connect to AWS anymore.
    full_df, indices_df = load_data(args)

    if platform.system() == 'Linux':
        args.model_path = '/home/loratech/PycharmProjects/models/'
        if not os.path.exists(args.model_path):
            os.makedirs(args.model_path)
        args.plot_path = '/home/loratech/PycharmProjects/plots/'
        if not os.path.exists(args.plot_path):
            os.makedirs(args.plot_path)
    else:
        args.model_path = 'C:/dlpa_master/model/'
        Path(args.model_path).mkdir(parents=True, exist_ok=True)
        args.plot_path = 'C:/dlpa_master/plots/'
        Path(args.plot_path).mkdir(parents=True, exist_ok=True)

    if platform.system() == 'Linux':
        args.model_path_clustering = '/home/loratech/PycharmProjects/models/clustering/'
        if not os.path.exists(args.model_path_clustering):
            os.makedirs(args.model_path_clustering)
    else:
        args.model_path_clustering = 'C:/dlpa_master/model/clustering/'
        Path(args.model_path_clustering).mkdir(parents=True, exist_ok=True)

    def hyper_fn(hypers):
        global full_df, indices_df

        vars(args).update(hypers)
        datasett = dataset_prep(full_df, args)

        # if args.data_period == 0:
        #     args.start_date, args.end_date = get_start_end_date(datasett, args.forward_date.isoweekday())
        #     datasett1 = mask_df(datasett, args)
        #     # if args.dow != args.forward_date.isoweekday():
        #     #     args.start_date, args.end_date = get_start_end_date(datasett, args.dow)
        #     #     datasett2 = mask_df(datasett, args)
        # else:
        # datasett1 = datasett

        candles_X, returns_X, returns_Y = dataset_p(datasett, args)

        if args.cnn_kernel_size != 0:
            candles_X = remove_outliers_on_all_stocks_each_ohlcv(candles_X, 5)
            candles_X = standardize_on_all_stocks_each_ohlcv(candles_X)

        # if args.dow != args.forward_date.isoweekday() and args.data_period == 0:
        #     candles_X_t, returns_X_t, returns_Y_t = dataset_p(datasett2, args)

        trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1, final_prediction2 = create_train_test_valid_ohlcv(
            returns_X, candles_X, returns_Y, args, hypers)

        # if False:# Just for checking
        #     main_close1 = datasett1.pivot_table(index=datasett1.index, columns='ticker', values='close_multiply', aggfunc='first')
        #     main_close2 = datasett2.pivot_table(index=datasett2.index, columns='ticker', values='close_multiply', aggfunc='first')

        # STAGE 1
        if args.data_period == 0:
            if args.stage == 1:
                stocks_list_temp = pd.DataFrame(args.stocks_list)
                args.save_cluster_files = False
                X1_list = []
                X2_list = []
                Y_list = []
                for i in range(5):

                    dataset_temp = no_train_dataset_creator(args, i)

                    candles_X_t, returns_X_t, returns_Y_t = dataset_p(dataset_temp, args)
                    _, _, _, _, _, _, _, _, testY, _, _ = create_train_test_valid_ohlcv(
                        returns_X_t, candles_X_t, returns_Y_t, args, hypers)

                    stocks_list_temp2 = pd.DataFrame(args.stocks_list)
                    m = stocks_list_temp.merge(stocks_list_temp2, how='outer', suffixes=['', '_'], indicator=True)

                    k = 0
                    for j in m[m._merge != 'both'].ticker:
                        testX1 = np.delete(testX1, stocks_list_temp[stocks_list_temp.ticker == j].index - k, 1)
                        testX2 = np.delete(testX2, stocks_list_temp[stocks_list_temp.ticker == j].index - k, 1)
                        k = k + 1

                    k = 0
                    for j in m[m._merge != 'both'].ticker:
                        testY = np.delete(testY, stocks_list_temp2[stocks_list_temp2.ticker == j].index - k, 1)
                        k = k + 1

                    stocks_list_temp = pd.DataFrame(m[m._merge == 'both'].ticker)
                    X1_list.append(testX1)
                    X2_list.append(testX2)
                    Y_list.append(testY)

                testX1 = np.stack(X1_list)
                testX2 = np.stack(X2_list)
                testY = np.stack(Y_list)

                testX1 = np.squeeze(testX1, axis=1)
                if args.use_candles:
                    testX2 = np.squeeze(testX2, axis=1)
                else:
                    testX2 = None
                testY = np.squeeze(testY, axis=1)
                args.stocks_list = stocks_list_temp
                args.save_cluster_files = True

            # STAGE 2 and 3
            else:
                args.save_cluster_files = False
                X1_list = []
                X2_list = []
                Y_list = []
                stock_list_temp = []
                for i in range(5):
                    dataset_temp = no_train_dataset_creator(args, i)

                    candles_X_t, returns_X_t, returns_Y_t = dataset_p(dataset_temp, args)
                    _, _, _, _, _, _, testX1, testX2, testY, _, _ = create_train_test_valid_ohlcv(
                        returns_X_t, candles_X_t, returns_Y_t, args, hypers)

                    X1_list.append(testX1)
                    X2_list.append(testX2)
                    Y_list.append(testY)
                    stock_list_temp.append(pd.DataFrame(args.stocks_list))

                result = stock_list_temp[0]
                for df in stock_list_temp[1:]:
                    result = pd.merge(result, df, how='inner')

                for nn in range(5):
                    m = result.merge(stock_list_temp[nn], how='outer', suffixes=['', '_'], indicator=True)
                    k = 0
                    for j in range(len(m[m._merge != 'both'])):
                        X1_list[nn] = np.delete(X1_list[nn], stock_list_temp[nn][
                            stock_list_temp[nn].ticker == m[m._merge != 'both'].iloc[j]['ticker']].index - k, 1)
                        X2_list[nn] = np.delete(X2_list[nn], stock_list_temp[nn][
                            stock_list_temp[nn].ticker == m[m._merge != 'both'].iloc[j]['ticker']].index - k, 1)
                        Y_list[nn] = np.delete(Y_list[nn], stock_list_temp[nn][
                            stock_list_temp[nn].ticker == m[m._merge != 'both'].iloc[j]['ticker']].index - k, 1)
                        k = k + 1

                testX1 = np.stack(X1_list)
                testX2 = np.stack(X2_list)
                testY = np.stack(Y_list)

                testX1 = np.squeeze(testX1, axis=1)
                if args.use_candles:
                    testX2 = np.squeeze(testX2, axis=1)
                else:
                    testX2 = None
                args.save_cluster_files = True
                testY = np.squeeze(testY, axis=1)

        # if args.dow != args.forward_date.isoweekday() and args.data_period == 0:
        #     _, _, _, _, _, _, testX1, testX2, testY, final_prediction1, final_prediction2 = create_train_test_valid_ohlcv(
        #         returns_X_t, candles_X_t, returns_Y_t, args, hypers)

        args.run_time_min = time.time()
        args.model_filename = model_filename_creator(args)

        if args.model_type == 0:
            best_val = dlpa(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY,
                            final_prediction1, final_prediction2, indices_df, args, hypers)
        elif args.model_type == 1:
            best_val = dlpm(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY,
                            final_prediction1, final_prediction2, indices_df, args, hypers)
        else:
            best_val = simple(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY,
                              final_prediction1, final_prediction2, indices_df, args, hypers)

        del trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1, final_prediction2

        return best_val

    best = fmin(hyper_fn, space=space, algo=tpe.suggest, max_evals=args.Hyperopt_runs)
    gc.collect()


def model_filename_creator(args):
    if args.model_type == 0:
        type = "DLPA"
    elif args.model_type == 1:
        type = "DLPM"
    else:
        type = "SIMPLE"

    return type + "_" + str(args.pc_number) + "_gpu" + str(args.gpu_number) + "_" + str(int(time.time())) + ".hdf5"


def no_train_dataset_creator(args, dow):
    start_date_temp = args.start_date
    forward_date_temp = args.forward_date
    end_date_temp = args.end_date

    args.forward_date = args.forward_date + BDay(dow)
    args.end_date = args.end_date + BDay(dow)

    # Week(args.lookback * 2) or BDay(args.lookback * 2) is used as lookback + extra periods to make sure that we
    # have full data for the
    if args.data_period == 0:
        args.start_date = args.forward_date - Week(args.lookback * 2) + BDay(1)
    else:
        args.start_date = args.forward_date - BDay(args.lookback * 2)

    dataset_temp = dataset_prep(full_df, args)

    args.start_date = start_date_temp
    args.forward_date = forward_date_temp
    args.end_date = end_date_temp

    return dataset_temp
