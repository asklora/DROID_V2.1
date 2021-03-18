import gc
import os
import platform
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
from hyperopt import fmin, tpe, hp
from hyperopt.pyll.base import scope
from pandas.tseries.offsets import Week, BDay

from dlpa.data.data_download import load_data
from dlpa.data.data_preprocess import (
    dataset_p, 
    normalize_candles_dlpm, 
    dataset_prep, 
    normalize_candles_dlpm_temp, 
    create_train_test_valid_ohlcv, 
    create_rv_df)
from global_vars import aws_columns_list
from dlpa.hypers.model_parameters_load import load_model_data
from dlpa.model.model_dlpa import full_model as dlpa
from dlpa.model.model_dlpm import full_model as dlpm
from dlpa.model.model_simple import full_model as simple


# from data.data_preprocess import standardize_on_all_stocks_each_ohlcv,normalize_on_all_stocks_each_ohlcv
# from data.data_preprocess import standardize_on_each_stocks_each_ohlcv,normalize_on_each_stocks_each_ohlcv
# from data.data_preprocess import remove_outliers_on_all_stocks_each_ohlcv,remove_outliers_on_each_stocks_each_ohlcv


def hypers(model_type, train_num, num_periods_to_predict, data_period, rv_1=False, rv_2=False, tomorrow=False, future=False, live=False, full_update=False, use_candles=True):
    # global full_df, indices_df, full_df_rv
    # Range of values that hyperopt is trained on.
    if train_num != 0:
        space = {
            'batch_size': scope.int(hp.quniform('batch_size', 7, 9, 1)),  # => 2**x batch sizes 2 exponential
        }
        # Lookback period
        if data_period == 0:  # weekly forecast
            if future:
                if num_periods_to_predict == 4:
                    temp_dic = {'lookback': hp.choice('lookback', [30])}  # 30 week lookback~ 7 months
                    space.update(temp_dic)
                else:
                    temp_dic = {'lookback': hp.choice('lookback', [56])}  # 56 week lookback~ 13 months
                    space.update(temp_dic)
            else:
                temp_dic = {'lookback': hp.choice('lookback', [15])}  # 15 week lookback
                space.update(temp_dic)
        else:  # daily forecast
            temp_dic = {'lookback': hp.choice('lookback', [20])}  # 20 day lookback
            space.update(temp_dic)
        # use 5X5 weekly OHLCV candles or just use weekly (or daily) close returns
        if use_candles:
            temp_dic = {
                'cnn_kernel_size': hp.choice('cnn_kernel_size', [128, 256])}  # larger number of kernels better
            space.update(temp_dic)
        else:  # just weekly/daily returns
            temp_dic = {'cnn_kernel_size': hp.choice('cnn_kernel_size', [0])}
            space.update(temp_dic)

        if model_type == 0:  # DLPA MODEL----------------------
            temp_dic = {'learning_rate': hp.choice('lr', [3, 5])}  # => 1e-x - learning rate
            space.update(temp_dic)
            # Attention model
            # TODO test DLPA for chart pattern forecasts
            space_update = {
                'num_hidden': hp.choice('num_hidden', [5, 9]),  # more hidden layers
                'num_hidden_att': hp.choice('num_hidden_att', [5, 9]),  # not significant
                'dropout': hp.choice('dropout', [0, 0.25]),  # low dropout - already underfitting
            }
            space.update(space_update)

        elif model_type == 1:  # DLPM MODEL----------------------
            temp_dic = {'learning_rate': hp.choice('lr', [5])}  # => 1e-x - learning rate
            space.update(temp_dic)
            # uses TWO inputs - candle AND weekly returns (wr)
            if space['cnn_kernel_size'] != 0:  # candles used - RECOMMEND
                if future:
                    space_update = {
                        'gru_nodes_mult': hp.choice('gru_nodes_mult', [0, 1]),
                        'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0]),
                        'gru_drop': hp.choice('gru_drop', [0, 0.25]),  # no dropout - already underfitting
                        'wr_gru_drop': hp.choice('wr_gru_drop', [0, 0.25]),  # no dropout - already underfitting
                        'gru_nodes': hp.choice('gru_nodes', [4, 8]),  # candle GRU nodes
                        'wr_gru_nodes': hp.choice('wr_gru_nodes', [4, 8]),  # weekly return GRU nodes
                    }
                else:
                    space_update = {
                        'gru_nodes_mult': hp.choice('gru_nodes_mult', [0, 1]),
                        'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0]),
                        'gru_drop': hp.choice('gru_drop', [0, 0.25]),  # no dropout - already underfitting
                        'wr_gru_drop': hp.choice('wr_gru_drop', [0, 0.25]),  # no dropout - already underfitting
                        'gru_nodes': hp.choice('gru_nodes', [4, 8]),  # candle GRU nodes
                        'wr_gru_nodes': hp.choice('wr_gru_nodes', [4, 8]),  # weekly return GRU nodes
                    }
            else:  # no candles - NOT RECOMMEND - DO NOT USE
                space_update = {
                    'wr_gru_nodes_mult': hp.choice('wr_gru_nodes_mult', [0, 1]),
                    'wr_gru_drop': hp.choice('wr_gru_drop', [0, 0.25]),  # no dropout - already underfitting
                }
            space.update(space_update)

        else:  # SIMPLE MODEL(args.model_type == 2)----------------------
            temp_dic = {'learning_rate': hp.choice('lr', [3, 5])}  # => 1e-x - learning rate
            space.update(temp_dic)
            # uses simple ANN either to CNN or weekly/daily returns
            if future:
                space_update = {
                    'num_layers': hp.choice('num_layers', [4, 8]),
                    'num_nodes': hp.choice('num_nodes', [8]),  # nodes/layer
                    'dropout': hp.choice('dropout', [0.25, 0.5]),  # model is overfitting
                }
            else:
                space_update = {
                    'num_layers': hp.choice('num_layers', [4, 8]),
                    'num_nodes': hp.choice('num_nodes', [8]),  # nodes/layer
                    'dropout': hp.choice('dropout', [0.25, 0.5]),  # model is overfitting
                }
            space.update(space_update)

    else:
        space = {'temp_var': hp.choice('temp_var', [1])}

    aws_columns_list = aws_columns_list

    # You can set the number of recent weeks for updating as load_data's argument.
    # For example load_data(3) means that the function will update the most recent 3 weeks from sql server too.
    # 'Update = false' means that if there is a local database it won't connect to AWS anymore.
    if rv_1 or rv_2:
        full_df, indices_df = load_data()

        full_df_rv = create_rv_df(full_df, indices_df)

        # full_df_min = full_df.trading_day.min()
        # full_df_rv_min = full_df_rv.trading_day.min()
        # date_min = max(full_df_min, full_df_rv_min)
        #
        # full_df_max = full_df.trading_day.max()
        # full_df_rv_max = full_df_rv.trading_day.max()
        # date_max = min(full_df_max, full_df_rv_max)
        #
        # full_df = full_df[(full_df.trading_day >= date_min) & (full_df.trading_day <= date_max)]
        # full_df_rv = full_df_rv[(full_df_rv.trading_day >= date_min) & (full_df_rv.trading_day <= date_max)]

    elif args.beta_1 or args.beta_2:
        full_df, full_df_beta_adj, indices_df = load_data(args)
        # full_df_beta_adj = create_beta_df(full_df, indices_df, args)

        try:
            full_df_beta_adj.trading_day = full_df_beta_adj['trading_day'].dt.strftime('%Y-%m-%d')
        except:
            pass
        full_df_min = full_df.trading_day.min()
        full_df_beta_min = full_df_beta_adj.trading_day.min()
        date_min = max(full_df_min, full_df_beta_min)

        full_df_max = full_df.trading_day.max()
        full_df_beta_max = full_df_beta_adj.trading_day.max()
        date_max = min(full_df_max, full_df_beta_max)

        full_df = full_df[(full_df.trading_day >= date_min) & (full_df.trading_day <= date_max)]
        full_df_beta_adj = full_df_beta_adj[(full_df_beta_adj.trading_day >= date_min) & (full_df_beta_adj.trading_day <= date_max)]

        # aa= pd.concat([full_df, full_df_beta_adj]).drop_duplicates(keep=False)
    else:
        full_df, indices_df = load_data(args)

    args.indices_df = indices_df

    def limit_df(df, low_limit, high_limit):
        df = df.copy()
        df.loc[df < low_limit] = low_limit
        df.loc[df > high_limit] = high_limit
        return df

    if args.rv_1 or args.rv_2 or args.beta_1 or args.beta_2:
        full_df.open_multiple = limit_df(full_df.open_multiple, 0.8, 1.25)
        full_df.high_multiple = limit_df(full_df.high_multiple, 0.8, 1.25)
        full_df.low_multiple = limit_df(full_df.low_multiple, 0.8, 1.25)
        full_df.close_multiple = limit_df(full_df.close_multiple, 0.8, 1.25)

    # Making the multiples -> returns
    full_df.open_multiple = full_df.open_multiple - 1
    full_df.high_multiple = full_df.high_multiple - 1
    full_df.low_multiple = full_df.low_multiple - 1
    full_df.close_multiple = full_df.close_multiple - 1
    args.full_df = full_df
    del full_df

    if args.rv_1 or args.rv_2:
        full_df_rv.open_multiple = limit_df(full_df_rv.open_multiple, 0.5, 2)
        full_df_rv.high_multiple = limit_df(full_df_rv.high_multiple, 0.5, 2)
        full_df_rv.low_multiple = limit_df(full_df_rv.low_multiple, 0.5, 2)
        full_df_rv.close_multiple = limit_df(full_df_rv.close_multiple, 0.5, 2)

        full_df_rv.open_multiple = full_df_rv.open_multiple - 1
        full_df_rv.high_multiple = full_df_rv.high_multiple - 1
        full_df_rv.low_multiple = full_df_rv.low_multiple - 1
        full_df_rv.close_multiple = full_df_rv.close_multiple - 1

        args.full_df_rv = full_df_rv
        del full_df_rv

    if args.beta_1 or args.beta_2:
        full_df_beta_adj.open_multiple = limit_df(full_df_beta_adj.open_multiple, 0.5, 2)
        full_df_beta_adj.high_multiple = limit_df(full_df_beta_adj.high_multiple, 0.5, 2)
        full_df_beta_adj.low_multiple = limit_df(full_df_beta_adj.low_multiple, 0.5, 2)
        full_df_beta_adj.close_multiple = limit_df(full_df_beta_adj.close_multiple, 0.5, 2)

        full_df_beta_adj.open_multiple = full_df_beta_adj.open_multiple - 1
        full_df_beta_adj.high_multiple = full_df_beta_adj.high_multiple - 1
        full_df_beta_adj.low_multiple = full_df_beta_adj.low_multiple - 1
        full_df_beta_adj.close_multiple = full_df_beta_adj.close_multiple - 1

        args.full_df_beta_adj = full_df_beta_adj
        del full_df_beta_adj

    # ********************************************************************************************
    # ***************************** PATH creation ************************************************
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

    # ********************************************************************************************
    # ********************************************************************************************

    def hyper_fn(hypers):
        # global full_df, indices_df, full_df_rv

        vars(args).update(hypers)
        if args.data_period == 0:
            period = 'weekly'
        else:
            period = 'daily'
        args.remote_file_path = '/home/seoul/models/%s_%s/' % (period, str(args.forward_date.date()))

        if args.train_num == 0:
            if args.data_period == 0:
                period = 'weekly'
            else:
                period = 'daily'
            temp_forward_date = args.forward_date
            load_model_data(args)
            args.load_date = args.forward_date
            args.forward_date = temp_forward_date
            args.remote_file_path = '/home/seoul/models/%s_%s/' % (period, str(args.load_date.date()))

        if args.train_num < args.lookback:
            if args.data_period == 0:
                # We need to add 1 business day to the start since the week starts from it. e.g. test -> Fri =>
                # start-> Mon
                args.start_date = args.forward_date - Week(args.lookback + args.valid_num + 1) + BDay(1)
                args.end_date = args.forward_date + Week(args.test_num - 1)
            else:
                args.start_date = args.forward_date - BDay(args.lookback + args.valid_num + 1)
                args.end_date = args.forward_date + BDay(args.test_num)

        # Get the required subset of the whole data.
        datasett = dataset_prep(args.full_df, args)

        # Making the datasets for candles and returns
        if args.rv_1:
            datasett_rv = dataset_prep(args.full_df_rv, args)
            candles_X, returns_X, returns_Y = dataset_p(datasett, args, df2=datasett_rv)
        elif args.rv_2:
            datasett_rv = dataset_prep(args.full_df_rv, args)
            candles_X, returns_X, returns_Y = dataset_p(datasett_rv, args)
        elif args.beta_1:
            datasett_beta = dataset_prep(args.full_df_beta_adj, args)
            candles_X, returns_X, returns_Y = dataset_p(datasett, args, df2=datasett_beta)
        elif args.beta_2:
            datasett_beta = dataset_prep(args.full_df_beta_adj, args)
            candles_X, returns_X, returns_Y = dataset_p(datasett_beta, args)

        else:
            candles_X, returns_X, returns_Y = dataset_p(datasett, args)

        if args.cnn_kernel_size != 0:
            candles_X, mins, max_min_difference = normalize_candles_dlpm(candles_X)
            # candles_X, remove_outliers_factor = remove_outliers_on_all_stocks_each_ohlcv(candles_X, 5)
            # candles_X, mean_value, std_value = standardize_on_all_stocks_each_ohlcv(candles_X)

        # Creating the train, valid and test datasets.
        arr_list = []
        trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1, final_prediction2 = create_train_test_valid_ohlcv(
            returns_X, candles_X, returns_Y, args, hypers)

        # cond_train = np.sum(np.isnan(trainX1), axis=1) <= args.num_nans_to_skip
        # cond_train = np.squeeze(cond_train)
        #
        # if validX1 is not None:
        #     cond_valid = np.sum(np.isnan(validX1), axis=1) <= args.num_nans_to_skip
        #     cond_valid = np.squeeze(cond_valid)
        # else:
        #     cond_valid = None
        #
        # def nan_removal(arr, condition, data_type):
        #     if arr is not None:
        #         if data_type == 'x':
        #             arr = arr[condition, :, :]
        #         else:
        #             arr = arr[condition, :]
        #         return np.nan_to_num(arr)
        #     else:
        #         return None
        #
        # trainX1 = nan_removal(trainX1, cond_train, 'x')
        # trainX2 = nan_removal(trainX2, cond_train, 'x')
        # validX1 = nan_removal(validX1, cond_valid, 'x')
        # validX2 = nan_removal(validX2, cond_valid, 'x')
        #
        # trainY = nan_removal(trainY, cond_train, 'y')
        # validY = nan_removal(validY, cond_valid, 'y')

        # ********************************************************************************************
        # ********************************* STAGE 1 **************************************************

        # In stage 1 we keep the testX intact and move the testY on days of week. For example if our model is trained on
        # a week based on friday, we will have a testX based on Fri and 5 testY for Fri, Mon, ..., Thu.
        # We will have 5 test results based on each day of the week starting from the training day of week.
        if args.data_period == 0 and args.go_live is not True:
            if args.stage == 1:
                # Note: we need to have uniform stock list for all of the Xs and Ys all days of week.

                # Saving the stocks list of trainX.
                stocks_list_temp_x = pd.DataFrame(args.stocks_list)

                stocks_list_temp = []
                args.save_cluster_files = False
                X1_list = []
                X2_list = []
                Y_list = []

                valid_num_temp = args.valid_num
                train_num_temp = args.train_num
                dow_temp = args.dow
                _, _, args.dow = datetime.date(args.forward_date).isocalendar()

                args.train_num = 2 * args.lookback
                args.valid_num = 0
                # Iteration on 5 days of week.
                for i in range(5):
                    # Creating a small dataset for preparing the testYs.
                    dataset_temp = no_train_dataset_creator(args, i)

                    # Creating candles and returns for the small dataset.
                    candles_X_t, returns_X_t, returns_Y_t = dataset_p(dataset_temp, args)

                    # Creating the testY for each iteration of the loop.
                    _, _, _, _, _, _, _, _, testY, _, _ = create_train_test_valid_ohlcv(
                        returns_X_t, candles_X_t, returns_Y_t, args, hypers)

                    # Saving the stocks list for each day of the week. Each day can have different stocks.
                    stocks_list_temp.append(pd.DataFrame(args.stocks_list))

                    X1_list.append(testX1)
                    X2_list.append(testX2)
                    Y_list.append(testY)

                # Finding the common stocks between all the X and Ys in all days of week.
                common_stocks = reduce(lambda left, right: pd.merge(left, right, on='ticker'), stocks_list_temp)
                common_stocks = stocks_list_temp_x.merge(common_stocks, on='ticker')

                Y_list2 = []
                X1_list2 = []
                X2_list2 = []

                # Finding the stocks that are not common in testX.
                intersection_x = stocks_list_temp_x.merge(common_stocks, how='outer', on='ticker',
                                                          indicator=True).query('_merge != "both"').drop('_merge',
                                                                                                         1).index.tolist()

                # Finding the stocks that are not common in testYs.
                for i in range(len(Y_list)):
                    intersection_y = stocks_list_temp[i].merge(common_stocks, how='outer', on='ticker',
                                                               indicator=True).query('_merge != "both"').drop(
                        '_merge', 1).index.tolist()

                    # Deleting the not common stocks in testXs and testYs.
                    Y_list2.append(np.delete(Y_list[i], intersection_y, 1))
                    X1_list2.append(np.delete(X1_list[i], intersection_x, 1))
                    X2_list2.append(np.delete(X2_list[i], intersection_x, 1))

                testX1 = np.stack(X1_list2)
                testX2 = np.stack(X2_list2)
                testY = np.stack(Y_list2)

                testX1 = np.squeeze(testX1, axis=1)
                if args.use_candles:
                    testX2 = np.squeeze(testX2, axis=1)
                else:
                    testX2 = None
                testY = np.squeeze(testY, axis=1)
                args.stocks_list = stocks_list_temp_x
                args.save_cluster_files = True
                args.valid_num = valid_num_temp
                args.train_num = train_num_temp
                args.dow = dow_temp
                # np.squeeze(X2_list[0], axis=5)
                # testX2_temp = np.squeeze(X2_list[0], axis=5)
                # np.savetxt("stage1.csv", testX2_temp[0][16][args.lookback -1], delimiter=",")
            elif args.stage == 2:
                # ********************************************************************************************
                # ********************************* STAGE 2 and 3 ********************************************

                # In stage 2 and 3 we have the testX and the testY move on each day of the week.
                # For example if our model is trained on a week based on friday, we will have 5 test results
                # (testX and testY pairs) for Fri, Mon, ..., Thu.
                # We will have 5 test results based on each day of the week starting from the training day of week.

                args.save_cluster_files = False
                X1_list = []
                X2_list = []
                Y_list = []
                stock_list_temp = []
                valid_num_temp = args.valid_num
                train_num_temp = args.train_num
                dow_temp = args.dow
                _, _, args.dow = datetime.date(args.forward_date).isocalendar()

                args.train_num = 2 * args.lookback + 1
                args.valid_num = 0
                # Iteration on 5 days of week.
                for i in range(5):
                    # Creating a small dataset for preparing the testYs.
                    dataset_temp = no_train_dataset_creator(args, i)

                    if args.dow + i > 5:
                        args.dow = 1
                    else:
                        args.dow = args.dow + i

                    # Creating candles and returns for the small dataset.
                    candles_X_t, returns_X_t, returns_Y_t = dataset_p(dataset_temp, args)

                    if args.cnn_kernel_size != 0:
                        candles_X_t = normalize_candles_dlpm_temp(candles_X_t, mins, max_min_difference)
                        # candles_X_t = remove_outliers_on_all_stocks_each_ohlcv_temp_db(candles_X_t, remove_outliers_factor)
                        # candles_X_t = standardize_on_all_stocks_each_ohlcv_temp_db(candles_X_t, mean_value, std_value)

                    # Creating the testX and testY for each iteration of the loop.
                    _, _, _, _, _, _, testX1, testX2, testY, _, _ = create_train_test_valid_ohlcv(
                        returns_X_t, candles_X_t, returns_Y_t, args, hypers)
                    # test_temp1= np.squeeze(testX2, axis=5)
                    X1_list.append(testX1)
                    X2_list.append(testX2)
                    Y_list.append(testY)
                    stock_list_temp.append(pd.DataFrame(args.stocks_list))

                args.valid_num = valid_num_temp
                args.train_num = train_num_temp
                args.dow = dow_temp

                # Finding the common list of stocks between all days of week.
                result = stock_list_temp[0]
                for df in stock_list_temp[1:]:
                    result = pd.merge(result, df, how='inner')

                # Creating unified testXs and testYs.
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
                # testX2_temp = np.squeeze(testX2, axis=5)
                # np.savetxt("stage2.csv", testX2_temp[0][16][args.lookback -1], delimiter=",")

        args.train_len = len(trainX1)
        if validX1 is not None:
            args.valid_len = len(validX1)
        else:
            args.valid_len = args.train_len * 0.2
            args.train_len = args.train_len * 0.8

        if args.train_num != 0:
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
        gc.collect()
        return best_val

    best = fmin(hyper_fn, space=space, algo=tpe.suggest, max_evals=args.Hyperopt_runs)

    args.full_df = []
    args.full_df_rv = []
    # del full_df
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
    # Creates a small dataset for test datasets.
    start_date_temp = args.start_date
    forward_date_temp = args.forward_date
    end_date_temp = args.end_date

    args.forward_date = args.forward_date + BDay(dow)
    args.end_date = args.end_date + BDay(dow)

    # Week(args.lookback * 2) or BDay(args.lookback * 2) is used as lookback + extra periods to make sure that we
    # have full data for the train valid test dataset creation
    if args.data_period == 0:
        args.start_date = args.forward_date - Week(args.lookback * 2) + BDay(1)
    else:
        args.start_date = args.forward_date - BDay(args.lookback * 2)

    dataset_temp = dataset_prep(args.full_df, args)

    args.start_date = start_date_temp
    args.forward_date = forward_date_temp
    args.end_date = end_date_temp

    return dataset_temp
