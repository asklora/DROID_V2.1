import multiprocessing
import os
import sys
import time
from datetime import datetime as dt
import datetime
import numpy as np
import pandas as pd
from joblib import dump
from pandas.tseries.offsets import Week, BDay
from scipy import stats
from sklearn.cluster import KMeans
from multiprocessing import Pool
from sqlalchemy import create_engine

import global_vars


def dataset_p(df, args, df2=None):
    # The main function. This function creates the candles and return datasets.

    # Creating the candles dataset.
    if args.cnn_kernel_size != 0:
        args.data_type = 0  # 0->X_Candles or 1->X_returns or 2->Y_returns
        args.candle_type = args.candle_type_candles
        if args.rv_1 or args.beta_1:
            df_X0 = data_merging_clipping(df2, args)
            df_X0 = data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False)
        else:
            df_X0 = data_merging_clipping(df, args)
            df_X0 = data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False)
    else:
        df_X0 = None

    # Creating the X return dataset.
    args.candle_type = args.candle_type_returnsX
    if args.cluster_x:
        args.clustering_model_filename = args.model_path_clustering + "model_x_" + str(int(time.time())) + ".joblib"
        args.cluster_centers_filename = args.model_path_clustering + "centers__x" + str(int(time.time())) + ".npy"
        df_X1 = cluster_(df, args, args.cluster_no_x)
    else:
        df_X1 = returns_(df, args)


    # Creating the Y return dataset.
    args.candle_type = args.candle_type_returnsY
    if args.cluster_y:
        args.clustering_model_filename = args.model_path_clustering + "model_y_" + str(int(time.time())) + ".joblib"
        args.cluster_centers_filename = args.model_path_clustering + "centers__y" + str(int(time.time())) + ".npy"
        df_Y_temp = cluster_(df, args, args.cluster_no_y)
        args.num_bins = args.cluster_no_y
    else:
        df_Y_temp = returns_(df, args)
    df_Y = create_Y(df_Y_temp)

    return df_X0, df_X1, df_Y


def insert_missing_dates(df):
    # I removed this function by fixing the input data.
    # This function insert missing dates. if a date is missing for all the stocks.
    df = df.resample("B").sum()
    df[df == 0] = np.nan
    return df

def insert_missing_stocks(df, col_list):
    # I removed this function by fixing the input data.
    # This function insert missing stocks for each candle. We had this issue that the volume
    # was missing for the whole data so this function makes sure that this won"t happen anymore.
    # This function is ran for each candle later and fills the missing stocks for that candles.

    df = df.reindex(columns=col_list).fillna(np.nan)
    return df

def data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False):
    # This function does three things.
    # 1- Making the candles datetimeindex pandas and clean them by inserting missing dates and stocks for each candles.
    # 2- Creates weekly dataset based on the desired day of week(only for weekly period).
    # 3- Concatenate all the datasets together.

    day_list = day_generator(dow)
    # 1- Making the candles datetimeindex pandas and clean them by inserting missing dates and stocks for each candles.
    if candle_type == 0:
        # use pandas df to not worry about row,col orders
        main_open = df.pivot_table(index=df.index, columns="ticker", values="open_multiple", aggfunc="first",
                                   dropna=False)
        # check for any completely missing stock names and fill in so that concat errors don"t occur
        main_open = insert_missing_dates(main_open)
        main_high = df.pivot_table(index=df.index, columns="ticker", values="high_multiple", aggfunc="first",dropna=False)
        main_high = insert_missing_dates(main_high)
        main_low = df.pivot_table(index=df.index, columns="ticker", values="low_multiple", aggfunc="first",dropna=False)
        main_low = insert_missing_dates(main_low)
        main_close = df.pivot_table(index=df.index, columns="ticker", values="close_multiple", aggfunc="first",dropna=False)
        main_close = insert_missing_dates(main_close)
        main_volume = df.pivot_table(index=df.index, columns="ticker", values="volume_adj", aggfunc="first", dropna=False)
        main_volume = insert_missing_dates(main_volume)

        # Fixing the stocks with missing values for any of the candles for the whole period
        col_list = (main_open.append([main_high, main_low, main_close, main_volume], sort=True)).columns.tolist()
        main_open = insert_missing_stocks(main_open, col_list)
        main_high = insert_missing_stocks(main_high, col_list)
        main_low = insert_missing_stocks(main_low, col_list)
        main_close = insert_missing_stocks(main_close, col_list)
        main_volume = insert_missing_stocks(main_volume, col_list)
        stocks_list = col_list

        main_open = main_open[main_open.index.dayofweek < 5]
        main_high = main_high[main_high.index.dayofweek < 5]
        main_low = main_low[main_low.index.dayofweek < 5]
        main_close = main_close[main_close.index.dayofweek < 5]
        main_volume = main_volume[main_volume.index.dayofweek < 5]

        datasets = [main_open, main_high, main_low, main_close, main_volume]
        # a = set(main_low.columns).intersection(set(main_close.columns))
        # b = set(main_low.columns).union(set(main_close.columns))
        # b - set(main_low.columns)
    elif candle_type == 1:
        main_open = df.pivot_table(index=df.index, columns="ticker", values="open_multiple", aggfunc="first",
                                   dropna=False)
        main_open = insert_missing_dates(main_open)
        datasets = [main_open]
        stocks_list = main_open.columns


    elif candle_type == 2:
        main_high = df.pivot_table(index=df.index, columns="ticker", values="high_multiple", aggfunc="first",
                                   dropna=False)
        main_high = insert_missing_dates(main_high)
        datasets = [main_high]
        stocks_list = main_high.columns

    elif candle_type == 3:
        main_low = df.pivot_table(index=df.index, columns="ticker", values="low_multiple", aggfunc="first",
                                  dropna=False)
        main_low = insert_missing_dates(main_low)
        datasets = [main_low]
        stocks_list = main_low.columns

    elif candle_type == 4:
        if not tac_flag:
            main_close = df.pivot_table(index=df.index, columns="ticker", values="close_multiple", aggfunc="first",
                                        dropna=False)
        else:
            main_close = df.pivot_table(index=df.index, columns="ticker", values="tac_price", aggfunc="first",
                                        dropna=False)

        main_close = insert_missing_dates(main_close)
        datasets = [main_close]
        stocks_list = main_close.columns

    elif candle_type == 5:
        main_volume = df.pivot_table(index=df.index, columns="ticker", values="volume", aggfunc="first", dropna=False)
        main_volume = insert_missing_dates(main_volume)
        datasets = [main_volume]
    else:
        sys.exit("Unknown candle_type. It should be either \"0\" ->all or \"1\" ->open, or \"2\" ->high, or \"3\" ->low, or \"4\" ->close, or \"5\" ->volume!")
    # 2- Creates weekly dataset based on the desired day of week(only for weekly period).
    # 3- Concatenate all the datasets together.
    if data_period == 0:
        for i in range(len(day_list)):  # should start on the dow specified default [1,2,3,4,5]
            if i == 0:
                for j in range(len(datasets)):
                    if j == 0:
                        l2 = datasets[j][datasets[j].index.weekday + 1 == day_list[i]].values
                        l2 = l2[..., np.newaxis]
                    else:
                        l2_t = datasets[j][datasets[j].index.weekday + 1 == day_list[i]].values
                        l2_t = l2_t[..., np.newaxis]
                        l2 = np.concatenate((l2, l2_t), axis=2)
                l = l2[..., np.newaxis]
            else:
                for j in range(len(datasets)):
                    if j == 0:
                        l2 = datasets[j][datasets[j].index.weekday + 1 == day_list[i]].values
                        l2 = l2[..., np.newaxis]
                    else:
                        l2_t = datasets[j][datasets[j].index.weekday + 1 == day_list[i]].values
                        l2_t = l2_t[..., np.newaxis]
                        l2 = np.concatenate((l2, l2_t), axis=2)
                l_t = l2[..., np.newaxis]
                l = np.concatenate((l, l_t), axis=3)

    elif data_period == 1:
        for j in range(len(datasets)):
            if j == 0:
                l = datasets[j].values
                l = l[..., np.newaxis]
            else:
                l_t = datasets[j].values
                l_t = l_t[..., np.newaxis]
                l = np.concatenate((l, l_t), axis=2)
    else:
        sys.exit("Unknown data period. It should be either \"0\" ->weekly or \"1\" -> daily!")

    return l, stocks_list


def returns_(df, tac_flag=False):
    if not tac_flag:
        # This function creates return dataset from given dataframe.
        aa = data_merging_clipping(df, args) + 1
        aa, stocks_list = data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False)
        aa += 1
    else:
        aa = data_merging_clipping(df, args)
        aa, stocks_list = data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False)
        

    if args.data_period == 1:
        bb = aa - 1
        bb = np.reshape(bb, (bb.shape[0], bb.shape[1]))
        return bb
    elif args.data_period == 0:
        if not args.tac_flag:
            bb = np.nanprod(aa, axis=3) - 1
        else:
            bb = np.nanmean(aa, axis=3)
        bb = np.reshape(bb, (bb.shape[0], bb.shape[1]))
        bb[bb == 0] = np.nan
        return bb

    else:
        sys.exit("Unknown data period. It should be either \"0\" ->weekly or \"1\" -> daily!")


def cluster_(df, args, cluster_no):
    # This function clusters the data. If used it will be replaced by returns.
    aa = data_merging_clipping(df, args) + 1
    aa, stocks_list = data_merging_clipping(df, dow, candle_type, data_period, tac_flag=False)
    aa += 1
    if args.data_period == 1:
        bb = aa.reshape(-1, aa.shape[-1])

        # We have to fill all the nans with 1(for multiples) since Kmeans can"t work with the data that has nans.
        bb = np.nan_to_num(bb, 1)

        clustering_model = KMeans(n_clusters=cluster_no).fit(bb)
        clustering = clustering_model.predict(bb)
        cluster_centers = np.zeros((cluster_no, bb.shape[-1]))

        # Finding cluster centers for later inference.
        for i in range(cluster_no):
            cluster_centers[i, :] = np.mean(bb[clustering == i], axis=0)

        if args.save_cluster_files:
            dump(clustering_model, args.clustering_model_filename)
            np.save(args.cluster_centers_filename, cluster_centers)

        clustering = np.reshape(clustering, (aa.shape[0], aa.shape[1]))
        return clustering
    elif args.data_period == 0:
        bb = aa.reshape(-1, aa.shape[-1])

        # We have to fill all the nans with 1(for multiples) since Kmeans won"t work with data that has nans.
        bb = np.nan_to_num(bb, 1)

        # Adding mean of returns as the 6th column.
        mean_temp = np.nanmean(bb, axis=1)
        mean_temp = mean_temp[..., np.newaxis]
        bb = np.append(bb, mean_temp, axis=1)

        clustering_model = KMeans(n_clusters=cluster_no).fit(bb)
        clustering = clustering_model.predict(bb)
        cluster_centers = np.zeros((cluster_no, bb.shape[-1]))

        # Finding cluster centers for later inference.
        for i in range(cluster_no):
            cluster_centers[i, :] = np.mean(bb[clustering == i], axis=0)

        clustering = np.reshape(clustering, (aa.shape[0], aa.shape[1]))

        if args.save_cluster_files:
            np.save(args.cluster_centers_filename, cluster_centers)  # save
            dump(clustering_model, args.clustering_model_filename)

        return clustering
    else:
        sys.exit("Unknown data period. It should be either \"0\" ->weekly or \"1\" -> daily!")


def create_Y(df):
    # This function shift the returns by one period.
    e = np.empty_like(df)
    e[-1:] = np.nan
    e[:-1] = df[1:]
    return e


def dataset_prep(df, args):
    # This function slices the downloaded data for the desired date range.

    # Get rid of the warnings for dataset operations
    pd.options.mode.chained_assignment = None

    df["time"] = df["trading_day"].astype("datetime64[ns]")
    max(df.time).weekday()
    # This makes sure that we have enough data to make a whole week for the last week.
    if max(df.time) < args.end_date:
        if args.data_period == 0:
            args.end_date = args.end_date - Week(1)
    # if max(df.time).weekday() != args.end_date.weekday():
    #     if args.data_period == 0:
    #         args.end_date = args.end_date - Week(1)

    # Slicing the data for the desired time frame.
    datasett = df[(df.time >= args.start_date) & (df.time <= args.end_date)]

    # MAKE TIME INDEX PANDAS
    datasett = datasett.set_index("time")

    return datasett


def day_generator(i):
    # Creates days of week for desired start day. e.g. start day->Tue, week will be Tue, Wed,...Mon.
    ll = [None] * 5
    for j in range(5):
        if i + j + 1 > 5:
            ll[j] = i + j - 4
        else:
            ll[j] = i + j + 1
    return ll


# ******************************************************************************
# The following functions do exactly as their name suggests.
# They are designed to work only on candles dataset.

def normalize_candles_dlpm(nump, stdev_range=5):
    # This method is based on what stephen did before on DLPm and the formulas are given by him.

    # Since the Normalization is done on all the stocks at all he times at the same time, the calculated
    # constants(mins, max_min_difference) are saved for later use(in small dataset creation for inference).
    arr = np.copy(nump)
    real_min = np.zeros(arr.shape[2] - 1)
    mean_value = np.zeros(arr.shape[2] - 1)
    std_value = np.zeros(arr.shape[2] - 1)
    max_min_difference = np.zeros(arr.shape[2] - 1)
    mins = np.zeros(arr.shape[2] - 1)

    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            real_min[i] = np.nanmin(arr[:, :, i, :].flatten())
            mean_value[i] = np.nanmean(arr[:, :, i, :].flatten())
            std_value[i] = np.nanstd(arr[:, :, i, :].flatten())
            max_min_difference[i] = std_value[i] * stdev_range * 2
            mins[i] = mean_value[i] - (std_value[i] * stdev_range)
            if mins[i] < real_min[i]:
                mins[i] = real_min[i]

            arr[:, :, i, :] = 2 * (arr[:, :, i, :] - mins[i]) / max_min_difference[i] - 1
            arr[:, :, i, :] = np.minimum(np.maximum(arr[:, :, i, :], -1), 1)

    else:
        for i in range(arr.shape[2] - 1):
            real_min[i] = np.nanmin(arr[:, :, i].flatten())
            mean_value[i] = np.nanmean(arr[:, :, i].flatten())
            std_value[i] = np.nanstd(arr[:, :, i].flatten())
            max_min_difference[i] = std_value[i] * stdev_range * 2
            mins[i] = mean_value[i] - (std_value[i] * stdev_range)
            if mins[i] < real_min[i]: mins[i] = real_min[i]

            arr[:, :, i] = 2 * (arr[:, :, i] - mins[i]) / max_min_difference[i] - 1
            arr[:, :, i] = np.minimum(np.maximum(arr[:, :, i], -1), 1)

    return arr, mins, max_min_difference


def normalize_candles_dlpm_temp(nump, mins, max_min_difference):
    arr = np.copy(nump)

    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i, :] = 2 * (arr[:, :, i, :] - mins[i]) / max_min_difference[i] - 1
            arr[:, :, i, :] = np.minimum(np.maximum(arr[:, :, i, :], -1), 1)

    else:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i] = 2 * (arr[:, :, i] - mins[i]) / max_min_difference[i] - 1
            arr[:, :, i] = np.minimum(np.maximum(arr[:, :, i], -1), 1)

    return arr


def standardize_on_all_stocks_each_ohlcv(nump):
    arr = np.copy(nump)
    mean_value = np.zeros(arr.shape[2] - 1)
    std_value = np.zeros(arr.shape[2] - 1)
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            mean_value[i] = np.nanmean(arr[:, :, i, :].flatten())
            std_value[i] = np.nanstd(arr[:, :, i, :].flatten())
            arr[:, :, i, :] = (arr[:, :, i, :] - np.nanmean(arr[:, :, i, :].flatten())) / np.nanstd(
                arr[:, :, i, :].flatten())

    else:
        for i in range(arr.shape[2] - 1):
            mean_value[i] = np.nanmean(arr[:, :, i].flatten())
            std_value[i] = np.nanstd(arr[:, :, i].flatten())
            arr[:, :, i] = (arr[:, :, i] - np.nanmean(arr[:, :, i].flatten())) / np.nanstd(arr[:, :, i].flatten())

    return arr, mean_value, std_value


def standardize_on_all_stocks_each_ohlcv_temp_db(nump, mean_value, std_value):
    arr = np.copy(nump)
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i, :] = (arr[:, :, i, :] - mean_value[i]) / std_value[i]
    else:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i] = (arr[:, :, i] - mean_value[i]) / std_value[i]
    return arr


def normalize_on_all_stocks_each_ohlcv(nump, int1=0, int2=1):
    arr = np.copy(nump)
    for i in range(arr.shape[2] - 1):
        arr[:, :, i, :] = (int2 - int1) * (arr[:, :, i, :] - np.nanmin(arr[:, :, i, :].flatten())) / (
                np.nanmax(arr[:, :, i, :].flatten()) - np.nanmin(arr[:, :, i, :].flatten())) + int1
    return arr


def standardize_on_each_stocks_each_ohlcv(nump):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(arr.shape[2] - 1):
            arr[:, j, i, :] = (arr[:, j, i, :] - np.nanmean(arr[:, j, i, :].flatten())) / np.nanstd(
                arr[:, j, i, :].flatten())
    return arr


def normalize_on_each_stocks_each_ohlcv(nump, int1=0, int2=1):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(arr.shape[2] - 1):
            arr[:, j, i, :] = (int2 - int1) * (arr[:, j, i, :] - np.nanmin(arr[:, j, i, :].flatten())) / (
                    np.nanmax(arr[:, j, i, :].flatten()) - np.nanmin(arr[:, j, i, :].flatten())) + int1
    return arr


def remove_outliers_on_all_stocks_each_ohlcv(nump, int1=5):
    arr = np.copy(nump)
    a = np.zeros(arr.shape[2] - 1)
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            a[i] = np.nanstd(arr[:, :, i, :].flatten()) * int1
            arr[:, :, i, :] = np.clip(arr[:, :, i, :], -np.nanstd(arr[:, :, i, :].flatten()) * int1,
                                      np.nanstd(arr[:, :, i, :].flatten()) * int1)
    else:
        for i in range(arr.shape[2] - 1):
            a[i] = np.nanstd(arr[:, :, i].flatten()) * int1
            arr[:, :, i] = np.clip(arr[:, :, i], -np.nanstd(arr[:, :, i].flatten()) * int1,
                                   np.nanstd(arr[:, :, i].flatten()) * int1)

    return arr, a


def remove_outliers_on_all_stocks_each_ohlcv_temp_db(nump, remove_outliers_factor):
    arr = np.copy(nump)
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i, :] = np.clip(arr[:, :, i, :], -remove_outliers_factor[i], remove_outliers_factor[i])
    else:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i] = np.clip(arr[:, :, i], -remove_outliers_factor[i], remove_outliers_factor[i])
    return arr


def remove_outliers_on_each_stocks_each_ohlcv(nump, int1=5):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(arr.shape[2] - 1):
            arr[:, j, i, :] = np.clip(arr[:, j, i, :], -np.nanstd(arr[:, j, i, :].flatten()) * int1,
                                      np.nanstd(arr[:, j, i, :].flatten()) * int1)
    return arr


# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

def create_dataset(input_df, output_df, args):
    # This function creates the lookback data batches.
    dataX, dataY = [], []
    for i in range(len(input_df) - args.lookback - args.num_periods_to_predict + 1):
        a = input_df[i:i + args.lookback]
        b = output_df[i + args.lookback - 1:i + args.lookback + args.num_periods_to_predict - 1]
        if args.tomorrow:
            b = np.delete(b,0,0)

        if args.future:
            b = b + 1
            bb = np.nanprod(b, axis=0) -1
            bb = bb[np.newaxis, ...]
            b = bb

        # b[np.isnan(b)] = 0
        dataX.append(a)
        dataY.append(b)
    return np.array(dataX), np.array(dataY)


def equal_bin(input_df, num_bins):
    # Creates the binning for classification
    sep = (input_df.size / float(num_bins)) * np.arange(1, num_bins + 1)
    idx = sep.searchsorted(np.arange(input_df.size))
    return idx[input_df.argsort().argsort()]


def clean_categorize_ohlcv(input_df11, input_df22, output_dff, args, test_flag=False):
    # This function gets three dataframes. They are X1 (weekly returns), X2 (candles), Y (weekly return).
    # Like trainX1, trainX2  and trainY.
    if input_df11 is not None:
        input_df1 = np.copy(input_df11)
        input_df2 = np.copy(input_df22)
        output_df = np.copy(output_dff)
    else:
        input_df1 = None
        input_df2 = None
        output_df = None

    if input_df1 is not None:
        # Binning the output dataframe for classification.
        if not args.cluster_y:
            output_df = np.apply_along_axis(equal_bin, -1, output_df, args.num_bins)
        input_df1 = input_df1.transpose(0, 2, 1)

        if args.cnn_kernel_size != 0:
            if args.data_period == 0:
                input_df2 = np.transpose(input_df2, [0, 2, 1, 3, 4])
            else:
                input_df2 = np.transpose(input_df2, [0, 2, 1, 3])

        if not test_flag:
            input_df1 = input_df1.reshape(-1, args.lookback)
            if args.cnn_kernel_size != 0:
                if args.data_period == 0:
                    if args.candle_type_candles == 0:
                        input_df2 = input_df2.reshape(-1, args.lookback, 5, 5)
                    else:
                        input_df2 = input_df2.reshape(-1, args.lookback, 1, 5)
                else:
                    if args.candle_type_candles == 0:
                        input_df2 = input_df2.reshape(-1, args.lookback, 5)
                    else:
                        input_df2 = input_df2.reshape(-1, args.lookback, 1)

        output_df = output_df.transpose(0, 2, 1)
        if not test_flag:
            # This if statement removes the data batches that have number of nans higher than num_nans_to_skip.
            output_df = output_df.reshape(-1, output_df.shape[-1])
            input_df1 = input_df1[~np.isnan(output_df[:, -1]), :]
            if args.cnn_kernel_size != 0:
                input_df2 = input_df2[~np.isnan(output_df[:, -1]), :]
            output_df = output_df[~np.isnan(output_df[:, -1])]

            output_df = output_df[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            if args.cnn_kernel_size != 0:
                input_df2 = input_df2[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            input_df1 = input_df1[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]

        input_df1 = np.nan_to_num(input_df1)
        if args.cnn_kernel_size != 0:
            input_df2 = np.nan_to_num(input_df2)
        output_df = np.nan_to_num(output_df)

        output_df = output_df.astype(int)
        if not args.cluster_x:
            if args.use_capping:
                # Caps the input dataframe to desired values.(min and max) Only the weekly returns.
                input_df1 = np.clip(input_df1, -0.3, 0.5)

            if args.embedding_flag and args.cnn_kernel_size != 0:
                # We need to add 2 to the data so to make it positive for embeddings.
                input_df1 = input_df1 + 2

        # Round the input dataset for weekly returns.
        input_df1 = np.around(input_df1 * 10 ** args.accuracy_for_embedding, args.accuracy_for_embedding)
        input_df1 = input_df1.astype(int)

    return input_df1, input_df2, output_df


def create_train_test_valid_ohlcv(input_dataset11, input_dataset22, output_datasett, args, hypers):
    # This function prepares the train test validation datasets for the ohlcv model. So we will have two input
    # datasets and one output datasets as function"s inputs. The function returns trainX1(weekly returns), trainX2(
    # candles) trainY(weekly return), ....
    input_dataset1 = np.copy(input_dataset11)
    input_dataset2 = np.copy(input_dataset22)
    output_dataset = np.copy(output_datasett)

    lookback = args.lookback
    num_periods_to_predict = args.num_periods_to_predict
    valid_num = args.valid_num
    test_num = args.test_num

    # 1 should be deducted for final_production.
    # Input_df1 is week_ret which contains both x and y for all models.
    train_size = int(len(input_dataset1)) - valid_num - test_num
    valid_size = valid_num
    test_size = test_num

    # Creating trainX1, trainX2 and trainY datasets
    trainX1, trainY = create_dataset(input_dataset1[0:train_size], output_dataset[0:train_size], args)
    trainY[np.isnan(trainY)] = 0
    if args.cnn_kernel_size != 0:
        trainX2, _ = create_dataset(input_dataset2[0:train_size], output_dataset[0:train_size], args)
    else:
        trainX2 = None

    if args.train_num != 0:
        # The following line shuffle the whole datasets in the stocks direction.
        trainX1T, trainY1, trainX2T = shuffle(trainX1, trainY, trainX2, axis=2)

    # most likely we will just use args.stock_percentage = 0.1
    if args.stock_percentage > 0.5 and args.stock_percentage != 1:
        args.stock_percentage = 0.5
    if args.stock_percentage < 0.1 and args.stock_percentage != 1:
        args.stock_percentage = 0.1

    if args.train_num != 0:
        temp = int(trainX1T.shape[2] * args.stock_percentage)

        if args.stock_percentage != 1:
            # This if statement uses only a percentage of input data to use. and sets each of them to a gpu.
            if args.gpu_number == 0:
                trainX1 = trainX1T[:, :, :temp]
                if args.cnn_kernel_size != 0:
                    trainX2 = trainX2T[:, :, :temp]
                else:
                    trainX2 = None
                trainY = trainY1[:, :, :temp]
            else:
                trainX1 = trainX1T[:, :, temp:2 * temp]
                if args.cnn_kernel_size != 0:
                    trainX2 = trainX2T[:, :, temp:2 * temp]
                else:
                    trainX2 = None
                trainY = trainY1[:, :, temp:2 * temp]

    if valid_num != 0:
        # Creating validX1, validX2 and validY datasets
        validX1, validY = create_dataset(
            input_dataset1[train_size - lookback - num_periods_to_predict + 1:train_size + valid_size],
            output_dataset[train_size - lookback - num_periods_to_predict + 1:train_size + valid_size], args)
        validY[np.isnan(validY)] = 0

        if args.cnn_kernel_size != 0:
            validX2, _ = create_dataset(
                input_dataset2[train_size - lookback - num_periods_to_predict + 1:train_size + valid_size],
                output_dataset[
                train_size - lookback - num_periods_to_predict + 1:train_size + valid_size], args)
        else:
            validX2 = None
    else:
        validX1, validX2, validY = None, None, None

    if test_num != 0:
        # Creating testX1, testX2 and testY datasets
        testX1, testY = create_dataset(
            input_dataset1[
            train_size + valid_size - lookback - num_periods_to_predict + 1:train_size + valid_size + test_size],
            output_dataset[
            train_size + valid_size - lookback - num_periods_to_predict + 1:train_size + valid_size + test_size], args)

        args.mask_Y = ~np.isnan(testY)
        args.testY_not_nan = np.sum(args.mask_Y)
        args.mask_X = ~((np.sum(np.isnan(testX1[:, :]), axis=1)) > args.num_nans_to_skip)
        args.testX_not_nan = np.sum(args.mask_X)
        # testY[np.isnan(testY)] = 0
        args.test_mask = args.mask_X & args.mask_Y
        args.test_mask_sum = np.sum(args.test_mask)
        testY[np.isnan(testY)] = 0

        if args.cnn_kernel_size != 0:
            testX2, _ = create_dataset(
                input_dataset2[
                train_size + valid_size - lookback - num_periods_to_predict + 1:train_size + valid_size + test_size],
                output_dataset[
                train_size + valid_size - lookback - num_periods_to_predict + 1:train_size + valid_size + test_size],
                args)
        else:
            testX2 = None

    else:
        testX1, testX2, testY = None, None, None

    # final_prediction is the last batch of the dataset. In other words it is the batch that will be used to predict
    # the next week return(from now) which means we don"t have outputs for it.
    #
    # 1 should be deducted because of the input y.
    final_prediction1 = input_dataset1[len(input_dataset1) - lookback:len(input_dataset1)]
    final_prediction1[np.isnan(final_prediction1)] = 0
    final_prediction1 = final_prediction1[np.newaxis, ...]
    final_prediction1_y_temp = np.zeros((1, num_periods_to_predict, final_prediction1.shape[2]))

    if args.cnn_kernel_size != 0:
        final_prediction2 = input_dataset2[len(input_dataset2) - lookback:len(input_dataset2)]
        final_prediction2[np.isnan(final_prediction2)] = 0
        final_prediction2 = final_prediction2[np.newaxis, ...]
    else:
        final_prediction2 = None

    # Clean from nans and categorize the outputs for classification.
    if args.train_num != 0:
        trainX1, trainX2, trainY = clean_categorize_ohlcv(trainX1, trainX2, trainY, args)
        validX1, validX2, validY = clean_categorize_ohlcv(validX1, validX2, validY, args)

    testX1, testX2, testY = clean_categorize_ohlcv(testX1, testX2, testY, args, True)
    final_prediction1, final_prediction2, _ = clean_categorize_ohlcv(final_prediction1, final_prediction2,
                                                                     final_prediction1_y_temp, args, True)
    final_prediction1 = np.squeeze(final_prediction1)
    final_prediction2 = np.squeeze(final_prediction2)
    # Finding the unique number of returns and outputs for embedding.
    if validX1 is not None:
        temp0X = validX1
        temp0Y = validY
    else:
        temp0X = trainX1
        temp0Y = trainY

    if args.train_num != 0:
        args.unique_num_of_returns = int(np.nanmax(np.concatenate((trainX1, temp0X))) + 1)
        args.unique_num_of_outputs = int(np.nanmax(np.concatenate((trainY, temp0Y))) + 1)

    if args.model_type == 0 and not args.embedding_flag:
        trainX1 = trainX1.reshape(-1, trainX1.shape[1], 1)
        final_prediction1 = final_prediction1.reshape(-1, final_prediction1.shape[1], 1)
        if validX1 is not None:
            validX1 = validX1.reshape(-1, validX1.shape[1], 1)
    elif args.model_type != 0:
        if args.train_num != 0:
            trainX1 = trainX1.reshape(-1, trainX1.shape[1], 1)
        final_prediction1 = final_prediction1.reshape(-1, final_prediction1.shape[1], 1)
        if validX1 is not None:
            validX1 = validX1.reshape(-1, validX1.shape[1], 1)

    if args.data_period != 0 and args.cnn_kernel_size != 0:
        if args.train_num != 0:
            trainX2 = trainX2[..., np.newaxis]
        final_prediction2 = final_prediction2[..., np.newaxis]
        if validX1 is not None:
            validX2 = validX2[..., np.newaxis]
        if testX1 is not None:
            testX2 = testX2[..., np.newaxis]

    if args.cnn_kernel_size != 0:
        if args.train_num != 0:
            trainX2 = trainX2[..., np.newaxis]
        final_prediction2 = final_prediction2[..., np.newaxis]
        if validX1 is not None:
            validX2 = validX2[..., np.newaxis]
        if testX1 is not None:
            testX2 = testX2[..., np.newaxis]

    return trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1, final_prediction2


def shuffle(x, y, z, axis=0):
    # This function shuffles the input datasets on desired axis
    n_axis = len(x.shape)
    t = np.arange(n_axis)
    t[0] = axis
    t[axis] = 0

    if z is not None:
        n_axis2 = len(z.shape)
        t2 = np.arange(n_axis2)
        t2[0] = axis
        t2[axis] = 0
        zt = np.transpose(z.copy(), t2)

    xt = np.transpose(x.copy(), t)
    yt = np.transpose(y.copy(), t)

    rng_state = np.random.get_state()
    np.random.shuffle(xt)
    np.random.set_state(rng_state)
    np.random.shuffle(yt)
    if z is not None:
        np.random.set_state(rng_state)
        np.random.shuffle(zt)
        shuffled_z = np.transpose(zt, t2)
    shuffled_x = np.transpose(xt, t)
    shuffled_y = np.transpose(yt, t)
    if z is None:
        shuffled_z = None
    return shuffled_x, shuffled_y, shuffled_z


def test_data_reshape(df1, df2, args, hypers):
    df1 = df1[..., np.newaxis]
    return df1, df2

def create_tac_prices(full_df, args):
    full_df["trading_day"] = pd.to_datetime(full_df["trading_day"])
    full_df = full_df.infer_objects()

    close_df = full_df.pivot_table(
        index="trading_day", columns="ticker", values="close_price", aggfunc="first", dropna=False)
    close_multiple_df = full_df.pivot_table(
        index="trading_day", columns="ticker", values="close_multiple", aggfunc="first", dropna=False)

    close_multiple_df = close_multiple_df.reindex(index=close_df.index)

    close_df = close_df[close_df.index.dayofweek < 5]
    close_multiple_df = close_multiple_df[close_multiple_df.index.dayofweek < 5]

    close_multiple_df = close_multiple_df.ffill().bfill()
    close_df = close_df.ffill().bfill()

    tac_df = pd.DataFrame().reindex_like(close_df)

    tac_df.iloc[-1, :] = close_df.iloc[-1, :]

    for i in range(len(tac_df) - 2, -1, -1):
        tac_df.iloc[i, :] = tac_df.iloc[i + 1, :] / close_multiple_df.iloc[i + 1, :]

    tac_df["trading_day"] = tac_df.index
    tac_df = tac_df.melt(id_vars="trading_day", var_name="ticker", value_name="tac_price")
    close_multiple_df.iloc[-1,0]
    return tac_df


def create_rv_df(full_df2, indices_df2):
    full_df = full_df2.copy()
    indices_df = indices_df2.copy()
    indices_df.loc[indices_df["index"]=="0#.ETF", "index"] = "0#.SPX"
    full_df["trading_day"] = pd.to_datetime(full_df["trading_day"])
    indices_list = [".HSLI", ".TWII", ".SXXGR", ".SPX", ".KS200", ".FTSE", ".STI", ".CSI300", ".N225"]
    full_df = full_df.merge(indices_df, on="ticker", how="left")

    full_df_indices = full_df.loc[full_df.ticker.isin(indices_list),["trading_day", "index", "close_multiple","open_multiple", "high_multiple", "low_multiple"]]
    full_df_indices.rename(columns={"open_multiple": "open_multiple_ind", "high_multiple": "high_multiple_ind","low_multiple": "low_multiple_ind","close_multiple": "close_multiple_ind"}, inplace=True)

    full_df = full_df.merge(full_df_indices, on=["trading_day", "index"], how="left")

    full_df[["open_multiple_ind", "high_multiple_ind", "low_multiple_ind", "close_multiple_ind"]] = full_df[["open_multiple_ind", "high_multiple_ind", "low_multiple_ind", "close_multiple_ind"]].fillna(value=1)

    full_df.open_multiple = full_df.open_multiple - full_df.open_multiple_ind + 1
    full_df.high_multiple = full_df.high_multiple - full_df.high_multiple_ind + 1
    full_df.low_multiple = full_df.low_multiple - full_df.low_multiple_ind + 1
    full_df.close_multiple = full_df.close_multiple - full_df.close_multiple_ind + 1

    full_df = full_df.infer_objects()

    return full_df