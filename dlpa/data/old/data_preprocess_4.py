import sys
from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import *


def dataset_p(df, args):
    if args.cnn_kernel_size != 0:
        args.data_type = 0  # 0->X_Candles or 1->X_returns or 2->Y_returns
        args.candle_type = args.candle_type_candles
        df_X0 = data_merging_clipping(df, args)
    else:
        df_X0 = None

    args.candle_type = args.candle_type_returnsX
    # args.data_type = 1  # 0->X_Candles or 1->X_returns or 2->Y_returns
    df_X1 = returns_(df, args)

    # args.data_type = 2  # 0->X_Candles or 1->X_returns or 2->Y_returns
    args.candle_type = args.candle_type_returnsY
    df_Y_temp = returns_(df, args)
    df_Y = create_Y(df_Y_temp)

    return df_X0, df_X1, df_Y


def insert_missing_dates(df, args):
    df = df.resample('B').sum()
    df[df == 0] = np.nan
    return df


def insert_missing_stocks(df, col_list):
    df = df.reindex(columns=col_list).fillna(np.nan)
    return df


def data_merging_clipping(df, args):
    day_list = day_generator(args.dow)
    if args.candle_type == 0:
        main_open = df.pivot_table(index=df.index, columns='ticker', values='open_multiply', aggfunc='first')
        main_open = insert_missing_dates(main_open, args)

        main_high = df.pivot_table(index=df.index, columns='ticker', values='high_multiply', aggfunc='first')
        main_high = insert_missing_dates(main_high, args)

        main_low = df.pivot_table(index=df.index, columns='ticker', values='low_multiply', aggfunc='first')
        main_low = insert_missing_dates(main_low, args)

        main_close = df.pivot_table(index=df.index, columns='ticker', values='close_multiply', aggfunc='first')
        main_close = insert_missing_dates(main_close, args)

        main_volume = df.pivot_table(index=df.index, columns='ticker', values='volume', aggfunc='first')
        main_volume = insert_missing_dates(main_volume, args)

        # Fixing the stocks with missing values for any of the candles for the whole period
        col_list = (main_open.append([main_high, main_low, main_close, main_volume], sort=True)).columns.tolist()
        main_open = insert_missing_stocks(main_open, col_list)
        main_high = insert_missing_stocks(main_high, col_list)
        main_low = insert_missing_stocks(main_low, col_list)
        main_close = insert_missing_stocks(main_close, col_list)
        main_volume = insert_missing_stocks(main_volume, col_list)
        args.stocks_list = col_list

        datasets = [main_open, main_high, main_low, main_close, main_volume]



    # train_cols = main_close1.columns
    # test_cols = main_close2.columns
    #
    # common_cols = train_cols.intersection(test_cols)
    # train_not_test = train_cols.difference(test_cols)
    elif args.candle_type == 1:
        main_open = df.pivot_table(index=df.index, columns='ticker', values='open_multiply', aggfunc='first')
        main_open = insert_missing_dates(main_open, args)
        datasets = [main_open]
        args.stocks_list = main_open.columns


    elif args.candle_type == 2:
        main_high = df.pivot_table(index=df.index, columns='ticker', values='high_multiply', aggfunc='first')
        main_high = insert_missing_dates(main_high, args)
        datasets = [main_high]
        args.stocks_list = main_high.columns

    elif args.candle_type == 3:
        main_low = df.pivot_table(index=df.index, columns='ticker', values='low_multiply', aggfunc='first')
        main_low = insert_missing_dates(main_low, args)
        datasets = [main_low]
        args.stocks_list = main_low.columns

    elif args.candle_type == 4:
        main_close = df.pivot_table(index=df.index, columns='ticker', values='close_multiply', aggfunc='first')
        main_close = insert_missing_dates(main_close, args)
        datasets = [main_close]
        args.stocks_list = main_close.columns

    elif args.candle_type == 5:
        main_volume = df.pivot_table(index=df.index, columns='ticker', values='volume', aggfunc='first')
        main_volume = insert_missing_dates(main_volume, args)
        datasets = [main_volume]
    else:
        sys.exit('Unknown candle_type. It should be either "0" ->all or "1" ->open, or "2" ->high, or "3" ->low, '
                 'or "4" ->close, or "5" ->volume!')
    # i=1
    if args.data_period == 0:
        for i in range(len(day_list)):
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

                # l33 = l.merge(l_t, how='outer')
    # l_t3=l_t.reshape((l_t.shape[0],l_t.shape[1]))
    #     ded = datasets[j][datasets[j].index.weekday == 4]
    elif args.data_period == 1:
        for j in range(len(datasets)):
            if j == 0:
                l = datasets[j].values
                l = l[..., np.newaxis]
            else:
                l_t = datasets[j].values
                l_t = l_t[..., np.newaxis]
                l = np.concatenate((l, l_t), axis=2)
    else:
        sys.exit('Unknown data period. It should be either "0" ->weekly or "1" -> daily!')

    return l


def returns_(df, args):
    aa = data_merging_clipping(df, args) + 1
    if args.data_period == 1:
        bb = aa - 1
        bb = np.reshape(bb, (bb.shape[0], bb.shape[1]))
        # bb[bb == 2] = np.nan
        return bb
    elif args.data_period == 0:
        bb = np.nanprod(aa, axis=3) - 1
        bb = np.reshape(bb, (bb.shape[0], bb.shape[1]))
        bb[bb == 0] = np.nan
        return bb
    else:
        sys.exit('Unknown data period. It should be either "0" ->weekly or "1" -> daily!')


def create_Y(df):
    # This function shift the returns by one period.
    e = np.empty_like(df)
    e[-1:] = np.nan
    e[:-1] = df[1:]
    return e


def get_start_end_date(df, dow):
    # This function finds the start and end date for weekly basis
    if dow == 1:
        start_date = df.index.min() + relativedelta(weekday=TU(1))
        end_date = df.index.max() + relativedelta(weekday=MO(-1))
    elif dow == 2:
        start_date = df.index.min() + relativedelta(weekday=WE(1))
        end_date = df.index.max() + relativedelta(weekday=TU(-1))
    elif dow == 3:
        start_date = df.index.min() + relativedelta(weekday=TH(1))
        end_date = df.index.max() + relativedelta(weekday=WE(-1))
    elif dow == 4:
        start_date = df.index.min() + relativedelta(weekday=FR(1))
        end_date = df.index.max() + relativedelta(weekday=TH(-1))
    elif dow == 5:
        start_date = df.index.min() + relativedelta(weekday=MO(1))
        end_date = df.index.max() + relativedelta(weekday=FR(-1))
    else:
        sys.exit(
            'Unknown day of week(dow). It should be either "1"->Mon or "2"->Tue or "3"->Wed or "4"->Thu or "5"->Fri!')
    return start_date, end_date


def mask_df(df, args):
    mask = (df.index >= args.start_date) & (df.index <= args.end_date)
    df = df.loc[mask]
    # for
    return df


def year_week_day(y, w, d):
    return datetime.strptime(f'{y} {w} {d}', '%G %V %u')


def dataset_prep(df, args):
    # This function prepares the downloaded data for using in the models.
    # We have three main datasets:
    # 1 - Candles for each week
    # 2 - Weekly return (percentage) for each week.
    # 3 - Weekly forward returns (the return of the next week).

    # Get rid of the warnings for dataset operations
    pd.options.mode.chained_assignment = None

    df['time'] = df['trading_day'].astype('datetime64[ns]')

    t1 = year_week_day(args.start_year, args.start_week, args.start_day)
    t2 = year_week_day(args.end_year, args.end_week, args.end_day)

    # if t1 < year_week_day(2009, 53, 5):
    #     df2 = pd.DataFrame([[df.iloc[0][0], year_week_day(2009, 53, 5), 0, 0, 0, 0, 0, year_week_day(2009, 53, 5)]],
    #                        columns=df.columns)
    #     df = df.append(df2)

    # Slicing the data for the desired time frame.
    datasett = df[(df.time >= t1) & (df.time <= t2)]

    # MAKE TIME INDEX PANDAS
    datasett = datasett.set_index('time')

    return datasett


def day_generator(i):
    ll = [None] * 5
    temp = 0
    for j in range(5):
        if i + j + 1 > 5:
            ll[j] = i + j - 4
        else:
            ll[j] = i + j + 1
    return ll


# ******************************************************************************
# The following functions do exactly as their name suggests.
# They are designed to work only on candles dataset.
def standardize_on_all_stocks_each_ohlcv(nump):
    arr = np.copy(nump)
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i, :] = (arr[:, :, i, :] - np.nanmean(arr[:, :, i, :].flatten())) / np.nanstd(
                arr[:, :, i, :].flatten())
    else:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i] = (arr[:, :, i] - np.nanmean(arr[:, :, i].flatten())) / np.nanstd(
                arr[:, :, i].flatten())
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
    if len(arr.shape) == 4:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i, :] = np.clip(arr[:, :, i, :], -np.nanstd(arr[:, :, i, :].flatten()) * int1,
                                      np.nanstd(arr[:, :, i, :].flatten()) * int1)
    else:
        for i in range(arr.shape[2] - 1):
            arr[:, :, i] = np.clip(arr[:, :, i], -np.nanstd(arr[:, :, i].flatten()) * int1,
                                   np.nanstd(arr[:, :, i].flatten()) * int1)
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
        b[np.isnan(b)] = 0
        dataX.append(a)
        dataY.append(b)
    return np.array(dataX), np.array(dataY)


def equal_bin(input_df, num_bins):
    # Creates the binning for classification
    sep = (input_df.size / float(num_bins)) * np.arange(1, num_bins + 1)
    idx = sep.searchsorted(np.arange(input_df.size))
    return idx[input_df.argsort().argsort()]


def clean_categorize_ohlcv(input_df1, input_df2, output_df, args, test_flag=False):
    # This function gets three dataframes. They are X1 (weekly returns), X2 (candles), Y (weekly return).
    # Like trainX1, trainX2  and trainY.
    # This function is used only for weekly returns and candles(dlpa_weekly_ohclv).

    if input_df1 is not None:
        # Binning the output dataframe for classification.
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
            # This if statment removes the data batches that have number of nans higher than num_nans_to_skip.
            output_df = output_df.reshape(-1, output_df.shape[-1])
            input_df1 = input_df1[~np.isnan(output_df[:, -1]), :]
            if args.cnn_kernel_size != 0:
                input_df2 = input_df2[~np.isnan(output_df[:, -1]), :]
            output_df = output_df[~np.isnan(output_df[:, -1])]

            output_df = output_df[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            if args.cnn_kernel_size != 0:
                input_df2 = input_df2[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            input_df1 = input_df1[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]

        # This will set all the nans survived from the last if satement to zeros.
        input_df1 = np.nan_to_num(input_df1)
        if args.cnn_kernel_size != 0:
            input_df2 = np.nan_to_num(input_df2)
        output_df = np.nan_to_num(output_df)

        output_df = output_df.astype(int)
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


def create_train_test_valid_ohlcv(input_dataset1, input_dataset2, output_dataset, args, hypers):
    # This function prepares the train test validation datasets for the ohlcv model. So we will have two input
    # datasets and one output datasets as function's inputs. The function returns trainX1(weekly returns), trainX2(
    # candles) trainY(weekly return), ....
    lookback = args.lookback
    num_periods_to_predict = args.num_periods_to_predict
    valid_num = args.valid_num
    test_num = args.test_num

    # 2 should be deducted. one for final_production and one for the fact that we have the y embedded inside x.
    # Input_df1 is week_ret which contains both x and y for all models.
    train_size = int(len(input_dataset1)) - valid_num - test_num
    valid_size = valid_num
    test_size = test_num

    # Creating trainX1, trainX2 and trainY datasets
    trainX1, trainY = create_dataset(input_dataset1[0:train_size], output_dataset[0:train_size], args)
    if args.cnn_kernel_size != 0:
        trainX2, _ = create_dataset(input_dataset2[0:train_size], output_dataset[0:train_size], args)
    else:
        trainX2 = None

    # The following line shuffle the whole datasets in the stocks direction.
    trainX1T, trainY1, trainX2T = shuffle(trainX1, trainY, trainX2, axis=2)

    if args.stock_percentage > 0.5 and args.stock_percentage != 1:
        args.stock_percentage = 0.5
    if args.stock_percentage < 0.1 and args.stock_percentage != 1:
        args.stock_percentage = 0.1
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
    # the next week return(from now) which means we don't have outputs for it.
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

    args.unique_num_of_returns = int(np.nanmax(np.concatenate((trainX1, temp0X))) + 1)
    args.unique_num_of_outputs = int(np.nanmax(np.concatenate((trainY, temp0Y))) + 1)

    if args.model_type == 0 and not args.embedding_flag:
        trainX1 = trainX1.reshape(-1, trainX1.shape[1], 1)
        final_prediction1 = final_prediction1.reshape(-1, final_prediction1.shape[1], 1)
        if validX1 is not None:
            validX1 = validX1.reshape(-1, validX1.shape[1], 1)
    elif args.model_type != 0:
        trainX1 = trainX1.reshape(-1, trainX1.shape[1], 1)
        final_prediction1 = final_prediction1.reshape(-1, final_prediction1.shape[1], 1)
        if validX1 is not None:
            validX1 = validX1.reshape(-1, validX1.shape[1], 1)

    if args.data_period != 0 and hypers['cnn_kernel_size'] != 0:
        trainX2 = trainX2[..., np.newaxis]
        final_prediction2 = final_prediction2[..., np.newaxis]
        if validX1 is not None:  # WHY validX1 is not None? why not validx2?
            validX2 = validX2[..., np.newaxis]
        if testX1 is not None:
            testX2 = testX2[..., np.newaxis]

    if hypers['cnn_kernel_size'] != 0:
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
    # if hypers['cnn_kernel_size'] != 0 and args.data_period != 0:
    #     df2 = df2[..., np.newaxis]
    return df1, df2
