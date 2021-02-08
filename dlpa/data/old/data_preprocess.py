from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def dataset_prep(df, args):
    # This function prepares the downloaded data for using in the models.
    # We have three main datasets:
    # 1 - Candles for each week
    # 2 - Weekly return (percentage) for each week.
    # 3 - Weekly forward returns (the return of the next week).
    start_year = args.start_year
    start_week = args.start_week
    end_year = args.end_year
    end_week = args.end_week

    # Get rid of the warnings for dataset operations
    pd.options.mode.chained_assignment = None

    # Slicing the data for the desired time frame.
    dataset = df[
        ((df['year'] == start_year) & (df['week_num'] >= start_week)) | (
                (df['year'] > start_year) & (df['year'] < end_year)) | (
                (df['year'] == end_year) & (df['week_num'] <= end_week))]

    # MAKE TIME INDEX PANDAS
    dataset['date_str'] = dataset.year.map(str) + '-' + dataset.week_num.map(str) + '1'
    dataset['date_date'] = pd.to_datetime(dataset.date_str, format="%Y-%W%w")
    dataset = dataset.set_index('date_date')

    # Creates a pivot table for each of the main datasets.
    # This means that we will have a dataframe which its indices are time and its columns are stocks.
    # The missing data will be filled with nans.
    main_candles_X1 = dataset.pivot_table(index=dataset.index, columns='ticker', values='candles', aggfunc='first')
    main_week_ret_X2 = dataset.pivot_table(index=dataset.index, columns='ticker', values='week_ret_per')
    main_forward_ret_Y = dataset.pivot_table(index=dataset.index, columns='ticker', values='forward_returns')

    # Shifting forward returns by one week, so each datetime row has its own return now(it is not forward anymore)
    main_forward_ret_Y = main_forward_ret_Y.append(
        pd.DataFrame(index=main_forward_ret_Y.tail(1).index + timedelta(weeks=1)), sort=True)
    main_forward_ret_Y = main_forward_ret_Y.shift(1)

    # Since after shifting the first row will be removed we copy it from week_ret_per/100
    main_forward_ret_Y.iloc[0] = main_week_ret_X2.iloc[0] / 100

    # Saving the stocks_list for later outputing to AWS
    args.stocks_list = list(main_forward_ret_Y.columns.values)

    # Saving the year and week number of the test dataset for reporting to AWS
    end_y = main_forward_ret_Y.index[-1].year
    end_w = main_forward_ret_Y.index[-1].week

    tt1 = year_week(end_y, end_w)

    # We have to deduct 2 from dat. 1 for final_production and 1 for the fact that forward return is one week ahead.
    dd1 = timedelta(days=7 * (args.test_num + 1))
    uu2 = tt1 - dd1
    args.end_y, args.end_w = datetime.date(uu2).isocalendar()[0:2]

    # Fill nan values with [5,5] lists to math other candles.
    a = np.empty(shape=(5, 5))
    a[:] = np.nan
    a = a.tolist()
    for column in main_candles_X1.columns:
        main_candles_X1[column] = main_candles_X1[column].apply(lambda d: d if isinstance(d, list) else a)

    # Convert datasets to numpy array for data processing(standardization , normalization,...)
    candles_X1 = np.stack(np.array(main_candles_X1.values).tolist())
    week_ret__per_X2 = np.stack(np.array(main_week_ret_X2))
    week_ret_Y = np.stack(np.array(main_forward_ret_Y))

    # The dates are sorted from beginning of the time to the most recent

    # If I wanted to reverse the timeindex dataframe.
    # main_candles_X1=main_candles_X1.iloc[::-1]
    # main_week_ret_X2=main_week_ret_X2.iloc[::-1]
    # main_forward_ret_Y=main_forward_ret_Y.iloc[::-1]
    return candles_X1, week_ret__per_X2, week_ret_Y


# ******************************************************************************
# The following functions do exactly as their name suggests.
# They are designed to work only on candles dataset.
def standardize_on_all_stocks_each_ohlcv(nump):
    arr = np.copy(nump)
    for i in range(4):
        arr[:, :, i, :] = (arr[:, :, i, :] - np.nanmean(arr[:, :, i, :].flatten())) / np.nanstd(
            arr[:, :, i, :].flatten())
    return arr


def normalize_on_all_stocks_each_ohlcv(nump, int1=0, int2=1):
    arr = np.copy(nump)
    for i in range(4):
        arr[:, :, i, :] = (int2 - int1) * (arr[:, :, i, :] - np.nanmin(arr[:, :, i, :].flatten())) / (
                np.nanmax(arr[:, :, i, :].flatten()) - np.nanmin(arr[:, :, i, :].flatten())) + int1
    return arr


def standardize_on_each_stocks_each_ohlcv(nump):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(4):
            arr[:, j, i, :] = (arr[:, j, i, :] - np.nanmean(arr[:, j, i, :].flatten())) / np.nanstd(
                arr[:, j, i, :].flatten())
    return arr


def normalize_on_each_stocks_each_ohlcv(nump, int1=0, int2=1):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(4):
            arr[:, j, i, :] = (int2 - int1) * (arr[:, j, i, :] - np.nanmin(arr[:, j, i, :].flatten())) / (
                    np.nanmax(arr[:, j, i, :].flatten()) - np.nanmin(arr[:, j, i, :].flatten())) + int1
    return arr


def remove_outliers_on_all_stocks_each_ohlcv(nump, int1=5):
    arr = np.copy(nump)
    for i in range(4):
        arr[:, :, i, :] = np.clip(arr[:, :, i, :], -np.nanstd(arr[:, :, i, :].flatten()) * int1,
                                  np.nanstd(arr[:, :, i, :].flatten()) * int1)
    return arr


def remove_outliers_on_each_stocks_each_ohlcv(nump, int1=5):
    arr = np.copy(nump)
    for j in range(arr.shape[1]):
        for i in range(4):
            arr[:, j, i, :] = np.clip(arr[:, j, i, :], -np.nanstd(arr[:, j, i, :].flatten()) * int1,
                                      np.nanstd(arr[:, j, i, :].flatten()) * int1)
    return arr


# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

def create_dataset(input_df, output_df, args):
    # This function creates the lookback data batches.
    dataX, dataY = [], []
    for i in range(len(input_df) - args.lookback - args.num_weeks_to_predict + 1):
        a = input_df[i:i + args.lookback]
        b = output_df[i + args.lookback:i + args.lookback + args.num_weeks_to_predict]
        b[np.isnan(b)] = 0
        dataX.append(a)
        dataY.append(b)
    return np.array(dataX), np.array(dataY)


def equal_bin(input_df, num_bins):
    # Creates the binning for classification
    sep = (input_df.size / float(num_bins)) * np.arange(1, num_bins + 1)
    idx = sep.searchsorted(np.arange(input_df.size))
    return idx[input_df.argsort().argsort()]


def clean_categorize(input_df, output_df, args, test_flag=False):
    # This function gets two dataframes. They are X (weekly returns) and Y (weekly return).
    # Like trainX1  and trainY.
    # This function is used only for weekly returns(dlpa_weekly).

    if input_df is not None:
        # Binning the output dataframe for classification.
        output_df = np.apply_along_axis(equal_bin, -1, output_df, args.num_bins)
        input_df = input_df.transpose(0, 2, 1)
        if not test_flag:
            input_df = input_df.reshape(-1, input_df.shape[-1])

        output_df = output_df.transpose(0, 2, 1)
        if not test_flag:
            # This if statment removes the data batches that have number of nans higher than num_nans_to_skip.
            output_df = output_df.reshape(-1, output_df.shape[-1])
            input_df = input_df[~np.isnan(output_df[:, -1]), :]
            output_df = output_df[~np.isnan(output_df[:, -1])]
            output_df = output_df[np.sum(np.isnan(input_df), axis=1) <= args.num_nans_to_skip, :]
            input_df = input_df[np.sum(np.isnan(input_df), axis=1) <= args.num_nans_to_skip, :]

        # This will set all the nans survived from the last if satement to zeros.
        input_df = np.nan_to_num(input_df)
        output_df = np.nan_to_num(output_df)

        output_df = output_df.astype(int)

        if args.use_capping:
            # Caps the input dataframe to desired values.(min and max)
            input_df = np.clip(input_df, -0.3, 0.5)

        if args.embedding_flag:
            # We need to add 2 to the data so to make it positive for embeddings.
            input_df = input_df + 2

        # Round the input dataset for weekly returns.
        input_df = np.around(input_df * 10 ** args.accuracy_for_embedding, args.accuracy_for_embedding)
        input_df = input_df.astype(int)

    return input_df, output_df


def clean_categorize_ohlcv(input_df1, input_df2, output_df, args, test_flag=False):
    # This function gets three dataframes. They are X1 (weekly returns), X2 (candles), Y (weekly return).
    # Like trainX1, trainX2  and trainY.
    # This function is used only for weekly returns and candles(dlpa_weekly_ohclv).

    if input_df1 is not None:
        # Binning the output dataframe for classification.
        output_df = np.apply_along_axis(equal_bin, -1, output_df, args.num_bins)
        input_df1 = input_df1.transpose(0, 2, 1)
        input_df2 = np.transpose(input_df2, [0, 2, 1, 3, 4])

        if not test_flag:
            input_df1 = input_df1.reshape(-1, args.lookback)
            input_df2 = input_df2.reshape(-1, args.lookback, 5, 5)

        output_df = output_df.transpose(0, 2, 1)
        if not test_flag:
            # This if statment removes the data batches that have number of nans higher than num_nans_to_skip.
            output_df = output_df.reshape(-1, output_df.shape[-1])
            input_df1 = input_df1[~np.isnan(output_df[:, -1]), :]
            input_df2 = input_df2[~np.isnan(output_df[:, -1]), :]
            output_df = output_df[~np.isnan(output_df[:, -1])]

            output_df = output_df[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            input_df2 = input_df2[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]
            input_df1 = input_df1[np.sum(np.isnan(input_df1), axis=1) <= args.num_nans_to_skip, :]

        # This will set all the nans survived from the last if satement to zeros.
        input_df1 = np.nan_to_num(input_df1)
        input_df2 = np.nan_to_num(input_df2)
        output_df = np.nan_to_num(output_df)

        output_df = output_df.astype(int)
        if args.use_capping:
            # Caps the input dataframe to desired values.(min and max) Only the weekly returns.
            input_df1 = np.clip(input_df1, -0.3, 0.5)

        if args.embedding_flag:
            # We need to add 2 to the data so to make it positive for embeddings.
            input_df1 = input_df1 + 2

        # Round the input dataset for weekly returns.
        input_df1 = np.around(input_df1 * 10 ** args.accuracy_for_embedding, args.accuracy_for_embedding)
        input_df1 = input_df1.astype(int)

    return input_df1, input_df2, output_df


def create_train_test_valid_ohlcv(input_dataset1, input_dataset2, output_dataset, args):
    # This function prepares the train test validation datasets for the ohlcv model. So we will have two input
    # datasets and one output datasets as function's inputs. The function returns trainX1(weekly returns), trainX2(
    # candles) trainY(weekly return), ....
    lookback = args.lookback
    num_weeks_to_predict = args.num_weeks_to_predict
    valid_num = args.valid_num
    test_num = args.test_num

    # 2 should be deducted. one for final_production and one for the fact that we have the y embedded inside x.
    # Input_df1 is week_ret which contains both x and y for all models.
    train_size = int(len(input_dataset1)) - valid_num - test_num - 2
    valid_size = valid_num
    test_size = test_num

    # Creating trainX1, trainX2 and trainY datasets
    trainX1, trainY = create_dataset(input_dataset1[0:train_size], output_dataset[0:train_size], args)
    trainX2, _ = create_dataset(input_dataset2[0:train_size], output_dataset[0:train_size], args)

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
            trainX2 = trainX2T[:, :, :temp]
            trainY = trainY1[:, :, :temp]
        else:
            trainX1 = trainX1T[:, :, temp:2 * temp]
            trainX2 = trainX2T[:, :, temp:2 * temp]
            trainY = trainY1[:, :, temp:2 * temp]

    if valid_num != 0:
        # Creating validX1, validX2 and validY datasets
        validX1, validY = create_dataset(
            input_dataset1[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size],
            output_dataset[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size], args)
        validX2, _ = create_dataset(
            input_dataset2[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size],
            output_dataset[
            train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size], args)
    else:
        validX1, validX2, validY = None, None, None

    if test_num != 0:
        # Creating testX1, testX2 and testY datasets
        testX1, testY = create_dataset(
            input_dataset1[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size],
            output_dataset[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size], args)
        testX2, _ = create_dataset(
            input_dataset2[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size],
            output_dataset[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size], args)

    else:
        testX1, testX2, testY = None, None, None

    # final_prediction is the last batch of the dataset. In other words it is the batch that will be used to predict
    # the next week return(from now) which means we don't have outputs for it.
    #
    # 1 should be deducted because of the input y.
    final_prediction1 = input_dataset1[len(input_dataset1) - lookback - 1:len(input_dataset1) - 1]
    final_prediction1[np.isnan(final_prediction1)] = 0
    final_prediction1 = final_prediction1[np.newaxis, ...]
    final_prediction1_y_temp = np.zeros((1, num_weeks_to_predict, final_prediction1.shape[2]))

    final_prediction2 = input_dataset2[len(input_dataset2) - lookback:len(input_dataset2)]
    final_prediction2[np.isnan(final_prediction2)] = 0
    final_prediction2 = final_prediction2[np.newaxis, ...]

    # Clean from nans and categorize the outputs for classification.
    trainX1, trainX2, trainY = clean_categorize_ohlcv(trainX1, trainX2, trainY, args)
    validX1, validX2, validY = clean_categorize_ohlcv(validX1, validX2, validY, args)

    testX1, testX2, testY = clean_categorize_ohlcv(testX1, testX2, testY, args, True)
    final_prediction1, final_prediction2, _ = clean_categorize_ohlcv(final_prediction1, final_prediction2,
                                                                     final_prediction1_y_temp, args, True)

    # Finding the unique number of returns and outputs for embedding.
    if validX1 is not None:
        temp0X = validX1
        temp0Y = validY
    else:
        temp0X = trainX1
        temp0Y = trainY

    args.unique_num_of_returns = int(np.nanmax(np.concatenate((trainX1, temp0X))) + 1)
    args.unique_num_of_outputs = int(np.nanmax(np.concatenate((trainY, temp0Y))) + 1)

    return trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1, final_prediction2


def create_train_test_valid(input_dataset, output_dataset, args):
    # This function prepares the train test validation datasets for the weekly model. So we will have one input
    # dataset and one output datasets as function's inputs. The function returns trainX(weekly returns),
    # trainY(weekly return), ....
    lookback = args.lookback
    num_weeks_to_predict = args.num_weeks_to_predict
    valid_num = args.valid_num
    test_num = args.test_num

    # 2 should be deducted. one for final_production and one for the fact that we have the y embedded inside x.
    # Input_df1 is week_ret which contains both x and y for all models.
    train_size = int(len(input_dataset)) - valid_num - test_num - 2
    valid_size = valid_num
    test_size = test_num

    # Creating trainX and trainY datasets
    trainX, trainY = create_dataset(input_dataset[0:train_size], output_dataset[0:train_size], args)

    # The following line shuffle the whole datasets in the stocks direction.
    trainX1, trainY1, _ = shuffle(trainX, trainY, None, axis=2)

    if args.stock_percentage > 0.5 and args.stock_percentage != 1:
        args.stock_percentage = 0.5
    if args.stock_percentage < 0.1 and args.stock_percentage != 1:
        args.stock_percentage = 0.1
    temp = int(trainX.shape[2] * args.stock_percentage)
    if args.stock_percentage != 1:
        # This if statement uses only a percentage of input data to use. and sets each of them to a gpu.
        if args.gpu_number == 0:
            trainX = trainX1[:, :, :temp]
            trainY = trainY1[:, :, :temp]
        else:
            trainX = trainX1[:, :, temp:2 * temp]
            trainY = trainY1[:, :, temp:2 * temp]

    if valid_num != 0:
        # Creating validX and validY datasets
        validX, validY = create_dataset(
            input_dataset[train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size],
            output_dataset[
            train_size - lookback - num_weeks_to_predict + 1:train_size + valid_size], args)
    else:
        validX, validY = None, None

    if test_num != 0:
        # Creating testX1 and testY datasets
        testX, testY = create_dataset(
            input_dataset[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size],
            output_dataset[
            train_size + valid_size - lookback - num_weeks_to_predict + 1:train_size + valid_size + test_size], args)
    else:
        testX, testY = None, None

    # final_prediction is the last batch of the dataset. In other words it is the batch that will be used to predict
    # the next week return(from now) which means we don't have outputs for it.
    #
    # 1 should be deducted because of the input y.
    final_prediction = input_dataset[len(input_dataset) - lookback - 1:len(input_dataset) - 1]
    final_prediction[np.isnan(final_prediction)] = 0
    final_prediction = final_prediction[np.newaxis, ...]
    final_prediction_y_temp = np.zeros((1, num_weeks_to_predict, final_prediction.shape[2]))

    # Clean from nans and categorize the outputs for classification.
    trainX, trainY = clean_categorize(trainX, trainY, args)
    validX, validY = clean_categorize(validX, validY, args)

    testX, testY = clean_categorize(testX, testY, args, True)
    final_prediction, _ = clean_categorize(final_prediction, final_prediction_y_temp, args, True)

    # Finding the unique number of returns and outputs for embedding.
    if validX is not None:
        temp0X = validX
        temp0Y = validY
    else:
        temp0X = trainX
        temp0Y = trainY

    args.unique_num_of_returns = int(np.nanmax(np.concatenate((trainX, temp0X))) + 1)
    args.unique_num_of_outputs = int(np.nanmax(np.concatenate((trainY, temp0Y))) + 1)

    return trainX, trainY, validX, validY, testX, testY, final_prediction


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


def year_week(y, w):
    return datetime.strptime(f'{y} {w} 1', '%G %V %u')
