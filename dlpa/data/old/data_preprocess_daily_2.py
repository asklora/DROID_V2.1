import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import *


class dataset:
    def __init__(self, df, args):
        self.dow = args.dow
        self.day_list = day_generator(self.dow)
        self.df = df
        self.period = 0  # 0 = weekly or 1 = daily
        self.start_date, self.end_date = self.get_start_end_date()
        self.X_cnn_candle = 'all'  # for X_candles
        self.X_return_candle = 'close'
        self.candle_type = 4  # 0->all 1->open 2->high 3->low 4->close 5->volume
        self.data_type = 0  # 0->X_Candles or 1->X_returns or 2->Y_returns
        self.shape_type = 0  # 0->Candles or 1->returns

    def produce(self):
        self.mask_df()
        self.data_merging_clipping(self.candle_type)

        if self.data_type == 0:
            df = self.returns_(self.candle_type)
        elif self.data_type == 1:
            df = self.returns_(self.candle_type)
        else:
            sys.exit('Unknown data type. It should be either "0"->X or "1"->Y!')
        aa = self.create_X_Y(df)
        return aa

    def data_merging_clipping(self, flag):
        if flag == 0:
            main_open = self.df.pivot_table(index=self.df.index, columns='ticker', values='open_multiply',
                                            aggfunc='first')
            main_high = self.df.pivot_table(index=self.df.index, columns='ticker', values='high_multiply',
                                            aggfunc='first')
            main_low = self.df.pivot_table(index=self.df.index, columns='ticker', values='low_multiply',
                                           aggfunc='first')
            main_close = self.df.pivot_table(index=self.df.index, columns='ticker', values='close_multiply',
                                             aggfunc='first')
            main_volume = self.df.pivot_table(index=self.df.index, columns='ticker', values='volume',
                                              aggfunc='first')
            datasets = [main_open, main_high, main_low, main_close, main_volume]

        if flag == 1:
            main_open = self.df.pivot_table(index=self.df.index, columns='ticker', values='open_multiply',
                                            aggfunc='first')
            datasets = [main_open]

        if flag == 2:
            main_high = self.df.pivot_table(index=self.df.index, columns='ticker', values='high_multiply',
                                            aggfunc='first')
            datasets = [main_high]

        if flag == 3:
            main_low = self.df.pivot_table(index=self.df.index, columns='ticker', values='low_multiply',
                                           aggfunc='first')
            datasets = [main_low]

        if flag == 4:
            main_close = self.df.pivot_table(index=self.df.index, columns='ticker', values='close_multiply',
                                             aggfunc='first')
            datasets = [main_close]

        if flag == 5:
            main_volume = self.df.pivot_table(index=self.df.index, columns='ticker', values='volume',
                                              aggfunc='first')
            datasets = [main_volume]
        if self.period == 0:
            for i in range(len(self.day_list)):
                if i == 0:
                    for j in range(len(datasets)):
                        if j == 0:
                            l2 = datasets[j][datasets[j].index.weekday == self.day_list[i]].values
                            l2 = l2[..., np.newaxis]
                        else:
                            l2_t = datasets[j][datasets[j].index.weekday == self.day_list[i]].values
                            l2_t = l2_t[..., np.newaxis]
                            l2 = np.concatenate((l2, l2_t), axis=2)
                    l = l2[..., np.newaxis]
                else:
                    for j in range(len(datasets)):
                        if j == 0:
                            l2 = datasets[j][datasets[j].index.weekday == self.day_list[i]].values
                            l2 = l2[..., np.newaxis]
                        else:
                            l2_t = datasets[j][datasets[j].index.weekday == self.day_list[i]].values
                            l2_t = l2_t[..., np.newaxis]
                            l2 = np.concatenate((l2, l2_t), axis=2)
                    l_t = l2[..., np.newaxis]
                    l = np.concatenate((l, l_t), axis=3)

        elif self.period == 1:
            for j in range(len(datasets)):
                if j == 0:
                    l = datasets[j].values
                    l = l[..., np.newaxis]
                else:
                    l_t = datasets[j].values
                    l_t = l_t[..., np.newaxis]
                    l = np.concatenate((l, l_t), axis=2)
        else:
            sys.exit('Unknown period. It should be either "0" ->weekly or "1" -> daily!')

        return l

    def returns_(self, candle):
        aa = self.data_merging_clipping(candle) - 1
        if self.period == 1:
            return np.nanprod(aa, axis=2) + 1
        elif self.period == 0:
            return np.nanprod(aa, axis=3) + 1
        else:
            sys.exit('Unknown period. It should be either "0" ->weekly or "1" -> daily!')

    def create_X_Y(self, df):
        if self.data_type == 0:
            return df
        elif self.data_type == 1:
            df = np.reshape(df, (df.shape[0], df.shape[1]))
            e = np.empty_like(df)
            e[-1:] = np.nan
            e[:-1] = df[1:]
            e = e[..., np.newaxis]
            return e
        else:
            sys.exit('Unknown data type. It should be either "0"->X or "1"->Y!')

    def candles_(self):
        return self.data_merging_clipping('all')

    def get_start_end_date(self):
        if self.dow == 0:
            start_date = self.df.index.min() + relativedelta(weekday=TU(1))
            end_date = self.df.index.max() + relativedelta(weekday=MO(-1))
        if self.dow == 1:
            start_date = self.df.index.min() + relativedelta(weekday=WE(1))
            end_date = self.df.index.max() + relativedelta(weekday=TU(-1))
        if self.dow == 2:
            start_date = self.df.index.min() + relativedelta(weekday=TH(1))
            end_date = self.df.index.max() + relativedelta(weekday=WE(-1))
        if self.dow == 3:
            start_date = self.df.index.min() + relativedelta(weekday=FR(1))
            end_date = self.df.index.max() + relativedelta(weekday=TH(-1))
        if self.dow == 4:
            start_date = self.df.index.min() + relativedelta(weekday=MO(1))
            end_date = self.df.index.max() + relativedelta(weekday=FR(-1))
        return start_date, end_date

    def mask_df(self):
        mask = (self.df.index >= self.start_date) & (self.df.index <= self.end_date)
        self.df = self.df.loc[mask]


def year_week_day(y, w, d):
    return datetime.strptime(f'{y} {w} {d}', '%G %V %u')


def dataset_prep_1(df, args):
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

    # Slicing the data for the desired time frame.
    dataset = df[(df.time >= t1) & (df.time <= t2)]

    # MAKE TIME INDEX PANDAS
    # dataset['date_str'] = dataset.year.map(str) + '-' + dataset.week_num.map(str) + '1'
    # dataset['date_date'] = pd.to_datetime(dataset.date_str, format="%Y-%W%w")
    dataset = dataset.set_index('time')

    # Creates a pivot table for each of the main datasets.
    # This means that we will have a dataframe which its indices are time and its columns are stocks.
    # The missing data will be filled with nans.

    return dataset


def day_generator(i):
    ll = [None] * 5
    temp = 0
    for j in range(5):
        if i + j + 1 > 4:
            ll[j] = i + j - 4
        else:
            ll[j] = i + j + 1
    return ll


def dataset_shift(df, args):
    # Shifting datasets back to a same date for concatenating

    flag = True
    while flag:
        if df.head(1).index.weekday == args.dow:
            df.head(1).drop
            flag = False
        else:
            df.head(1).drop

    return df
    # day_list = day_generator(args.dow)
    # t = [i for i in range(len(day_list)) if day_list[i] == df.index.weekday[0]]
    # for i in range(1,t[0]+1):
    #     if df.index.weekday[0]==0:
    #         df = df.append(pd.DataFrame(index=df.head(1).index - timedelta(days=3)), sort=True)
    #     else:
    #         df = df.append(pd.DataFrame(index=df.head(1).index - timedelta(days=1)), sort=True)
    # return df


def dataset_prep_2(dataset, args):
    day_list = day_generator(args.dow)
    dataset.index.min().weekday()
    dataset.index.max().weekday()
    day_generator(2)
    main_open = dataset.pivot_table(index=dataset.index, columns='ticker', values='open_multiply', aggfunc='first')
    # #shifting datasets
    # t = [i for i in range(len(day_list)) if day_list[i] == main_open.index.weekday[0]]
    # for i in range(1,t[0]+1):
    #     if main_open.index.weekday[0]==0:
    #         main_open = main_open.append(pd.DataFrame(index=main_open.head(1).index - timedelta(days=3)), sort=True)
    #     else:
    #         main_open = main_open.append(pd.DataFrame(index=main_open.head(1).index - timedelta(days=1)), sort=True)
    # main_open = dataset_shift(main_open,args)

    main_high = dataset.pivot_table(index=dataset.index, columns='ticker', values='high_multiply', aggfunc='first')
    # main_high = dataset_shift(main_high,args)

    main_low = dataset.pivot_table(index=dataset.index, columns='ticker', values='low_multiply', aggfunc='first')
    # main_low = dataset_shift(main_low,args)

    main_close = dataset.pivot_table(index=dataset.index, columns='ticker', values='close_multiply',
                                     aggfunc='first')
    # main_close = dataset_shift(main_close,args)

    main_volume = dataset.pivot_table(index=dataset.index, columns='ticker', values='volume', aggfunc='first')
    # main_volume = dataset_shift(main_volume,args)

    # if args.candle == 'open':
    #     main_open = dataset.pivot_table(index=dataset.index, columns='ticker', values='open_multiply', aggfunc='first')
    # elif args.candle == 'high':
    #     main_high = dataset.pivot_table(index=dataset.index, columns='ticker', values='high_multiply', aggfunc='first')
    # elif args.candle == 'low':
    #     main_low = dataset.pivot_table(index=dataset.index, columns='ticker', values='low_multiply', aggfunc='first')
    # elif args.candle == 'close':
    #     main_close = dataset.pivot_table(index=dataset.index, columns='ticker', values='close_multiply',
    #                                      aggfunc='first')
    # elif args.candle == 'volume':
    #     main_volume = dataset.pivot_table(index=dataset.index, columns='ticker', values='volume', aggfunc='first')
    len(main_high[main_high.index.weekday == args.dow])
    if args.data_period == 0:
        if args.all_candle is False:
            if args.candle == 'open':
                return main_open[main_open.index.weekday == args.dow]
            if args.candle == 'high':
                return main_high[main_high.index.weekday == args.dow]
            if args.candle == 'low':
                return main_low[main_low.index.weekday == args.dow]
            if args.candle == 'close':
                return main_close[main_close.index.weekday == args.dow]
            if args.candle == 'volume':
                return main_volume[main_volume.index.weekday == args.dow]
        else:

            datasets = [main_open, main_high, main_low, main_close, main_volume]
            # [i for i in range(len(datasets)) len(datasets[i][main_open.index.weekday == args.dow])]
            l = []
            for i in range(len(day_list)):
                l2 = []
                for j in range(len(datasets)):
                    l2.append(datasets[j][datasets[j].index.weekday == day_list[i]])
                l.append(l2)
            aa = np.stack(np.array(l))

            temp0 = datasets[j][datasets[j].index.weekday == day_list[i]].values
            temp0 = temp0[..., np.newaxis, np.newaxis]

            # temp1 = np.concatenate(temp1,temp0)

            main1 = main_high[main_high.index.weekday == day_list[i]].values
            main1 = main1[..., np.newaxis, np.newaxis]

            main2 = main_low[main_low.index.weekday == day_list[i]].values
            main2 = main2[..., np.newaxis, np.newaxis]

            main3 = main_close[main_close.index.weekday == day_list[i]].values
            main3 = main3[..., np.newaxis, np.newaxis]

            main4 = main_volume[main_volume.index.weekday == day_list[i]].values
            main4 = main4[..., np.newaxis, np.newaxis]

            # main_f = np.concatenate((main, main1, main2, main3, main4), axis=2)

            main = main_open[main_open.index.weekday == day_list[0]].values
            main = main_open[main_open.index.weekday == day_list[0]].values
            main = main[..., np.newaxis]
            for i in range(1, len(day_list)):
                main = np.append(main, main_open[main_open.index.weekday == day_list[i]].values)

            return

            print('hi')

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

    # We have to deduct 2 from data. 1 for final_production and 1 for the fact that forward return is one week ahead.
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
