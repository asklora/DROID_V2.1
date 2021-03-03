import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta

from classic.data_transfer import benchmark_data_download
from sqlalchemy import create_engine, and_
from multiprocessing import cpu_count

from general.slack import report_to_slack
from global_vars import time_to_expiry, bots_list

def check_bot_list(bot_list):
    if (type(bot_list) == type(None)):
        bot_list = bots_list
    return bot_list

def check_time_to_exp(time_to_exp):
    if (type(time_to_exp) == type(None)):
        time_to_exp = time_to_expiry
    return time_to_exp

def move_nans_to_top(df):
    # This function will shift the nans to the top.
    for col in df:
        temp = df[col].copy()
        m = temp.isnull()
        temp1 = temp[m].append(temp[~m]).reset_index(drop=True)
        temp1.index = temp.index
        df[col] = temp1
    return df


def lookback_creator(df, days, days_back, args):
    # This function will create lookback batches.
    lookback_df = df[len(df) - days_back:len(df) - days]
    return lookback_df


def nearest(items, pivot):
    # This function finds the nearest trading day to the picked start day by user.
    return min(items, key=lambda x: abs(x - pivot))


def Sl_Tp(prices, stop_loss, take_profit, dates, stock_list):
    # This function checks which events are triggered.
    prices_df = pd.DataFrame(prices, index=dates, columns=stock_list)

    tp_indices = np.argmax(prices >= take_profit, axis=0)
    tp_indices[tp_indices == 0] = np.where(prices[0, tp_indices == 0] >= take_profit[tp_indices == 0], 0, 500)

    sl_indices = np.argmax(prices <= stop_loss, axis=0)
    sl_indices[sl_indices == 0] = np.where(prices[0, sl_indices == 0] <= stop_loss[sl_indices == 0], 0, 500)

    sl_indices[sl_indices == 500] = -1
    tp_indices[tp_indices == 500] = -1

    sl_dates = pd.DataFrame(index=range(sl_indices.shape[0]), columns=['event_date'])
    tp_dates = pd.DataFrame(index=range(tp_indices.shape[0]), columns=['event_date'])
    sl_dates.event_date = dates[sl_indices.T]
    tp_dates.event_date = dates[tp_indices.T]

    events = sl_dates.where(sl_dates < tp_dates, tp_dates)
    events['event'] = 'NT'
    events['event_date'] = sl_dates.where(sl_dates < tp_dates, tp_dates)
    events.loc[events['event_date'] == sl_dates['event_date'], 'event'] = 'SL'
    events.loc[events['event_date'] == tp_dates['event_date'], 'event'] = 'TP'
    events.loc[events['event_date'] == dates[-1], 'event'] = 'NT'

    events['ticker'] = stock_list
    for i in range(len(events['event_date'])):
        events.loc[i, 'event_price'] = prices_df.loc[events.loc[i, 'event_date'], events.loc[i, 'ticker']]

    return_full = prices / prices[0, :] - 1
    sl_return = pd.DataFrame(index=range(1), columns=range(sl_indices.shape[0]))
    tp_return = pd.DataFrame(index=range(1), columns=range(sl_indices.shape[0]))
    for i in range(sl_indices.shape[0]):
        sl_return.iloc[0, i] = return_full[sl_indices[i], i]
        tp_return.iloc[0, i] = return_full[tp_indices[i], i]

    sl_indices[sl_indices == -1] = 500
    tp_indices[tp_indices == -1] = 500

    end_return = prices[-1, :] / prices[0, :] - 1

    return_values = np.where(sl_indices < tp_indices, sl_return.values, tp_return.values)
    return_values = np.where(sl_indices == tp_indices, end_return, return_values)
    events['return'] = return_values.T

    return events


def get_close_vol(multiplier, days_in_year=256):
    # The function for calculating volatility.
    returns = multiplier
    log_returns = np.log(returns)
    log_returns_sq = np.square(log_returns)
    var = np.mean(log_returns_sq)
    result = np.sqrt(var * days_in_year)

    #max 45% and min 20%
    #result = np.where(result<0.2,0.2,np.where(result>0.45,0.45,result))
    result[result<0.2] = 0.2
    result[result>0.5] = 0.5
    return result

