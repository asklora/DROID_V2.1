import datetime
import os
import sys
import time
import uuid
from datetime import datetime as dt
import tensorflow as tf
from pandas.tseries.offsets import BDay, Week
from dlpa.hypers.hypers import hypers


def gpu_mac_address(gpu_number):
    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_number)

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
        pc_number = "Iman"
    if str(hex(uuid.getnode())) == '0xc9d92c510e5':
        pc_number = "PC4"
    if str(hex(uuid.getnode())) == '0xac1f6b153eba':
        pc_number = "PC3"
    if str(hex(uuid.getnode())) == '0x309c23270a79':
        pc_number = "PC2"
    if str(hex(uuid.getnode())) == '0x49226d8eb81':
        pc_number = "PC1"
    if str(hex(uuid.getnode())) == '0xb06ebf5cd358':
        pc_number = "Stephen"
    
    return pc_number

def jump_period(period_jump, test_num, train_num, valid_num, forward_year, forward_week, forward_day, go_live=False, data_period=0):
    # Overall we have three dates. The main one is test date which is given by the user. The auxiliary ones are the
    # start date and the end date, which are created by the code to get the needed data for model training and testing
    # form the main dataframe.

    forward_date = dt.strptime(f'{forward_year} {forward_week} {forward_day}', '%G %V %u')

    if data_period == 0:
        # We need to add 1 business day to the start since the week starts from it. e.g. test -> Fri => start-> Mon
        start_date = forward_date - Week(train_num + valid_num + 1) + BDay(1)
        end_date = forward_date + Week(test_num - 1)
    else:
        start_date = forward_date - BDay(train_num + valid_num + 1)
        end_date = forward_date + BDay(test_num)

    if start_date > dt.fromtimestamp(time.time()):
        sys.exit('The start date is after today!')

    flag = True
    jump_value = Week(0)
    while flag:
        # The loop which jumps forward all data by a number of periods.

        start_date = start_date + jump_value
        end_date = end_date + jump_value
        forward_date = forward_date + jump_value

        if not go_live:
            d = datetime.date.today()
            if data_period == 0:
                if d.weekday() > 4:
                    while d.weekday() != 4:
                        d -= BDay(1)
                else:
                    d -= Week(1)
            else:
                if d.weekday() > 4:
                    while d.weekday() > 4:
                        d -= BDay(1)
                else:
                    d -= BDay(1)

            if forward_date.date() > d:
                if data_period == 0:
                    sys.exit(f"We don't have weekly data for {forward_date.date()}!")
                if data_period == 1:
                    sys.exit(f"We don't have daily data for {forward_date.date()}!")
        # The START of the main code.
        hypers()

        if go_live:
            sys.exit('Finished!')
        if data_period == 0:
            jump_value = Week(period_jump)
        else:
            jump_value = BDay(period_jump)

        if forward_date >= dt.fromtimestamp(time.time()):
            flag = False
    return start_date, end_date, flag, jump_value