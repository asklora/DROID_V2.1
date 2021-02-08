import os
import sys
import time
import uuid
from datetime import datetime

import tensorflow as tf
from pandas.tseries.offsets import BDay, Week

from hypers.hypers import hypers


def year_week_day(y, w, d):
    return datetime.strptime(f'{y} {w} {d}', '%G %V %u')


def gpu_mac_address(args):
    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_number)

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
        args.pc_number = "Iman"
    if str(hex(uuid.getnode())) == '0xc9d92c510e5':
        args.pc_number = "PC4"
    if str(hex(uuid.getnode())) == '0xac1f6b153eba':
        args.pc_number = "PC3"
    if str(hex(uuid.getnode())) == '0x309c23270a79':
        args.pc_number = "PC2"
    if str(hex(uuid.getnode())) == '0x49226d8eb81':
        args.pc_number = "PC1"


def jump_period(args):
    t2_1 = year_week_day(args.forward_year, args.forward_week, args.forward_day)
    args.forward_date = year_week_day(args.forward_year, args.forward_week, args.forward_day)

    if args.data_period == 0:
        u0 = args.forward_date - Week(args.train_num + args.valid_num + 1)
    else:
        u0 = args.forward_date - BDay(args.train_num + args.valid_num + 1)

    args.start_year, args.start_week, args.start_day = datetime.date(u0).isocalendar()

    if u0 > datetime.fromtimestamp(time.time()):
        sys.exit('The start date is after today!')

    flag = True
    d = Week(0)
    while flag:
        # The loop which jumps forward all data by a number of weeks.

        t1 = year_week_day(args.start_year, args.start_week, args.start_day)

        u1 = t1 + d
        t2_1 = t2_1 + d

        # Needed for making the test date as anchor (only for end_date)
        if args.data_period == 0:
            u2_2 = t2_1 + Week(args.test_num - 1)
        else:
            u2_2 = t2_1 + BDay(args.test_num - 1)

        args.start_year, args.start_week, args.start_day = datetime.date(u1).isocalendar()
        args.end_year, args.end_week, args.end_day = datetime.date(u2_2).isocalendar()
        args.forward_date = args.forward_date + d
        args.forward_year, args.forward_week, args.forward_day = datetime.date(args.forward_date).isocalendar()
        hypers(args)

        # Needed for week jump
        if args.data_period == 0:
            d = Week(args.period_jump)
            # d = timedelta(weeks=args.period_jump)
        else:
            d = BDay(args.period_jump)
        if u2_2 > datetime.fromtimestamp(time.time()):
            flag = False
