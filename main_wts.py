import sys
import time
import numpy as np
from datetime import datetime

from pandas.tseries.offsets import BDay
from dlpa.hypers.hyper_run import gpu_mac_address, jump_period
from dlpa.hypers.hypers import hypers
from dlpa.hypers.model_parameters_load import download_model_data
from global_vars import aws_columns_list
# live_future_predict4:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main_portfolio.py --live --future --num_periods_to_predict 4
# live_future_predict13:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main_portfolio.py --live --future --num_periods_to_predict 13

# live_rv2_future_predict4_fullupdate:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main.py --live --rv_2  --future --num_periods_to_predict 4 --db_full_update
# live_future_rv2_predict13:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main.py --live --future --rv_2 --num_periods_to_predict 13
# live_gpu0_rv2_fullupdate:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main.py --live --gpu_number 0 --rv_2 --db_full_update
# live_gpu1_rv2:
# 	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DLPA/main.py --live --gpu_number 1 --rv_2

model_type=1  # use DLPM
epoch=25  # 25 epochs seem enough (more takes too much time)
Hyperopt_runs=10 # don't need more than 20
forward_year
forward_week=1 #v2 default = 1
forward_dayt=5  #v2 default = 5
dow=5
data_period=0  # the period to forecast - one week ahead of one day ahead?
stage=0  # not going to explore other stages yet
stock_percentage=1.0
train_num=390 # number of periods to train before test period (and valid periods if >0) 390 for weekly 1250 for daily
valid_num=0 # valid_num = 0 => random shuffle 20%, else chronological periods before test period
test_num=1  # test_num should BE 1.
period_jump=1 # JUMP <>1 for model tuning and sampling - should go to 1 for prod
update_lookback=2 # how many EXTRA periods to update?
num_periods_to_predict=1 # needs to be >1 for --future
num_bins=3
num_nans_to_skip=1# Nan FILTERING
seed=123
best_train_acc=1e-1
best_valid_acc=1e-1
test_acc_1=0
test_acc_2=0
test_acc_3=0
test_acc_4=0
test_acc_5=0
lowest_train_loss=1e-1
lowest_valid_loss=1e-1
best_valid_epoch=1
lowest_train_epoch=1
unique_num_of_returns=1
unique_num_of_outputs=1
timestamp=time.time()
gpu_number=0
pc_number='unknown'
db_end_date=datetime.today() - BDay(2)

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************
np.random.seed(seed)

if __name__ == "__main__":
    if rv_1 or rv_2:
        master_daily_df = True
        master_daily_rv_df = False
        master_daily_tac_df = False
        master_daily_beta_adj_df = False

    if tomorrow:
        # This will predict tomorrow instead of today in daily mode.
        model_type = 2
        data_period = 1
        num_periods_to_predict = 2

    if future:
        # this is for predicting MULTIPLE periods (4 weeks = 1 month or 13 weeks = 3 months)
        data_period = 0
        model_type = 2
        if num_periods_to_predict <= 1:
            sys.exit('num_periods_to_predict > 1!')

    if test_stock_output_flag: #test but also want stock output data
        test_output_flag = True

    if live:
        # The following args make sure that the live will be set on stage 0 and will go into production mode.
        stage = 0
        production_output_flag = True
        # args.go_portfolio = False
        d = datetime.date.today()
        update = True
        valid_num = 0
        test_num = 0
        dow = 5
        if data_period == 0:
            while d.weekday() != 4:
                d += BDay(1)
            forward_date = d
            train_num = 450
        else:
            if d.weekday() > 4:
                while d.weekday() != 0:
                    d += BDay(1)
            forward_date = d
            train_num = 1000

        if live_debug:
            train_num = 60
        forward_year, forward_week, forward_day = forward_date.isocalendar()
    if train_num == 0:
        production_output_flag = True
        # args.go_portfolio = False

        aws_columns_list = aws_columns_list
        forward_date = datetime.strptime(f'{forward_year} {forward_week} {forward_day}', '%G %V %u')
        Hyperopt_runs = 1
        model_data = download_model_data(forward_date, data_period=data_period)
        model_data.reset_index(drop=True, inplace=True)

        for index, row in model_data.iterrows():
            temp_data = row
            hypers()
        sys.exit('Finished inferring!')

    # This function sets the gpu number and records the mc address.
    pc_number = gpu_mac_address(gpu_number)

    # This function jumps the whole code by the given number of periods.
    start_date, end_date, flag, jump_value = jump_period(period_jump, test_num, train_num, valid_num, forward_year, forward_week, forward_day, go_live=live, data_period=data_period)