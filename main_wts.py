import sys
import time
import numpy as np
from datetime import datetime

from pandas.tseries.offsets import BDay
from dlpa.hypers.hyper_run import gpu_mac_address, jump_period
from dlpa.hypers.hypers import hypers
from dlpa.hypers.model_parameters_load import download_model_data
from global_vars import aws_columns_list, seed, period_jump, model_type, data_period, gpu_number, num_periods_to_predict

def main_process(gpu_number = gpu_number, num_periods_to_predict = num_periods_to_predict, rv_1=False, rv_2=False, tomorrow=False, future=False, live=False, full_update=False):
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

def live_rv2_future_predict4_fullupdate():
    predict = 4
    main_process(predict = predict, rv_2=True, future=True, live=True, full_update=True)

def live_future_rv2_predict13():
    predict = 13
    main_process(predict = predict, rv_2=True, future=True, live=True)

def live_gpu0_rv2_fullupdate():
    gpu_number = 0
    main_process(gpu_number = gpu_number, rv_2=True, live=True, full_update=True)

def live_gpu1_rv2():
    gpu_number = 1
    main_process(gpu_number = gpu_number, rv_2=True, live=True)

if __name__ == "__main__":
    np.random.seed(seed)
    print("Process Here")
    