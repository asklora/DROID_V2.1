import argparse
import datetime
import sys
import time
from datetime import datetime as dt

import numpy as np
from pandas.tseries.offsets import BDay

import global_vars
from dlpa.hypers.hyper_run import gpu_mac_address, jump_period
# ******************************************************************************
# *******************  PARAMETERS  *********************************************
# ******************************************************************************
# from hypers.portfolio_parameters_load import load_parameters
from dlpa.hypers.hypers import hypers
from dlpa.hypers.model_parameters_load import download_model_data

parser = argparse.ArgumentParser()
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************

# DLPA works best withOUT candles - the embedding for OHLCV conv for input into attention is not robust
# DLPM works best WITH candles gives bad results without candles
# SIMPLE is fastest and achieve 95% efficacy of DLPM
# we won't use DLPA - its the middle, not as accurate as DLPM and not as fast as SIMPLE

# 0 -> DLPA,
# 1 -> DLPM,
# 2 -> SIMPLE,
parser.add_argument('--model_type', type=int, default=1)  # use DLPM
# *******************  MODEL PARAMETERS  ***************************************
# ******************************************************************************
parser.add_argument('--epoch', type=int, default=25,
                    help='Number of epochs.')  # 25 epochs seem enough (more takes too much time)
parser.add_argument('--Hyperopt_runs', type=int, default=10,
                    help='number of runs in hyperopt loop.')  # don't need more than 20

# TEST date - spot date
parser.add_argument('--forward_year', type=int, help='The year of the test date.')
parser.add_argument('--forward_week', type=int, default=1, help='The week of the test date.') #v2 default = 1
parser.add_argument('--forward_day', type=int, default=5, help='Day of week for test date')  #v2 default = 5
# 1 -> Mon,
# 2 -> Tue,
# 3 -> Wed,
# 4 -> Thu,
# 5 -> Fri

# train on other days of the week
parser.add_argument('--dow', type=int, default=5, help='Day of week for training.')
# 1 -> Mon,
# 2 -> Tue,
# 3 -> Wed,
# 4 -> Thu,
# 5 -> Fri
# TODO We should be able to separate day of week for X and Y. In all train, valid and test dataset.
# Steps:
# 1- download the prices too. Already we are just downloading multiples.
# 2- calculate the Y candles using forward_price/spot_price -1. Current way of calculating using product of multiples
# only works fo close. (this can be done any day of week for Y)
# 3- Connect the new calculated Y to the old X that we have.
# 4- Go to the create_train_test_valid_ohlcv function.

# *******************  DATASET PARAMETERS  *************************************
# ******************************************************************************
parser.add_argument('--data_period', type=int, default=0)  # the period to forecast - one week ahead of one day ahead?
# 0 -> weekly,
# 1 -> daily,
parser.add_argument('--stage', type=int, default=0)  # not going to explore other stages yet
parser.add_argument('--stock_percentage', type=float, default=1.0,
                    help='The percentage of the stocks to use in the whole data.')

parser.add_argument('--train_num', type=int, default=390,
                    help='Number of training periods.')  # number of periods to train before test period (and valid periods if >0) 390 for weekly 1250 for daily
parser.add_argument('--valid_num', type=int, default=0,
                    help='Number of validation periods.')  # valid_num = 0 => random shuffle 20%, else chronological periods before test period
parser.add_argument('--test_num', type=int, default=1, help='Number of test periods.')  # test_num should BE 1.

# USE CANDLES
parser.add_argument('--use_candles', dest='use_candles', action='store_true')
parser.add_argument('--no_use_candles', dest='use_candles', action='store_false')
parser.set_defaults(use_candles=True)

# USE CANDLES
parser.add_argument('--tomorrow', dest='tomorrow', action='store_true')
parser.add_argument('--no_tomorrow', dest='tomorrow', action='store_false')
parser.set_defaults(tomorrow=False)

# USE CANDLES
parser.add_argument('--future', dest='future', action='store_true')
parser.add_argument('--no_future', dest='future', action='store_false')
parser.set_defaults(future=False)

# CANDLE TYPE - which of OHLCV to use
# 0 -> all,
# 1 -> open,
# 2 -> high,
# 3 -> low,
# 4 -> close
# 5 -> volume
parser.add_argument('--candle_type_candles', type=int, default=0)  # this should DEFAULT TO 0 - use all candles
parser.add_argument('--candle_type_returnsY', type=int, default=4)  # this should DEFAULT TO 4 - use close
parser.add_argument('--candle_type_returnsX', type=int, default=4)  # this should DEFAULT TO 4 - use close

# JUMP <>1 for model tuning and sampling - should go to 1 for prod
parser.add_argument('--period_jump', type=int, default=1,
                    help='All the dates are jumped forward by this amount(based on data_period).')
# UPDATE Params
parser.add_argument('--update_lookback', type=int, default=2,
                    help="You can set the number of recent weeks for updating as load_data's argument.")  # how many EXTRA periods to update?
# 'Update = false' means that if there is a local database it won't connect to AWS anymore.
parser.add_argument('--update_db', dest='update', action='store_true')
parser.add_argument('--no_update_db', dest='update', action='store_false')
parser.set_defaults(update=False)

parser.add_argument('--db_full_update', dest='db_full_update',
                    action='store_true')  # will FULLY download (overwrite) the complete dataset
parser.add_argument('--no_db_full_update', dest='db_full_update',
                    action='store_false')  # DEFAULT to no full downloads (just update)
parser.set_defaults(db_full_update=False)

# *******************  DATA PARAMETERS  ****************************************
# ******************************************************************************
parser.add_argument('--num_periods_to_predict', type=int, default=1,
                    help='Number of weeks or days to predict.')  # needs to be >1 for --future

parser.add_argument('--num_bins', type=int, default=3, help='Number of bins for output classification.')

parser.add_argument('--num_nans_to_skip', type=int, default=1,
                    help='Number of nans to skip for data preperation.')  # Nan FILTERING

# *******************  TO AWS PARAMETERS  **************************************
# ******************************************************************************
# If set true, the production results for each stock will be written to AWS.
parser.add_argument('--pr_output', dest='production_output_flag',
                    action='store_true')  # FOR PRODUCTION: LIVE + BACKTEST
parser.add_argument('--no_pr_output', dest='production_output_flag', action='store_false')  # FOR TESTING
parser.set_defaults(production_output_flag=True)

#############################DON'T NEED#################################################################################
parser.add_argument('--save_ec2', dest='save_ec2', action='store_true')  # DEFAULT to save ALL model weights to EC2
parser.add_argument('--no_save_ec2', dest='save_ec2',
                    action='store_false')  # OVERWRITE and don't save model weights to EC2
parser.set_defaults(save_ec2=False)
############################################################################################################


parser.add_argument('--test', dest='test_output_flag',
                    action='store_true')  # FOR test: BACKTEST
parser.add_argument('--no_test', dest='test_output_flag', action='store_false')  # FOR TESTING
parser.set_defaults(test_output_flag=False)

parser.add_argument('--test_stock_output', dest='test_stock_output_flag',
                    action='store_true')  # FOR test: BACKTEST; test but also want stock output data
parser.add_argument('--no_test_stock_output', dest='test_stock_output_flag', action='store_false')  # FOR TESTING
parser.set_defaults(test_stock_output_flag=False)



parser.add_argument('--production_table_name', type=str, default=global_vars.production_inference_table_name,
                    help='The production table name in AWS.')

#############################DON'T NEED#################################################################################
# If set true, the plots will be saved to hard drive.
parser.add_argument('--save_plots', dest='save_plots', action='store_true')
parser.add_argument('--no_save_plots', dest='save_plots', action='store_false')
parser.set_defaults(save_plots=False)

parser.add_argument('--save_cluster_files', dest='save_cluster_files', action='store_true')
parser.add_argument('--no_save_cluster_files', dest='save_cluster_files', action='store_false')
parser.set_defaults(save_cluster_files=True)
############################################################################################################

# parser.add_argument('--test_table_name', type=str, default='test_data', help='The test table name in AWS.')

parser.add_argument('--model_data_table_name', type=str, default=global_vars.test_model_data_table_name,
                    # for sample testing,...
                    help='The model data table name in AWS.')
parser.add_argument('--production_model_data_table_name', type=str,
                    default=global_vars.production_model_data_table_name,  # for production
                    help='The model data table name in AWS.')

# parser.add_argument('--save_plots', dest='test_output_flag', action='store_true')
# parser.add_argument('--no_save_plots', dest='test_output_flag', action='store_false')
# parser.set_defaults(feature=False)
#

# ******************************************************************************
# *******************  PROGRAM PARAMETERS  *************************************
# ******************************************************************************

# *******************  PATHS  **************************************************
# ******************************************************************************
# parser.add_argument('--db_url', type=str, default=global_vars.DB_PROD_URL, help='Database URL')
# parser.add_argument('--droid_url', type=str, default=global_vars.DB_DROID_URL, help='Database URL')
parser.add_argument('--model_path', type=str)
parser.add_argument('--plot_path', type=str)

parser.add_argument('--live', dest='go_live', action='store_true')
parser.add_argument('--no_live', dest='go_live', action='store_false')
parser.set_defaults(go_live=False)

parser.add_argument('--portfolio', dest='go_portfolio', action='store_true')
parser.add_argument('--no_portfolio', dest='go_portfolio', action='store_false')
parser.set_defaults(go_portfolio=False)

parser.add_argument('--seed', type=int, default=123, help='Random seed')
# parser.add_argument("--mode", default='client')
# parser.add_argument("--port", default=64891)

# Dummy args
parser.add_argument('--best_train_acc', type=float, default=1e-1)
parser.add_argument('--best_valid_acc', type=float, default=1e-1)
parser.add_argument('--test_acc_1', type=float, default=0)
parser.add_argument('--test_acc_2', type=float, default=0)
parser.add_argument('--test_acc_3', type=float, default=0)
parser.add_argument('--test_acc_4', type=float, default=0)
parser.add_argument('--test_acc_5', type=float, default=0)
parser.add_argument('--lowest_train_loss', type=float, default=1e-1)
parser.add_argument('--lowest_valid_loss', type=float, default=1e-1)
parser.add_argument('--best_valid_epoch', type=int, default=1)
parser.add_argument('--lowest_train_epoch', type=int, default=1)
parser.add_argument('--stocks_list', type=list)

parser.add_argument('--unique_num_of_returns', type=int, default=1)
parser.add_argument('--unique_num_of_outputs', type=int, default=1)
parser.add_argument('--timestamp', type=float, default=time.time())
parser.add_argument('--model_run_time', type=float)
parser.add_argument('--gpu_number', type=int, default=0)
parser.add_argument('--pc_number', type=str, default='unknown')

# TODO for cluster + DLPA------------------------------------------------------
# not using embedding (was mostly for DLPA) -
parser.add_argument('--use_embedding', dest='embedding_flag', action='store_true')
parser.add_argument('--no_use_embedding', dest='embedding_flag', action='store_false')
parser.set_defaults(embedding_flag=False)
parser.add_argument('--accuracy_for_embedding', type=int, default=2, help='e.g. 3 means: 0.0012 -> 1')
# 'use capping on accuracy for embedding'
parser.add_argument('--use_capping', dest='use_capping', action='store_true')
parser.add_argument('--no_use_capping', dest='use_capping', action='store_false')
parser.set_defaults(use_capping=True)
# clustering args
parser.add_argument('--cluster_no_x', type=int, default=25, help='Number of test periods.')
parser.add_argument('--cluster_no_y', type=int, default=35, help='Number of test periods.')
parser.add_argument('--cluster_x', dest='cluster_x', action='store_true')
parser.add_argument('--no_cluster_x', dest='cluster_x', action='store_false')
parser.set_defaults(cluster_x=False)
parser.add_argument('--cluster_y', dest='cluster_y', action='store_true')
parser.add_argument('--no_cluster_y', dest='cluster_y', action='store_false')
parser.set_defaults(cluster_y=False)

parser.add_argument('--live_debug', dest='live_debug', action='store_true')
parser.add_argument('--no_live_debug', dest='live_debug', action='store_false')
parser.set_defaults(live_debug=False)

parser.add_argument('--rv_1', dest='rv_1', action='store_true')
parser.add_argument('--no_rv_1', dest='rv_1', action='store_false')
parser.set_defaults(rv_1=False)

parser.add_argument('--rv_2', dest='rv_2', action='store_true')
parser.add_argument('--no_rv_2', dest='rv_2', action='store_false')
parser.set_defaults(rv_2=False)

#############################DON'T NEED#################################################################################
parser.add_argument('--beta_1', dest='beta_1', action='store_true')
parser.add_argument('--no_beta_1', dest='beta_1', action='store_false')
parser.set_defaults(beta_1=False)
parser.add_argument('--beta_2', dest='beta_2', action='store_true')
parser.add_argument('--no_beta_2', dest='beta_2', action='store_false')
parser.set_defaults(beta_2=False)
############################################################################################################

parser.add_argument('--tac_flag', dest='tac_flag', action='store_true')
parser.add_argument('--no_tac_flag', dest='tac_flag', action='store_false')
parser.set_defaults(tac_flag=False)

parser.add_argument('--master_daily_df', dest='master_daily_df', action='store_true')
parser.add_argument('--no_master_daily_df', dest='master_daily_df', action='store_false')
parser.set_defaults(master_daily_df=True)

parser.add_argument('--master_daily_rv_df', dest='master_daily_rv_df', action='store_true')
parser.add_argument('--no_master_daily_rv_df', dest='master_daily_rv_df', action='store_false')
parser.set_defaults(master_daily_rv_df=False)

parser.add_argument('--master_daily_tac_df', dest='master_daily_tac_df', action='store_true')
parser.add_argument('--no_master_daily_tac_df', dest='master_daily_tac_df', action='store_false')
parser.set_defaults(master_daily_tac_df=False)

parser.add_argument('--master_daily_beta_adj_df', dest='master_daily_beta_adj_df', action='store_true')
parser.add_argument('--no_master_daily_beta_adj_df', dest='master_daily_beta_adj_df', action='store_false')
parser.set_defaults(master_daily_beta_adj_df=False)

parser.add_argument('--db_end_date', default=dt.today() - BDay(2))

args = parser.parse_args()

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************
np.random.seed(args.seed)

if __name__ == "__main__":
    if args.rv_1 or args.rv_2:
        args.master_daily_df = True
        args.master_daily_rv_df = False
        args.master_daily_tac_df = False
        args.master_daily_beta_adj_df = False

    if args.beta_1 or args.beta_2:
        args.master_daily_df = True
        args.master_daily_rv_df = False
        args.master_daily_tac_df = False
        args.master_daily_beta_adj_df = True

    if args.tomorrow:
        # This will predict tomorrow instead of today in daily mode.
        args.model_type = 2
        args.data_period = 1
        args.num_periods_to_predict = 2

    if args.future:
        # this is for predicting MULTIPLE periods (4 weeks = 1 month or 13 weeks = 3 months)
        args.data_period = 0
        args.model_type = 2
        if args.num_periods_to_predict <= 1:
            sys.exit('num_periods_to_predict > 1!')

    if args.test_stock_output_flag: #test but also want stock output data
        args.test_output_flag = True

    if args.go_live:
        # The following args make sure that the live will be set on stage 0 and will go into production mode.
        args.stage = 0
        args.production_output_flag = True
        # args.go_portfolio = False
        d = datetime.date.today()
        args.update = True
        args.valid_num = 0
        args.test_num = 0
        args.dow = 5
        if args.data_period == 0:
            while d.weekday() != 4:
                d += BDay(1)
            args.forward_date = d
            args.train_num = 450
        else:
            if d.weekday() > 4:
                while d.weekday() != 0:
                    d += BDay(1)
            args.forward_date = d
            args.train_num = 1000

        if args.live_debug:
            args.train_num = 60
        args.forward_year, args.forward_week, args.forward_day = args.forward_date.isocalendar()

    if args.train_num == 0:
        args.production_output_flag = True
        # args.go_portfolio = False

        args.aws_columns_list = global_vars.aws_columns_list
        args.forward_date = dt.strptime(f'{args.forward_year} {args.forward_week} {args.forward_day}', '%G %V %u')
        args.Hyperopt_runs = 1
        model_data = download_model_data(args)
        model_data.reset_index(drop=True, inplace=True)

        for index, row in model_data.iterrows():
            args.temp_data = row
            hypers(args)
        sys.exit('Finished inferring!')

    # This function sets the gpu number and records the mc address.
    pc_number = gpu_mac_address(args)

    # This function jumps the whole code by the given number of periods.
    jump_period(args)