from datetime import date
import os
import sys
import platform
import pandas as pd
from pandas.tseries.offsets import BDay

from bot.final_model import populate_vol_infer
from bot.main_file import populate_bot_data
from general.sql_query import get_active_universe
from general.table_name import get_bot_data_table_name
from bot.data_download import (
    get_backtest_latest_date, 
    get_bot_data_latest_date, 
    get_data_vol_surface_ticker, 
    get_executive_data_download, get_new_ticker_from_bot_backtest, 
    get_new_tickers, get_new_tickers_from_bot_data, get_volatility_latest_date)
from general.slack import report_to_slack
from general.date_process import dateNow, datetimeNow, droid_start_date_buffer, str_to_date, timeNow, droid_start_date
from global_vars import saved_model_path

# 	main_exec.py --option_maker_daily --exec_index 0#.FTSE --add_inferred
# 	main_exec.py --option_maker_daily_ucdc --exec_index 0#.FTSE --add_inferred
# 	main.py --bot_backtest_updates --bot_index 0#.FTSE
# 	main_exec.py --bot_labeler_infer_daily --exec_index 0#.FTSE
# 	main.py --latest_bot_ranking --bot_index 0#.FTSE

# 	main_exec.py --option_maker_daily --exec_index 0#.SPX
# 	main_exec.py --option_maker_daily_ucdc --exec_index 0#.SPX
# 	main.py --bot_backtest_updates --bot_index 0#.SPX
# 	main_exec.py --bot_labeler_infer_daily --exec_index 0#.SPX
# 	main.py --latest_bot_ranking --bot_index 0#.SPX
# 	main_exec.py --benchmark
# 	main_exec.py --benchmark_ucdc

# option_training:
# 	main_exec.py --train_model
# 	main_exec.py --bot_labeler_train
# 	@/sbin/shutdown -r now
    
# def initial_data():
#     args.
#     args.active_tickers_list = get_active_tickers_list(args)
#     pd.options.mode.chained_assignment = None
#     gpu_mac_address(args)
#     if args.go_production:
#         args.valid_size = 0.2
#         args.test_size = 0.0
#     else:
#         args.valid_size = 0.15
#         args.test_size = 0.25
#     index_to_etf = pd.read_csv("executive/index_to_etf.csv", names=["index", "etf"])
#     etf_list = index_to_etf.etf.unique().tolist()
# def bot_labeler_train():
# def bot_labeler_infer_history():
# def bot_labeler_infer_daily():
# def bot_labeler_infer_live():
# def bot_labeler_performance_history():
# def benchmark():
#     time_to_exp = ["2w", "4w", "1m", "8w", "2m", "3m", "6m"]
#     report_to_slack("{} : === UNO STATISTICS COMPLETED ===".format(dateNow()))
# def benchmark_ucdc():
#     time_to_exp = ["2w", "4w", "1m", "8w", "2m", "3m", "6m"]
#     report_to_slack("{} : === UCDC STATISTICS COMPLETED ===".format(dateNow()))

def daily_uno(ticker=None, currency_code=None, infer=True):
    data_prep_daily(ticker=ticker, currency_code=currency_code)
    if(infer):
        infer_daily(ticker=ticker, currency_code=currency_code)

def daily_ucdc(ticker=None, currency_code=None, infer=True):
    data_prep_daily(ticker=ticker, currency_code=currency_code)
    if(infer):
        infer_daily(ticker=ticker, currency_code=currency_code)

def daily_classic(ticker=None, currency_code=None):
    data_prep_daily(ticker=ticker, currency_code=currency_code)

def data_prep_daily(ticker=None, currency_code=None):
    print("{} : === {} DATA PREPERATION STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, daily=True)
    print("{} : === DATA PREPERATION COMPLETED ===".format(dateNow()))
    if type(ticker) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), ticker))
    elif type(currency_code) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), currency_code))
    else:
        report_to_slack("{} : === DATA PREPERATION DAILY COMPLETED ===".format(dateNow()))

def data_prep_check_new_ticker(ticker=None, currency_code=None):
    print("{} : === DATA PREPERATION CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    date_identifier = "trading_day"
    new_ticker = get_new_tickers_from_bot_data(start_date, start_date2, date_identifier, ticker=ticker, currency_code=currency_code)
    if (len(new_ticker) > 0):
        new_ticker = new_ticker["ticker"].tolist()
        print(f"Found {len(new_ticker)} New Ticker {tuple(new_ticker)}")
        populate_bot_data(start_date=start_date, end_date=end_date, ticker=new_ticker, new_ticker=True)
        print("{} : === DATA PREPERATION CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === DATA PREPERATION CHECK NEW TICKER COMPLETED ===".format(dateNow()))

def data_prep_history():
    print("{} : === DATA PREPERATION HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    print("Data preparation history started!")
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    ticker = get_active_universe["ticker"].tolist()

    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, history=True)
    print("{} : === DATA PREPERATION HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === DATA PREPERATION HISTORY COMPLETED ===".format(dateNow()))

def infer_daily(ticker=None, currency_code=None):
    print("{} : === {} VOLATILITY INFER STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, daily=True)

    print("{} : === VOLATILITY INFER COMPLETED ===".format(dateNow()))
    if type(ticker) != type(None):
        report_to_slack("{} : === {} VOLATILITY INFER COMPLETED ===".format(dateNow(), ticker))
    elif type(currency_code) != type(None):
        report_to_slack("{} : === {} VOLATILITY INFER COMPLETED ===".format(dateNow(), currency_code))
    else:
        report_to_slack("{} : === VOLATILITY INFER DAILY COMPLETED ===".format(dateNow()))

def infer_history():
    print("{} : === {} VOLATILITY INFER HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    ticker = get_active_universe["ticker"].tolist()
    main_df = get_executive_data_download(start_date, end_date, ticker=ticker)
    print(main_df)

    populate_vol_infer(start_date, end_date, ticker=ticker, history=True)
    print("{} : === VOLATILITY INFER HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === VOLATILITY INFER HISTORY COMPLETED ===".format(dateNow()))

def train_model(ticker=None, currency_code=None):
    print("{} : === {} VOLATILITY TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, train_model=False)
    print("{} : === VOLATILITY TRAIN MODEL COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === VOLATILITY TRAIN MODEL COMPLETED ===".format(dateNow()))

def option_maker_uno_check_new_ticker(ticker=None, currency_code=None, mod=False, option_maker=False, null_filler=False):
    print("{} : === OPTION MAKER UNO CHECK NEW TICKER STARTED ===".format(dateNow()))

    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    new_ticker = get_new_ticker_from_bot_backtest(ticker=ticker, currency_code=currency_code, uno=True, mod=mod)
    print(new_ticker)
    if (len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        if option_maker:
            option_fn(args)
        if null_filler:
            fill_nulls(args)
        print("{} : === OPTION MAKER UNO CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === OPTION MAKER UNO CHECK NEW TICKER COMPLETED ===".format(dateNow()))

def option_maker_daily_uno(ticker=None, currency_code=None, mod=False, option_maker=False, null_filler=False):
    print("{} : === {} OPTION MAKER UNO STARTED ===".format(dateNow()))
    latest_dates_db = get_backtest_latest_date(ticker=ticker, currency_code=currency_code, mod=mod, uno=True)
    start_date = latest_dates_db["max_date"].min()
    end_date = str_to_date(dateNow())
    latest_date = get_volatility_latest_date(ticker=ticker, currency_code=currency_code, infer=True)
    if start_date is None:
        start_date = latest_date
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if not month_horizon:
            args.month_horizon = [1, 3]
    if option_maker:
        option_fn(args)
        print("Option creation is finished")
    if null_filler:
        fill_nulls(args)

    print("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))

def option_maker_history_uno(mod=False, option_maker=False, null_filler=False):
    print("{} : === {} OPTION MAKER UNO HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    if option_maker:
        option_fn_full(args)
        print("Option creation full options is finished")

    if null_filler:
        fill_nulls(args)
    
    print("{} : === OPTION MAKER UNO HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UNO HISTORY COMPLETED ===".format(dateNow()))

def option_maker_ucdc_check_new_ticker(ticker=None, currency_code=None, mod=False, option_maker=False, null_filler=False):
    print("{} : === OPTION MAKER UCDC CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    new_ticker = get_new_ticker_from_bot_backtest(ticker=ticker, currency_code=currency_code, ucdc=True, mod=mod)
    print(new_ticker)
    if (len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        if option_maker:
            option_fn_ucdc(args)
        if null_filler:
            fill_nulls_ucdc(args)
        print("{} : === OPTION MAKER UCDC CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === OPTION MAKER UCDC CHECK NEW TICKER COMPLETED ===".format(dateNow()))

def option_maker_daily_ucdc(ticker=None, currency_code=None, mod=False, option_maker=False, null_filler=False):
    print("{} : === {} OPTION MAKER UNO STARTED ===".format(dateNow()))
    latest_dates_db = get_backtest_latest_date(ticker=ticker, currency_code=currency_code, mod=mod, ucdc=True)
    start_date = latest_dates_db["max_date"].min()
    end_date = str_to_date(dateNow())
    latest_date = get_volatility_latest_date(ticker=ticker, currency_code=currency_code, infer=True)
    if start_date is None:
        start_date = latest_date
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    if option_maker:
        option_fn_ucdc(args)

    if null_filler:
        fill_nulls_ucdc(args)
    print("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))

def option_maker_history_ucdc(mod=False, option_maker=False, null_filler=False):
    print("{} : === {} OPTION MAKER UCDC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())

    if option_maker:
        option_fn_ucdc(args)

    if null_filler:
        fill_nulls_ucdc(args)
    
    print("{} : === OPTION MAKER HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UCDC HISTORY COMPLETED ===".format(dateNow()))