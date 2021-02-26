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
from bot.data_download import get_bot_data_latest_date, get_data_vol_surface_ticker, get_executive_data_download, get_new_tickers

from general.slack import report_to_slack
from general.date_process import dateNow, datetimeNow, droid_start_date_buffer, str_to_date, timeNow, droid_start_date
from global_vars import saved_path, saved_model_path

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

def data_prep_daily(ticker=None, currency_code=None):
    print("{} : === {} DATA PREPERATION STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, daily=True)
    if type(ticker) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), ticker))
    elif type(currency_code) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), currency_code))
    else:
        report_to_slack("{} : === DATA PREPERATION DAILY COMPLETED ===".format(dateNow()))

def data_prep_check_new_ticker(currency_code=None):
    print("{} : === DATA PREPERATION CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    date_identifier = "trading_day"
    table_name = get_bot_data_table_name()
    new_ticker = get_new_tickers(currency_code, start_date, start_date2, date_identifier, table_name)
    if (len(new_ticker) > 0):
        new_ticker = new_ticker["ticker"].tolist()
        print(f"Found {len(new_ticker)} New Ticker {tuple(new_ticker)}")
        populate_bot_data(start_date=start_date, end_date=end_date, ticker=new_ticker, new_ticker=True)
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
    report_to_slack("{} : === DATA PREPERATION HISTORY COMPLETED ===".format(dateNow()))

def infer_daily(ticker=None, currency_code=None):
    print("{} : === {} VOLATILITY INFER STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    populate_vol_infer(start_date, end_date, ticker=None, currency_code=None, train_model=False, daily=False, history=False)
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

    populate_vol_infer(start_date, end_date, ticker=None, currency_code=None, train_model=False, daily=False, history=False)

    report_to_slack("{} : === VOLATILITY INFER HISTORY COMPLETED ===".format(dateNow()))

def train_model(ticker=None, currency_code=None):
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    print(f"Model Training Start {datetimeNow()}")
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    
    final_model_fun(main_df)
    print(f"Finished Model Training  {datetimeNow()}")

def option_maker_daily(index):
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
                if args.exec_index is None:
                print('Please input the desired index!')
                sys.exit()

            args.latest_date = get_latest_date(args)
            print('Data preparation daily started!')
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f'The start date is set as: {args.start_date}')

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f'The end date is set as: {args.end_date}')

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, '%Y-%m-%d').date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, '%Y-%m-%d').date()
            main_fn(args)

            #Check New Ticker
            args.start_date = dt.now().date() - relativedelta(years=args.history_num_years)
            args.start_date2 = dt.now().date() - relativedelta(years=args.history_num_years - 1)
            args.end_date = dt.now().date()
            print(f'The start date is set as: {args.start_date}')
            print(f'The end date is set as: {args.end_date}')
            date_identifier = "trading_day"
            table_name = args.executive_data_table_name
            new_ticker = get_new_tickers_list_from_aws(args, date_identifier, table_name)
            try:
                if (len(new_ticker) > 0):
                    print(new_ticker)
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker['ticker'].tolist()
                    args.data_prep_daily = False
                    args.data_prep_daily_new_ticker = True
                    main_fn(args)
            except Exception as e:
                print(e)
            report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(str(dt.now()), args.exec_index), args)
    args.option_maker = True
            args.null_filler = True

            if args.exec_index is None:
                print("Please input the desired index!")
                sys.exit()

            # The following lines finds the last date of executive data and will use that as the start of the live mode.
            # This helps if some days are skipped in the live mode.
            args.latest_dates_db = download_production_executive_max_date(args)
            # args.start_date = temp_df.spot_date.iloc[0]
            args.start_date = args.latest_dates_db["spot_date", "max"].min()

            args.latest_date = get_latest_date(args)
            print("Option making daily started!")
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()
    if not args.month_horizon:
                args.month_horizon = [1, 3]
            args.new_ticker = False
            if args.option_maker:
                option_fn(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls(args)

            if args.option_maker_daily :
                args.start_date = dt.now().date() - relativedelta(years=3)
                table_name = args.executive_production_table_name
                new_ticker = get_new_tickers_list_detail_from_aws(args, args.start_date, table_name)
                print(new_ticker)
                if (len(new_ticker) > 0):
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker["ticker"].tolist()
                    args.start_date = dt.now().date() - relativedelta(years=3)
                    args.end_date = dt.now().date()
                    args.latest_dates_db = new_ticker
                    args.new_ticker = True
                    print(args.tickers_list)
                    print(args.start_date)
                    print(args.end_date)
                    print(args.latest_dates_db)
                    status = option_fn(args)
                    if(status):
                        fill_nulls(args)
    report_to_slack("{} : === {} Executive UNO Create Option Completed ===".format(str(dt.now()),args.exec_index), args)
def option_maker_history():
    if args.start_date is None:
                args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()
    args.new_ticker = False
            if args.option_maker:
                option_fn_full(args)
                print("Option creation full options is finished")

            if args.null_filler:
                fill_nulls(args)

def option_maker_daily_ucdc():
    args.option_maker = True
            args.null_filler = True

            if args.exec_index is None:
                print("Please input the desired index!")
                sys.exit()

            # The following lines finds the last date of executive data and will use that as the start of the live mode.
            # This helps if some days are skipped in the live mode.
            args.latest_dates_db = download_production_executive_max_date(args)
            # args.start_date = temp_df.spot_date.iloc[0]
            args.start_date = args.latest_dates_db["spot_date", "max"].min()

            args.latest_date = get_latest_date(args)
            print("Option making daily started!")
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()
        args.new_ticker = False
            if args.option_maker:
                option_fn_ucdc(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls_ucdc(args)
            
            if args.option_maker_daily_ucdc :
                args.start_date = dt.now().date() - relativedelta(years=3)
                table_name = args.executive_production_ucdc_table_name
                new_ticker = get_new_tickers_list_detail_from_aws(args, args.start_date, table_name)
                print(new_ticker)
                if (len(new_ticker) > 0):
                    ticker_length = len(new_ticker)
                    print(f"Found {ticker_length} New Ticker")
                    args.tickers_list = new_ticker["ticker"].tolist()
                    args.start_date = dt.now().date() - relativedelta(years=3)
                    args.end_date = dt.now().date()
                    args.latest_dates_db = new_ticker
                    args.new_ticker = True
                    print(args.tickers_list)
                    print(args.start_date)
                    print(args.end_date)
                    print(args.latest_dates_db)
                    status = option_fn_ucdc(args)
                    if(status):
                        fill_nulls_ucdc(args)
    report_to_slack("{} : === {} Executive UCDC Create Option Completed ===".format(str(dt.now()),args.exec_index), args)
def option_maker_history_ucdc():
    if args.start_date is None:
                args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
            print(f"The start date is set as: {args.start_date}")

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f"The end date is set as: {args.end_date}")

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()
    if not args.month_horizon:
                args.month_horizon = [6]
            args.new_ticker = False
            if args.option_maker:
                option_fn_ucdc(args)
                print("Option creation is finished")

            if args.null_filler:
                fill_nulls_ucdc(args)