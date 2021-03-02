import argparse
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import global_vars
from bot.statistics_classic import bench_fn
from bot.data_transfer import (
    get_tickers_list_from_aws, 
    get_new_tickers_list_from_aws,
    download_production_sltp_max_date, 
    get_tickers_list_classic_from_aws, 
    download_production_classic_max_date)
from bot.main_file_classic import main_fn, fill_nulls, classic_vol_update
from general.general import timeNow
from general.slack import report_to_slack

lookback_horizon = 1

def check_new_ticker(args):
    args.start_date = datetime.now().date() - relativedelta(years=3)
    args.start_date2 = datetime.now().date() - relativedelta(years=2)
    args.end_date = datetime.now().date()
    new_ticker = get_new_tickers_list_from_aws(args)
    if(len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        new_ticker["spot_date"] = args.start_date
        args.tickers_list = new_ticker['ticker'].tolist()
        new_ticker = new_ticker[['ticker', 'spot_date']].groupby('ticker').agg({'spot_date': ['max']})
        args.latest_dates_db = new_ticker
        print(new_ticker)
        main_fn(args)
        fill_nulls(args)
    
if __name__ == "__main__":
    if args.month_horizon:
        args.month_horizon = [float(value) for value in args.month_horizon]
    else:
        args.month_horizon = [1, 3]

    args.tickers_list = get_tickers_list_from_aws(args)['ticker'].tolist()
    args.latest_dates_db = download_production_classic_max_date(args)

    if args.benchmark:
        start = timeNow()
        bench_fn(args)
        finish = timeNow()
        print(start)
        print(finish)
    else:
        if args.history:
            args.start_date = datetime.now().date() - relativedelta(years=3)
        else:
            args.start_date = datetime.now().date() - relativedelta(days=3)

        args.end_date = datetime.now().date()
        print("Start Option Maker Classic")
        main_fn(args)
        print("Start Filling Null")
        fill_nulls(args)
        if not args.history:
            check_new_ticker(args)
    sys.exit()

