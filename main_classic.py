import argparse
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import global_vars
from classic.statistics import bench_fn
from classic.data_transfer import (
    get_tickers_list_from_aws, 
    get_new_tickers_list_from_aws,
    download_production_sltp_max_date, 
    get_tickers_list_classic_from_aws, 
    download_production_classic_max_date)
from classic.main_file import main_fn, fill_nulls, classic_vol_update
from general.general import timeNow
from general.slack import report_to_slack

parser = argparse.ArgumentParser()

parser.add_argument('--history', dest='history', action='store_true')
parser.add_argument('--no_history', dest='history', action='store_false')
parser.set_defaults(history=False)  # When true will create a production history.

parser.add_argument('--benchmark', dest='benchmark', action='store_true')
parser.add_argument('--no_benchmark', dest='benchmark', action='store_false')
parser.set_defaults(benchmark=False)  # When true will create a production history.

parser.add_argument('--monthly', dest='monthly', action='store_true')
parser.add_argument('--no_monthly', dest='monthly', action='store_false')
parser.set_defaults(monthly=False)  # When true will create a production history.

parser.add_argument('--lookback_horizon', type=int, default=1)
parser.add_argument('--monthly_horizon', type=int)
parser.add_argument('--month_horizon', nargs='+')
parser.add_argument('--debug_mode', dest='debug_mode', action='store_true')
parser.add_argument('--no_debug_mode', dest='debug_mode', action='store_false')
parser.set_defaults(debug_mode=False)  # When true will create a production history.

parser.add_argument('--tac_data_table_name', type=str, default=global_vars.tac_data_table_name)
parser.add_argument('--holidays_table_name', type=str, default=global_vars.holidays_table_name)
parser.add_argument('--classic_statistics_table_name', type=str, default=global_vars.classic_statistics_table_name)
parser.add_argument('--sltp_production_table_name', type=str, default=global_vars.sltp_production_table_name)
parser.add_argument('--droid_universe_table_name', type=str, default=global_vars.droid_universe_table_name)

parser.add_argument('--slack_api', type=str, default=global_vars.slack_api,help='slack api')
# *******************  PATHS  **************************************************
# ******************************************************************************
args = parser.parse_args()


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

