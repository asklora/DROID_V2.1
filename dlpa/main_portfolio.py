import argparse
import sys

import global_vars
from portfolio.client import get_client_information
from portfolio.main_file import portfolio_maker, get_dates_list_from_aws, record_DLP_rating, dlp_rating_history

# ******************************************************************************
# *******************  PARAMETERS  *********************************************
# ******************************************************************************


parser = argparse.ArgumentParser(description='Loratech')
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************
parser.add_argument('--client_name', type=str, default='LORATECH')

parser.add_argument('--forward_date_start', type=str)
parser.add_argument('--forward_date_stop', type=str)
# parser.add_argument('--spot_date', type=str)

# 0 -> weekly,
# 1 -> daily,
# 2 -> Point in time (PIT),
parser.add_argument('--portfolio_period', type=int)

parser.add_argument('--signal_threshold', type=int, default=global_vars.signal_threshold)

parser.add_argument('--stock_table_name', type=str, default=global_vars.production_inference_table_name,
                    help='The production table name in AWS.')

parser.add_argument('--model_table_name', type=str, default=global_vars.production_model_data_table_name,
                    help='The production table name in AWS.')
# *******************  PATHS  **************************************************
# ******************************************************************************
# parser.add_argument('--db_url', type=str, default=global_vars.DB_PROD_URL, help='Database URL')
parser.add_argument('--model_path', type=str)
parser.add_argument('--plot_path', type=str)

parser.add_argument('--seed', type=int, default=123, help='Random seed')
parser.add_argument("--mode", default='client')
parser.add_argument("--port", default=64891)

parser.add_argument('--email', dest='send_email', action='store_true')
parser.add_argument('--no_email', dest='send_email', action='store_false')
parser.set_defaults(send_email=False)

parser.add_argument('--live', dest='go_live', action='store_true')
parser.add_argument('--no_live', dest='go_live', action='store_false')
parser.set_defaults(go_live=False)

parser.add_argument('--test', dest='go_test', action='store_true')
parser.add_argument('--no_test', dest='go_test', action='store_false')
parser.set_defaults(go_test=False)

parser.add_argument('--future', dest='future', action='store_true')
parser.add_argument('--no_future', dest='future', action='store_false')
parser.set_defaults(future=False)

parser.add_argument('--num_periods_to_predict', type=int, default=1,
                    help='Number of weeks or days to predict.')

parser.add_argument('--dlp_rating_history', dest='dlp_rating_history', action='store_true')
parser.add_argument('--no_dlp_rating_history', dest='no_dlp_rating_history', action='store_false')
parser.set_defaults(write_mode=False)

args = parser.parse_args()

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************


if __name__ == "__main__":
    if args.dlp_rating_history:
        dlp_rating_history(args)
        sys.exit("DLP RATING HISTORY DONE")

    if not args.go_live:
        args.go_test = False
        if args.forward_date_start is None and args.forward_date_stop is None:
            sys.exit('Please input either of the forward_date_start or forward_date_stop!')
        elif args.forward_date_start is None:
            args.forward_date_start = args.forward_date_stop
        elif args.forward_date_stop is None:
            args.forward_date_stop = args.forward_date_start

    if args.go_test:
        args.stock_table_name = global_vars.test_inference_table_name
        args.model_table_name = global_vars.test_model_data_table_name

    client_df = get_client_information(args)
    stock_num_list = client_df.top_x.unique()

    if not args.go_live:
        dates_list = get_dates_list_from_aws(args)

        for ddate in dates_list:
            args.forward_date = ddate
            args.spot_date = None

            forward_date_temp = args.forward_date
            spot_date_temp = args.spot_date

            for num in stock_num_list:
                args.stock_num = num
                args.index_list = client_df[client_df.top_x == num]['index_choice_id'].tolist()
                top_buy, top_hold, top_sell = portfolio_maker(args)
                args.forward_date = forward_date_temp
    else:
        if not args.future:
            for num in stock_num_list:
                args.stock_num = num
                args.index_list = client_df[client_df.top_x == num]['index_choice_id'].tolist()
                top_buy, top_hold, top_sell = portfolio_maker(args)
        else:
            args.portfolio_period = 0
            record_DLP_rating(args)



