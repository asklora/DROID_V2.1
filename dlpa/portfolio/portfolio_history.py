import argparse
import sys
from multiprocessing import cpu_count
import time

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from datetime import datetime

import global_vars
from portfolio.client import get_client_information
from portfolio.history_main_file import portfolio_maker, get_prices_from_aws, get_dates_list_from_aws,get_indices

parser = argparse.ArgumentParser(description='Loratech')
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************
parser.add_argument('--client_name', type=str, default='LORATECH')

parser.add_argument('--portfolio_period', type=int, default=0)

parser.add_argument('--signal_threshold', type=int, default=global_vars.signal_threshold)

parser.add_argument('--forward_date_start', default='2016-01-01', type=str)
parser.add_argument('--forward_date_stop', default=str(datetime.fromtimestamp(time.time()).date()), type=str)

parser.add_argument('--production_table_name', type=str, default=global_vars.production_inference_table_name,
                    help='The production table name in AWS.')
parser.add_argument("--mode", default='client')
parser.add_argument("--port", default=64891)
parser.add_argument('--model_table_name', type=str, default=global_vars.production_model_data_table_name,
                    help='The production table name in AWS.')
parser.add_argument('--master_ohlcv_table_name', type=str, default=global_vars.master_ohlcv_table_name,
                    help='The production table name in AWS.')
# *******************  PATHS  **************************************************
# ******************************************************************************

args = parser.parse_args()

args.go_live = False
args.spot_date = None
dates_list = get_dates_list_from_aws(args)
indices_df = get_indices()

client_df = get_client_information(args)
stock_num_list = client_df.top_x.unique()

topX_multiplier_list = [1, 1.5, 2.5]
signal_threshold_list = [0.1, 0.15, 0.20, 0.50]

for ii in topX_multiplier_list:
    for jj in signal_threshold_list:
        client_df.top_x = (client_df.top_x * ii).astype('int')
        args.signal_threshold = jj

        top_stocks = portfolio_maker(client_df, dates_list, args)

        prices_df = get_prices_from_aws(args)
        tri_df = prices_df.pivot_table(index='trading_day', columns='ticker', values='total_return_index',
                                       aggfunc='first', dropna=False)
        tri_df = tri_df.bfill()
        tri_df.index = pd.to_datetime(tri_df.index)
        tri_df["day_of_week"] = tri_df.index.dayofweek
        tri_df =  tri_df[tri_df["day_of_week"]==4]
        tri_df_shifted = tri_df.shift(periods=-1)

        tri_df['spot_date'] = tri_df.index
        tri_df = tri_df.melt(id_vars='spot_date', var_name="ticker", value_name="spot_tri")

        tri_df_shifted['spot_date'] = tri_df_shifted.index
        tri_df_shifted = tri_df_shifted.melt(id_vars='spot_date', var_name="ticker", value_name="forward_tri")

        final_tri = pd.merge(tri_df,tri_df_shifted, on=['spot_date', 'ticker'], how='inner')

        del tri_df,tri_df_shifted

        top_stocks['spot_date'] = top_stocks.spot_date.astype('str')
        final_tri['spot_date'] = final_tri.spot_date.astype('str')
        benchmark_tri = final_tri[final_tri['ticker'].isin(indices_df['benchmark'])].copy()
        final_df = pd.merge(final_tri,top_stocks, on=['spot_date', 'ticker'], how='right')
        # final_df['type'] = 'stocks'

        benchmark_tri['index'] = 1
        for index, row in indices_df.iterrows():
            benchmark_tri.loc[benchmark_tri['ticker'] == row.benchmark,'index'] = row['index']

        benchmark_tri.rename(columns={'spot_tri': 'spot_tri_bench', 'forward_tri': 'forward_tri_bench'}, inplace=True)
        del benchmark_tri['ticker']
        # benchmark_tri['type'] = 'benchmark'

        final_df = pd.merge(final_df,benchmark_tri, on=['spot_date', 'index'], how='left')


        final_df['wts_performance'] = final_df.forward_tri/final_df.spot_tri - 1
        final_df['bench_performance'] = final_df.forward_tri_bench/final_df.spot_tri_bench - 1
        final_df['direction'] = 1
        final_df['out_perf_direction'] = 1
        final_df['direction'] = final_df['direction'].where(final_df.forward_tri > final_df.spot_tri, 0)
        final_df['out_perf_direction'] = final_df['out_perf_direction'].where(final_df.wts_performance > final_df.bench_performance, 0)
        final_df['client_name'] = args.client_name

        if args.portfolio_period == 0:
            data_period = 'weekly'
        else:
            data_period = 'daily'

        final_df['portfolio_period'] = data_period
        final_df['signal_threshold'] = args.signal_threshold
        final_df['topX_multiplier'] = ii

        final_df = final_df.drop(final_df[final_df.wts_performance == 0].index)
        final_df = final_df.drop(final_df[final_df.bench_performance == 0].index)

        # db_url = global_vars.DB_PROD_URL
        engine = create_engine(global_vars.DB_TEST_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            final_df.to_sql(con=conn, name=global_vars.test_portfolio_performance_table_name, if_exists='append', index=False)

        print(f'Saved the data to {global_vars.test_portfolio_performance_table_name}!')