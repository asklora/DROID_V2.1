import sys
import time
from datetime import datetime
import datetime as dt
from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine, and_
from sqlalchemy import text

import global_vars
from global_vars import no_top_models
from functools import reduce


db_url = global_vars.DB_TEST_URL_READ
# db_url = global_vars.DB_PROD_URL
engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
table_name = global_vars.test_portfolio_performance_table_name
with engine.connect() as conn:
    metadata = db.MetaData()
    table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

    query = 'select * from '
    query = query + table_name

    ResultProxy = conn.execute(text(query))
    ResultSet = ResultProxy.fetchall()
    columns_list = conn.execute(text(query)).keys()

full_df = pd.DataFrame(ResultSet)

if len(full_df) == 0:
    sys.exit("We don't have inferred data.")
full_df.columns = columns_list
# *************************************************************
def performance_calculator(df):
    df['wts_tri_performance'] = df['wts_performance'] + 1
    df['bench_tri_performance'] = df['bench_performance'] + 1

    optimized_values = df.groupby(['index','signal_threshold','topX_multiplier']).agg({'wts_performance': ['mean'], 'bench_performance': ['mean'],
                                                       'direction': ['sum', 'count'],
                                                       'out_perf_direction': ['sum', 'count']})
    # optimized_values.reset_index(inplace=True)

    optimized_values2 = df.groupby(['index','spot_date','signal_threshold','topX_multiplier']).agg({'wts_tri_performance': ['mean'],
                                                  'bench_tri_performance': ['mean']})
    optimized_values2.reset_index(inplace=True)
    optimized_values2 = optimized_values2.groupby([('index',''),('signal_threshold',''),('topX_multiplier','')]).agg({('wts_tri_performance','mean'): ['prod','std'],
                                                  ('bench_tri_performance','mean'): ['prod','std']})
    # optimized_values2.reset_index(inplace=True)
    optimized_values[('direction', 'accuracy')] = optimized_values[('direction', 'sum')] \
                                                  / optimized_values[('direction', 'count')]
    optimized_values[('out_perf_direction', 'accuracy')] = optimized_values[('out_perf_direction', 'sum')] \
                                                           / optimized_values[('out_perf_direction', 'count')]

    optimized_values[('direction', 'accuracy')] = optimized_values[('direction', 'accuracy')] * 100
    optimized_values[('out_perf_direction', 'accuracy')] = optimized_values[('out_perf_direction', 'accuracy')] * 100
    optimized_values[('direction', 'accuracy')] = optimized_values[('direction', 'accuracy')].round(2)
    optimized_values[('out_perf_direction', 'accuracy')] = optimized_values[('out_perf_direction', 'accuracy')].round(2)

    optimized_values[('wts_performance', 'mean')] = optimized_values[('wts_performance', 'mean')] * 100
    optimized_values[('wts_performance', 'mean')] = optimized_values[('wts_performance', 'mean')].round(3)
    optimized_values[('bench_performance', 'mean')] = optimized_values[('bench_performance', 'mean')] * 100
    optimized_values[('bench_performance', 'mean')] = optimized_values[('bench_performance', 'mean')].round(3)

    optimized_values[('wts_tri_performance', 'prod')] = optimized_values2[('wts_tri_performance','mean','prod')] - 1
    optimized_values[('wts_tri_performance', 'prod')] = optimized_values2[('wts_tri_performance','mean','prod')].round(2)

    optimized_values[('bench_tri_performance', 'prod')] = optimized_values2[('bench_tri_performance','mean','prod')] - 1
    optimized_values[('bench_tri_performance', 'prod')] = optimized_values2[('bench_tri_performance','mean','prod')].round(2)

    optimized_values[('wts_tri_performance', 'std')] = optimized_values2[('wts_tri_performance','mean','std')]
    optimized_values[('bench_tri_performance', 'std')] = optimized_values2[('bench_tri_performance','mean','std')]

    optimized_values = optimized_values.drop(
        [('direction', 'sum'), ('direction', 'count'), ('out_perf_direction', 'sum'),
         ('out_perf_direction', 'count')], axis=1)
    return optimized_values

def sorting_fun(df):
    df.reset_index(inplace=True)

    optimized_values = df.groupby([('signal_threshold',''),('topX_multiplier','')])\
        .agg({('wts_tri_performance','prod'): ['mean'], ('bench_tri_performance','prod'): ['mean'],
              ('wts_tri_performance','std'): ['mean'], ('bench_tri_performance','std'): ['mean']})

    optimized_values[('sort_parameter','','')] = optimized_values[('wts_tri_performance', 'prod','mean')] - optimized_values[('bench_tri_performance', 'prod','mean')]
    optimized_values[('sort_parameter','','')] = optimized_values[('wts_tri_performance', 'prod','mean')] / optimized_values[('wts_tri_performance', 'std','mean')]
    optimized_values = optimized_values.sort_values(by=[('sort_parameter', '')], ascending=False)

    optimized_values.reset_index(inplace=True)

    return optimized_values
# **************************** Full **************************
full_df.to_csv('full_df_top_25.csv')

full_df['direction'].sum()/len(full_df['direction'])
full_df['out_perf_direction'].sum()/len(full_df['out_perf_direction'])

full_df['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df)
optimized_values.to_csv('performance_per_index_full.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_full.csv')
config_2.to_csv('report_config_2_full.csv')
config_3.to_csv('report_config_3_full.csv')

performance_all = sorting_fun(optimized_values)
performance_all.to_csv('performance_per_config_full.csv')

# *********************************** 2016  *********************************
start_date = '2016-01-01'
end_date = '2017-01-01'
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_2016.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_2016.csv')
config_2.to_csv('report_config_2_2016.csv')
config_3.to_csv('report_config_3_2016.csv')

performance_2016 = sorting_fun(optimized_values)
performance_2016.to_csv('performance_per_config_2016.csv')
# *********************************** 2017  *********************************
start_date = '2017-01-01'
end_date = '2018-01-01'
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_2017.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_2017.csv')
config_2.to_csv('report_config_2_2017.csv')
config_3.to_csv('report_config_3_2017.csv')

performance_2017 = sorting_fun(optimized_values)
performance_2017.to_csv('performance_per_config_2017.csv')
# *********************************** 2018  *********************************
start_date = '2018-01-01'
end_date = '2019-01-01'
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_2018.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_2018.csv')
config_2.to_csv('report_config_2_2018.csv')
config_3.to_csv('report_config_3_2018.csv')

performance_2018 = sorting_fun(optimized_values)
performance_2018.to_csv('performance_per_config_2018.csv')
# *********************************** 2019  *********************************
start_date = '2019-01-01'
end_date = '2020-01-01'
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_2019.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_2019.csv')
config_2.to_csv('report_config_2_2019.csv')
config_3.to_csv('report_config_3_2019.csv')

performance_2019 = sorting_fun(optimized_values)
performance_2019.to_csv('performance_per_config_2019.csv')

# *********************************** 1 year  *********************************
end_date = pd.Timestamp(dt.date.today())
start_date = end_date - pd.DateOffset(years=1)
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_1_year.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_1_year.csv')
config_2.to_csv('report_config_2_1_year.csv')
config_3.to_csv('report_config_3_1_year.csv')

performance_2_year = sorting_fun(optimized_values)
performance_2_year.to_csv('performance_per_config_1_year.csv')

# *********************************** 2 year  *********************************
end_date = pd.Timestamp(dt.date.today())
start_date = end_date - pd.DateOffset(years=2)
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_2_year.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_2_year.csv')
config_2.to_csv('report_config_2_2_year.csv')
config_3.to_csv('report_config_3_2_year.csv')

performance_2_year = sorting_fun(optimized_values)
performance_2_year.to_csv('performance_per_config_2_year.csv')
# *********************************** 3 year  *********************************
end_date = pd.Timestamp(dt.date.today())
start_date = end_date - pd.DateOffset(years=3)
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_3_year.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_3_year.csv')
config_2.to_csv('report_config_2_3_year.csv')
config_3.to_csv('report_config_3_3_year.csv')

performance_3_year = sorting_fun(optimized_values)
performance_3_year.to_csv('performance_per_config_3_year.csv')
# *********************************** 4 year  *********************************
end_date = pd.Timestamp(dt.date.today())
start_date = end_date - pd.DateOffset(years=4)
full_df['spot_date'] = pd.to_datetime(full_df['spot_date'])
mask = (full_df['spot_date'] >= start_date) & (full_df['spot_date'] < end_date)
full_df_temp = full_df.loc[mask].copy()

full_df_temp['direction'].sum()/len(full_df_temp['direction'])
full_df_temp['out_perf_direction'].sum()/len(full_df_temp['out_perf_direction'])
full_df_temp['wts_performance'].mean().round(5)

optimized_values = performance_calculator(full_df_temp)
optimized_values.to_csv('performance_per_index_4_year.csv')

optimized_values_temp = optimized_values.reset_index()
config_1 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.1) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_2 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 1))]
config_3 = optimized_values_temp[
    ((optimized_values_temp[('signal_threshold', '')] == 0.5) & (optimized_values_temp[('topX_multiplier', '')] == 3))]
config_1.to_csv('report_config_1_4_year.csv')
config_2.to_csv('report_config_2_4_year.csv')
config_3.to_csv('report_config_3_4_year.csv')

performance_4_year = sorting_fun(optimized_values)
performance_4_year.to_csv('performance_per_config_4_year.csv')
# ******************************************************************************
for i in range(0,12):
    top_rows_no = i

    columns_list = [('signal_threshold','',''),('topX_multiplier','','')]

    data_frames = [performance_all[columns_list].head(top_rows_no),
                   performance_2016[columns_list].head(top_rows_no),
                   performance_2017[columns_list].head(top_rows_no),
                   performance_2018[columns_list].head(top_rows_no),
                   performance_2019[columns_list].head(top_rows_no)]
                   # performance_2_year[columns_list].head(top_rows_no),
                   # performance_3_year[columns_list].head(top_rows_no),
                   # performance_4_year[columns_list].head(top_rows_no)]

    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=[('signal_threshold','',''),
                                                                   ('topX_multiplier','','')],how='inner'), data_frames)

    columns_list_2 = [('signal_threshold','',''),('topX_multiplier','',''),('wts_tri_performance','prod','mean'),
                      ('bench_tri_performance','prod','mean')]

    final_df = pd.merge(df_merged,performance_2016[columns_list_2],how='inner')
    final_df.rename(columns={'wts_tri_performance': 'wts_tri_performance_2016',
                             'bench_tri_performance': 'bench_tri_performance_2016'},level=0, inplace=True)

    final_df = pd.merge(final_df,performance_2017[columns_list_2],how='inner')
    final_df.rename(columns={'wts_tri_performance': 'wts_tri_performance_2017',
                             'bench_tri_performance': 'bench_tri_performance_2017'},level=0, inplace=True)

    final_df = pd.merge(final_df,performance_2018[columns_list_2],how='inner')
    final_df.rename(columns={'wts_tri_performance': 'wts_tri_performance_2018',
                             'bench_tri_performance': 'bench_tri_performance_2018'},level=0, inplace=True)

    final_df = pd.merge(final_df,performance_2019[columns_list_2],how='inner')
    final_df.rename(columns={'wts_tri_performance': 'wts_tri_performance_2019',
                             'bench_tri_performance': 'bench_tri_performance_2019'},level=0, inplace=True)

    final_df = pd.merge(final_df,performance_all[columns_list_2],how='inner')
    final_df.rename(columns={'wts_tri_performance': 'wts_tri_performance_all',
                             'bench_tri_performance': 'bench_tri_performance_all'},level=0, inplace=True)


    final_df.to_csv(f'performance_top_{top_rows_no}.csv')

# ******************************************************************************