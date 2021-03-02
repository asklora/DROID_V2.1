import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta

import global_vars
from classic.data_transfer import benchmark_data_download
from sqlalchemy import create_engine, and_
from multiprocessing import cpu_count
from sqlalchemy.types import Date, BIGINT, TEXT
from general.slack import report_to_slack
import sqlalchemy as db
from classic.data_transfer import upsert_data_to_db

def bench_fn(args):

    def statistic_calculator(df):
        """For each ticker use the past monthly (horizon + 3 months) OR 1 years OR 3 years- eligible rows =
         all dates that have expired or events triggered
         num_rows = # rows in the ticker in past months/1 year/3 years with today > expiration or events triggered
         https://loratechai.atlassian.net/wiki/spaces/WEB/pages/56623116/Classic+SLTP+plan+template+Req
         """

        df2 = df.copy()
        df = df2.copy()
        bench_df = pd.DataFrame()

        # ******************************************************************************************
        # Adding columns(or dummy columns) to df for further calculations.

        df['return'] = df['return'].astype(float)
        # df['expiry_return'] = df['expiry_return'].astype(float)
        # df['bm'] = df.expiry_price.astype(float) / df.spot_price.astype(float) - 1
        df = df.assign(expiry_return=df['expiry_return'].astype(float))
        df = df.assign(bm=df.expiry_price.astype(float) / df.spot_price.astype(float) - 1)
        # dummy columns
        df = df.assign(tp_num=0)
        df = df.assign(sl_num=0)
        df = df.assign(return_pos=0)
        df = df.assign(return_neg=0)

        # ******************************************************************************************
        # These will be used for counting the occurrences.
        # df['return_sign'] = np.sign(df['return'])
        df.loc[df['event'] == 'SL', 'sl_num'] = 1
        df.loc[df['event'] == 'TP', 'tp_num'] = 1
        df.loc[df['return'] > 0, 'return_pos'] = 1
        df.loc[df['return'] < 0, 'return_neg'] = 1

        # ******************************************************************************************

        #  avg return where returns >0 and where returns  < 0
        ret_pos_neg = df.groupby(['ticker','month_to_exp'])['return'].agg([('return_neg', lambda x: x[x < 0].mean()),
                                                                           ('return_pos', lambda x: x[x > 0].mean())])
        ret_pos_neg.reset_index(inplace=True)

        # df['duration_days'] = round(df.duration * 365)

        # duration = (event_date - spot_date) / 365
        df = df.assign(duration_days=round(df.duration * 365))

        # days_pos = df[df['event'] > 'SL'].groupby(['ticker', 'month_to_exp']).agg({'duration_days': ['sum']})
        # days_pos.reset_index(inplace=True)

        d = {'mean':'avg_days_max_profit'}
        days_tp_sl = df.groupby(['ticker','month_to_exp', 'event']).agg({'duration_days': ['mean']}).rename(columns=d)
        days_tp_sl.columns = days_tp_sl.columns.droplevel(0)
        days_tp_sl.reset_index(inplace=True)

        # avg_days = average (event_date - spot_date)
        days_events = df.groupby(['ticker','month_to_exp']).agg({'duration_days': ['mean']})
        days_events.reset_index(inplace=True)
        bench_df['avg_days'] = days_events['duration_days','mean']

        returns = df.groupby(['ticker', 'month_to_exp']).agg({'return': ['count','mean', 'min'], 'return_pos': ['sum'],
                                                              'return_neg': ['sum'], 'sl_num': ['sum'], 'tp_num': ['sum'],
                                                              'duration': ['mean'], 'bm': ['mean','min'], 'expiry_return': ['mean'], 'drawdown_return': ['min']})
        returns.reset_index(inplace=True)

        # returns_pos = df[df['return']>0].groupby(['ticker', 'month_to_exp']).agg({'return': ['count', 'mean']})
        # returns_pos.reset_index(inplace=True)

        # Adding ticker and month_to_exp to output df(bench_df)
        bench_df['ticker'] = returns['ticker','']
        bench_df['month_to_exp'] = returns['month_to_exp', '']

        # pct_profit = (returns >0) / num_rows
        bench_df['pct_profit'] = returns['return_pos', 'sum'] / returns['return', 'count']

        # pct_losses = (returns < 0) / num_rows
        bench_df['pct_losses'] = returns['return_neg', 'sum'] / returns['return', 'count']

        # avg_profit = avg return where returns >0
        bench_df['avg_profit'] = ret_pos_neg['return_pos']

        # avg_loss = avg return where returns  < 0
        bench_df['avg_loss'] = ret_pos_neg['return_neg']

        # avg_return = avg return
        bench_df['avg_return'] = returns['return', 'mean']

        # pct_tp (out of all cases) = num of TP rows / all rows
        bench_df['pct_max_profit'] = returns['tp_num', 'sum'] / returns['return', 'count']

        # pct_sl (out of all cases) = num of SL rows / all rows
        bench_df['pct_max_loss'] = returns['sl_num', 'sum'] / returns['return', 'count']

        # ann_avg_return = average  = avg_return / duration
        bench_df['ann_avg_return'] = returns['return', 'mean'] / returns['duration', 'mean']

        # ann_avg_return_bm = avg_bm_return = average expiry_return’s (X 12 for 1M and X4 for 3M)
        bench_df['ann_avg_return_bm'] = returns['expiry_return', 'mean']
        bench_df.loc[bench_df['month_to_exp'] == 0.25, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 48
        bench_df.loc[bench_df['month_to_exp'] == 0.5, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 24
        bench_df.loc[bench_df['month_to_exp'] == 1, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 12
        bench_df.loc[bench_df['month_to_exp'] == 2, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 6
        bench_df.loc[bench_df['month_to_exp'] == 3, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 4
        bench_df.loc[bench_df['month_to_exp'] == 6, 'ann_avg_return_bm'] = bench_df['ann_avg_return_bm'] * 2

        # avg_return_bm = avg_bm_return = average expiry_return’s
        bench_df['avg_return_bm'] = returns['expiry_return', 'mean']

        # avg_days_max_profit = average (event_date - spot_date) for event = ‘TP’
        bench_df = bench_df.merge(days_tp_sl[days_tp_sl.event == 'TP'], on=['ticker','month_to_exp'], how='outer')

        # avg_days_sl = average (event_date - spot_date) for event = ‘SL’
        days_tp_sl.rename(columns={'avg_days_max_profit': 'avg_days_max_loss'}, inplace=True)
        bench_df = bench_df.merge(days_tp_sl[days_tp_sl.event == 'SL'], on=['ticker','month_to_exp'], how='outer')
        bench_df = bench_df.drop(['event_x','event_y'], 1)
        bench_df.fillna({'avg_days_max_profit': 0, 'avg_days_max_loss': 0}, inplace=True)

        # max_loss_plan = min (return)
        bench_df['max_loss_bot'] = returns['return','min']

        # max_loss_bm = min (bm_returns)
        bench_df['max_loss_bm'] = returns['drawdown_return','min']

        bench_df = bench_df.infer_objects()

        return bench_df

    lookback_list = global_vars.statistics_lookback_list
    args.lookback_horizon = max(lookback_list)
    print("Get Classic Backtest")
    df = benchmark_data_download(args)
    print(df)

    table_name = global_vars.bot_statistics_table_name

    option_type_list = ['classic']
    bench_df_main = pd.DataFrame()

    for option_t in option_type_list:
        for lookback in lookback_list:
            df_t = df.copy()
            temp_date = datetime.date.today() - relativedelta(months=lookback)
            temp_df = df_t.loc[df_t.spot_date >= temp_date, :]
            if len(temp_df) != 0:
                bench_df_1 = statistic_calculator(temp_df)
                bench_df_1['lookback'] = lookback
                bench_df_1['option_type'] = option_t
                bench_df_main = bench_df_main.append(bench_df_1)
    
    # new changes uid
    bench_df_main['bot_type'] = 'CLASSIC'
    bench_df_main['uid'] = bench_df_main['bot_type']+"-"+bench_df_main['ticker']+bench_df_main['lookback'].astype(str)+bench_df_main['month_to_exp'].astype(str)
    bench_df_main['uid'] = bench_df_main['uid'].str.replace('.', '')

    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    else:
        db_url = global_vars.DB_DROID_URL_WRITE

    bench_df_main = bench_df_main.drop_duplicates(subset=["uid"], keep="first", inplace=False)
    upsert_data_to_db(args, "uid", TEXT, bench_df_main, table_name, method="update")
    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    #
    # with engine.connect() as conn:
    #     metadata = db.MetaData()
    #     table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
    #
    #     query = table0.delete().where(table0.columns.bot_type == 'CLASSIC')
    #     conn.execute(query)
    #
    #     bench_df_main.to_sql(con=conn, name=table_name,
    #                          schema='public', method='multi',
    #                          chunksize=20000, if_exists='append', index=False)
    # engine.dispose()
    report_to_slack("{} : === Classic statistics monthly completed ===".format(str(dt.now())), args)

    print(f'Saved the data to {table_name} for CLASSIC!')

