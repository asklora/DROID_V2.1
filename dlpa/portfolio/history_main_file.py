import datetime as dt
import mimetypes
import smtplib
import ssl
import sys
import time
from datetime import datetime
import datetime as dt
from sqlalchemy import text

from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine, and_

import global_vars
from global_vars import no_top_models


def portfolio_maker(client_df, dates_list, args):
    if args.portfolio_period is None:
        sys.exit('Please specify portfolio period.!')

    #
    # if args.forward_date is None and args.spot_date is None:
    #     sys.exit("Please specify either of the forward_date or spot_date!")
    # if args.forward_date is not None and args.spot_date is not None:
    #     sys.exit("Please specify only forward_date or spot_date!")

    # if args.forward_date is not None:
    #     args.forward_date = datetime.strptime(f'{args.forward_date}', '%Y-%m-%d').date()
    dates_list[:] = [datetime.strptime(f'{x}', '%Y-%m-%d').date() for x in dates_list]



    # if args.spot_date is not None:
    #     args.spot_date = datetime.strptime(f'{args.spot_date}', '%Y-%m-%d').date()
    #
    #     if args.portfolio_period == 0:
    #         args.forward_date = args.spot_date + Week(1)
    #     elif args.portfolio_period == 1:
    #         args.forward_date = args.spot_date + BDay(1)

    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = args.production_table_name
    table_models = args.model_table_name

    # args.forward_date = datetime.strptime(f'{args.portfolio_year} {args.portfolio_week} {args.portfolio_day}',
    #                                         '%G %V %u').strftime('%Y-%m-%d')

    # args.forward_date_0 = str(args.forward_date)
    # args.forward_date_1 = args.forward_date.strftime('%Y-%m-%d')

    dates_list_0 = [str(x) for x in dates_list]
    # dates_list_1 = [x.strftime('%Y-%m-%d') for x in dates_list]
    #
    # dates_list_0 == dates_list_1
    # dates_list_1 = dates_list.strftime('%Y-%m-%d')


    if args.portfolio_period == 0:
        period = 'weekly'
    else:
        period = 'daily'

    # *************************************************************************************************
    # ****************************** Download the models **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table_models = db.Table(table_models, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table_models.columns.data_period, table_models.columns.when_created, table_models.columns.forward_date,
             table_models.columns.best_train_acc,
             table_models.columns.best_valid_acc,
             table_models.columns.model_type, table_models.columns.model_filename,
             table_models.columns.pc_number]).where(and_(
            table_models.columns.data_period == period,
            table_models.columns.forward_date.in_(dates_list_0)))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    models_df = pd.DataFrame(ResultSet)
    models_df.columns = columns_list
    if len(models_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))

    # if models_df.shape[1] is not None:
    #     models_df.columns = ['data_period', 'when_created', 'forward_date', 'best_train_acc', 'best_valid_acc',
    #                          'model_type',
    #                          'model_filename', 'pc_number']

    models_df.fillna(value=0, inplace=True)
    # *************************************************************************************************
    # ****************************** Download the stocks **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
             table0.columns.spot_date, table0.columns.index,
             table0.columns.ticker,
             table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
             table0.columns.model_filename]).where(and_(
            table0.columns.data_period == period, table0.columns.forward_date.in_(dates_list_0)))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    full_df.columns = columns_list

    full_df.fillna(value=0, inplace=True)

    # *************************************************************************************************
    # ****************************** Pick the top models **********************************************
    top_stocks = pd.DataFrame(columns=['data_period', 'forward_date', 'index', 'ticker',
       'predicted_quantile_1', 'spot_date', 'counts', 'signal_strength_mean'])

    for date_t in dates_list:
        models_df_t = models_df[models_df['forward_date'].dt.date == date_t]
        models_df_t = models_df_t.sort_values(by=['best_valid_acc'], ascending=False)
        models_df_t = models_df_t.loc[models_df_t.pc_number.isin(['PC1','PC2','PC3','PC4'])]

        top_models_list = models_df_t.model_filename.head(no_top_models)

        full_df_t = full_df[full_df['forward_date'] == str(date_t)]
        full_df_t = full_df_t.loc[full_df_t['model_filename'].isin(top_models_list)]
        # full_df3= full_df
        # *************************************************************************************************
        full_df_t = full_df_t[full_df_t.signal_strength_1 > full_df_t.signal_strength_1.quantile(1 - args.signal_threshold)]

        gb = full_df_t.groupby(
            ['data_period', 'forward_date', 'index', 'ticker', 'predicted_quantile_1', 'spot_date'])
        portfolio_1 = gb.size().to_frame(name='counts')
        portfolio_1 = (portfolio_1.join(gb.agg({'signal_strength_1': 'mean'}).rename(
            columns={'signal_strength_1': 'signal_strength_mean'})).reset_index())
        sorted_df = portfolio_1.sort_values(by=['counts', 'signal_strength_mean'], ascending=False)

        sorted_df = sorted_df[sorted_df.predicted_quantile_1 == 2]
        sorted_df.reset_index(drop=True, inplace=True)
        for index, row in client_df.iterrows():
            top_stocks = top_stocks.append(sorted_df[sorted_df['index'] == row['index_choice_id']][0:row['top_x']])
    return top_stocks


def get_dates_list_from_aws(args):
    f_date = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
    s_date = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
    start_date = f_date
    stop_date = s_date + dt.timedelta(weeks=args.max_period_num)

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')
    # args.forward_date_start = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
    # args.forward_date_stop = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
    #
    # args.forward_date_start_1 = args.forward_date_start.strftime('%Y-%m-%d')
    # args.forward_date_stop_1 = args.forward_date_stop.strftime('%Y-%m-%d')
    # start_date = args.forward_date_start
    # stop_date = args.forward_date_stop
    if args.portfolio_period == 0:
        period = 'weekly'
    else:
        period = 'daily'

    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.production_table_name, metadata, autoload=True, autoload_with=conn)

        # query = db.select(
        #     [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
        #      table0.columns.spot_date, table0.columns.index,
        #      table0.columns.ticker,
        #      table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
        #      table0.columns.model_filename]).where(and_(
        #     table0.columns.data_period == period, table0.columns.forward_date >= args.forward_date_start_1,
        #     table0.columns.forward_date <= args.forward_date_stop_1))

        query = db.select([table0.columns.data_period, table0.columns.forward_date]).distinct().where(and_(
            table0.columns.data_period == period, table0.columns.forward_date >= start_date,
            table0.columns.forward_date <= stop_date))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list

    if len(full_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))

    # if full_df.shape[1] is not None:
    #     full_df.columns = ['data_period', 'when_created', 'forward_date', 'spot_date', 'index', 'ticker',
    #                        'predicted_quantile_1',
    #                        'signal_strength_1', 'model_filename']

    full_df.fillna(value=0, inplace=True)

    a = full_df['forward_date'].unique()
    return a


def get_prices_from_aws(args):
    f_date = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
    s_date = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
    start_date = f_date + dt.timedelta(weeks=-4)
    stop_date = s_date + dt.timedelta(weeks=args.max_period_num + 4)

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.master_ohlcv_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            ['*']).where(and_(
             table0.columns.trading_day >= start_date,
            table0.columns.trading_day <= stop_date, table0.columns.ticker.in_(args.tickers_list)))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have inferred data.")
    full_df.columns = columns_list

    return full_df


def signal_calculator(row,args):
    if row['counts'] > args.signal_counts_upper_limit:
        val = 2
    elif row['counts'] < args.signal_counts_lower_limit:
        val = 0
    else:
        val = 1
    return val

#
# def get_prices_from_aws(args):
#     f_date = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
#     s_date = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
#     start_date = f_date + dt.timedelta(weeks=-4)
#     stop_date = s_date + dt.timedelta(weeks=4)
#
#     start_date = start_date.strftime('%Y-%m-%d')
#     stop_date = stop_date.strftime('%Y-%m-%d')
#
#     db_url = global_vars.DB_PROD_URL_READ
#     engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
#
#     with engine.connect() as conn:
#         metadata = db.MetaData()
#         table0 = db.Table(args.master_ohlcv_table_name, metadata, autoload=True, autoload_with=conn)
#
#         query = db.select(
#             ['*']).where(and_(
#              table0.columns.trading_day >= start_date,
#             table0.columns.trading_day <= stop_date))
#
#         ResultProxy = conn.execute(query)
#         ResultSet = ResultProxy.fetchall()
#         columns_list = conn.execute(query).keys()
#
#     full_df = pd.DataFrame(ResultSet)
#
#     if len(full_df) == 0:
#         sys.exit("We don't have inferred data.")
#     full_df.columns = columns_list
#
#     return full_df
#
#
# def get_dates_list_from_aws(args):
#     f_date = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
#     s_date = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
#     start_date = f_date
#     stop_date = s_date
#
#     start_date = start_date.strftime('%Y-%m-%d')
#     stop_date = stop_date.strftime('%Y-%m-%d')
#     # args.forward_date_start = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
#     # args.forward_date_stop = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()
#     #
#     # args.forward_date_start_1 = args.forward_date_start.strftime('%Y-%m-%d')
#     # args.forward_date_stop_1 = args.forward_date_stop.strftime('%Y-%m-%d')
#     # start_date = args.forward_date_start
#     # stop_date = args.forward_date_stop
#     if args.portfolio_period == 0:
#         period = 'weekly'
#     else:
#         period = 'daily'
#
#     db_url = global_vars.DB_PROD_URL_READ
#     engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
#
#     with engine.connect() as conn:
#         metadata = db.MetaData()
#         table0 = db.Table(args.production_table_name, metadata, autoload=True, autoload_with=conn)
#
#         # query = db.select(
#         #     [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
#         #      table0.columns.spot_date, table0.columns.index,
#         #      table0.columns.ticker,
#         #      table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
#         #      table0.columns.model_filename]).where(and_(
#         #     table0.columns.data_period == period, table0.columns.forward_date >= args.forward_date_start_1,
#         #     table0.columns.forward_date <= args.forward_date_stop_1))
#
#         query = db.select([table0.columns.data_period, table0.columns.forward_date]).distinct().where(and_(
#             table0.columns.data_period == period, table0.columns.forward_date >= start_date,
#             table0.columns.forward_date <= stop_date))
#
#         ResultProxy = conn.execute(query)
#         ResultSet = ResultProxy.fetchall()
#         columns_list = conn.execute(query).keys()
#
#     full_df = pd.DataFrame(ResultSet)
#     full_df.columns = columns_list
#
#     if len(full_df) == 0:
#         sys.exit(str(
#             "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
#
#     # if full_df.shape[1] is not None:
#     #     full_df.columns = ['data_period', 'when_created', 'forward_date', 'spot_date', 'index', 'ticker',
#     #                        'predicted_quantile_1',
#     #                        'signal_strength_1', 'model_filename']
#
#     full_df.fillna(value=0, inplace=True)
#
#     a = full_df['forward_date'].unique()
#     return a


def get_indices():
    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    table_name = 'indices'
    with engine.connect() as conn:
        metadata = db.MetaData()

        query = 'select * from '
        query = query + table_name

        ResultProxy = conn.execute(text(query))
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(text(query)).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have inferred data.")
    full_df.columns = columns_list
    return full_df
