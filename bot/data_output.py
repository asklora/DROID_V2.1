import calendar
import datetime
import time
from argparse import Namespace
from datetime import datetime
from multiprocessing import cpu_count

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine, and_
import sqlalchemy as db

import global_vars
import uuid
from pangres import upsert
from sqlalchemy.types import Date, BIGINT, TEXT

def gpu_mac_address(args):
    if str(hex(uuid.getnode())) == '0xd861c24963':
        args.pc_number = "Iman"
    elif str(hex(uuid.getnode())) == '0xc9d92c510e5':
        args.pc_number = "PC4"
    elif str(hex(uuid.getnode())) == '0xac1f6b153eba':
        args.pc_number = "PC3"
    elif str(hex(uuid.getnode())) == '0x309c23270a79':
        args.pc_number = "PC2"
    elif str(hex(uuid.getnode())) == '0x49226d8eb81':
        args.pc_number = "PC1"
    elif str(hex(uuid.getnode())) == '0xb06ebf5cd358':
        args.pc_number = "Stephen"
    else:
        args.pc_number = "unknown"


def write_to_aws_model_data(hypers, args):
    # MODEL SUMMARIES
    # This function is used for outputting the model data for each run.

    rr = pd.DataFrame(hypers.items())
    rr = rr.transpose()
    rr.columns = rr.iloc[0]
    rr = rr.drop(rr.index[0])

    rr.insert(loc=0, column='when_created', value=datetime.fromtimestamp(time.time()))
    rr.insert(loc=1, column='start_date', value=args.start_date)
    rr.insert(loc=2, column='forward_date', value=args.forward_date)
    rr.insert(loc=3, column='valid_error', value=args.valid_error)
    rr.insert(loc=4, column='test_error', value=args.test_error)
    rr.insert(loc=5, column='valid_size', value=args.valid_size)
    rr.insert(loc=6, column='test_size', value=args.test_size)
    rr.insert(loc=7, column='model_type', value=args.model_type)
    rr['spot_date'] = args.spot_date
    rr['pc_number'] = args.pc_number
    rr['model_filename'] = args.model_filename




    # rr = rr[args.aws_columns_list]
    rr = rr.infer_objects()
    # datetime.strptime(f'{y} {w} {d}', '%G %V %u')
    # ************************** Cleaning the data to look better. ************************************
    rr = rr.round(
        {'bagging_fraction': 4, 'feature_fraction': 4, 'min_gain_to_split': 4, 'valid_error': 4, 'test_error': 4})

    rr.reset_index(drop=True, inplace=True)

    if args.go_production:
        db_url = global_vars.DB_PROD_URL_WRITE
        # if writing to production_data table, i.e., write out the stocks - keep in "live"
        table_name = args.production_lgbm_model_data_table_name
        engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            rr.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
        engine.dispose()

        print(f'Saved the data to {table_name}!')
    else:
        db_url = global_vars.DB_TEST_URL_WRITE# if writing to production_data table, i.e., write out the stocks - keep in "live"
        table_name = args.test_lgbm_model_data_table_name  # this is for sample testing,....
        engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            rr.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
        engine.dispose()

        print(f'Saved the data to {table_name}!')

def write_to_aws_stock_data(main_pred, args):
    # Stock SUMMARIES
    # This function is used for outputting the stock data for top models.



    main_pred.insert(loc=0, column='when_created', value=datetime.fromtimestamp(time.time()))
    main_pred['forward_date'] = args.forward_date
    main_pred['spot_date'] = args.spot_date
    # main_pred['used_period'] = args.period
    main_pred = main_pred.round({'volatility': 4})

    main_pred.reset_index(drop=True, inplace=True)

    if args.go_production:
        db_url = global_vars.DB_DROID_URL_WRITE # if writing to production_data table, i.e., write out the stocks - keep in "live"
        table_name = args.production_lgbm_stock_table_name
        engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
        engine.dispose()

    else:
        db_url = global_vars.DB_DROID_URL_WRITE
        table_name = args.test_lgbm_stock_table_name  # this is for sample testing,....
        engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
        engine.dispose()

    print(f'Saved the data for {args.forward_date} to {table_name}!')

def write_to_aws_sltp_production(main_pred, args):
    # main_pred.insert(loc=0, column='when_created', value=datetime.fromtimestamp(time.time()))
    # main_pred['forward_date'] = args.forward_date
    # main_pred['spot_date'] = args.spot_date
    # # main_pred['used_period'] = args.period
    # main_pred = main_pred.round({'volatility': 4})

    # main_pred.reset_index(drop=True, inplace=True)

    db_url = global_vars.DB_DROID_URL_WRITE # if writing to production_data table, i.e., write out the stocks - keep in "live"
    table_name = args.sltp_production_table_name

    main_pred = main_pred.drop_duplicates(subset=["uid"], keep="first", inplace=False)
    main_pred = main_pred.set_index("uid")
    #print(main_pred)
    dtype={
        "uid":TEXT,
    }
    engines_droid = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid, df=main_pred, chunksize=10000, table_name=table_name, if_row_exists="update", dtype=dtype)
    #print("DATA INSERTED TO " + table_name)
    engines_droid.dispose()

    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    # with engine.connect() as conn:
    #     main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
    # engine.dispose()

    # print(f'Saved the data for {args.forward_date} to {table_name}!')


def write_to_aws_executive_production(main_pred, args):
    if args.debug_mode or args.option_maker_history_full or args.option_maker_history_full_ucdc:
        db_url = global_vars.DB_TEST_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    else:
        db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"

    table_name = args.executive_production_table_name

    if args.option_maker_history_full or args.option_maker_history_full_ucdc:
        split_no = 30
    elif args.option_maker_history or args.option_maker_history_ucdc:
        split_no = 90
    else:
        split_no = 10

    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    if args.option_maker_history_full:
        table_name = table_name + '_full'
    elif args.option_maker_history_full_ucdc:
        table_name = args.executive_production_ucdc_table_name + '_full'
    elif args.option_maker_history_ucdc or args.option_maker_daily_ucdc or args.option_maker_live_ucdc:
        table_name = args.executive_production_ucdc_table_name
    else:
        table_name = table_name

    if args.add_inferred:
        flag = 1
    else:
        flag = 0

    if args.modified:
        table_name = table_name + '_mod'
        flag_modified = 1
    else:
        flag_modified = 0

    main_pred = main_pred.drop_duplicates(subset=["uid"], keep="first", inplace=False)
    main_pred = main_pred.set_index("uid")
    dtype={
        "uid":TEXT,
    }
    engines_droid = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid, df=main_pred, chunksize=10000, table_name=table_name, if_row_exists="update", dtype=dtype)
    engines_droid.dispose()

    # with engine.connect() as conn:
    #     try:
    #         metadata = db.MetaData()
    #         table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

    #         if args.option_maker_history or args.option_maker_history_full or args.option_maker_history_ucdc or args.option_maker_history_full_ucdc:
    #             query = table0.delete().where(and_(table0.columns.inferred == flag,
    #                                                table0.columns.spot_date >= args.start_date,
    #                                                table0.columns.spot_date <= args.end_date,
    #                                                table0.columns.modified == flag_modified))

    #             conn.execute(query)
    #             # chunksize = int(len(main_pred) / split_no)
    #             # if (chunksize == 0):
    #             #     chunksize = 1
    #             main_pred.to_sql(con=conn, name=table_name, schema='public', method='multi',
    #                              chunksize=20000, if_exists='append', index=False)
    #         else:
    #             # query = table0.delete().where(and_(table0.columns.spot_date >= args.start_date,
    #             #                                    table0.columns.spot_date <= args.end_date,
    #             #                                    table0.columns.modified == flag_modified,
    #             #                                    table0.columns.ticker.in_(args.tickers_list)))
    #             # conn.execute(query)
    #             main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
    #     except Exception as e:
    #         if len(main_pred) > split_no:
    #             #chunksize = int(len(main_pred) / split_no)
    #             main_pred.to_sql(con=conn, name=table_name, schema='public', method='multi',
    #                              chunksize=20000, if_exists='replace', index=False)
    #         else:
    #             main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='replace', index=False)

    #     # query = table0.delete().where(table0.columns.spot_date >= args.start_date)

    # engine.dispose()

    # print(f'Saved the data to {table_name}!')

# def upsert_data_to_db(args, index, index_type, data, table_name, method="update"):
#     if args.debug_mode:
#         db_url = global_vars.DB_TEST_URL_WRITE
#     else:
#         db_url = global_vars.DB_DROID_URL_WRITE

#     if args.modified:
#         table_name = table_name + "_mod"
#     print(f'=== Upsert data {table_name} to database ===')
#     data = data.set_index(index)
#     print(data)
#     dtype={
#         index:index_type,
#     }
#     engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     upsert(engine=engine,
#             df=data,
#             chunksize=10000,
#             table_name=table_name,
#             if_row_exists=method,
#             dtype=dtype)
#     print("DATA INSERTED TO " + table_name)
#     engine.dispose()

# def upsert_data_to_database(args, index, index_type, data, method="update"):
#     if args.debug_mode or args.option_maker_history_full or args.option_maker_history_full_ucdc:
#         db_url = global_vars.DB_TEST_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
#     else:
#         db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"

#     table_name = args.executive_production_table_name

#     if args.option_maker_history_full:
#         table_name = table_name + '_full'
#     elif args.option_maker_history_full_ucdc:
#         table_name = args.executive_production_ucdc_table_name + '_full'
#     elif args.option_maker_history_ucdc or args.option_maker_daily_ucdc or args.option_maker_live_ucdc:
#         table_name = args.executive_production_ucdc_table_name

#     if args.modified:
#         table_name = table_name + '_mod'
#     # if args.option_maker_history_full or args.option_maker_history_full_ucdc or args.option_maker_history_ucdc or args.option_maker_history or args.option_maker_history_ucdc:
#     #     print(f'=== Replace data {table_name} to database ===')
#     #     engines_droid = create_engine(db_url)
#     #     with engines_droid.connect() as conn:
#     #         data.to_sql( table_name, con=conn, schema=None, if_exists='replace', index=False, index_label=None)
#     #     print("DATA INSERTED TO " + table_name)
#     # else:
#     print(f'=== Upsert data {table_name} to database ===')
#     data = data.set_index(index)
#     print(data)
#     dtype={
#         index:index_type,
#     }
#     engines_droid = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     upsert(engine=engines_droid, df=data, chunksize=10000, table_name=table_name, if_row_exists=method, dtype=dtype)
#     print("DATA INSERTED TO " + table_name)
#     engines_droid.dispose()

def write_to_aws_executive_production_null(main_pred, args):
    if args.debug_mode or args.option_maker_history_full or args.option_maker_history_full_ucdc:
        db_url = global_vars.DB_TEST_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    else:
        db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"

    table_name = args.executive_production_table_name

    if args.option_maker_history_full:
        table_name = table_name + '_full'
    elif args.option_maker_history_full_ucdc:
        table_name = args.executive_production_ucdc_table_name + '_full'
    elif args.option_maker_history_ucdc or args.option_maker_daily_ucdc or args.option_maker_live_ucdc:
        table_name = args.executive_production_ucdc_table_name
    else:
        table_name = table_name

    if args.add_inferred:
        flag = 1
    else:
        flag = 0

    if args.modified:
        table_name = table_name + '_mod'
        flag_modified = 1
    else:
        flag_modified = 0

    #print(f'=== Upsert data {table_name} to database ===')
    main_pred = main_pred.drop_duplicates(subset=["uid"], keep="first", inplace=False)
    main_pred = main_pred.set_index("uid")
    #print(main_pred)
    dtype={
        "uid":TEXT,
    }

    engines_droid = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid, df=main_pred, chunksize=10000, table_name=table_name, if_row_exists="update", dtype=dtype)
    #print("DATA INSERTED TO " + table_name)
    engines_droid.dispose()

    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # with engine.connect() as conn:
    #     metadata = db.MetaData()
    #     table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

    #     if args.small_dataset:
    #         if args.exec_ticker is not None or args.exec_index is not None:
    #             query = table0.delete().where(and_(table0.columns.event == None, table0.columns.inferred == flag,
    #                                                table0.columns.modified == flag_modified,
    #                                                table0.columns.ticker.in_(args.tickers_list)))
    #         else:
    #             query = table0.delete().where(and_(table0.columns.event == None, table0.columns.inferred == flag,
    #                                                table0.columns.modified == flag_modified))
    #     else:
    #         if args.exec_ticker is not None or args.exec_index is not None:
    #             query = table0.delete().where(and_(table0.columns.event == None, table0.columns.inferred == flag,
    #                                                table0.columns.modified == flag_modified,
    #                                                table0.columns.ticker.in_(args.tickers_list),
    #                                                table0.columns.spot_date == main_pred.spot_date.max()))
    #         else:
    #             query = table0.delete().where(and_(table0.columns.event == None, table0.columns.inferred == flag,
    #                                                table0.columns.modified == flag_modified,
    #                                                table0.columns.spot_date == main_pred.spot_date.max()))

    #     conn.execute(query)
    #     if args.debug_mode or args.small_dataset:
    #         main_pred.to_sql(con=conn, name=table_name, schema='public', if_exists='append', index=False)
    #     else:
    #         # chunksize = (int(len(main_pred) / 100))
    #         # if (chunksize == 0):
    #         #     chunksize = 1
    #         main_pred.to_sql(con=conn, name=table_name, schema='public', method='multi',
    #                          chunksize=20000, if_exists='append', index=False)
    # engine.dispose()

    # print(f'Saved the data to {table0}!')