import sys
from multiprocessing import cpu_count
import datetime

import pandas as pd
import sqlalchemy as db
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from pangres import upsert
from sqlalchemy import create_engine, and_
from sqlalchemy.types import Date, BIGINT, TEXT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
import global_vars


def get_tickers_list_from_aws(args):
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(global_vars.droid_universe_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker]).where(table0.columns.is_active == True)

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list

    if len(full_df) == 0:
        sys.exit("We don't have inferred data.")
    print(f"Downloaded tickers list from {table0}.")

    return full_df

def get_new_tickers_list_from_aws(args):
    db_url = global_vars.DB_DROID_URL_READ
    print("Get Data From DROID")
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        # query = f"select du.ticker "
        # query += f"from {global_vars.droid_universe_table_name} du "
        # query += f"left join {global_vars.sltp_production_table_name} cb on du.ticker = cb.ticker and cb.spot_date>='2017-10-15' "
        # query += f"where du.is_active=True "
        # query += f"group by du.ticker "
        # query += f"having count(cb.spot_date) <= 1000;"
        query = f"select du.ticker, coalesce(result1.count_data, 0), coalesce(result2.count_price, 0) "
        query += f"from {global_vars.droid_universe_table_name} du "
        query += f"left join (select ticker, coalesce(count(cb.spot_date), 0) as count_data from {global_vars.sltp_production_table_name} cb where cb.spot_date>='{args.start_date}' group by cb.ticker) result1 on result1.ticker=du.ticker "
        query += f"left join (select ticker, coalesce(count(mo.trading_day), 0) * 2 as count_price from {global_vars.tac_data_table_name} mo where mo.trading_day>='{args.start_date2}' group by mo.ticker) result2 on result2.ticker=du.ticker "
        query += f"where du.is_active=True and "
        query += f"coalesce(result1.count_data, 0) < coalesce(result2.count_price, 0) - 6 "
        query += f"order by count_data;"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")

    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    # with engine.connect() as conn:
    #     metadata = db.MetaData()
    #     table0 = db.Table(global_vars.droid_universe_table_name, metadata, autoload=True, autoload_with=conn)

    #     query = db.select([table0.columns.ticker]).where(table0.columns.is_active == True)

    #     ResultProxy = conn.execute(query)
    #     ResultSet = ResultProxy.fetchall()
    #     columns_list = ResultProxy.keys()
    # engine.dispose()

    # full_df = pd.DataFrame(ResultSet)
    # full_df.columns = columns_list

    # if len(data) == 0:
    #     sys.exit("We don't have inferred data.")
    # print(f"Downloaded tickers list from {table0}.")

    return data

def get_tickers_list_classic_from_aws(args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(global_vars.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.ticker.distinct()])

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list

    if len(full_df) == 0:
        sys.exit("We don't have inferred data.")
    print(f"Downloaded tickers list from {table0}.")

    return full_df

def tac_data_download_by_ticker(args):
    # The tac data is the TRI adjusted prices.
    start_date = args.start_date
    stop_date = args.end_date
    # if args.history:
    #     start_date = start_date - relativedelta(years=4)
    # else:
    #     start_date = start_date - BDay(30)
    start_date = start_date - relativedelta(years=4)

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    table_name = global_vars.droid_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.tac_data_table_name, metadata, autoload=True, autoload_with=conn)
        query = db.select(
            ['*']).where(and_(table0.columns.trading_day >= start_date,
                              table0.columns.trading_day <= stop_date,
                              table0.columns.ticker.in_(args.tickers_list)))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        query2 = db.select([table0.columns.ticker]).where(table0.columns.is_active == True)

        ResultProxy2 = conn.execute(query2)
        ResultSet2 = ResultProxy2.fetchall()
        columns_list2 = ResultProxy2.keys()

    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have tac data.")
    full_df.columns = columns_list

    tickers = pd.DataFrame(ResultSet2)
    tickers.columns = columns_list2

    full_df = full_df[full_df.ticker.isin(tickers.ticker)]

    return full_df

def tac_data_download(args):
    # The tac data is the TRI adjusted prices.
    start_date = args.start_date
    stop_date = args.end_date
    # if args.history:
    #     start_date = start_date - relativedelta(years=4)
    # else:
    #     start_date = start_date - BDay(30)
    start_date = start_date - relativedelta(years=4)

    start_date = start_date.strftime('%Y-%m-%d')
    stop_date = stop_date.strftime('%Y-%m-%d')

    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    table_name = global_vars.droid_universe_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.tac_data_table_name, metadata, autoload=True, autoload_with=conn)
        query = db.select(['*']).where(and_(table0.columns.trading_day >= start_date,
                              table0.columns.trading_day <= stop_date))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()

        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        query2 = db.select([table0.columns.ticker]).where(table0.columns.is_active == True)

        ResultProxy2 = conn.execute(query2)
        ResultSet2 = ResultProxy2.fetchall()
        columns_list2 = ResultProxy2.keys()

    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have tac data.")
    full_df.columns = columns_list

    tickers = pd.DataFrame(ResultSet2)
    tickers.columns = columns_list2

    full_df = full_df[full_df.ticker.isin(tickers.ticker)]

    return full_df


def download_holidays(args):
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.holidays_table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select()

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have holidays data.")
    full_df.columns = columns_list

    return full_df


def download_production_sltp(args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select()

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have sltp data.")
    full_df.columns = columns_list

    return full_df


def download_production_sltp_null(args):
    month_to_exp = tuple(args.month_horizon)
    month_to_exp = str(month_to_exp).replace(",)", ")")
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    print("Get Data From DROID")
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {args.sltp_production_table_name} where event is null and "
        query += f"ticker in (select du.ticker from {args.droid_universe_table_name} du where du.is_active=True) and "
        query += f"month_to_exp in {month_to_exp}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")

    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # with engine.connect() as conn:
    #     metadata = db.MetaData()
    #     table0 = db.Table(, metadata, autoload=True, autoload_with=conn)
    #     query = table0.select().where(table0.columns.event == None)
    #     ResultProxy = conn.execute(query)
    #     ResultSet = ResultProxy.fetchall()
    #     columns_list = ResultProxy.keys()
    # engine.dispose()
    # full_df = pd.DataFrame(ResultSet)
    if len(data) == 0:
        sys.exit("We don't have sltp data.")
    #data.columns = columns_list
    return data


# def upsert_data_to_database(db_url, index, index_type, table_name, data, how="update"):
#     print(f'=== Update {table_name} to database ===')
#     data = data.set_index(index)
#     print(data)
#     dtype = {
#         index: index_type,
#     }
#     engines_droid = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     upsert(engine=engines_droid,
#            df=data,
#            chunksize=20000,
#            table_name=table_name,
#            if_row_exists=how,
#            dtype=dtype)
#     print("DATA INSERTED TO " + table_name)
#     engines_droid.dispose()


def write_to_aws_sltp_production(main_pred, args):
    print("Inserting Data to Database")
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_WRITE
    else:
        db_url = global_vars.DB_DROID_URL_WRITE

    upsert_data_to_database(db_url, "uid", TEXT, args.sltp_production_table_name, main_pred, how="ignore")
    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # if args.history:
    #
    #     with engine.connect() as conn:
    #         metadata = db.MetaData()
    #         table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)
    #
    #         query = table0.delete()
    #         # else:
    #             # query = table0.delete().where(table0.columns.spot_date >= args.start_date - BDay(3))
    #
    #         # query = table0.delete().where(table0.columns.spot_date >= args.start_date)
    #         conn.execute(query)
    #     engine.dispose()
    #     with engine.connect() as conn:
    #         main_pred.to_sql(con=conn, name=args.sltp_production_table_name, schema='public', method='multi',
    #                          chunksize=int(len(main_pred) / 15), if_exists='append', index=False)
    #     engine.dispose()
    #
    # else:
    #     upsert_data_to_database(db_url, "uid", TEXT, args.sltp_production_table_name, main_pred, how="ignore")
    #print(f'Saved the data to {table0}!')

def write_to_aws_sltp_production_null(main_pred, args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_WRITE
    else:
        db_url = global_vars.DB_DROID_URL_WRITE

    upsert_data_to_database(db_url, "uid", TEXT, args.sltp_production_table_name, main_pred, how="update")

    print(f'Saved the data to {args.sltp_production_table_name}!')

    # db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
    # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # with engine.connect() as conn:
    #     metadata = db.MetaData()
    #     table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)
    #
    #     query = table0.delete().where(table0.columns.event == None)
    #
    #     conn.execute(query)
    #     main_pred.to_sql(con=conn, name=args.sltp_production_table_name, schema='public', method='multi',
    #                      chunksize=int(len(main_pred) / 15), if_exists='append', index=False)
    # engine.dispose()
    #
    # print(f'Saved the data to {table0}!')


def download_production_sltp_max_date(args):
    # This is done to find the last run of live mode.
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select().order_by(table0.columns.spot_date.desc()).limit(1)
        # where(table0.columns.spot_date == max(table0.columns.spot_date))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()
    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have sltp data.")
    full_df.columns = columns_list

    return full_df


def benchmark_data_download(args):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)
        #
        # if args.monthly:
        #     query = table0.select().where(and_(table0.columns.event != None,
        #                                        table0.columns.spot_date >= datetime.date.today()
        #                                        - relativedelta(months=args.monthly_horizon + 3)))
        # else:
        #     query = table0.select().where(and_(table0.columns.event != None,
        #                                        table0.columns.spot_date >= datetime.date.today()
        #                                        - relativedelta(years=args.lookback_horizon)))
        query = table0.select().where(and_(table0.columns.event != None,
                                           table0.columns.month_to_exp.in_(args.month_horizon),
                                           table0.columns.spot_date >= datetime.date.today()
                                           - relativedelta(months=args.lookback_horizon)))
        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have sltp data.")
    full_df.columns = columns_list

    return full_df


# def write_to_aws_merge_latest_price(main_pred, args):
#     db_url = global_vars.DB_DROID_URL_WRITE  # if writing to production_data table, i.e., write out the stocks - keep in "live"
#     engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
#     with engine.connect() as conn:
#         main_pred.to_sql('temp_table', engine, if_exists='replace')

#         query = """
#             UPDATE latest_price_updates AS f
#             SET classic_vol = t.classic_vol
#             FROM temp_table AS t
#             WHERE f.ticker = t.ticker
#         """

#         conn.execute(query)

#     engine.dispose()

#     print(f'Update the data in latest_price_updates!')

def write_to_aws_merge_latest_price(result, args):
    if not args.debug_mode:
        db_url = global_vars.DB_DROID_URL_WRITE
        resultdict = result.to_dict('records')
        engine = db.create_engine(db_url)
        sm = sessionmaker(bind=engine)
        session = sm()

        metadata = db.MetaData(bind=engine)

        datatable = db.Table("latest_price_updates", metadata, autoload=True)
        stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam('ticker')).values({
            'classic_vol': bindparam('classic_vol'),
            'ticker': bindparam('ticker')

        })
        session.execute(stmt,resultdict)

        session.flush()
        session.commit()
        engine.dispose()

        print(f'Update the data in latest_price_updates!')


def download_production_classic_max_date(args):
    print("Get Classic Max Date")
    if args.history:
        return 0
    # This is done to find the last run of live mode.
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_READ
    else:
        db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.sltp_production_table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select().order_by(table0.columns.spot_date.desc()).where(and_(table0.columns.ticker.in_(args.tickers_list),
                                                                                     table0.columns.month_to_exp.in_(args.month_horizon)))

        # query = table0.select().order_by(table0.columns.spot_date.desc()).limit(1)
        # where(table0.columns.spot_date == max(table0.columns.spot_date))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()
    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit("We don't have sltp data.")
    full_df.columns = columns_list

    temp = full_df[['ticker', 'spot_date']].groupby('ticker').agg({'spot_date': ['max']})

    return temp

def upsert_data_to_db(args, index, index_type, data, table_name, method="update"):
    if args.debug_mode:
        db_url = global_vars.DB_TEST_URL_WRITE
    else:
        db_url = global_vars.DB_DROID_URL_WRITE

    print(f'=== Upsert data {table_name} to database ===')
    data = data.set_index(index)
    print(data)
    dtype={
        index:index_type,
    }
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engine,
            df=data,
            chunksize=10000,
            table_name=table_name,
            if_row_exists=method,
            dtype=dtype)
    print("DATA INSERTED TO " + table_name)
    engine.dispose()