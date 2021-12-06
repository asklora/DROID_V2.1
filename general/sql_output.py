from general.data_process import tuple_data
from general.slack import report_to_slack
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from multiprocessing import cpu_count as cpucount
from sqlalchemy.types import DATE, BIGINT, TEXT, INTEGER, BOOLEAN, Integer
from general.sql_process import db_read, db_write, get_debug_url, alibaba_db_url, DB_URL_ALIBABA_PROD
from pangres import upsert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
from general.date_process import dateNow, timestampNow, str_to_date
from general.sql_query import get_order_performance_by_ticker
from general.table_name import get_data_dividend_table_name, get_data_split_table_name, get_latest_price_table_name, get_orders_position_performance_table_name, get_orders_position_table_name, get_universe_consolidated_table_name, get_universe_table_name
from general.data_process import tuple_data

def execute_query(query, table=None):
    print(f"Execute Query to Table {table}")
    engine = create_engine(db_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        result = conn.execute(query)
    engine.dispose()
    return True

def truncate_table(table_name):
    query = f"truncate table {table_name}"
    data = execute_query(query, table=table_name)
    return True

def replace_table_datebase_ali(data, table_name):
    print(f"=== Replace Table to ALIBABA Database on Table {table_name} ===")
    engine = create_engine(alibaba_db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        extra = {'con': conn, 'index': False, 'if_exists': 'replace', 'method': 'multi', 'chunksize': 1000}
        data.to_sql(table_name, **extra)
    engine.dispose()
    return True


def insert_data_to_database(data, table, how="append"):
    print(f"=== Insert Data to Database on Table {table} ===")
    engine = create_engine(db_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            data.to_sql(
                table,
                if_exists=how,
                index=False,
                chunksize=20000,
                con=conn
            )
        engine.dispose()
        print(f"DATA INSERTED TO {table}")
    except Exception as ex:
        print(f"error: ", ex)

def upsert_data_to_database(data, table, primary_key, how="update", cpu_count=False, Text=False, Date=False, Int=False, Bigint=False, Bool=False, debug=False):
    try:
        print(f"=== Upsert Data to Database on Table {table} ===")
        data = data.drop_duplicates(subset=[primary_key], keep="first", inplace=False)
        data = data.dropna(subset=[primary_key])
        data = data.set_index(primary_key)
        if(Text):
            data_type={primary_key:TEXT}
        elif(Date):
            data_type={primary_key:DATE}
        elif(Int):
            data_type={primary_key:Integer}
        elif(Bigint):
            data_type={primary_key:BIGINT}
        elif(Bool):
            data_type={primary_key:BOOLEAN}
        else:
            data_type={primary_key:TEXT}
        
        if(cpu_count):
            engine = create_engine(db_write, pool_size=cpucount(), max_overflow=-1, isolation_level="AUTOCOMMIT")
        else:
            engine = create_engine(db_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
        upsert(engine=engine,
            df=data,
            table_name=table,
            if_row_exists=how,
            chunksize=20000,
            dtype=data_type)
        print(f"DATA UPSERT TO {table}")
        engine.dispose()
    except Exception as e:
        report_to_slack(f"===  ERROR IN UPSERT DB === Error : {e}")


def upsert_data_to_database_ali(data, table, primary_key, how="replace", cpu_count=False, Text=False, Date=False, Int=False, Bigint=False, Bool=False):
    print(f"=== Upsert Data to ALIBABA Database on Table {table} ===")
    data = data.drop_duplicates(subset=[primary_key], keep="first", inplace=False)
    data = data.dropna(subset=[primary_key])
    data = data.set_index(primary_key)
    if (Text):
        data_type = {primary_key: TEXT}
    elif (Date):
        data_type = {primary_key: DATE}
    elif (Int):
        data_type = {primary_key: Integer}
    elif (Bigint):
        data_type = {primary_key: BIGINT}
    elif (Bool):
        data_type = {primary_key: BOOLEAN}
    else:
        data_type = {primary_key: TEXT}

    if (cpu_count):
        engine = create_engine(alibaba_db_url, pool_size=cpucount(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(alibaba_db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engine,
           df=data,
           table_name=table,
           if_row_exists=how,
           chunksize=20000,
           dtype=data_type)
    print(f"DATA UPSERT TO {table}")
    engine.dispose()