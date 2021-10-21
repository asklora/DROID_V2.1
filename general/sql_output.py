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
    if(debug):
        try:
            engine = create_engine(get_debug_url(), pool_size=cpucount(), max_overflow=-1, isolation_level="AUTOCOMMIT")
            upsert(engine=engine,
                df=data,
                table_name=table,
                if_row_exists=how,
                chunksize=20000,
                dtype=data_type)
            print(f"DATA UPSERT TO {table}")
            engine.dispose()
        except Exception as e:
            report_to_slack(f"===  ERROR IN UPSERT TEST DB ===")
            report_to_slack(str(e))

def upsert_data_to_database_ali(data, table, primary_key, how="replace", cpu_count=False, Text=False, Date=False, Int=False,
                                Bigint=False, Bool=False):
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

def update_universe_consolidated_data_to_database(data, table):
    for index, row in data.iterrows():
        is_active = row["is_active"]
        updated = row["updated"]
        isin = row["isin"]
        cusip = row["cusip"]
        sedol = row["sedol"]
        consolidated_ticker = row["consolidated_ticker"]
        permid = row["permid"]
        uid = row["uid"]
        query = f"update {table} set "
        query += f"is_active={is_active}, "
        query += f"updated='{updated}', "
        query += f"isin='{isin}', "
        query += f"cusip='{cusip}', "
        query += f"sedol='{sedol}', "
        query += f"consolidated_ticker='{consolidated_ticker}', "
        query += f"permid='{permid}' "
        query += f"where uid='{uid}'"
        execute_query(query, table=table)
    return True

def update_fundamentals_score_in_droid_universe_daily(data, table):
    print(f"=== Update Data to Database on Table {table} ===")
    data = data[["ticker","mkt_cap"]]
    resultdict = data.to_dict("records")
    engine = db.create_engine(db_write)
    sm = sessionmaker(bind=engine)
    session = sm()
    metadata = db.MetaData(bind=engine)
    datatable = db.Table(table, metadata, autoload=True)
    stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam("ticker")).values({
        "mkt_cap": bindparam("mkt_cap"),
        "ticker": bindparam("ticker")

    })
    session.execute(stmt,resultdict)
    session.flush()
    session.commit()
    engine.dispose()
    print(f"DATA UPDATE TO {table}")

def fill_null_company_desc_with_ticker_name():
    query = f"update {get_universe_table_name()} set company_description=ticker_fullname "
    query += f"WHERE is_active=True and company_description is null or company_description = 'NA' or company_description = 'N/A';"
    data = execute_query(query, table=get_universe_table_name())
    return data

def fill_null_quandl_symbol():
    query = f"update {get_universe_table_name()} set quandl_symbol=split_part(ticker, '.', 1) "
    query += f"WHERE is_active=True and quandl_symbol is null and currency_code = 'USD'"
    data = execute_query(query, table=get_universe_table_name())
    return data

def update_consolidated_activation_by_ticker(ticker=None, is_active=True):
    query = f"update {get_universe_consolidated_table_name()} set is_active={is_active} "
    query += f"WHERE origin_ticker in {tuple_data(ticker)}"
    data = execute_query(query, table=get_universe_consolidated_table_name())
    return data
    
def delete_data_on_database(table, condition, delete_ticker=False):
    old_date = dateNow()
    query = f"delete from {table} where {condition} "
    if(delete_ticker):
        query += f" and ticker not in (select ticker from {get_universe_table_name()} where is_active=True)"
    data = execute_query(query, table=table)
    return data

def delete_old_dividends_on_database():
    old_date = dateNow()
    query = f"delete from {get_data_dividend_table_name()} where ex_dividend_date <= '{old_date}'"
    data = execute_query(query, table=get_data_dividend_table_name())
    return data

def clean_latest_price():
    table_name = get_latest_price_table_name()
    query = f"delete from {table_name} where ticker not in (select ticker from universe where is_active = True)"
    data = execute_query(query, table=table_name)
    return data

def update_capital_change(ticker):
    table_name = get_latest_price_table_name()
    query = f"update {table_name} set capital_change = null where ticker='{ticker}'"
    data = execute_query(query, table=table_name)
    return data

def update_universe_where_currency_code_null():
    table_name = get_universe_table_name()
    query = f"update {table_name} set is_active = false where currency_code is null"
    data = execute_query(query, table=table_name)
    return data

def update_all_data_by_capital_change(ticker, trading_day, capital_change, price, percent_change):
    table_name = get_data_split_table_name()
    data = pd.DataFrame({"ticker":[ticker], "data_type":["DSS"], "intraday_date":[trading_day], 
    "capital_change":[capital_change], "price":[price], "percent_change":[percent_change]}, index=[0])
    upsert_data_to_database(data, table_name, "ticker", how="update", cpu_count=True, Text=True)

    table_name = get_orders_position_table_name()
    query = f"update {table_name} set "
    query += f"entry_price = entry_price * {capital_change}, "
    query += f"max_loss_price = max_loss_price * {capital_change}, "
    query += f"target_profit_price = target_profit_price * {capital_change}, "
    query += f"share_num = round(share_num / {capital_change}) "
    query += f"where ticker = '{ticker}' and spot_date < '{trading_day}' and is_live = True;"
    data = execute_query(query, table=table_name)

    table_name = get_orders_position_performance_table_name()
    query = f"update {table_name} set "
    query += f"last_spot_price = last_spot_price * {capital_change}, "
    query += f"last_live_price = last_live_price * {capital_change}, "
    query += f"share_num = round(share_num * {capital_change}), "
    query += f"option_price = option_price * {capital_change}, "
    query += f"strike = strike * {capital_change}, "
    query += f"barrier = barrier * {capital_change}, "
    query += f"strike_2 = strike_2 * {capital_change} "
    query += f"from (select position_uid from {get_orders_position_table_name()} where ticker='{ticker}' and is_live = True) result "
    query += f"where {table_name}.created < '{trading_day}' and "
    query += f"{table_name}.order_id=result.order_id;"
    data = execute_query(query, table=table_name)

    table_name = get_orders_position_performance_table_name()
    performance = get_order_performance_by_ticker(ticker=ticker, trading_day=trading_day)
    performance = performance[["performance_uid", "order_summary", "position_uid", "order_uid"]]
    for index, row in performance.iterrows():
        row["order_summary"]["hedge_shares"] = round(row["order_summary"]["hedge_shares"] / capital_change)
    upsert_data_to_database(performance, table_name, "performance_uid", how="update", cpu_count=True, Text=True)

def update_ingestion_count(source='dsws', n_ingest=0, dsws=True):
    ''' record total number of ingestion of every month

    Parameters
    ----------
    source :        Str, data source name (default=dsws)
    n_ingest :      Int, num of data ingested (including Null returns)
    dsws :          Boolean, determine which EIKON-DSWS account used for ingestion (default=True)

    Returns
    -------
    append ingestion record df to Alibaba Prod DB "ingestion_count"

    '''
    ingest_dict = {'source': '{}{}'.format(source, 1-int(dsws)),       # default = #0 account
                   'date': str_to_date(dateNow()[:-2]+'01'),
                   'count': n_ingest,
                   'last_update': timestampNow(),
                   }
    print(f"=== [{n_ingest}] new ingestion from [{ingest_dict['source']}] ===")

    engine = create_engine(DB_URL_ALIBABA_PROD, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql('SELECT * FROM ingestion_count', conn)
        r = (data['source']==ingest_dict['source']) & (data['date']==ingest_dict['date'])
        if any(r):
            data.loc[r, 'count'] += ingest_dict['count']
            data.loc[r, 'last_update'] = ingest_dict['last_update']     # UTC time
        else:
            data = data.append(pd.DataFrame(ingest_dict, index=[0]))
        extra = {'con': conn, 'index': False, 'if_exists': 'replace', 'method': 'multi', 'chunksize': 1000}
        data.to_sql('ingestion_count', **extra)
    engine.dispose()
    return True

def update_ingestion_update_time(table_name, finish=False):
    ''' update last update time for tables

    Parameters
    ----------
    table_name : Str, Name of the table ingested
    finish :     Boolean, False on start of Ingestion, True on end of Ingestion

    '''

    last_update = timestampNow()
    print(f"=== [{table_name}] ingestion latest update_time: [{last_update}] (Finish = [{finish}]) ===")

    ingest_dict = {'table_name': table_name,
                   'last_update': last_update,
                   'finish': finish,
                   }

    engine = create_engine(DB_URL_ALIBABA_PROD, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql('SELECT * FROM ingestion_update_time', conn)
        r = (data['table_name']==ingest_dict['table_name'])
        if any(r):
            data.loc[r, 'last_update'] = ingest_dict['last_update']      # UTC time
            data.loc[r, 'finish'] = ingest_dict['finish']
        else:
            data = data.append(pd.DataFrame(ingest_dict, index=[0]))
        extra = {'con': conn, 'index': False, 'if_exists': 'replace', 'method': 'multi', 'chunksize': 1000}
        data.to_sql('ingestion_update_time', **extra)
    engine.dispose()
    return True
