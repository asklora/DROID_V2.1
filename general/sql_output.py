import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.types import DATE, BIGINT, TEXT, INTEGER, BOOLEAN
from general.sql_process import db_read, db_write
from pangres import upsert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
from general.sql_query import read_query
from general.table_name import get_universe_table_name

def insert_data_to_database(data, table, how="replace"):
    print("=== Insert Data to Database on Table {table} ===")
    # engine = create_engine(db_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    # try:
    #     with engine.connect() as conn:
    #         data.to_sql(
    #             table,
    #             if_exists=how,
    #             index=False,
    #             chunksize=20000,
    #             con=conn
    #         )
    #     engine.dispose()
    #     print("DATA INSERTED TO " + table)
    # except Exception as ex:
    #     print("error: ", ex)

def upsert_data_to_database(data, table, primary_key, how="update", Text=False, Date=False, Int=False, Bigint=False, Bool=False):
    print("=== Upsert Data to Database on Table {table} ===")
    data = data.drop_duplicates(subset=[primary_key], keep="first", inplace=False)
    data = data.set_index(primary_key)
    if(Text):
        data_type={primary_key:TEXT}
    elif(Date):
        data_type={primary_key:DATE}
    elif(Int):
        data_type={primary_key:INTEGER}
    elif(Bigint):
        data_type={primary_key:BIGINT}
    elif(Bool):
        data_type={primary_key:BOOLEAN}
    else:
        data_type={primary_key:TEXT}
    engines_droid = create_engine(db_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=data,
           table_name=table,
           if_row_exists=how,
           chunksize=20000,
           dtype=data_type)
    print("DATA UPSERT TO " + table)
    engines_droid.dispose()

def update_fundamentals_score_in_droid_universe_daily(data, table):
    print("=== Update Data to Database on Table {table} ===")
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
    print("DATA UPDATE TO " + table)

def fill_null_company_desc_with_ticker_name():
    query = f"update {get_universe_table_name()} set company_description=ticker_fullname "
    query += f"WHERE is_active=True and company_description is null or company_description = 'NA' or company_description = 'N/A';"
    data = read_query(query, table=get_universe_table_name())
    return data

def fill_null_quandl_symbol():
    query = f"update {get_universe_table_name()} set quandl_symbol=split_part(ticker, '.', 1) "
    query += f"WHERE is_active=True and quandl_symbol is null and currency_code = 'USD'"
    data = read_query(query, table=get_universe_table_name())
    return data

def delete_old_dividends_on_database():
    old_date = dateNow()
    query = f"delete from {dividens_table} where ex_dividend_date < '{old_date}'"
    data = read_query(query, table=get_universe_table_name())
    return data