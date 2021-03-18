from general.table_name import get_calendar_table_name, get_client_table_name
from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine

import global_vars
from general.sql_query import read_query, tuple_data

def add_client_to_aws(Client_name, index_list, no_desired_tickers):
    to_aws = pd.DataFrame(index_list, columns=["index"])
    to_aws["client_name"] = Client_name
    to_aws["top_X"] = no_desired_tickers

    cols = to_aws.columns.tolist()
    cols = [cols[1]] + [cols[0], cols[2]]
    to_aws = to_aws[cols]

    engine = create_engine(global_vars.DB_PROD_URL_WRITE, pool_size=cpu_count(), max_overflow=-1,
                           isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        to_aws.to_sql(con=conn, name=global_vars.client_table_name, if_exists="append", index=False)

def get_client_information(client=None):
    table_name = get_client_table_name()
    query = f"select * from {table_name} "
    if(type(client) != type(None)):
        query += f"uid in {tuple_data(client)}"
    data = read_query(query, table_name, cpu_counts=True)
    return data

# def get_client_information():
#     engine = db.create_engine(global_vars.DB_PROD_URL_READ, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

#     table0 = get_calendar_table_name

#     with engine.connect() as conn:
#         metadata = db.MetaData()
#         table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

#         query = db.select([table0.columns.client_code_id, table0.columns.index_choice_id, table0.columns.top_x]).where(
#             table0.columns.client_code_id == args.client_name)

#         ResultProxy = conn.execute(query)
#         ResultSet = ResultProxy.fetchall()
#         columns_list = conn.execute(query).keys()


#     full_df = pd.DataFrame(ResultSet)
#     full_df.columns = columns_list
#     # if full_df.shape[1] == 3:
#     #     full_df.columns = ["client_name", "index", "top_X"]

#     return full_df
