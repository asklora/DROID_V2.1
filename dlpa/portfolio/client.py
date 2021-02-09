from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine

from dlpa import global_vars


def add_client_to_aws(Client_name, index_list, no_desired_tickers):
    to_aws = pd.DataFrame(index_list, columns=["currency_code"])
    to_aws["Client_name"] = Client_name
    to_aws["top_X"] = no_desired_tickers

    cols = to_aws.columns.tolist()
    cols = [cols[1]] + [cols[0], cols[2]]
    to_aws = to_aws[cols]

    engine = create_engine(global_vars.DB_PROD_URL_WRITE, pool_size=cpu_count(), max_overflow=-1,
                           isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        to_aws.to_sql(con=conn, name=global_vars.client_table_name, if_exists="append", index=False)


# list_1 = [ "0#.N225", "0#.KS200", "0#.TWII", "0#.HSLI", "0#.CSI300", "0#.FTSE", "0#.SXXE", "0#.SPX"]
# list_2 = ["0#.FTFBMKLCI", "0#.JKLQ45", "0#.SET50", "0#.NSEI", "0#.STI"]
#
# Client_name = "LORATECH"
#
# add_client_to_aws(Client_name, list_1, 10)
# add_client_to_aws(Client_name, list_2, 5)


def get_client_information(args):
    engine = db.create_engine(global_vars.DB_PROD_URL_READ, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = global_vars.client_table_name

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select([table0.columns.client_code_id, table0.columns.index_choice_id, table0.columns.top_x]).where(
            table0.columns.client_code_id == args.client_name)

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()


    full_df = pd.DataFrame(ResultSet)
    full_df.columns = columns_list
    # if full_df.shape[1] == 3:
    #     full_df.columns = ["client_name", "index", "top_X"]

    return full_df
