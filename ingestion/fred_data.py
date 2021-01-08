import sys
import requests
import pandas as pd
import numpy as np
import sqlalchemy as db
from sqlalchemy import create_engine
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from universe import DroidQuandlSymbols, NullQuandlSymbol
from general.slack import report_to_slack
from general.SqlProcedure import PgFunctions
from general.general import dateNow, backdate_by_year
from sqlalchemy.types import String, Date, Float, INT, Text
from pangres import upsert


data_source = 'https://fred.stlouisfed.org/graph/fredgraph.csv?'
api_key = 'waqWZxLx2dx84mTcsn_w'
end_date = dateNow()
start_date = backdate_by_year(4)
fred_data_table = 'fred_data'

# 'https://fred.stlouisfed.org/graph/fredgraph.csv?&id=BAMLH0A1HYBBEY&cosd=2015-11-09&coed=2020-11-09'
def read_fred_csv():
    print(f'=== Read Fred Data ===')
    try :
        query = f"{data_source}id=BAMLH0A1HYBBEY&cosd='{start_date}'&coed={end_date}"
        data = pd.read_csv(query)
        data = data.rename(columns={'DATE': 'trading_day', 'BAMLH0A1HYBBEY': 'data'})
        return data
    except Exception as ex:
        print("error: ", ex)
        return []

def upsert_data_to_database(args, data):
    print('=== Insert Data to database ===')
    data = data.set_index('trading_day')
    print(data)
    sys.exit(1)
    dtype = {'trading_day':Date}
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=data,
           table_name=fred_data_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + fred_data_table)
    engines_droid.dispose()

def insert_quandl_to_database(args, data):
    print('=== Insert Data to database ===')
    engine_prod = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    try:
        with engine_prod.connect() as prod:
            metadata = db.MetaData()
            data.to_sql(
                fred_data_table,
                if_exists='replace',
                index=False,
                con=prod
            )
        engine_prod.dispose()
        print("DATA INSERTED TO " + fred_data_table)
    except Exception as ex:
        print("error: ", ex)

def update_fred_data_to_database(args):
    data = read_fred_csv()
    data["data"] = np.where(data["data"]== ".", 0, data["data"])
    data['data'] = data['data'].astype(float)
    if(len(data) > 0):
        insert_quandl_to_database(args, data)
        report_to_slack("{} : === FRED DATA DROID UPDATED ===".format(str(datetime.now())), args)