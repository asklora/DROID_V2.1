

import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.types import String, Date, Float, INT, Text
from general.slack import report_to_slack
from datetime import datetime
from data_source.DSWS import get_interest_rate_from_dsws
from general.general import forwarddate_by_day, dateNow
from pangres import upsert
interest_rate_table = 'interest_rate'

def get_interest_rate(args):
    print('Get All interest_rate')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f'select ticker, currency, days_to_maturity, ingestion_field from {interest_rate_table}'
        all_universe = pd.read_sql(query, con=conn)
    engine.dispose()
    all_universe = pd.DataFrame(all_universe)
    return all_universe

def upsert_interest_rate_to_database(args, result):
    print('=== Insert Interest Rate to database ===')
    result = result.set_index('ticker')
    dtype = {'ticker':Text,
             'days_to_maturity':INT,
             'maturity':Date,
             'update_date':Date,}
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=interest_rate_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + interest_rate_table)
    engines_droid.dispose()

    # print('=== Insert interest_rate to database ===')
    # engines_droid = create_engine(args.db_url_droid_write)
    # with engines_droid.connect() as conn:
    #     result.to_sql(
    #         interest_rate_table,
    #         con=conn,
    #         schema=None,
    #         if_exists='replace',
    #         index=False,
    #         index_label=None
    #     )
    # print("DATA INSERTED TO " + interest_rate_table)
    # engines_droid.dispose()

def update_interest_rate_from_dsws(args):
    universe = get_interest_rate(args)
    print(f"Total Universe = {len(universe)}")
    data = pd.DataFrame({'ticker':[],'raw_data':[],
                    'currency':[],'days_to_maturity':[],'ingestion_field':[],
                    'maturity':[],'update_date':[], 'rate':[]}, index=[])
    indentifier = 'ticker'
    print("== Calculating Interest Rate ==")
    for index, row in universe.iterrows():
        ticker = row['ticker']
        currency = row['currency']
        ingestion_field = row['ingestion_field']
        days_to_maturity = row['days_to_maturity']
        filter_field = ingestion_field.split(',')
        result = get_interest_rate_from_dsws(args, indentifier, ticker, filter_field)
        try:
            if (result.loc[0, 'RY'] != 'NA'):
                result['raw_data'] = result.loc[0, 'RY']
                result = result.drop(columns='RY')
            else:
                result['raw_data'] = 0
                result = result.drop(columns='RY')
        except Exception as e:
            print(e)

        try:
            if (result.loc[0, 'IR'] != 'NA'):
                result['raw_data'] = result.loc[0, 'IR']
                result = result.drop(columns={'IR', 'IB', 'IO'})
            elif (result.loc[0, 'IB'] != 'NA') and (result.loc[0, 'IO'] != 'NA'):
                result['raw_data'] = (result.loc[0, 'IB'] + result.loc[0, 'IO']) / 2
                result = result.drop(columns={'IR', 'IB', 'IO'})
            elif (result.loc[0, 'IB'] != 'NA'):
                result['raw_data'] = result.loc[0, 'IB']
                result = result.drop(columns={'IR', 'IB', 'IO'})
            else:
                result['raw_data'] = result.loc[0, 'IO']
                result = result.drop(columns={'IR', 'IB', 'IO'})
        except Exception as e:
            print(e)
        result['currency'] = currency
        result['days_to_maturity'] = days_to_maturity
        result['ingestion_field'] = ingestion_field
        result['maturity'] = forwarddate_by_day(days_to_maturity)
        result['update_date'] = dateNow()
        result['rate'] = result['raw_data'] / 100
        data = data.append(result)
    data.reset_index(inplace=True)
    data = data.drop(columns={'index'})
    print("== Interest Rate Calculated ==")
    print(data)
    result.to_csv('interest_rate.csv')
    upsert_interest_rate_to_database(args, data)
    
    report_to_slack("{} : === Interest Rate Updated ===".format(str(datetime.now())), args)