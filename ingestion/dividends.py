import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.types import String, Date, Float, INT, Text
from general.slack import report_to_slack
from datetime import datetime
from data_source.DSWS import get_dividends_history_from_dsws
from universe import DroidUniverse
from general.general import backdate_by_year, dateNow, backdate_by_week
from pangres import upsert

dividens_table = 'dividends'

def delete_old_dividends_on_database(args):
    old_date = dateNow()
    print('=== Deleting Old Dividends on database ===')
    engine = create_engine(args.db_url_droid_write)
    with engine.connect() as conn:
        query = f"delete from {dividens_table} where ex_dividend_date < '{old_date}'"
        result = conn.execute(query)
    engine.dispose()
    print('=== Old Dividends Deleted on database ===')
    del result

def upsert_dividends_to_database(args, result):
    print('=== Insert Dividends to database ===')
    result = result.set_index('uid')
    dtype = {'uid':Text,
             'ticker':Text,
             'ex_dividend_date':Date}
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=dividens_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + dividens_table)
    engines_droid.dispose()

def get_dividends_from_dsws(args, universe, start_date, end_date):
    identifier='ticker'
    filter_field = ['UDDE']
    ticker = universe[['ticker']]
    result = get_dividends_history_from_dsws(args, start_date, end_date, ticker, filter_field, use_ticker=True, identifier=identifier)
    if(len(result)) > 0 :
        result = result.reset_index()
        result = result.drop(columns='index')
        result = result.rename(columns={'UDDE': 'amount'})
        result = result.dropna(subset=['amount'])
        result['ex_dividend_date'] = result['ex_dividend_date'].astype(str)
        result['uid']=result['ex_dividend_date'] + result['ticker']
        result['uid']=result['uid'].str.replace("-", "").str.replace(".", "")
        result['uid']=result['uid'].str.strip()
        result['ex_dividend_date'] = pd.to_datetime(result['ex_dividend_date'])
        print(result)
        #result.to_csv('dividens.csv')
        upsert_dividends_to_database(args, result)  
        report_to_slack("{} : === Dividens Updated ===".format(str(datetime.now())), args)

def update_dividends_from_dsws(args):
    universe = DroidUniverse(args)
    end_date = dateNow()
    start_date = backdate_by_year(4)
    filter_field = ['UDDE']
    if(len(universe)) > 0 :
        delete_old_dividends_on_database(args)
        get_dividends_from_dsws(args, universe, start_date, end_date)

# def find_new_dividends(args):
#     universe = new_dividends_Ticker(args)
#     if(len(universe)) > 0 :
#         end_date = dateNow()
#         start_date = backdate_by_year(4)
#         get_dividends_from_dsws(args, universe, start_date, end_date)

# def check_dividends_error_ticker(args,filter_field, start_date, end_date):
#     result = check_ticker_available_in_DSWS(args, filter_field, start_date, end_date)
#     result.to_csv('dividends_not_available_ticker.csv')
#     return result[['ticker']]

# def remove_error_ticker_from_universe(universe, excluded_ticker):
#     exclude= excluded_ticker['ticker'].tolist()
#     print(exclude)
#     exclude = universe['ticker'].isin(exclude)
#     universe = universe[~exclude]
#     print(universe)
#     return universe