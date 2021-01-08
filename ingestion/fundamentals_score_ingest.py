import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import Date, BIGINT, TEXT
import sqlalchemy as db
from datetime import datetime
from general.slack import report_to_slack
from general.general import backdate_by_year, dateNow, backdate_by_month
from data_source.DSWS import (
    check_ticker_available_in_DSWS, 
    TestDSWSField, 
    get_fundamentals_quarterly_from_dsws, 
    get_fundamentals_score_from_dsws_by_field,
    get_fundamentals_score_from_dsws
    )
from universe import UniverseEntityType
from pangres import upsert
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam

fundamentals_score_table = 'fundamentals_score'
weekly_column_name = {'WC05255': 'eps', 
                                    'WC05480': 'bps', 
                                    'WC18100A': 'ev',
                                    'WC18262A': 'ttm_rev',
                                    'WC08005': 'mkt_cap',
                                    'WC18309A': 'ttm_ebitda',
                                    'WC18311A': 'ttm_capex',
                                    'WC18199A': 'net_debt',
                                    'WC08372': 'roe',
                                    'WC05510': 'cfps',
                                    'WC08636A': 'peg',
                                    'BPS1FD12' : 'bps1fd12',
        	                        'EBD1FD12' : 'ebd1fd12',
                                    'EVT1FD12' : 'evt1fd12',
                                    'EPS1FD12' : 'eps1fd12',
                                    'SAL1FD12' : 'sal1fd12',
                                    'CAP1FD12' : 'cap1fd12'}
biweekly_column_name = {'WC05255': 'eps', 
                                    'WC05480': 'bps', 
                                    'WC08005': 'mkt_cap',
                                    'WC08372': 'roe',
                                    'WC05510': 'cfps',
                                    'BPS1FD12' : 'bps1fd12',
        	                        'EBD1FD12' : 'ebd1fd12',
                                    'EVT1FD12' : 'evt1fd12',
                                    'EPS1FD12' : 'eps1fd12',
                                    'SAL1FD12' : 'sal1fd12',
                                    'CAP1FD12' : 'cap1fd12'}
daily_column_name = {'WC08005': 'mkt_cap'}

def update_fundamentals_score_in_droid_universe_daily(args, result):
    print('=== Update Fundamentals Score Daily to database ===')
    result = result[["ticker","mkt_cap"]]
    resultdict = result.to_dict('records')
    engine = db.create_engine(args.db_url_droid_write)
    sm = sessionmaker(bind=engine)
    session = sm()

    metadata = db.MetaData(bind=engine)

    datatable = db.Table(fundamentals_score_table, metadata, autoload=True)
    stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam('ticker')).values({
        'mkt_cap': bindparam('mkt_cap'),
        'ticker': bindparam('ticker')

    })
    session.execute(stmt,resultdict)

    session.flush()
    session.commit()
    engine.dispose()
    print('=== Fundamentals Score Daily Updated ===')

def update_fundamentals_score_in_droid_universe_biweekly(args, result):
    print('=== Update Fundamentals Score Daily to database ===')
    result = result[["ticker","eps","bps","mkt_cap","roe","cfps",
                     "bps1fd12","ebd1fd12","evt1fd12","eps1fd12","sal1fd12","cap1fd12"]]
    resultdict = result.to_dict('records')
    engine = db.create_engine(args.db_url_droid_write)
    sm = sessionmaker(bind=engine)
    session = sm()

    metadata = db.MetaData(bind=engine)

    datatable = db.Table(fundamentals_score_table, metadata, autoload=True)
    stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam('ticker')).values({
        'eps': bindparam('eps'),
        'bps': bindparam('bps'),
        'mkt_cap': bindparam('mkt_cap'),
        'roe': bindparam('roe'),
        'cfps': bindparam('cfps'),
        'bps1fd12': bindparam('bps1fd12'),
        'ebd1fd12': bindparam('ebd1fd12'),
        'evt1fd12': bindparam('evt1fd12'),
        'eps1fd12': bindparam('eps1fd12'),
        'sal1fd12': bindparam('sal1fd12'),
        'cap1fd12': bindparam('cap1fd12'),
        'ticker': bindparam('ticker')

    })
    session.execute(stmt,resultdict)

    session.flush()
    session.commit()
    engine.dispose()
    print('=== Fundamentals Score Daily Updated ===')

def insert_fundamentals_score_to_database_weekly(args, result):
    print('=== Insert Fundamentals Score to database ===')
    result = result.set_index('ticker')
    dtype={
        'ticker':TEXT
    }
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=fundamentals_score_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA INSERTED TO " + fundamentals_score_table)
    engines_droid.dispose()
    del result

def get_fundamentals_score(args, universe, start_date, end_date, filter_field, column_name):
    ticker = universe[['ticker']]
    identifier = 'ticker'
    #filter_field = ['WC18100A']
    result, except_field = get_fundamentals_score_from_dsws(args, start_date, end_date, ticker, identifier, filter_field, use_ticker=True, identifier = identifier)
    print(len(except_field))
    if len(except_field) == 0 :
        second_result = []
    else:
        second_result = get_fundamentals_score_from_dsws_by_field(args, start_date, end_date, except_field, identifier, filter_field, use_ticker=True, identifier = identifier)
    try:
        print(len(result))
        print(len(second_result))
        if(len(result) == 0):
            result = second_result
        elif(len(second_result) == 0):
            result = result
        else :
            result = result.append(second_result)
        print(result)
        #data = data.drop(columns='level_0')
    except Exception as e:
        print(e)
        print(len(result))
        print(len(second_result))
        result = second_result
        print(result)
    print(result)
    result = result.rename(columns=column_name)
    result.reset_index(inplace = True)
    result = result.drop(columns={'index', 'level_0'})
    print(result)
    return result
    

def update_fundamentals_score_from_dsws_weekly(args):
    end_date = dateNow()
    start_date = backdate_by_month(12)
    universe = UniverseEntityType(args)
    filter_field = ['WC05255',
                    'WC05480',
                    'WC18100A',
                    'WC18262A',
                    'WC08005',
                    'WC18309A',
                    'WC18311A',
                    'WC18199A',
                    'WC08372',
                    'WC05510',
                    'WC08636A',
                    'BPS1FD12',
        	        'EBD1FD12',
                    'EVT1FD12',
                    'EPS1FD12',
                    'SAL1FD12',
                    'CAP1FD12']
    if(len(universe)) > 0 :
        result = get_fundamentals_score(args, universe, start_date, end_date, filter_field, weekly_column_name)
        insert_fundamentals_score_to_database_weekly(args, result)
        result.to_csv("Fundamental_Signal_From_DSWS.csv")
        report_to_slack("{} : === Fundamentals Score Weekly Updated ===".format(str(datetime.now())), args)

def update_fundamentals_score_from_dsws_biweekly(args):
    end_date = dateNow()
    start_date = backdate_by_month(12)
    universe = UniverseEntityType(args)
    filter_field = ['WC05255',
                    'WC05480',
                    'WC08005',
                    'WC08372',
                    'WC05510',
                    'BPS1FD12',
        	        'EBD1FD12',
                    'EVT1FD12',
                    'EPS1FD12',
                    'SAL1FD12',
                    'CAP1FD12']
    if(len(universe)) > 0 :
        result = get_fundamentals_score(args, universe, start_date, end_date, filter_field, biweekly_column_name)
        update_fundamentals_score_in_droid_universe_biweekly(args, result)
        report_to_slack("{} : === Fundamentals Score Biweekly Updated ===".format(str(datetime.now())), args)

def update_fundamentals_score_from_dsws_daily(args):
    end_date = dateNow()
    start_date = backdate_by_month(12)
    universe = UniverseEntityType(args)
    filter_field = ['WC08005']
    if(len(universe)) > 0 :
        result = get_fundamentals_score(args, universe, start_date, end_date, filter_field, daily_column_name)
        update_fundamentals_score_in_droid_universe_daily(args, result)
        report_to_slack("{} : === Fundamentals Score Daily Updated ===".format(str(datetime.now())), args)
