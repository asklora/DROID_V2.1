import requests
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from universe import DroidQuandlSymbols, NullQuandlSymbol
from general.slack import report_to_slack
from general.SqlProcedure import PgFunctions
from general.general import dateNow, backdate_by_year
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam


data_source = 'https://www.quandl.com/api/v3/datasets/OPT'
api_key = 'waqWZxLx2dx84mTcsn_w'
end_date = dateNow()
start_date = backdate_by_year(4)
quald_orats_table = 'droid_quandl_orats_vols'
droid_universe_table = 'droid_universe'

# https://www.quandl.com/api/v3/datasets/OPT/SCHO.csv?start_date='2020-07-07'&end_date='2020-09-03'&api_key=waqWZxLx2dx84mTcsn_w
def read_quandl_csv(ticker, quandl_symbol):
    print(f'=== Read {quandl_symbol} Quandl ===')
    try :
        query = f"{data_source}/{quandl_symbol}.csv?start_date='{start_date}'&end_date='{end_date}'&api_key={api_key}"
        chunck_data = pd.read_csv(query)
        chunck_data['ticker'] = ticker
        chunck_data['date'] = chunck_data['date'].astype(str)
        chunck_data['uid']=chunck_data['date'] + chunck_data['ticker']
        chunck_data['uid']=chunck_data['uid'].str.replace("-", "").str.replace(".", "")
        chunck_data['uid']=chunck_data['uid'].str.strip()
        chunck_data['date'] = pd.to_datetime(chunck_data['date'])
        return chunck_data
    except Exception as ex:
        print("error: ", ex)
        return []

def update_quandl_symbol_in_droid_universe(args, result):
    resultdict = result.to_dict('records')
    engine = db.create_engine(args.db_url_droid_write)
    sm = sessionmaker(bind=engine)
    session = sm()

    metadata = db.MetaData(bind=engine)

    datatable = db.Table(droid_universe_table, metadata, autoload=True)
    stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam('ticker')).values({
        'quandl_symbol': bindparam('quandl_symbol'),
        'ticker': bindparam('ticker')

    })
    session.execute(stmt,resultdict)

    session.flush()
    session.commit()
    engine.dispose()

def get_data_from_quandl(args):
    print('=== Getting data from Quandl ===')
    data_quandl = pd.DataFrame({'date':[],'stockpx':[],
                    'iv30':[],'iv60':[],'iv90':[],
                    'm1atmiv':[],'m1dtex':[],
                    'm2atmiv':[],'m2dtex':[],
                    'm3atmiv':[],'m3dtex':[],
                    'm4atmiv':[],'m4dtex':[],
                    'slope':[],'deriv':[],
                    'slope_inf':[],'deriv_inf':[],
                    '10dclsHV':[],'20dclsHV':[],'60dclsHV':[],'120dclsHV':[],'252dclsHV':[],
                    '10dORHV':[],'20dORHV':[],'60dORHV':[],'120dORHV':[],'252dORHV':[]}, index=[])
    quandl_symbol_list = DroidQuandlSymbols(args)
    for index, row in quandl_symbol_list.iterrows():
        ticker = row['ticker']
        quandl_symbol = row['quandl_symbol']
        data_from_quandl = read_quandl_csv(ticker, quandl_symbol)
        if (len(data_from_quandl) > 0):
            data_quandl = data_quandl.append(data_from_quandl)
    data_quandl = data_quandl.rename(columns={'date': 'trading_day'})
    print('=== Getting data from Quandl DONE ===')
    return data_quandl[['ticker', 'trading_day','stockpx','iv30','iv60','iv90','m1atmiv', 'm1dtex','m2atmiv','m2dtex','m3atmiv','m3dtex','m4atmiv','m4dtex','slope','deriv','slope_inf', 'deriv_inf', 'uid']]


def insert_quandl_to_database(args, data_quandl):
    print('=== Insert Quandl to database ===')
    engine_prod = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    try:
        with engine_prod.connect() as prod:
            metadata = db.MetaData()
            data_quandl.to_sql(
                quald_orats_table,
                if_exists='replace',
                index=False,
                con=prod
            )
        engine_prod.dispose()
        print("DATA INSERTED TO " + quald_orats_table)
    except Exception as ex:
        print("error: ", ex)


def update_quandl_orats_from_quandl(args):
    result = get_data_from_quandl(args)
    insert_quandl_to_database(args, result)
    #result.to_csv('hasil_quandl.csv')
    print(result)
    PgFunctions(args.db_url_droid_write, 'calculate_vol_surface_parameters')
    PgFunctions(args.db_url_droid_write, 'calculate_latest_vol_updates_us')
    report_to_slack("{} : === Quandl Ingested ===".format(str(datetime.now())), args)
    print('=== Quandl Orats DONE ===')
    try:
        r = requests.get(f'{args.urlAPIAsklora}/api-helper/calc_latest_bot/?index=US')
        if r.status_code == 200:
            report_to_slack("{} : === LATEST BOT UPDATES QUANDL SUCCESS === : ".format(str(datetime.now())), args)
    except Exception as e:
        report_to_slack("{} : === LATEST BOT UPDATES QUANDL ERROR === : {}".format(str(datetime.now()), e), args)
        print(e)

def update_quandl_symbols_to_database(args):
    ticker = NullQuandlSymbol(args)
    if(len(ticker) > 0):
        ticker["quandl_symbol"] = ""
        for i in range(len(ticker)):
            tick = ticker['ticker'].loc[i]
            result = tick.split('.')
            ticker["quandl_symbol"].loc[i] = result[0]
        print(ticker)
        update_quandl_symbol_in_droid_universe(args, ticker)
        tickerlist = tuple(ticker["ticker"])
        report_to_slack("{} : === New Quandl Symbol Added {} ===".format(str(datetime.now()), tickerlist), args)
        print('=== New Quandl Symbol Added ===')