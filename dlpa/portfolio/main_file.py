import datetime as dt
import mimetypes
import smtplib
import ssl
import sys
import time
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine, and_
from sqlalchemy.types import Date

import global_vars
from global_vars import no_top_models


def portfolio_maker(args):
    if args.portfolio_period is None:
        sys.exit('Please specify portfolio period.!')

    if args.go_live:
        d = dt.date.today()
        if args.portfolio_period == 0:
            while d.weekday() != 4:
                d += BDay(1)
            args.forward_date = d
        elif args.portfolio_period == 1:
            if d.weekday() > 4:
                while d.weekday() != 0:
                    d += BDay(1)
            args.forward_date = d
    else:
        if args.forward_date is None and args.spot_date is None:
            sys.exit("Please specify either of the forward_date or spot_date!")
        if args.forward_date is not None and args.spot_date is not None:
            sys.exit("Please specify only forward_date or spot_date!")

        if args.forward_date is not None:
            args.forward_date = datetime.strptime(f'{args.forward_date}', '%Y-%m-%d').date()

        if args.spot_date is not None:
            args.spot_date = datetime.strptime(f'{args.spot_date}', '%Y-%m-%d').date()

            if args.portfolio_period == 0:
                args.forward_date = args.spot_date + Week(1)
            elif args.portfolio_period == 1:
                args.forward_date = args.spot_date + BDay(1)

    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = args.stock_table_name
    table_models = args.model_table_name

    # args.forward_date = datetime.strptime(f'{args.portfolio_year} {args.portfolio_week} {args.portfolio_day}',
    #                                         '%G %V %u').strftime('%Y-%m-%d')

    args.forward_date_0 = str(args.forward_date)
    args.forward_date_1 = args.forward_date.strftime('%Y-%m-%d')

    if args.portfolio_period == 0:
        period = 'weekly'
    else:
        period = 'daily'

    # *************************************************************************************************
    # ****************************** Download the models **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table_models = db.Table(table_models, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table_models.columns.data_period, table_models.columns.when_created, table_models.columns.forward_date,
             table_models.columns.best_train_acc,
             table_models.columns.best_valid_acc,
             table_models.columns.model_type, table_models.columns.model_filename,
             table_models.columns.pc_number]).where(and_(
            table_models.columns.data_period == period, table_models.columns.forward_date == args.forward_date_0,
            table_models.columns.num_periods_to_predict == args.num_periods_to_predict))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    models_df = pd.DataFrame(ResultSet)
    if len(models_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    models_df.columns = columns_list

    # if models_df.shape[1] is not None:
    #     models_df.columns = ['data_period', 'when_created', 'forward_date', 'best_train_acc', 'best_valid_acc',
    #                          'model_type',
    #                          'model_filename', 'pc_number']

    models_df.fillna(value=0, inplace=True)
    # *************************************************************************************************
    # ****************************** Download the stocks **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
             table0.columns.spot_date, table0.columns.index,
             table0.columns.ticker,
             table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
             table0.columns.model_filename]).where(and_(
            table0.columns.data_period == period, table0.columns.forward_date == args.forward_date_1))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    full_df.columns = columns_list

    # if full_df.shape[1] is not None:
    #     full_df.columns = ['data_period', 'when_created', 'forward_date', 'spot_date', 'index', 'ticker',
    #                        'predicted_quantile_1',
    #                        'signal_strength_1', 'model_filename']

    full_df.fillna(value=0, inplace=True)

    # *************************************************************************************************
    # ****************************** Pick the top models **********************************************
    models_df = models_df.sort_values(by=['best_valid_acc'], ascending=False)
    models_df = models_df.loc[models_df.pc_number.isin(['PC1','PC2','PC3','PC4', 'Stephen'])]

    top_models_list = models_df.model_filename.head(no_top_models)
    full_df = full_df.loc[full_df['model_filename'].isin(top_models_list)]

    # *************************************************************************************************
    full_df = full_df[full_df.signal_strength_1 > full_df.signal_strength_1.quantile(1 - args.signal_threshold)]

    gb = full_df.groupby(
        ['data_period', 'forward_date', 'index', 'ticker', 'predicted_quantile_1', 'spot_date'])
    portfolio_1 = gb.size().to_frame(name='counts')
    portfolio_1 = (portfolio_1.join(gb.agg({'signal_strength_1': 'mean'}).rename(
        columns={'signal_strength_1': 'signal_strength_mean'})).reset_index())

    # top_stocks = portfolio_1.sort_values(by=['counts', 'signal_strength_mean'], ascending=False)

    # list_1 = [ '0#.N225', '0#.KS200', '0#.TWII', '0#.HSLI', '0#.CSI300', '0#.FTSE', '0#.SXXE', '0#.SPX']
    # list_2 = ['0#.FTFBMKLCI', '0#.JKLQ45', '0#.SET50', '0#.NSEI', '0#.STI']

    #write out WTS DLP scores to DROID DB
    if args.go_live:
        portfolio_2= portfolio_1[portfolio_1.predicted_quantile_1 == 2] #only use the BUY scores
        engine = create_engine(global_vars.DB_DROID_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            query = f"update droid_universe set wts_rating=0 where ticker=ticker"
            result = conn.execute(query)

            temprows = portfolio_2
            for index, row in temprows.iterrows():
                ticker = row['ticker']
                dlp_rating = row['counts']
                dlp_ss = row['signal_strength_mean']
                query = f"update droid_universe set wts_rating ={dlp_rating} where ticker='{ticker}'"
                query2 = f"update droid_universe set wts_rating2 ={dlp_ss} where ticker='{ticker}'"
                result = conn.execute(query)
                result2 = conn.execute(query2)

        engine.dispose()

    def pick_top_stocks(portfolio, index, number_desired_stocks=10):
        sorted_df = portfolio.sort_values(by=['counts', 'signal_strength_mean'], ascending=False)

        top_buy = sorted_df[(sorted_df.predicted_quantile_1 == 2) & (sorted_df['index'] == index)][
                      ['forward_date', 'index', 'ticker', 'spot_date']][0:number_desired_stocks]
        top_hold = sorted_df[(sorted_df.predicted_quantile_1 == 1) & (sorted_df['index'] == index)][
                       ['forward_date', 'index', 'ticker', 'spot_date']][0:number_desired_stocks]
        top_sell = sorted_df[(sorted_df.predicted_quantile_1 == 0) & (sorted_df['index'] == index)][
                       ['forward_date', 'index', 'ticker', 'spot_date']][0:number_desired_stocks]

        top_buy.reset_index(inplace=True, drop=True)
        top_hold.reset_index(inplace=True, drop=True)
        top_sell.reset_index(inplace=True, drop=True)

        top_buy['rank'] = top_buy.index + 1
        top_hold['rank'] = top_hold.index + 1
        top_sell['rank'] = top_sell.index + 1
        return top_buy, top_hold, top_sell


    top_buy, top_hold, top_sell = pick_top_stocks(portfolio_1, args.index_list[0], args.stock_num)
    for i in range(1, len(args.index_list)):
        a, b, c = pick_top_stocks(portfolio_1, args.index_list[i], args.stock_num)
        top_buy = top_buy.append(a)
        top_hold = top_hold.append(b)
        top_sell = top_sell.append(c)

    if args.portfolio_period == 0:
        top_buy['prediction_period'] = 'weekly'
        top_hold['prediction_period'] = 'weekly'
        top_sell['prediction_period'] = 'weekly'
    elif args.portfolio_period == 1:
        top_buy['prediction_period'] = 'daily'
        top_hold['prediction_period'] = 'daily'
        top_sell['prediction_period'] = 'daily'
    else:
        top_buy['prediction_period'] = 'PIT'
        top_hold['prediction_period'] = 'PIT'
        top_sell['prediction_period'] = 'PIT'

    top_buy['when_created'] = datetime.fromtimestamp(time.time())
    top_hold['when_created'] = datetime.fromtimestamp(time.time())
    top_sell['when_created'] = datetime.fromtimestamp(time.time())

    top_buy['type'] = 'Buy'
    top_hold['type'] = 'Hold'
    top_sell['type'] = 'Sell'

    top_buy['client_name'] = args.client_name
    top_hold['client_name'] = args.client_name
    top_sell['client_name'] = args.client_name

    top_buy['top_x'] = args.stock_num
    top_hold['top_x'] = args.stock_num
    top_sell['top_x'] = args.stock_num
    
    top_buy['uid'] = top_buy['spot_date']+top_buy['index']+top_buy['type']+top_buy['ticker']+'|'+top_buy['client_name']
    top_buy['uid'] = top_buy['uid'].str.replace('0#.','').str.replace('-','').str.replace('.','')
    top_hold['uid'] = top_hold['spot_date']+top_hold['index']+top_hold['type']+top_hold['ticker']+'|'+top_hold['client_name']
    top_hold['uid'] = top_hold['uid'].str.replace('0#.','').str.replace('-','').str.replace('.','')
    top_sell['uid'] = top_sell['spot_date']+top_sell['index']+top_sell['type']+top_sell['ticker']+'|'+top_sell['client_name']
    top_sell['uid'] = top_sell['uid'].str.replace('0#.','').str.replace('-','').str.replace('.','')

    top_buy = top_buy.infer_objects()
    top_hold = top_hold.infer_objects()
    top_sell = top_sell.infer_objects()


    db_url = global_vars.DB_PROD_URL_WRITE
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    # check table client portfolios for dupes before each top_xxx.to_sql
    # DELETE WHERE spot_date = top_buy['spot_date'] AND forward_date=top_buy['forward_date'], AND client_name =top_buy['client_name ']  AND index = top_buy['index '] AND type = 'Buy'
    with engine.connect() as conn:
        temprows = top_buy.loc[top_buy['rank'] == 1]
        for index, row in temprows.iterrows():
            spot_date = row['spot_date']
            forward_date = row['forward_date']
            client_name = row['client_name']
            index = row['index']
            query = f"delete from client_portfolios where spot_date='{spot_date}' AND forward_date='{forward_date}' AND client_name='{client_name}' AND index='{index}'  AND type ='Buy'"
            #print(query)
            result = conn.execute(query)
            #print(result)

    # # check table client portfolios for dupes before each top_hold.to_sql
    # # DELETE WHERE spot_date = top_hold['spot_date'] AND forward_date=top_hold['forward_date'], AND client_name =top_hold['client_name ']  AND index = top_hold['index '] AND type = 'Hold'
    # temprows= top_hold.loc[top_buy['rank']==1]
    # for index, row in temprows.iterrows():
    #     spot_date = row['spot_date']
    #     forward_date = row['forward_date']
    #     client_name = row['client_name']
    #     index = row['index']
    #     query = f"delete from client_portfolios where spot_date='{spot_date}' AND forward_date='{forward_date}' AND client_name='{client_name}' AND index='{index}'  AND type ='Hold'"
    #     print(query)
    #     result = engine.execute(query)
    #     print(result)
    #
    # # check table client portfolios for dupes before each top_sell.to_sql
    # # DELETE WHERE spot_date = top_sell['spot_date'] AND forward_date=top_sell['forward_date'], AND client_name =top_sell['client_name ']  AND index = top_sell['index '] AND type = 'Sell'
    # temprows= top_sell.loc[top_buy['rank']==1]
    # for index, row in temprows.iterrows():
    #     spot_date = row['spot_date']
    #     forward_date = row['forward_date']
    #     client_name = row['client_name']
    #     index = row['index']
    #     query = f"delete from client_portfolios where spot_date='{spot_date}' AND forward_date='{forward_date}' AND client_name='{client_name}' AND index='{index}'  AND type ='Sell'"
    #     print(query)
    #     result = engine.execute(query)
    #     print(result)
    if args.go_test:
        table = global_vars.test_portfolio_table_name
    else:
        table = global_vars.production_portfolio_table_name
    with engine.connect() as conn:
        top_buy.to_sql(con=conn,
        name=table,
        if_exists='append',
        index=False,
        dtype={
            'spot_date': Date,
            'forward_date': Date
            })
        # top_hold.to_sql(con=conn, name='client_portfolios', if_exists='append', index=False)
        # top_sell.to_sql(con=conn, name='client_portfolios', if_exists='append', index=False)

    return top_buy, top_hold, top_sell

    # if args.send_email:
    #     top_buy.to_csv('top_buy_top_10.csv')
    #     top_hold.to_csv('top_hold_top_10.csv')
    #     top_sell.to_csv('top_sell_top_10.csv')
    # ******************************************************

    # top_buy, top_hold, top_sell = pick_top_stocks(portfolio_1, list_2[0], 5)
    # for i in range(1,len(list_2)):
    #     a, b, c = pick_top_stocks(portfolio_1, list_2[i], 5)
    #     top_buy = top_buy.append(a)
    #     top_hold = top_hold.append(b)
    #     top_sell = top_sell.append(c)
    #
    # if args.portfolio_period == 0:
    #     top_buy['prediction_period'] = 'weekly'
    #     top_hold['prediction_period'] = 'weekly'
    #     top_sell['prediction_period'] = 'weekly'
    # elif args.portfolio_period == 1:
    #     top_buy['prediction_period'] = 'daily'
    #     top_hold['prediction_period'] = 'daily'
    #     top_sell['prediction_period'] = 'daily'
    # else:
    #     top_buy['prediction_period'] = 'PIT'
    #     top_hold['prediction_period'] = 'PIT'
    #     top_sell['prediction_period'] = 'PIT'
    #
    # top_buy['when_created'] =datetime.fromtimestamp(time.time())
    # top_hold['when_created'] =datetime.fromtimestamp(time.time())
    # top_sell['when_created'] =datetime.fromtimestamp(time.time())
    #
    # if args.send_email:
    #     top_buy.to_csv('top_buy_top_5.csv')
    #     top_hold.to_csv('top_hold_top_5.csv')
    #     top_sell.to_csv('top_sell_top_5.csv')


def send_email(args):
    # csv_list = ['top_buy_top_10.csv','top_hold_top_10.csv','top_sell_top_10.csv','top_buy_top_5.csv','top_hold_top_5.csv','top_sell_top_5.csv']
    # csv_list = ['top_buy_top_10.csv',  'top_buy_top_5.csv']
    USERNAME = "asklora@loratechai.com"
    PASSWORD = "askLORA20$"

    if args.portfolio_period == 0:
        portfolio_period = 'weekly'
    elif args.portfolio_period == 1:
        portfolio_period = 'daily'
    else:
        portfolio_period = 'PIT'

    mail_content = "Hello, The %s portfolios for %s." % (portfolio_period, args.forward_date.strftime('%Y-%m-%d'))
    send_from = 'asklora@loratechai.com'

    send_to = ['stepchoi@loratechai.com', 'imansaj@loratechai.com', 'info@loratechai.com', 'john.kim@loratechai.com']
    send_to = ['imansaj@loratechai.com', 'stepchoi@loratechai.com']  # For debugging. Should be commented normally.

    subject = "%s portfolios for %s." % (portfolio_period, args.forward_date.strftime('%Y-%m-%d'))
    msg = MIMEMultipart()
    msg['From'] = send_from
    x = COMMASPACE.join(send_to)
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    for file in args.csv_list:
        path = file
        # Open the files in binary mode.  Let the MIMEImage class automatically
        # guess the specific image type.
        ctype, encoding = mimetypes.guess_type(path)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        if maintype == 'text':
            with open(path) as fp:
                # Note: we should handle calculating the charset
                outer = MIMEText(fp.read(), _subtype=subtype)
        else:
            with open(path, 'rb') as fp:
                outer = MIMEBase(maintype, subtype)
                outer.set_payload(fp.read())
            encoders.encode_base64(outer)
        outer.add_header('Content-Disposition', 'attachment', filename=file)
        msg.attach(outer)

    msg.attach(MIMEText(mail_content, 'plain'))
    context = ssl.create_default_context()

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        # server = smtplib.SMTP_SSL('smtp.gmail.com', 465) # if
        server.ehlo()
        server.starttls(context=context)
        server.login(USERNAME, PASSWORD)
        server.sendmail(send_from, send_to, msg.as_string())  # need as_string
        server.close()

        print('Email sent!')
    except Exception as e:
        print('Something went wrong...')
        print(e)


def get_dates_list_from_aws(args):
    args.forward_date_start = datetime.strptime(f'{args.forward_date_start}', '%Y-%m-%d').date()
    args.forward_date_stop = datetime.strptime(f'{args.forward_date_stop}', '%Y-%m-%d').date()

    args.forward_date_start_1 = args.forward_date_start.strftime('%Y-%m-%d')
    args.forward_date_stop_1 = args.forward_date_stop.strftime('%Y-%m-%d')

    if args.portfolio_period == 0:
        period = 'weekly'
    else:
        period = 'daily'

    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(args.stock_table_name, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
             table0.columns.spot_date, table0.columns.index,
             table0.columns.ticker,
             table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
             table0.columns.model_filename]).where(and_(
            table0.columns.data_period == period, table0.columns.forward_date >= args.forward_date_start_1,
            table0.columns.forward_date <= args.forward_date_stop_1))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date_start, args.portfolio_period)))
    full_df.columns = columns_list

    # if full_df.shape[1] is not None:
    #     full_df.columns = ['data_period', 'when_created', 'forward_date', 'spot_date', 'index', 'ticker',
    #                        'predicted_quantile_1',
    #                        'signal_strength_1', 'model_filename']

    full_df.fillna(value=0, inplace=True)

    a = full_df['forward_date'].unique()
    return a


def record_DLP_rating(args): #record 4wk and 13wk DLP ratings to DROID DB
    if args.portfolio_period is None:
        sys.exit('Please specify portfolio period.!')
    if args.num_periods_to_predict<1:
        sys.exit('Need periods_to_predict <1!')
    if args.go_live:
        d = dt.date.today()

        while d.weekday() != 4:
            d += BDay(1)
        args.forward_date = d


    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = args.stock_table_name
    table_models = args.model_table_name

    # args.forward_date = datetime.strptime(f'{args.portfolio_year} {args.portfolio_week} {args.portfolio_day}',
    #                                         '%G %V %u').strftime('%Y-%m-%d')

    args.forward_date_0 = str(args.forward_date)
    args.forward_date_1 = args.forward_date.strftime('%Y-%m-%d')

    if args.portfolio_period == 0:
        period = 'weekly'
    else:
        sys.exit('No records for daily DLP scores!')

    # *************************************************************************************************
    # ****************************** Download the models **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table_models = db.Table(table_models, metadata, autoload=True, autoload_with=conn)
        # *************************************************************************************************
        # ****************************** Download model data **********************************************
        query = db.select(
            [table_models.columns.data_period, table_models.columns.when_created, table_models.columns.forward_date,
             table_models.columns.best_train_acc,
             table_models.columns.best_valid_acc,
             table_models.columns.model_type, table_models.columns.model_filename,
             table_models.columns.pc_number]).where(and_(
            table_models.columns.data_period == period, table_models.columns.forward_date == args.forward_date_0,
            table_models.columns.num_periods_to_predict == args.num_periods_to_predict))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    models_df = pd.DataFrame(ResultSet)
    if len(models_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    models_df.columns = columns_list


    models_df.fillna(value=0, inplace=True)
    # *************************************************************************************************
    # ****************************** Download stock data **********************************************
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table0, metadata, autoload=True, autoload_with=conn)

        query = db.select(
            [table0.columns.data_period, table0.columns.when_created, table0.columns.forward_date,
             table0.columns.spot_date, table0.columns.index,
             table0.columns.ticker,
             table0.columns.predicted_quantile_1, table0.columns.signal_strength_1,
             table0.columns.model_filename]).where(and_(
            table0.columns.data_period == period, table0.columns.forward_date == args.forward_date_1))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        sys.exit(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    full_df.columns = columns_list
    full_df.fillna(value=0, inplace=True)

    # *************************************************************************************************
    # ****************************** Pick the top models **********************************************
    models_df = models_df.sort_values(by=['best_valid_acc'], ascending=False) #sort by valid acc
    models_df = models_df.loc[models_df.pc_number.isin(['PC1','PC2','PC3','PC4', 'Stephen'])]

    top_models_list = models_df.model_filename.head(no_top_models) #take top TEN models by valid_acc
    full_df = full_df.loc[full_df['model_filename'].isin(top_models_list)]

    # *************************************************************************************************
    full_df = full_df[full_df.signal_strength_1 > full_df.signal_strength_1.quantile(1 - args.signal_threshold)]

    gb = full_df.groupby(
        ['data_period', 'forward_date', 'index', 'ticker', 'predicted_quantile_1', 'spot_date'])
    portfolio_1 = gb.size().to_frame(name='counts')
    portfolio_1 = (portfolio_1.join(gb.agg({'signal_strength_1': 'mean'}).rename(
        columns={'signal_strength_1': 'signal_strength_mean'})).reset_index())


    if args.go_live:
        portfolio_2= portfolio_1[portfolio_1.predicted_quantile_1 == 2] #only use the BUY scores
        engine = create_engine(global_vars.DB_DROID_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            if args.num_periods_to_predict == 4:
                query = f"update droid_universe set dlp_1m=0 where ticker=ticker"
            elif args.num_periods_to_predict == 1:
                query = f"update droid_universe set wts_rating=0 where ticker=ticker"
                query = f"update droid_universe set wts_rating2=0 where ticker=ticker"
            else:
                query = f"update droid_universe set dlp_3m=0 where ticker=ticker"
            result = conn.execute(query)

            temprows = portfolio_2
            for index, row in temprows.iterrows():
                ticker = row['ticker']
                dlp_rating = row['counts']
                dlp_ss = row['signal_strength_mean']
                if args.num_periods_to_predict == 4:
                    query = f"update droid_universe set dlp_1m ={dlp_rating} where ticker='{ticker}'"
                elif args.num_periods_to_predict == 1:
                    query = f"update droid_universe set wts_rating ={dlp_rating} where ticker='{ticker}'"
                    query2 = f"update droid_universe set wts_rating2 ={dlp_ss} where ticker='{ticker}'"
                    result2 = conn.execute(query2)
                else:
                    query = f"update droid_universe set dlp_3m ={dlp_rating} where ticker='{ticker}'"
                result = conn.execute(query)
        engine.dispose()

def get_last_dlp_rating_date(args):
    db_url = global_vars.DB_DROID_URL_READ
    print("Get Data From DROID")
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select max(forward_date) as max_date from dlp_rating_history;"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    if(data.loc[0, 'max_date'] == None):
        return (datetime(2019, 12, 1).date()).strftime("%Y-%m-%d")
    print("DONE")
    return str(data.loc[0, 'max_date'])

def dlp_rating_history(args): #record 4wk and 13wk DLP ratings to DROID DB
    #Data Initial
    num_periods_to_predict = [1, 4, 13]
    if not args.forward_date_start:
        start_date = get_last_dlp_rating_date(args)
    else :
        start_date = args.forward_date_start
    if not args.forward_date_stop:
        today = datetime.now().date()
    else :
        today = datetime.strptime(args.forward_date_start, "%Y-%m-%d").date()
    
    #Make Start Date to Friday
    start_date_friday = datetime.strptime(start_date, "%Y-%m-%d").date()
    while start_date_friday.weekday() != 4:
        start_date_friday -= BDay(1)

    period = 'weekly'
    args.portfolio_period = 0

    #Calculate DLP Rating Historical
    while start_date_friday <= today:
        args.forward_date = start_date_friday
        print(start_date_friday)
        insert_first_data_to_database(args)
        for periods in num_periods_to_predict:
            print(periods)
            try:
                args.num_periods_to_predict = periods
                result = process_dlp_rating_history(args)
                insert_data_to_database(args, result)
            except Exception as e:
                print(e)
        start_date_friday = (start_date_friday + BDay(5)).date()
        del result
    start_date = start_date_friday

    #Insert Data to Lora Test Picks Table
    Test(args)

def Test(args):
    # *************************************************************************************************
    # ****************************** Download stock data **********************************************
    production_model_stock_data_table = args.stock_table_name
    db_url = global_vars.DB_DROID_URL_READ
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    result = pd.DataFrame({'spot_date':[],'index':[],
                    'ticker_1':[],'ticker_2':[],'ticker_3':[]}, index=[])
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select distinct index, forward_date from dlp_rating_history where forward_date>='{args.forward_date}' order by forward_date, index"
        data = pd.read_sql(query, con=conn)
        columns_list = conn.execute(query).keys()
        data = pd.DataFrame(data)

        for index, row in data.iterrows():
            indices = row['index']
            forward_date = row['forward_date']
            query = f"select ticker, ribbon_score, wts_score, forward_date, index from dlp_rating_history where index='{indices}' and forward_date = '{forward_date}' order by ribbon_score DESC, wts_score DESC limit 3"
            temp_result = pd.read_sql(query, con=conn)
            temp_result = pd.DataFrame(temp_result)
            temp_result = pd.DataFrame({'spot_date':[forward_date],'index':[indices],
                'ticker_1':[temp_result.loc[0, 'ticker']],
                'ticker_2':[temp_result.loc[1, 'ticker']],
                'ticker_3':[temp_result.loc[2, 'ticker']]}, index=[index])
            result = result.append(temp_result)
        print(result)
    engine.dispose()

    db_url = global_vars.DB_DROID_URL_WRITE
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        for index, row in result.iterrows():
            indices = row['index']
            ticker_1 = row['ticker_1']
            ticker_2 = row['ticker_2']
            ticker_3 = row['ticker_3']
            spot_date = row['spot_date']
            query = f"insert into lora_test_picks (spot_date, index, ticker_1, ticker_2, ticker_3) VALUES ('{spot_date}', '{indices}', '{ticker_1}', '{ticker_2}' ,'{ticker_3}') ON CONFLICT DO NOTHING;"
            result = conn.execute(query)
    engine.dispose()

def process_dlp_rating_history(args):
    #Get Model Data
    models_df, columns_list = get_production_model_data(args)
    models_df = pd.DataFrame(models_df)
    if len(models_df) == 0:
        raise TypeError(str("We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    models_df.columns = columns_list
    models_df.fillna(value=0, inplace=True)
    
    #Get Model Stock Data
    full_df, columns_list = get_production_model_stock_data(args)
    full_df = pd.DataFrame(full_df)
    if len(full_df) == 0:
        raise TypeError(str("We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
    full_df.columns = columns_list
    full_df.fillna(value=0, inplace=True)

    # *************************************************************************************************
    # ****************************** Pick the top models **********************************************
    models_df = models_df.sort_values(by=['best_valid_acc'], ascending=False) #sort by valid acc
    models_df = models_df.loc[models_df.pc_number.isin(['PC1','PC2','PC3','PC4', 'Stephen'])]

    top_models_list = models_df.model_filename.head(no_top_models) #take top TEN models by valid_acc
    full_df = full_df.loc[full_df['model_filename'].isin(top_models_list)]

    # *************************************************************************************************
    full_df = full_df[full_df.signal_strength_1 > full_df.signal_strength_1.quantile(1 - args.signal_threshold)]

    gb = full_df.groupby(
        ['data_period', 'forward_date', 'index', 'ticker', 'predicted_quantile_1', 'spot_date'])
    portfolio_1 = gb.size().to_frame(name='counts')
    portfolio_1 = (portfolio_1.join(gb.agg({'signal_strength_1': 'mean'}).rename(
        columns={'signal_strength_1': 'signal_strength_mean'})).reset_index())

    del models_df, full_df, gb, top_models_list
    return portfolio_1

def insert_first_data_to_database(args):
    engine = create_engine(global_vars.DB_DROID_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        forward_date = args.forward_date
        query = f"update dlp_rating_history set dlp_1m=0, dlp_3m=0, wts_rating=0, wts_rating2=0, "
        query += f"fundamentals_value=du.fundamentals_value, fundamentals_quality=du.fundamentals_quality, "
        query += f"wts_score=0, ribbon_score=0 "
        query += f"from droid_universe du where forward_date='{forward_date}'"
        result = conn.execute(query)

        query = f"insert into dlp_rating_history (uid, ticker, index, forward_date, dlp_1m, dlp_3m, wts_rating, wts_rating2, fundamentals_value, fundamentals_quality, wts_score, ribbon_score) "
        query += f"(select '{forward_date}' || '_' || du.ticker, du.ticker, du.index, '{forward_date}'::date, 0, 0, 0 ,0, du.fundamentals_value, du.fundamentals_quality, 0, 0 from droid_universe du where du.is_active=True)"
        query += f"ON CONFLICT (uid) DO NOTHING"
        result = conn.execute(query)
    engine.dispose()

def insert_data_to_database(args, data):
    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    portfolio_2= data[data.predicted_quantile_1 == 2]
    #Make UID
    portfolio_2['forward_date'] = portfolio_2['forward_date'].astype(str)
    portfolio_2['uid']=portfolio_2['forward_date'] + "_" + portfolio_2['ticker']
    portfolio_2['uid']=portfolio_2['uid'].str.replace(" 00:00:00", "")
    portfolio_2['uid']=portfolio_2['uid'].str.strip()
    portfolio_2['forward_date'] = pd.to_datetime(portfolio_2['forward_date'])
    engine = create_engine(global_vars.DB_DROID_URL_WRITE, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        temprows = portfolio_2
        for index, row in temprows.iterrows():
            ticker = row['ticker']
            dlp_rating = row['counts']
            dlp_ss = row['signal_strength_mean']
            uid = row['uid']
            if args.num_periods_to_predict == 4:
                query = f"update dlp_rating_history set dlp_1m ={dlp_rating} where uid='{uid}'"
            elif args.num_periods_to_predict == 1:
                query = f"update dlp_rating_history set wts_rating ={dlp_rating} where uid='{uid}'"
                query2 = f"update dlp_rating_history set wts_rating2 ={dlp_ss} where uid='{uid}'"
                result2 = conn.execute(query2)
            else:
                query2 = f"update dlp_rating_history set dlp_3m ={dlp_rating}, wts_score = dlp_1m + {dlp_rating} + wts_rating + wts_rating + fundamentals_value + fundamentals_quality where uid='{uid}'"
                result2 = conn.execute(query2)
                query = f"update dlp_rating_history set ribbon_score = result.ribbon_score "
                query+= f"from (select du3.uid, du3.ticker,du3.index, du3.forward_date, du3.wts_score, (du3.st + du3.mt + du3.gq + du3.gv) as ribbon_score from "
                query+= f"(select du2.uid, du2.ticker, du2.index,du2.forward_date, "
                query+= f"CASE WHEN (du2.wts_rating + du2.dlp_1m) >= 11 THEN 1 ELSE 0 END AS st, "
                query+= f"CASE WHEN (du2.dlp_3m) >= 7 THEN 1 ELSE 0 END AS mt, "
                query+= f"CASE WHEN (du2.fundamentals_quality) >= 5 THEN 1 ELSE 0 END AS gq, "
                query+= f"CASE WHEN (du2.fundamentals_value) >= 5 THEN 1 ELSE 0 END AS gv, "
                query+= f"wts_rating*2 + dlp_1m  + dlp_3m +fundamentals_quality + fundamentals_value AS wts_score, "
                query+= f"du2.wts_rating, du2.dlp_1m, du2.dlp_3m, du2.fundamentals_value, du2.fundamentals_quality "
                query+= f"from (select du.uid, du.ticker, du.index, du.forward_date, "
                query+= f"du.wts_rating, du.dlp_1m, du.dlp_3m, "
                query+= f"du.fundamentals_value, du.fundamentals_quality from dlp_rating_history du) du2) du3 "
                query+= f"where du3.uid = '{uid}') result "
            result = conn.execute(query)
    engine.dispose()

def get_production_model_stock_data(args):
    # *************************************************************************************************
    # ****************************** Download stock data **********************************************
    production_model_stock_data_table = args.stock_table_name
    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    forward_date = args.forward_date.strftime("%Y-%m-%d")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select pmsd.data_period, pmsd.when_created, pmsd.forward_date, pmsd.spot_date, pmsd.index, pmsd.ticker, pmsd.predicted_quantile_1, pmsd.signal_strength_1, pmsd.model_filename "
        query += f"from {production_model_stock_data_table} pmsd "
        query += f"where pmsd.forward_date='{forward_date}'"
        data = pd.read_sql(query, con=conn)
        columns_list = conn.execute(query).keys()
    data = pd.DataFrame(data)
    engine.dispose()
    return data, columns_list

def get_production_model_data(args):
    production_model_data_table = args.model_table_name
    forward_date = args.forward_date.strftime("%Y-%m-%d")
    db_url = global_vars.DB_PROD_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    

    with engine.connect() as conn:
        metadata = db.MetaData()
        production_model_data_table = db.Table(production_model_data_table, metadata, autoload=True, autoload_with=conn)
        # *************************************************************************************************
        # ****************************** Download model data **********************************************
        query = db.select(
            [production_model_data_table.columns.data_period, production_model_data_table.columns.when_created, production_model_data_table.columns.forward_date,
             production_model_data_table.columns.best_train_acc,
             production_model_data_table.columns.best_valid_acc,
             production_model_data_table.columns.model_type, production_model_data_table.columns.model_filename,
             production_model_data_table.columns.pc_number]).where(and_(production_model_data_table.columns.forward_date == args.forward_date,
             production_model_data_table.columns.num_periods_to_predict == args.num_periods_to_predict))

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = conn.execute(query).keys()

    engine.dispose()
    return ResultSet, columns_list