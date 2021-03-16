import argparse
import datetime as dt
import sys

from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy as db
from pandas.tseries.offsets import BDay, Week
from sqlalchemy import create_engine, and_
from sqlalchemy.types import Date

import global_vars
from global_vars import no_top_models
from portfolio.main_file import get_dates_list_from_aws

# ******************************************************************************
# *******************  PARAMETERS  *********************************************
# ******************************************************************************


parser = argparse.ArgumentParser(description='Loratech')
# *******************  DATA PERIOD  ********************************************
# ******************************************************************************
parser.add_argument('--client_name', type=str, default='LORATECH')

parser.add_argument('--forward_date_start', type=str)
parser.add_argument('--forward_date_stop', type=str)

parser.add_argument('--portfolio_period', type=int, default=0)

parser.add_argument('--signal_threshold', type=int, default=global_vars.signal_threshold)

parser.add_argument('--stock_table_name', type=str, default=global_vars.test_inference_table_name,
                    help='The production table name in AWS.')

parser.add_argument('--model_table_name', type=str, default=global_vars.test_model_data_table_name,
                    help='The production table name in AWS.')
# *******************  PATHS  **************************************************
# ******************************************************************************
# parser.add_argument('--db_url', type=str, default=global_vars.DB_DL_TEST_URL, help='Database URL')
parser.add_argument('--model_path', type=str)
parser.add_argument('--plot_path', type=str)

parser.add_argument('--seed', type=int, default=123, help='Random seed')
parser.add_argument("--mode", default='client')
parser.add_argument("--port", default=64891)



parser.add_argument('--num_periods_to_predict', type=int, default=4,
                    help='Number of weeks or days to predict.')

args = parser.parse_args()


def record_test_DLP_rating(args): #record 4wk and 13wk DLP ratings to test DB

    db_url = global_vars.DB_TEST_URL_READ
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")

    table0 = args.stock_table_name
    table_models = args.model_table_name

    # args.forward_date = datetime.strptime(f'{args.portfolio_year} {args.portfolio_week} {args.portfolio_day}',
    #                                         '%G %V %u').strftime('%Y-%m-%d')

    args.forward_date_0 = str(args.forward_date)
    args.forward_date_1 = args.forward_date

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
        print(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
        return
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
        print(str(
            "We don't have inferred data for the %s with data period %s." % (args.forward_date, args.portfolio_period)))
        return
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



    portfolio_2= portfolio_1[portfolio_1.predicted_quantile_1 == 2] #only use the BUY scores
    portfolio_2 = portfolio_2.drop(columns = ['data_period','predicted_quantile_1']).rename(columns = {"counts": "dlp_rating"})
    portfolio_2 = portfolio_2.assign(num_periods_to_predict = args.num_periods_to_predict)
    engine = create_engine(DB_DL_TEST_URL, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    table = 'test_score_results'
    with engine.connect() as conn:

        portfolio_2.to_sql(con=conn,
        name=table,
        index = False,
        if_exists='append',
        )

    engine.dispose()

if __name__ == "__main__":

    dates_list = get_dates_list_from_aws(args)

    if args.forward_date_start is None and args.forward_date_stop is None:
        sys.exit('Please input either of the forward_date_start or forward_date_stop!')
    elif args.forward_date_start is None:
        args.forward_date_start = args.forward_date_stop
    elif args.forward_date_stop is None:
        args.forward_date_stop = args.forward_date_start

    args.stock_table_name = global_vars.test_inference_table_name
    args.model_table_name = global_vars.test_model_data_table_name

    for ddate in dates_list:
        args.forward_date = ddate
        args.spot_date = None

        forward_date_temp = args.forward_date
        spot_date_temp = args.spot_date
        record_test_DLP_rating(args)