import datetime
import sys
import time
from datetime import datetime as dt
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine, and_, or_
from multiprocessing import cpu_count
from sqlalchemy.types import Date, BIGINT, TEXT
from pangres import upsert
import global_vars
from executive.data_download import get_tickers_list_from_aws, executive_data_download, get_outputs_tickers, \
    get_uno_backtest, get_latest_date, tac_data_download
from executive.data_output import upsert_data_to_db
from dateutil.relativedelta import relativedelta
from joblib import dump, load
import lightgbm as lgb
import xgboost as xgb
import sqlalchemy as db
from tqdm import tqdm

from executive.final_model import model_trainer, bot_infer, find_rank


def main_fun(args):
    # ************************************************************************
    # *********************** Data download **********************************

    main_df = pd.DataFrame()
    if args.bot_labeler_train:
        # ******************************** Data download for training *************************
        print('Started training bot labeler !')
        args.train_model = True

        if args.start_date is None:
            args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
        print(f'The start date is set as: {args.start_date}')

        if args.end_date is None:
            args.end_date = datetime.date.today()
        print(f'The end date is set as: {args.end_date}')

        if type(args.start_date) == str:
            args.start_date = dt.strptime(args.start_date, '%Y-%m-%d').date()

        if type(args.end_date) == str:
            args.end_date = dt.strptime(args.end_date, '%Y-%m-%d').date()

        args.tickers_list = get_tickers_list_from_aws()['ticker'].tolist()

        main_df = executive_data_download(args)
        output_tickers = get_outputs_tickers(args)
        # Just taking the rows that we have output for them.
        main_df = main_df[main_df.ticker.isin(output_tickers)]

    else:
        if args.bot_labeler_infer_history or args.bot_labeler_performance_history or args.bot_labeler_train:
            # ********************** Data download for Bot labeler inference history **********************
            print('Bot labeler inference history started!')
            if args.start_date is None:
                args.start_date = datetime.date.today() - relativedelta(years=args.history_num_years)
            print(f'The start date is set as: {args.start_date}')

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f'The end date is set as: {args.end_date}')

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, '%Y-%m-%d').date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, '%Y-%m-%d').date()

            ticker_list = get_tickers_list_from_aws()
            if args.exec_index is not None:
                ticker_list = ticker_list.loc[ticker_list["index"].isin(args.exec_index)]
            args.tickers_list = ticker_list['ticker'].tolist()

            main_df = executive_data_download(args)

        # *************************************** Daily *****************************************
        if args.bot_labeler_infer_daily:
            # ********************** Data download for Bot labeler inference daily **********************
            if args.exec_index is None:
                print('Please input the desired index!')
                sys.exit()

            args.latest_date = get_latest_date(args)
            print(f'{args.exec_index} Bot labeler inference daily started!')
            if args.start_date is None:
                args.start_date = args.latest_date
            print(f'The start date is set as: {args.start_date}')

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f'The end date is set as: {args.end_date}')

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, '%Y-%m-%d').date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, '%Y-%m-%d').date()

            main_df = executive_data_download(args)

        # *************************************** Live *****************************************
        if args.bot_labeler_infer_live:
            # ********************** Data download for Bot labeler inference live **********************
            print('Bot labeler inference live started!')
            if args.start_date is None:
                args.start_date = datetime.date.today()
            print(f'The start date is set as: {args.start_date}')

            if args.end_date is None:
                args.end_date = datetime.date.today()
            print(f'The end date is set as: {args.end_date}')

            if type(args.start_date) == str:
                args.start_date = dt.strptime(args.start_date, '%Y-%m-%d').date()

            if type(args.end_date) == str:
                args.end_date = dt.strptime(args.end_date, '%Y-%m-%d').date()

            args.tickers_list = get_tickers_list_from_aws()['ticker'].tolist()
            temp = get_tickers_list_from_aws()

            index_to_etf = pd.read_csv('/home/loratech/PycharmProjects/DROID/executive/index_to_etf.csv',
                                       names=['index', 'etf'])
            etf_list = index_to_etf.etf.unique().tolist()

            if args.live_index is not None:
                args.tickers_list = (temp[temp['index'].isin(args.live_index)].ticker).tolist()
                # args.tickers_list.extend(etf_list)
            if args.live_ticker is not None:
                args.tickers_list = args.live_ticker
                # args.tickers_list.extend(etf_list)

            main_df = executive_data_download(args)

    X_columns = ['c2c_vol_0_21', 'c2c_vol_126_252', 'c2c_vol_21_42',
                 'c2c_vol_252_504', 'c2c_vol_42_63', 'c2c_vol_63_126',
                 'kurt_0_504', 'total_returns_0_21',
                 'rs_vol_0_21', 'rs_vol_126_252', 'rs_vol_21_42', 'vix_value',
                 'rs_vol_252_504', 'rs_vol_42_63', 'rs_vol_63_126',
                 'total_returns_0_63', 'total_returns_21_126', 'total_returns_21_231',
                 'atm_volatility_spot_x', 'atm_volatility_one_year_x',
                 'atm_volatility_infinity_x', 'total_returns_0_63_x',
                 'total_returns_21_126_x', 'total_returns_0_21_x',
                 'total_returns_21_231_x', 'c2c_vol_0_21_x', 'c2c_vol_21_42_x', 'c2c_vol_42_63_x',
                 'c2c_vol_63_126_x', 'c2c_vol_126_252_x', 'c2c_vol_252_504_x', 'industry_code']

    # ***************************************************************************************************
    # ******************************************** Data preprocessing ***********************************
    # ***************************************************************************************************

    # Adding vix to the input data
    args.vix_f = True

    cols_temp = ['c2c_vol_0_21', 'c2c_vol_126_252', 'c2c_vol_21_42', 'total_returns_0_63',
                 'c2c_vol_252_504', 'c2c_vol_42_63', 'c2c_vol_63_126', 'total_returns_21_126',
                 'kurt_0_504', 'total_returns_21_231', 'atm_volatility_spot_x',
                 'atm_volatility_infinity_x', 'trading_day', 'ticker',
                 'rs_vol_0_21', 'rs_vol_126_252', 'rs_vol_21_42', 'rs_vol_252_504', 'rs_vol_42_63',
                 'rs_vol_63_126', 'total_returns_0_21', 'atm_volatility_one_year_x', 'total_returns_0_63_x',
                 'total_returns_21_126_x', 'total_returns_0_21_x', 'vix_value',
                 'total_returns_21_231_x', 'c2c_vol_0_21_x', 'c2c_vol_21_42_x', 'c2c_vol_42_63_x',
                 'c2c_vol_63_126_x', 'c2c_vol_126_252_x', 'c2c_vol_252_504_x',
                 'industry_code']

    main_df_copy_no_fund = main_df[cols_temp].copy()
    del main_df

    main_df_copy_no_fund = main_df_copy_no_fund.infer_objects()
    main_df_copy_no_fund['industry_code'] = main_df_copy_no_fund['industry_code'].astype(float)

    for col in X_columns:
        try:
            main_df_copy_no_fund[col] = main_df_copy_no_fund[col].fillna(
                main_df_copy_no_fund.groupby('ticker')[col].transform('mean'))
        except:
            main_df_copy_no_fund = main_df_copy_no_fund.fillna(0)

    tac_df = tac_data_download(args)

    main_df_copy_no_fund.rename(columns={'trading_day': 'spot_date'}, inplace=True)
    tac_df.rename(columns={'trading_day': 'spot_date', 'tri_adjusted_price': 'spot_price'}, inplace=True)

    bots_list = global_vars.bots_list
    if args.bot_type :
        args.bot_type = [x.lower() for x in args.bot_type]
        bots_list = args.bot_type
    final_df = main_df_copy_no_fund

    Y_columns = []
    rank_columns = []
    print(bots_list)
    for bot in bots_list:
        print(bot)
        # Download and preprocess the output data for each bot.
        df = get_uno_backtest(bot, args)
        df['return'] = df['return'].astype(float)
        print(df)
        if bot == 'classic':
            df['option_type'] = 'classic'
            df['ret'] = df['return'] - 0.01  #0.5% X 2 (in/out) slippage/comms
        else:
            df['delta_churn'] = df['delta_churn'].astype(float)
            df['ret'] = df['return'] - df['delta_churn'] * 0.005 # 0.5% slippage/comms
        df = df[['ticker', 'pnl', 'ret', 'option_type', 'month_to_exp', 'spot_date', 'spot_price']]
        df.loc[df.ret >= global_vars.bot_labeler_threshold, 'pnl_class'] = 1 #greater than threshold to deem "profitable"
        df.loc[df.ret < global_vars.bot_labeler_threshold, 'pnl_class'] = 0 #greater than threshold to deem "profitable"
        option_type_list = df.option_type.unique()
        month_exp_list = df.month_to_exp.unique()
        if (args.month_exp):
            args.month_exp = [float(i) for i in args.month_exp]
            month_exp_list = args.month_exp
        for opt_type in option_type_list:
            print(opt_type)
            for month_exp in month_exp_list:
                if (month_exp < 1) :
                    month_column = str(int(month_exp * 4)) + "w"
                else :
                    month_column = str(int(month_exp)) + "m"
                #if not (opt_type == "classic" and month_exp == 6):
                Y_columns.extend([f'{bot}_{opt_type}_{month_column}_pnl_class'])
                rank_columns.extend([f'{bot}_{opt_type}_{month_column}_pnl_class_prob'])
                df2 = df.loc[(df.option_type == opt_type) & (df.month_to_exp == month_exp), :]
                df2 = df2[df2['pnl'].notna()]
                df2.rename(columns={'pnl': f'{bot}_{opt_type}_{month_column}_pnl',
                    'pnl_class': f'{bot}_{opt_type}_{month_column}_pnl_class'}, inplace=True)

                df2 = df2.drop_duplicates(['ticker', 'spot_date'], keep='last')
                final_df = final_df.merge(df2[['ticker', 'spot_date', f'{bot}_{opt_type}_{month_column}_pnl_class',
                    f'{bot}_{opt_type}_{month_column}_pnl']], on=['ticker', 'spot_date'], how='left')
    final_df = final_df.merge(tac_df[['ticker', 'spot_date', 'spot_price']], on=['ticker', 'spot_date'], how='left')
    args.Y_columns = Y_columns
    args.X_columns = X_columns
    args.rank_columns = rank_columns
    # **************************************************************************************
    # **************************** Bot labeler history ************************************
    # **************************************************************************************

    if args.bot_labeler_performance_history:
        # table_name = global_vars.bot_labeler_history_table_name + 'check'
        # db_url = global_vars.DB_TEST_URL_WRITE
        # try:
        #     engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
        #     with engine.connect() as conn:
        #         metadata = db.MetaData()
        #         table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)
        #
        #         query = table0.delete().where(table0.columns.model_type == args.model_type)
        #
        #         conn.execute(query)
        # except:
        #     pass

        infer_df, latest_df = bot_infer(final_df, args)
        infer_df = infer_df.merge(tac_df[['ticker', 'index', 'spot_date']], on=['ticker', 'spot_date'], how='left')
        infer_df['spot_date'] = infer_df['spot_date'].astype(str)
        infer_df['uid'] = infer_df['spot_date'] + "_" + infer_df['ticker']
        infer_df['spot_date'] = pd.to_datetime(infer_df['spot_date'])

        latest_df = latest_df.merge(tac_df[['ticker', 'index', 'spot_date']], on=['ticker', 'spot_date'], how='left')

        data = []
        max_date = final_df.spot_date.max()
        max_training_date = max_date - relativedelta(
            years=(args.history_num_years - args.bot_labeler_training_num_years))
        while max_training_date.weekday() != 4:
            max_training_date = max_training_date - BDay(1)

        while max_training_date < dt.today():
            train_df = final_df[final_df.spot_date <= max_training_date]
            infer_df = final_df[(final_df.spot_date > max_training_date) &
                                (final_df.spot_date <= max_training_date + BDay(5))]

            report = model_trainer(train_df, infer_df, args)

            report = report.apply(lambda x: find_rank(x, rank_columns), axis=1)

            columns_list = report.columns
            final_columns = ['ticker', 'spot_date', 'spot_price', 'when_created', 'model_type']
            for col in columns_list:
                if 'rank' in col:
                    final_columns.extend([col])

            report = report[final_columns]
            #print(report)
            if (len(data) == 0):
                #print("First Data")
                data = report
            else:
                #print("Append")
                data = data.append(report)
            #print(max_training_date)

            max_training_date = max_training_date + BDay(5)
        data['spot_date'] = data['spot_date'].astype(str)
        data['uid'] = data['spot_date'] + data['ticker']
        data['spot_date'] = pd.to_datetime(data['spot_date'])
        data = data.drop_duplicates(subset='uid', keep="first")
        print(data)
        data.to_csv("data_bot_labeler_performance_history.csv")
        upsert_data_to_db(args, "uid", TEXT, data, global_vars.bot_labeler_history_table_name, method="update")
        upsert_data_to_db(args, "ticker", TEXT, latest_df, global_vars.latest_bot_rankings_table_name, method="update")
        # upsert_data_to_database(args, "uid", TEXT, data, method="update")
        # upsert_data_to_database("uid", TEXT, global_vars.bot_labeler_history_table_name, data, how="update")
        print('Finished bot history labelling.')

    # **************************************************************************************
    # **************************** Bot labeler daily ************************************
    # **************************************************************************************
    if args.bot_labeler_train:
        model_trainer(final_df, None, args, just_train=True)
        print('Finished bot labelling model training.')

    if args.bot_labeler_infer_daily or args.bot_labeler_infer_live:
        infer_df, latest_df = bot_infer(final_df, args)
        infer_df = infer_df.merge(tac_df[['ticker', 'index', 'spot_date']], on=['ticker', 'spot_date'], how='left')
        infer_df['spot_date'] = infer_df['spot_date'].astype(str)
        infer_df['uid']=infer_df['spot_date'] + "_" + infer_df['ticker']
        infer_df['spot_date'] = pd.to_datetime(infer_df['spot_date'])

        latest_df = latest_df.merge(tac_df[['ticker', 'index', 'spot_date']], on=['ticker', 'spot_date'], how='left')

        try:
            upsert_data_to_db(args, "uid", TEXT, infer_df, global_vars.bot_rankings_table_name, method="update")
            # upsert_data_to_database(args, "uid", TEXT, infer_df, method="update")
            # upsert_data_to_database("uid", TEXT, global_vars.bot_rankings_table_name, infer_df, how="update")
        except Exception as e:
            print(e)

        try:
            upsert_data_to_db(args, "ticker", TEXT, latest_df, global_vars.latest_bot_rankings_table_name, method="update")
            # upsert_data_to_database(args, "ticker", TEXT, latest_df, method="update")
            # upsert_data_to_database("ticker", TEXT, global_vars.latest_bot_rankings_table_name, latest_df, how="update")
        except Exception as e:
            print(e)

        # db_url = global_vars.DB_DROID_URL_WRITE
        # engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
        # with engine.connect() as conn:
        #     infer_df.to_sql(con=conn, name=global_vars.bot_rankings_table_name, schema='public',
        #                     if_exists='append', index=False)
        #     try:
        #         metadata = db.MetaData()
        #         table0 = db.Table(global_vars.latest_bot_rankings_table_name, metadata, autoload=True, autoload_with=conn)

        #         query = table0.delete().where(or_(table0.columns.spot_date != latest_df.spot_date.max(),
        #                                            table0.columns.ticker.in_(latest_df.ticker)))

        #         conn.execute(query)

        #         latest_df.to_sql(con=conn, name=global_vars.latest_bot_rankings_table_name, schema='public',
        #                         if_exists='append', index=False)
        #     except Exception as e:
        #         print(e)
        #         latest_df.to_sql(con=conn, name=global_vars.latest_bot_rankings_table_name, schema='public',
        #                         if_exists='append', index=False)
        
        print('Finished adding bot rankings daily or live.')

    # **************************************************************************************
    # **************************************************************************************
    # **************************************************************************************

def bot_stats_report(args):
    tqdm.pandas()

    table_name = global_vars.bot_labeler_history_table_name
    db_url = global_vars.DB_TEST_URL_WRITE
    engine = create_engine(db_url, pool_size=cpu_count(), max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        table0 = db.Table(table_name, metadata, autoload=True, autoload_with=conn)

        query = table0.select()

        ResultProxy = conn.execute(query)
        ResultSet = ResultProxy.fetchall()
        columns_list = ResultProxy.keys()
    engine.dispose()

    full_df = pd.DataFrame(ResultSet)

    if len(full_df) == 0:
        print(f"We don't have {table_name} data.")
        sys.exit()
    full_df.columns = columns_list
    full_df = full_df[full_df.model_type == 'rf']

    # **************************************************************************************

    report = pd.DataFrame(index=range(len(full_df.model_type.unique())))
    for j in range(len(full_df.model_type.unique())):
        model_type_name = full_df.model_type.unique()[j]
        # report.loc[j, 'model_type'] = full_df.model_type.unique()[j]

        for i in range(len(full_df.rank_1.unique())):
            report.loc[j * len(full_df.rank_1.unique()) + i, 'model_type'] = full_df.model_type.unique()[j]
            report.loc[j * len(full_df.rank_1.unique()) + i, 'rank'] = i + 1
            report.loc[j * len(full_df.rank_1.unique()) + i, f'pnl_spot_avg'] = np.nanmean(
                full_df.loc[full_df.model_type == model_type_name, f'pnl_avg_rank_{i + 1}'].values)
            report.loc[j * len(full_df.rank_1.unique()) + i, f'pnl_spot_std'] = np.nanstd(
                full_df.loc[full_df.model_type == model_type_name, f'pnl_avg_rank_{i + 1}'].values)
            report.loc[j * len(full_df.rank_1.unique()) + i, f'acc_avg'] = np.nanmean(
                full_df.loc[full_df.model_type == model_type_name, f'acc_rank_{i + 1}'].values)

    report.to_csv('report_bot_statistics.csv')

    report_2 = pd.DataFrame()
    for i in range(len(full_df.rank_1.unique())):
        report_2[f'rank_{i + 1}_classes'] = full_df[f'rank_{i + 1}'].value_counts(normalize=True) * 100

    report_2.to_csv('report_bot_classes_statistics.csv')

    print('Finished creating report for bot labeler!')


