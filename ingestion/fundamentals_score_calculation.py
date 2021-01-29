# import pandas as pd
# import numpy as np
# from sqlalchemy import create_engine
# from sqlalchemy.types import Date, BIGINT, TEXT
# import sqlalchemy as db
# from datetime import datetime
# from general.slack import report_to_slack
# from general.general import datetimeNow
# from pangres import upsert
# import sys
# from sklearn.preprocessing import RobustScaler, robust_scale, minmax_scale, MinMaxScaler
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.sql.expression import bindparam

# fundamentals_score_table = 'fundamentals_score'
# fundamentals_quality_score_table = 'fundamentals_quality_score'
# fundamentals_value_score_table = 'fundamentals_value_score'
# master_ohlctr_table = 'master_ohlctr'
# droid_universe_table = 'droid_universe'

# def update_fundamentals_as_null_to_database(args):
#     print("Updating Fundamentals Initial to Database")
#     engine_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     with engine_droid.connect() as conn:
#         metadata = db.MetaData()
#         query = f"update {droid_universe_table} set fundamentals_value=null, fundamentals_quality=null"
#         execution = conn.execute(query)
#     engine_droid.dispose()
#     print(f"Fundamentals Initial Updated to {droid_universe_table} table")

# def update_fundamentals_value_in_droid_universe(args, result):
#     update_fundamentals_as_null_to_database(args)
#     print('=== Update Fundamentals Value & Fundamentals Quality to database ===')
#     result = result[["ticker","fundamentals_value","fundamentals_quality"]]
#     print(result)
#     resultdict = result.to_dict('records')
#     engine = db.create_engine(args.db_url_droid_write)
#     sm = sessionmaker(bind=engine)
#     session = sm()

#     metadata = db.MetaData(bind=engine)

#     datatable = db.Table(droid_universe_table, metadata, autoload=True)
#     stmt = db.sql.update(datatable).where(datatable.c.ticker == bindparam('ticker')).values({
#         'fundamentals_value': bindparam('fundamentals_value'),
#         'fundamentals_quality': bindparam('fundamentals_quality'),
#         'ticker': bindparam('ticker')

#     })
#     session.execute(stmt,resultdict)

#     session.flush()
#     session.commit()
#     engine.dispose()
#     print('=== Fundamentals Value & Fundamentals Quality Updated ===')

# def get_fundamentals_score_from_db(args):
#     print('Get Fundamentals Score From Database')
#     engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     with engine.connect() as conn:
#         metadata = db.MetaData()
#         query = f'select fs.* from {fundamentals_score_table} fs inner join droid_universe du on fs.ticker = du.ticker where du.is_active = True'
#         all_universe = pd.read_sql(query, con=conn)
#     engine.dispose()
#     all_universe = pd.DataFrame(all_universe)
#     return all_universe

# def get_last_close_price_from_db(args):
#     print('Get Last Close Price From Database')
#     engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
#     with engine.connect() as conn:
#         metadata = db.MetaData()
#         query = f"select mo.ticker, mo.close, mo.index, substring(univ.industry_code from 0 for 3) as industry_code from master_ohlctr mo inner join droid_universe univ on univ.ticker=mo.ticker "
#         query = query + f"where univ.is_active=True and exists( select 1 from (select ticker, max(trading_day) max_date from master_ohlctr where close is not null group by ticker) filter where filter.ticker=mo.ticker and filter.max_date=mo.trading_day)"
#         data = pd.read_sql(query, con=conn)
#     engine.dispose()
#     result = pd.DataFrame(data)
#     return result

# def calculate_quality_value_score(args):
#     print('=== Calculating Fundamentals Value & Fundamentals Quality ===')
#     calculate_column = ["earnings_yield", "book_to_price", "ebitda_to_ev", "sales_to_price", "roic", "roe", "cf_to_price", "eps_growth", 
#                         "fwd_bps","fwd_ebitda_to_ev", "fwd_ey", "fwd_sales_to_price", "fwd_roic"]
#     fundamentals_score = get_fundamentals_score_from_db(args)
#     print(fundamentals_score)
#     close_price = get_last_close_price_from_db(args)
#     print(close_price)
#     fundamentals_score = close_price.merge(fundamentals_score, how="left", on='ticker')
#     fundamentals_score['earnings_yield'] = fundamentals_score['eps'] / fundamentals_score['close']
#     fundamentals_score['book_to_price'] = fundamentals_score['bps'] / fundamentals_score['close']
#     fundamentals_score['ebitda_to_ev'] = fundamentals_score['ttm_ebitda'] / fundamentals_score['ev']
#     fundamentals_score['sales_to_price'] = fundamentals_score['ttm_rev'] / fundamentals_score['mkt_cap']
#     fundamentals_score['roic'] = (fundamentals_score['ttm_ebitda'] - fundamentals_score['ttm_capex']) / (fundamentals_score['mkt_cap'] + fundamentals_score['net_debt'])
#     fundamentals_score['roe'] = fundamentals_score['roe']
#     fundamentals_score['cf_to_price'] = fundamentals_score['cfps'] / fundamentals_score['close']
#     fundamentals_score['eps_growth'] = fundamentals_score['peg']
#     fundamentals_score['fwd_bps'] = fundamentals_score['bps1fd12']  / fundamentals_score['close']
#     fundamentals_score['fwd_ebitda_to_ev'] = fundamentals_score['ebd1fd12']  / fundamentals_score['evt1fd12']
#     fundamentals_score['fwd_ey'] = fundamentals_score['eps1fd12']  / fundamentals_score['close']
#     fundamentals_score['fwd_sales_to_price'] = fundamentals_score['sal1fd12']  / fundamentals_score['mkt_cap']
#     fundamentals_score['fwd_roic'] = (fundamentals_score['ebd1fd12'] - fundamentals_score['cap1fd12']) / (fundamentals_score['mkt_cap'] + fundamentals_score['net_debt'])
#     fundamentals = fundamentals_score[["earnings_yield", 
#                                        "book_to_price", 
#                                        "ebitda_to_ev", 
#                                        "sales_to_price", 
#                                        "roic", 
#                                        "roe", 
#                                        "cf_to_price", 
#                                        "eps_growth", 
#                                        "index", 
#                                        "ticker", 
#                                        "industry_code",
#                                        "fwd_bps",
#                                        "fwd_ebitda_to_ev",
#                                        "fwd_ey",
#                                        "fwd_sales_to_price",
#                                        "fwd_roic"]]
#     print(fundamentals)

#     calculate_column_score = []
#     for column in calculate_column:
#         column_score = column + "_score"
#         column_mean = column + "_mean"
#         column_std = column + "_std"
#         column_upper= column + "_upper"
#         column_lower= column + "_lower"
#         mean = np.nanmean(fundamentals[column])
#         std = np.nanstd(fundamentals[column])
#         upper = mean + (std * 2)
#         lower = mean - (std * 2)
#         fundamentals[column_score] = np.where(fundamentals[column] > upper, upper, fundamentals[column])
#         fundamentals[column_score] = np.where(fundamentals[column_score] < lower, lower, fundamentals[column_score])
#         calculate_column_score.append(column_score)
#     print(calculate_column_score)
#     calculate_column_robust_score = []
#     for column in calculate_column:
#         column_score = column + "_score"
#         column_robust_score = column + "_robust_score"
#         fundamentals[column_robust_score] = robust_scale(fundamentals[column_score])
#         calculate_column_robust_score.append(column_robust_score)
#     fundamentals_robust_score_calc = fundamentals[calculate_column_robust_score]
#     scaler = MinMaxScaler().fit(fundamentals_robust_score_calc)
#     for column in calculate_column:
#         column_score = column + "_score"
#         column_robust_score = column + "_robust_score"
#         column_minmax_index = column + "_minmax_index"
#         column_minmax_industry = column + "_minmax_industry"
#         df_index = fundamentals[['index', column_robust_score]]
#         df_index = df_index.rename(columns = {column_robust_score : 'score'})
#         print(df_index)
#         df_industry = fundamentals[['industry_code', column_robust_score]]
#         df_industry = df_industry.rename(columns = {column_robust_score : 'score'})
#         print(df_industry)
#         fundamentals[column_minmax_index] = df_index.groupby('index').score.transform(lambda x: minmax_scale(x.astype(float)))
#         fundamentals[column_minmax_industry] = df_industry.groupby('industry_code').score.transform(lambda x: minmax_scale(x.astype(float)))

#         fundamentals[column_minmax_index] = np.where(fundamentals[column_minmax_index].isnull(), 0.4, fundamentals[column_minmax_index])

#         fundamentals[column_minmax_industry] = np.where(fundamentals[column_minmax_industry].isnull(), 0.4, fundamentals[column_minmax_industry])

#     #TWELVE points - everthing average yields 0.5 X 12 = 6.0 score
#     fundamentals['fundamentals_value'] = ((fundamentals['earnings_yield_minmax_index']) + 
#                                     fundamentals['earnings_yield_minmax_industry'] + 
#                                     fundamentals['book_to_price_minmax_index'] + 
#                                     fundamentals['book_to_price_minmax_industry'] + 
#                                     fundamentals['ebitda_to_ev_minmax_index'] + 
#                                     fundamentals['ebitda_to_ev_minmax_industry'] +
#                                     fundamentals['fwd_bps_minmax_industry'] + 
#                                     fundamentals['fwd_ebitda_to_ev_minmax_index'] + 
#                                     fundamentals['fwd_ebitda_to_ev_minmax_industry'] + 
#                                     fundamentals['fwd_ey_minmax_index']+ 
#                                     fundamentals['roe_minmax_industry']+ 
#                                     fundamentals['cf_to_price_minmax_index']).round(1)
#     fundamentals['fundamentals_quality'] = ((fundamentals['roic_minmax_index']) + 
#                                       fundamentals['roic_minmax_industry']+
#                                       fundamentals['cf_to_price_minmax_industry']+
#                                       fundamentals['eps_growth_minmax_index'] + 
#                                       fundamentals['eps_growth_minmax_industry'] + 
#                                       (fundamentals['fwd_ey_minmax_industry'] *2) + 
#                                       fundamentals['fwd_sales_to_price_minmax_industry']+ 
#                                       (fundamentals['fwd_roic_minmax_industry'] *2) +
#                                       fundamentals['earnings_yield_minmax_industry']).round(1)

#     print('=== Calculate Fundamentals Value & Fundamentals Quality DONE ===')
#     timeprint = str(datetimeNow())
#     update_fundamentals_value_in_droid_universe(args, fundamentals)
#     print(fundamentals)
#     report_to_slack("{} : === Fundamentals Value & Fundamentals Quality Updated ===".format(str(datetime.now())), args)
#     fundamentals.to_csv(f"fundamentals_calc_history/fundamentals_score_calculation{timeprint}.csv")
#     return True

# def calculate_fundamentals_score(args):
#     calculate_quality_value_score(args)
#     sys.exit(1)
#     return True