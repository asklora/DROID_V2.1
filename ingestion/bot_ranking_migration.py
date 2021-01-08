import sys
import global_vars
import pandas as pd
import numpy as np
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Text
from pangres import upsert
from general.general import dateNow

bot_labeler_history_table = 'bot_labeler_history'
bot_rankings_table = 'bot_rankings'
droid_universe_table = "droid_universe"

def get_index(args):
    print("Get Data From DROID")
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker, index from {droid_universe_table}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return data

def get_bot_ranking_hkpolyu(args, spot_date, forward_date):
    print("Get Data From HKPolyu")
    engine = create_engine(args.db_url_hkpolyu, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {bot_labeler_history_table} where spot_date >= '{spot_date}' and spot_date < '{forward_date}'"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    print("DONE")
    return data

def bot_ranking_ingestion(args):
    spot_date = '2019-10-14'
    forward_date = dateNow()

    index = get_index(args)

    bot_ranking_hkpolyu = get_bot_ranking_hkpolyu(args, spot_date, forward_date)
    bot_ranking_hkpolyu = bot_ranking_hkpolyu.merge(index, how="left", on=["ticker"])

    bot_ranking_hkpolyu['spot_date'] = bot_ranking_hkpolyu['spot_date'].astype(str)
    bot_ranking_hkpolyu['uid']=bot_ranking_hkpolyu['spot_date'] + "_" + bot_ranking_hkpolyu['ticker']
    bot_ranking_hkpolyu['spot_date'] = pd.to_datetime(bot_ranking_hkpolyu['spot_date'])

    bot_ranking = bot_ranking_hkpolyu[["uid", "index", "ticker", "spot_date", "model_type", "when_created", 
        "rank_1", "rank_2", "rank_3", "rank_4", "rank_5", "rank_6", "rank_7"]]

    rank_field = ["rank_1", "rank_2", "rank_3", "rank_4", "rank_5", "rank_6", "rank_7"]
    types_field = ["uno_ITM_3m","uno_ITM_1m","uno_OTM_3m","uno_OTM_1m","classic_classic_1m", "classic_classic_3m", "ucdc_ATM_6m"]

    for types in types_field:
        temp_df = pd.DataFrame({'ticker':[],'spot_date':[],f"{types}_pnl_class_prob":[]}, index=[])
        for rank in rank_field:
            temp = bot_ranking_hkpolyu.loc[bot_ranking_hkpolyu[rank] == types]
            temp = temp[["ticker", "spot_date", f"pnl_class_prob_{rank}"]]
            temp = temp.rename(columns={f"pnl_class_prob_{rank}" : f"{types}_pnl_class_prob"})
            temp_df = temp_df.append(temp)
        bot_ranking = bot_ranking.merge(temp_df, how="left", on=["ticker", "spot_date"])

    upsert_data_to_database(bot_ranking, args)

def upsert_data_to_database(result, args):
    print('=== Insert Bot Ranking to database ===')
    result = result.set_index('uid')
    dtype = {'uid':Text}
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=bot_rankings_table,
           if_row_exists='ignore',
           dtype=dtype)
    print("DATA INSERTED TO " + bot_rankings_table)
    engines_droid.dispose()