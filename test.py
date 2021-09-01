import pandas as pd
from general.sql_query import get_active_position_ticker, get_active_universe, get_consolidated_universe_data
from ingestion.data_from_dsws import update_currency_code_from_dsws, update_lot_size_from_dsws, update_mic_from_dsws
from general.date_process import datetimeNow
from ingestion.mongo_migration import firebase_user_update



if __name__ == "__main__":
    # user_id = [108, 109, 110]
    # currency_code = ["KRW"]
    # contoh = datetimeNow()
    # firebase_user_update(user_id=[119])
    # ticker = ["1179.HK", "9868.HK"]
    # update_currency_code_from_dsws(ticker = ticker)
    old_ticker = get_active_universe(currency_code=["HKD", "USD", "EUR"])
    universe_consolidated = get_consolidated_universe_data()
    universe_consolidated = universe_consolidated.loc[universe_consolidated["consolidated_ticker"].isin(old_ticker["ticker"].to_list())]

    new_ticker = pd.read_csv("/home/loratech/Downloads/all_universe.csv")
    active_position = get_active_position_ticker()
    print(old_ticker)
    print(new_ticker)
    print(active_position)
    print(universe_consolidated)
    delete_ticker = universe_consolidated.loc[~universe_consolidated["origin_ticker"].isin(new_ticker["origin_ticker"].to_list())]
    active_ticker = universe_consolidated.loc[universe_consolidated["origin_ticker"].isin(new_ticker["origin_ticker"].to_list())]
    print(delete_ticker)
    print(active_ticker)
    
    
    
    
    # delete_ticker = old_ticker.loc[~old_ticker["ticker"].isin(new_ticker["origin_ticker"].to_list())]
    # print(delete_ticker)
    # delete_ticker = delete_ticker.loc[~delete_ticker["ticker"].isin(active_position["ticker"].to_list())]
    # print(delete_ticker)
    # delete_ticker = delete_ticker.loc[~delete_ticker["ticker"].isin(active_position["ticker"].to_list())]
    # print(delete_ticker)
    # delete_ticker = delete_ticker.loc[~delete_ticker["ticker"].isin(active_position["ticker"].to_list())]
    # print(delete_ticker)
    