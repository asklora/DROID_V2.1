
from ingestion.data_from_dsws import update_currency_code_from_dsws, update_lot_size_from_dsws, update_mic_from_dsws
from general.date_process import datetimeNow
from ingestion.mongo_migration import firebase_user_update

def split_ticker():
    from general.sql_query import get_active_universe

    ticker = get_active_universe(currency_code=["GBP"])["ticker"].to_list()
    ticker1 = ticker[0:]
    
    ticker = get_active_universe(currency_code=["KRW"])["ticker"].to_list()
    ticker2 = ticker[0:100]
    ticker3 = ticker[100:]
    
    ticker = get_active_universe(currency_code=["HKD"])["ticker"].to_list()
    ticker4 = ticker[0:100]
    ticker5 = ticker[100:200]
    ticker6 = ticker[200:]
    
    ticker = get_active_universe(currency_code=["EUR"])["ticker"].to_list()
    ticker7 = ticker[0:100]
    ticker8 = ticker[100:200]
    ticker9 = ticker[200:]
    
    ticker = get_active_universe(currency_code=["CNY"])["ticker"].to_list()
    ticker10 = ticker[0:100]
    ticker11 = ticker[100:200]
    ticker12 = ticker[200:]

    ticker = get_active_universe(currency_code=["USD"])["ticker"].to_list()
    ticker13 = ticker[0:100]
    ticker14 = ticker[100:200]
    ticker15 = ticker[200:300]
    ticker16 = ticker[300:400]
    ticker17 = ticker[400:]

if __name__ == "__main__":
    user_id = [108, 109, 110]
    # currency_code = ["KRW"]
    contoh = datetimeNow()
    firebase_user_update(user_id=[119])
    # ticker = ["1179.HK", "9868.HK"]
    # update_currency_code_from_dsws(ticker = ticker)