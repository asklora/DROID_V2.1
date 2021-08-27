
from ingestion.data_from_dsws import update_currency_code_from_dsws, update_lot_size_from_dsws, update_mic_from_dsws
from general.date_process import datetimeNow
from ingestion.mongo_migration import firebase_user_update



if __name__ == "__main__":
    user_id = [108, 109, 110]
    # currency_code = ["KRW"]
    contoh = datetimeNow()
    firebase_user_update(user_id=[119])
    # ticker = ["1179.HK", "9868.HK"]
    # update_currency_code_from_dsws(ticker = ticker)