
from general.date_process import datetimeNow
from ingestion.mongo_migration import firebase_user_update


if __name__ == "__main__":
    user_id = [108, 109, 110]
    # currency_code = ["KRW"]
    contoh = datetimeNow()
    firebase_user_update(user_id=[119])
    print(contoh)
    print(datetimeNow())