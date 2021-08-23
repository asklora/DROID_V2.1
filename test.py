
from ingestion.mongo_migration import firebase_user_update


if __name__ == "__main__":
    user_id = [108, 109, 110]
    currency_code = ["KRW"]
    firebase_user_update(currency_code=currency_code)