import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd

def read_fred_csv(start_date, end_date):
    print(f"=== Read Fred Data ===")
    try :
        data_source = os.getenv("FRED_URL")
        query = f"{data_source}&cosd={start_date}&coed={end_date}"
        data = pd.read_csv(query)
        data = data.rename(columns={"DATE": "trading_day", "BAMLH0A1HYBBEY": "data"})
        return data
    except Exception as ex:
        print("error: ", ex)
        return []