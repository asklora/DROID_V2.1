import pandas as pd
from general.data_process import uid_maker
from global_vars import QUANDL_KEY, QUANDL_URL
# https://www.quandl.com/api/v3/datasets/OPT/SCHO.csv?start_date='2020-07-07'&end_date='2020-09-03'&api_key=waqWZxLx2dx84mTcsn_w
def read_quandl_csv(ticker, quandl_symbol, start_date, end_date):
    print(f"=== Read {quandl_symbol} Quandl ===")
    try :
        data_source = QUANDL_URL
        api_key = QUANDL_KEY
        query = f"{data_source}/{quandl_symbol}.csv?start_date='{start_date}'&end_date='{end_date}'&api_key={api_key}"
        data = pd.read_csv(query)
        data["ticker"] = ticker
        data = data.rename(columns={"date": "trading_day"})
        data = uid_maker(data)
        return data
    except Exception as ex:
        print("error: ", ex)
        print("This ticker " + ticker + " is Error with symbol " +quandl_symbol)
        return []