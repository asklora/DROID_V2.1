import numpy as np
import pandas as pd
from general.data_process import uid_maker
from sqlalchemy import create_engine

db_quant = "postgresql://asklora:AskLORAv2@pgm-3nscoa6v8c876g5xlo.pg.rds.aliyuncs.com:1924/postgres"
def read_query(query):
    engine = create_engine(db_quant, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        data = pd.read_sql(query, con=conn, chunksize=20000)
        data = pd.concat(data, axis=0, ignore_index=False)
    engine.dispose()
    data = pd.DataFrame(data)
    print(data)
    return data

def get_holiday(non_working_day=None, currency_code=None):
    table_name = "currency_calendar"
    conditions = ["True"]
    if non_working_day:
        conditions.append(f"non_working_day = '{non_working_day}'")
    if currency_code:
        conditions.append(f"currency_code = '{currency_code}'")
    query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)}"
    data = read_query(query)
    return data

def FillMissingDay(data, start, end):
    ''' Fill missing holidays (i.e. not weekend) '''

    result = data[["ticker", "trading_day"]]
    data["trading_day"] = pd.to_datetime(data["trading_day"])

    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start, end, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)

    # remove weekend
    result = result.loc[~result["trading_day"].dt.weekday.isin([5, 6])]
    result = result.merge(data, how="left", on=["ticker", "trading_day"])
    result["currency_code"] = result["currency_code"].ffill().bfill()

    # total return index ffill + dropna if still NaN
    result['total_return_index'] = result.groupby(['ticker'])['total_return_index'].ffill()
    result = result.dropna(subset=['total_return_index'])
    result = uid_maker(result)
    return result

def CountDatapoint(data):
    data["datapoint_per_day"] = data[["open", "high", "low", "close", "volume"]].count(axis=1)
    print("Update Holiday Date")
    holiday = get_holiday()[["non_working_day", "currency_code"]].rename(columns={"non_working_day":"trading_day"})
    holiday["trading_day"] = pd.to_datetime(holiday["trading_day"])
    holiday["holiday"] = True
    data = data.merge(holiday, on=["trading_day", "currency_code"], how="left")
    data['holiday'] = data['holiday'].fillna(False)
    print(data.loc[data["holiday"]])
    print("Fill Day Status")
    conditions = [
        (data["holiday"]),
        (data["datapoint_per_day"] >= 2)
    ]
    choices = ["holiday", "trading_day"]
    data["day_status"] = np.select(conditions, choices, default="missing")
    return data.drop(columns=["holiday"])
