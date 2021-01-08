import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
import pytz
from pytz import timezone
from datetime import datetime, timedelta
from pangres import upsert
from general.slack import report_to_slack
indices_table = 'indices'

def get_timezone_area(args):
    print('Get Timezone Area')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f'select index, utc_timezone_location from {indices_table} where still_live=True'
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_market_close(args):
    print('Get Market Close Time')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f'select index, market_close_time, utc_offset, close_ingestion_offset from {indices_table} where still_live=True'
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def convert_to_utc(market_close_time, utc_offset, close_ingestion_offset, plus_another_minutes):
    market_time = market_close_time.split(':')
    utc = utc_offset.split(':')
    close_offset = close_ingestion_offset.split(':')
    plus_another = plus_another_minutes.split(':')
    different_hours =int(market_time[0]) + int(utc[0]) + int(close_offset[0])
    if(int(utc[0]) < 0):
        different_minutes = int(market_time[1]) - int(utc[1]) + int(close_offset[1]) + int(plus_another[1])
    elif(int(close_offset[0]) < 0):
        different_minutes = int(market_time[1]) + int(utc[1]) - int(close_offset[1]) + int(plus_another[1])
    elif(int(utc[0]) < 0 & int(close_offset[0]) < 0):
        different_minutes = int(market_time[1]) - int(utc[1]) - int(close_offset[1]) + int(plus_another[1])
    else:
        different_minutes = int(market_time[1]) + int(utc[1]) + int(close_offset[1]) + int(plus_another[1])

    if(different_minutes >= 60):
        different_hours = different_hours + (int(different_minutes / 60))
        different_minutes = different_minutes % 60
    if(different_minutes < 0):
        different_hours = different_hours - 1
        different_minutes = 60 + different_minutes
    if(different_hours < 0):
        different_hours = 24 + different_hours
    if(different_hours >= 24):
        different_hours = different_hours - 24
    result = str(different_hours) + ":" + str(different_minutes) + ":00"
    return result

def update_utc_offset_to_database(args, data):
    print("Updating UTC Offset to Database")
    engine_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine_droid.connect() as conn:
        metadata = db.MetaData()
        for index, row in data.iterrows():
            utc_offset = row['utc_offset']
            indices = row['index']
            query = f"update {indices_table} set utc_offset='{utc_offset}' where index='{indices}'"
            result = conn.execute(query)
    engine_droid.dispose()
    print(f"UTC Offset Updated to {indices_table} table")

def update_classic_schedule_to_database(args, data):
    print("Updating classic_schedule to Database")
    engine_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine_droid.connect() as conn:
        metadata = db.MetaData()
        for index, row in data.iterrows():
            classic_schedule = row['classic_schedule']
            indices = row['index']
            query = f"update {indices_table} set classic_schedule='{classic_schedule}' where index='{indices}'"
            result = conn.execute(query)
    engine_droid.dispose()
    print(f"classic_schedule Updated to {indices_table} table")

def calculate_timezone(data):
    data["utc_offset_minutes"] = ""
    data["utc_offset"] = ""
    for i in range(len(data)):
        utc_timezone_location = data["utc_timezone_location"].loc[i]
        timezone_data = timezone(utc_timezone_location)
        timezone_now = datetime.now(timezone_data)
        result = timezone_now.utcoffset() / timedelta(minutes =15)
        data["utc_offset_minutes"].loc[i] = result
        if(result % 4 == 0):
            result = str(int(result / 4 * -1)) + ":00" + ":00"
            data["utc_offset"].loc[i] = result
        else:
            result = str(int(result * -1)) + ":" + str(int((result % 4) * 15)) + ":00"
            data["utc_offset"].loc[i] = result
    print(data)
    return data

def update_utc_offset(args):
    data = get_timezone_area(args)
    result = calculate_timezone(data)
    update_utc_offset_to_database(args, result)
    market_close = get_market_close(args)
    plus_another_minutes = "00:15:00"
    market_close["classic_schedule"] = ""
    for i in range(len(market_close)):
        market_close_time = market_close['market_close_time'].loc[i]
        utc_offset = market_close['utc_offset'].loc[i]
        close_ingestion_offset = market_close['close_ingestion_offset'].loc[i]
        market_close["classic_schedule"].loc[i] = convert_to_utc(market_close_time, utc_offset, close_ingestion_offset, plus_another_minutes)
    print(market_close)
    update_classic_schedule_to_database(args, market_close)
    
    report_to_slack("{} : === UTC Offset Updated ===".format(str(datetime.now())), args)