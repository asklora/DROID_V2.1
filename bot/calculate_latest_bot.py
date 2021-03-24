import sys
import json
import base64
import pandas as pd
import numpy as np
import time as tm
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.types import Date, BIGINT, TEXT
from datetime import datetime
from general.slack import report_to_slack
from general.general import dateNow, timeNow, forwarddate_by_month, backdate_by_day, datetimeNow
from data_source.DSWS import get_company_des_from_dsws
from universe import DroidUniverse
from pangres import upsert
import numpy as np
from datetime import datetime, timezone, time, timedelta, date
from dateutil.relativedelta import relativedelta
import executive.black_scholes as  uno
from universe import DroidUniverse, droid_universe_by_index, droid_universe_by_country_code

droid_universe_table = 'droid_universe'
latest_bot_updates_table = 'latest_bot_updates'
latest_vol_updates_table = 'latest_vol_updates'
calendar_table = 'calendar'
bot_option_type_table = 'bot_option_type'
latest_vol_updates_table = 'latest_vol_updates'
latest_price_updates_table = 'latest_price_updates'
daily_div_rates_table = 'daily_div_rates'
daily_interest_rates_table = 'daily_interest_rates'
indices_table = 'indices'

def get_currency_code(args, index):
    tuple_index = tuple(index.to_list())
    #tuple_index = "('0#.SPX')"
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select index, currency_code from {indices_table} where index in {tuple_index}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_daily_div_rates(args, t, ticker):
    tuple_ticker = tuple(ticker.to_list())
    #tuple_ticker = "('MSFT.O')"
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker, q from {daily_div_rates_table} where t={t} and ticker in {tuple_ticker}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_daily_interest_rates(args, currency_code, t):
    tuple_currency_code = tuple(currency_code.to_list())
    #tuple_currency_code = "('USD')"
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select currency_code, r from {daily_interest_rates_table} where t={t} and currency_code in {tuple_currency_code}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def insert_data_to_database(args, result):
    print('=== Upsert Data To Database ===')
    result = result.set_index('uid')
    dtype={
        "uid":TEXT,
        "ticker":TEXT
    }
    engines_droid = create_engine(args.db_url_droid_write, max_overflow=-1, isolation_level="AUTOCOMMIT")
    upsert(engine=engines_droid,
           df=result,
           table_name=latest_bot_updates_table,
           if_row_exists='update',
           dtype=dtype)
    print("DATA UPDATED TO " + latest_bot_updates_table)
    engines_droid.dispose()
    del result

def get_holiday_by_country_code(args, country_code, non_working_day):
    #print('=== Get Active Droid Universe ===')
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {calendar_table} where country_code='{country_code}' and non_working_day='{non_working_day}'"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_bot_option_type(args):
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {bot_option_type_table}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_latest_price_updates(args):
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select ticker, close as spot_price, classic_vol from {latest_price_updates_table}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_latest_vol_updates(args):
    engine = create_engine(args.db_url_droid_read, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * from {latest_vol_updates_table}"
        data = pd.read_sql(query, con=conn)
    engine.dispose()
    data = pd.DataFrame(data)
    return data

def get_expiry_date(args, country_code):
    expiry_1 = datetime.now().date() + relativedelta(months=1) - relativedelta(days=1)
    expiry_3 = datetime.now().date() + relativedelta(months=3) - relativedelta(days=1)
    expiry_6 = datetime.now().date() + relativedelta(months=6) - relativedelta(days=1)

    expiry_list = [expiry_1, expiry_3, expiry_6]

    del expiry_1, expiry_3, expiry_6

    for i in range(len(expiry_list)):
        while True:
            holiday = False
            non_working_day = expiry_list[i].strftime("%Y-%m-%d")
            data = get_holiday_by_country_code(args, country_code, non_working_day)
            if(len(data) > 0):
                holiday = True
            if ((holiday == False) and expiry_list[i].weekday() < 5):
                break
            else:
                expiry_list[i] = expiry_list[i] - relativedelta(days=1)

    expiry_1 = expiry_list[0].strftime("%Y-%m-%d")
    expiry_3 = expiry_list[1].strftime("%Y-%m-%d")
    expiry_6 = expiry_list[2].strftime("%Y-%m-%d")
    del expiry_list
    return expiry_1, expiry_3, expiry_6

def get_Classic(data, bot_horizon_month):
    # REQUIREMENT FOR CLASSIC

    # [max_loss_pct] = stop_loss = - (dur * classic_vol * 1.25)
    # dur = 0.288675 X 1 if 1 month
    # dur = 0.5 X 1.75 if 3 month
    # [target_profit_pct]= take profit = (dur * classic_vol)
    # dur = 0.288675 X 1 if 1 month
    # dur = 0.5 X 1.75 if 3 month

    data["classic_vol"] = np.where(data["classic_vol"].isnull(), 0.2, data["classic_vol"])
    data["classic_vol"] = np.where(data["classic_vol"] == 0, 0.2, data["classic_vol"])
    if bot_horizon_month == 1:
        dur = 0.288675 * 1
    elif bot_horizon_month == 3:
        dur = 0.5 * 1.75
    data["potential_max_loss"] = - (dur * data["classic_vol"] * 1.25)
    data["targeted_profit"] = (dur * data["classic_vol"])
    return data

def get_trq(args, data, bot_horizon_month):
    if bot_horizon_month == 1:
        expiry = data['expiry_1'].iloc[0]
    elif bot_horizon_month == 3:
        expiry = data['expiry_3'].iloc[0]
    elif bot_horizon_month == 6:
        expiry = data['expiry_6'].iloc[0]

    expiry = datetime.strptime(expiry, "%Y-%m-%d")
    daydiff = (expiry.date() - datetime.now().date()).days
    t = daydiff / 365
    if t == 0:
        t = 0.0027397260273972603
    #print(f"{expiry.date()} - {datetime.now().date()} = {daydiff}")
    #print(f"Horizon {bot_horizon_month} = {t}")
    data["t"] = t

    result_q = get_daily_div_rates(args, t, data["ticker"])
    if len(result_q) > 0:
        data = data.merge(result_q, how='left', on="ticker")
    else :
        data["q"] = 0
    del result_q

    result_r = get_daily_interest_rates(args, data["currency_code"], t)
    if len(result_r) > 0:
        data = data.merge(result_r, how='left', on='currency_code')
    else :
        data["r"] = 0
    del result_r

    data["q"] = np.where(data["q"].isnull(), 0, data["q"])
    data["r"] = np.where(data["r"].isnull(), 0, data["r"])
    return data

def get_vol(args, data):
    data["v0"] = uno.find_vol(1, data["t"], 
        data["atm_volatility_spot"], data["atm_volatility_one_year"],
        data["atm_volatility_infinity"], 12, 
        data["slope"], data["slope_inf"], data["deriv"], 
        data["deriv_inf"], data["r"], data["q"])
    
    data["v0"] = np.where(data["v0"] > 0.5, 0.5, data["v0"])
    data["v0"] = np.where(data["v0"] < 0.2, 0.2, data["v0"])

    data["vol_t_1"] = data["v0"] * (1/12)**0.5
    data["vol_t_3"] = data["v0"] * (3/12)**0.5
    data["vol_t_6"] = data["v0"] * (6/12)**0.5
    
    return data

def get_strike_barrier_rebate(data, bot_horizon_month, bot_option_type, bot_group):
    if bot_horizon_month == 1:
        vol = "vol_t_1"
    if bot_horizon_month == 3:
        vol = "vol_t_3"
    if bot_horizon_month == 6:
        vol = "vol_t_6"

    if bot_group == "UNO":
        if bot_option_type == 'OTM':
            data["strike"] = data["spot_price"] * (1 + data[vol] * 0.5)
        elif bot_option_type == 'ITM':
            data["strike"] = data["spot_price"] * (1 - data[vol] * 0.5)

        if bot_option_type == 'OTM':
            data["barrier"] = data["spot_price"] * (1 + data[vol] * 2)
        elif bot_option_type == 'ITM':
            data["barrier"] = data["spot_price"] * (1 + data[vol] * 1.5)
    
        data["rebate"] = data["barrier"] - data["strike"]

    if bot_group == "UCDC":
        data["strike"] = data["spot_price"]
        data["strike_2"] = data["spot_price"] * (1 - data["vol_t_6"] * 1.5)

    return data

def get_v1_v2_delta_option_price(data, bot_group):
    if bot_group == "UNO":
        data["v1"] = uno.find_vol(data["strike"] / data["spot_price"], data["t"], 
            data["atm_volatility_spot"], data["atm_volatility_one_year"],
            data["atm_volatility_infinity"], 12, 
            data["slope"], data["slope_inf"], data["deriv"], 
            data["deriv_inf"], data["r"], data["q"])
        data["v2"] = uno.find_vol(data["barrier"] / data["spot_price"], data["t"], 
            data["atm_volatility_spot"], data["atm_volatility_one_year"],
            data["atm_volatility_infinity"], 12, 
            data["slope"], data["slope_inf"], data["deriv"], 
            data["deriv_inf"], data["r"], data["q"])

        data["delta"] = uno.deltaUnOC(data["spot_price"], data["strike"], data["barrier"], data["rebate"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
        data["option_price"] = uno.Up_Out_Call(data["spot_price"], data["strike"], data["barrier"], data["rebate"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    elif bot_group == "UCDC":
        data["v1"] = uno.find_vol(data["strike"] / data["spot_price"], data["t"], 
            data["atm_volatility_spot"], data["atm_volatility_one_year"],
            data["atm_volatility_infinity"], 12, 
            data["slope"], data["slope_inf"], data["deriv"], 
            data["deriv_inf"], data["r"], data["q"])
        data["v2"] = uno.find_vol(data["strike_2"] / data["spot_price"], data["t"], 
            data["atm_volatility_spot"], data["atm_volatility_one_year"],
            data["atm_volatility_infinity"], 12, 
            data["slope"], data["slope_inf"], data["deriv"], 
            data["deriv_inf"], data["r"], data["q"])

        data["delta"] = uno.deltaRC(data["spot_price"], data["strike"], data["strike_2"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
        data["option_price"] = uno.Rev_Conv(data["spot_price"], data["strike"], data["strike_2"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    return data

def get_loss_profit(data, bot_group):
    if bot_group == "UNO":
        data["potential_max_loss"] = -1 * data["option_price"] / data["spot_price"]
        data["targeted_profit"] = (data["barrier"]-data["strike"]) / data["spot_price"]
    elif bot_group == "UCDC":
        data["potential_max_loss"] = (data["strike_2"]-data["strike"]) / data["spot_price"]
        data["targeted_profit"] = -1 * data["option_price"] / data["spot_price"]
    return data

def calculate_latest_bot_by_index(args, indices):
    print("== Start Calculating == " + str(timeNow()))
    if(indices == 'ALL'):
        data = DroidUniverse(args)
    elif(indices == 'US'):
        data = droid_universe_by_country_code(args, 'US', method=True)
    elif(indices == 'NOTUS'):
        data = droid_universe_by_country_code(args, 'US', method=False)
    else:
        data = droid_universe_by_index(args, indices)

    universe = data[["ticker", "country_code", "index"]]
    del data
    
    print("== Getting Currency Code == " + str(timeNow()))
    currency_code = get_currency_code(args, universe["index"])
    universe = universe.merge(currency_code, how='left', on='index')
    universe = universe.drop(columns=["index"])
    del currency_code
    
    print("== Calculating Expiry Date == " + str(timeNow()))
    #Calculating Expiry_1, Expiry_3, Expiry_6 for Different Country Code
    expiry_date = pd.DataFrame({'country_code':[],'expiry_1':[],'expiry_3':[],'expiry_6':[]}, index=[])
    country_code = universe['country_code'].drop_duplicates(keep='last').tolist()
    for item in country_code:
        try:
            expiry_1, expiry_3, expiry_6 = get_expiry_date(args, item)
            temp = [[item, expiry_1, expiry_3, expiry_6]] 
            temp = pd.DataFrame(temp, columns = ['country_code', 'expiry_1', 'expiry_3', 'expiry_6'])
            expiry_date = expiry_date.append(temp)
            del temp
        except Exception as e:
            print(e)
    universe = universe.merge(expiry_date, how='left', on='country_code')
    del country_code, expiry_date

    print("== Getting Volatility Data == " + str(timeNow()))
    #Getting Volatility
    latest_vol_updates = get_latest_vol_updates(args)
    universe = universe.merge(latest_vol_updates, how='left', on='ticker')
    universe = universe.drop(columns=["uid", "trading_day"])
    del latest_vol_updates

    print("== Getting Price Data == " + str(timeNow()))
    #Getting Volatility
    latest_price_updates = get_latest_price_updates(args)
    universe = universe.merge(latest_price_updates, how='left', on='ticker')
    del latest_price_updates
    #universe.to_csv("Test.csv")
    print(universe)
    universe = universe.dropna()
    bot_option_type = get_bot_option_type(args)
    for index, row in bot_option_type.iterrows():
        bot_type = row["bot_type"]
        bot_horizon_month = row["bot_horizon_month"]
        bot_group = row["bot_group"]
        bot_option_type = row["bot_option_type"]
        
        #CLASSIC BOT
        print(f"== Calculating {bot_type} on Option Type {bot_option_type} == " + str(timeNow()))
        if bot_group == "CLASSIC":
            result = get_Classic(universe, bot_horizon_month)
            result = result.drop(columns=["t"])
        else:
            result = get_trq(args, universe, bot_horizon_month)
            result = get_vol(args, result)
            result = get_strike_barrier_rebate(result, bot_horizon_month, bot_option_type, bot_group)
            result = get_v1_v2_delta_option_price(result, bot_group)
            result = get_loss_profit(result, bot_group)
            result = result.drop(columns=["classic_vol"])

        #Make UID
        result = result.drop(columns=["atm_volatility_spot",
                                    "atm_volatility_one_year",
                                    "atm_volatility_infinity",
                                    "slope",
                                    "slope_inf",
                                    "deriv",
                                    "deriv_inf",
                                    "country_code",
                                    "currency_code"])
        result["timestamp"] = datetimeNow()
        result["group"] = bot_group
        result["month_horizon"] = bot_horizon_month
        result["option_type"] = bot_option_type
        result["bot_type"] = bot_type
        result['uid'] = result['ticker'] + "_" + bot_type + "_" + bot_option_type 
        result['uid'] = result['uid'].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        result['uid'] = result['uid'].str.strip()
        #result.to_csv(f"Result {bot_type} {bot_option_type}.csv")
        print(result)
        #Inserting Data to Database
        insert_data_to_database(args, result)
        del result
    #print(universe)
    print("== Finish Calculating == " + str(timeNow()))
    return True
