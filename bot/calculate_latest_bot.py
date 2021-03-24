from general.sql_query import get_active_universe
from bot.data_download import get_data_dividend_daily_rates, get_data_interest_daily, get_holiday_by_day_and_currency_code
from threading import Condition
import pandas as pd
import numpy as np
from datetime import datetime
from general.date_process import timeNow, datetimeNow
import numpy as np
from dateutil.relativedelta import relativedelta
import bot.black_scholes as  uno
from general.sql_output import upsert_data_to_database
from global_vars import time_to_expiry
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

def get_expiry_date(currency_code, time_to_exp):
    expiry_dict = {}
    for time_exp in time_to_exp:
        time_exp_str = str(time_exp).replace(".", "")
        days = int(round((time_exp * 365), 0))
        expiry = datetime.now().date() + relativedelta(days=(days-1))
        # days = int(round((time_exp * 256), 0))
        # expiry = datetime.now().date() + BDay(days-1)
        column = f"expiry_{time_exp_str}"
        expiry_dict.update({column: expiry})
    for column, expiry_date in expiry_dict.items():
        while True:
            holiday = False
            data = get_holiday_by_day_and_currency_code(expiry_date, currency_code)
            if(len(data) > 0):
                holiday = True
            if ((holiday == False) and expiry_date.weekday() < 5):
                break
            else:
                expiry_date = expiry_date - relativedelta(days=1)
    return expiry_dict

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

def get_trq(data, bot_horizon_month):
    if bot_horizon_month == 1:
        expiry = data["expiry_1"].iloc[0]
    elif bot_horizon_month == 3:
        expiry = data["expiry_3"].iloc[0]
    elif bot_horizon_month == 6:
        expiry = data["expiry_6"].iloc[0]

    expiry = datetime.strptime(expiry, "%Y-%m-%d")
    t = (expiry.date() - datetime.now().date()).days
    #print(f"{expiry.date()} - {datetime.now().date()} = {daydiff}")
    #print(f"Horizon {bot_horizon_month} = {t}")
    data["t"] = t

    # result_q = get_daily_div_rates(args, t, data["ticker"])
    ticker = data["ticker"]

    result_q = get_data_dividend_daily_rates(condition=f" t={t} and ticker='{ticker}'")
    result_q = result_q[["ticker", "q"]]
    if len(result_q) > 0:
        data = data.merge(result_q, how="left", on="ticker")
    else :
        data["q"] = 0
    del result_q

    currency_code = data["currency_code"]
    result_r = get_data_interest_daily(condition=f" t={t} and currency_code='{currency_code}'")
    result_r = result_r[["ticker", "r"]]
    if len(result_r) > 0:
        data = data.merge(result_r, how="left", on="currency_code")
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
        if bot_option_type == "OTM":
            data["strike"] = data["spot_price"] * (1 + data[vol] * 0.5)
        elif bot_option_type == "ITM":
            data["strike"] = data["spot_price"] * (1 - data[vol] * 0.5)

        if bot_option_type == "OTM":
            data["barrier"] = data["spot_price"] * (1 + data[vol] * 2)
        elif bot_option_type == "ITM":
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

def populate_latest_bot_update(ticker=None, currency_code=None, time_to_exp=time_to_expiry):
    print("== Start Calculating == " + str(timeNow()))
    data = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = data[["ticker", "currency_code"]]
    del data
    print("== Calculating Expiry Date == " + str(timeNow()))
    expiry_date = pd.DataFrame()
    currency_code = universe["currency_code"].drop_duplicates(keep="last").tolist()
    for currency in currency_code:
        expiry_dict = get_expiry_date(currency_code, time_to_exp)
        temp = pd.DataFrame()
        temp["currency_code"] = [currency]
        for column, exp_date in expiry_dict.items():
            temp[column] = [exp_date]
        expiry_date = expiry_date.append(temp)
    universe = universe.merge(expiry_date, how="left", on="currency_code")
    del currency_code, expiry_date
    
    print("== Getting Volatility Data == " + str(timeNow()))
    #Getting Volatility
    latest_vol_updates = get_latest_vol_updates(args)
    universe = universe.merge(latest_vol_updates, how="left", on="ticker")
    universe = universe.drop(columns=["uid", "trading_day"])
    del latest_vol_updates
    import sys
    sys.exit(1)
    print("== Getting Price Data == " + str(timeNow()))
    #Getting Volatility
    latest_price_updates = get_latest_price_updates(args)
    universe = universe.merge(latest_price_updates, how="left", on="ticker")
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
        result["uid"] = result["ticker"] + "_" + bot_type + "_" + bot_option_type 
        result["uid"] = result["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        result["uid"] = result["uid"].str.strip()
        #result.to_csv(f"Result {bot_type} {bot_option_type}.csv")
        print(result)
        #Inserting Data to Database
        insert_data_to_database(args, result)
        del result
    #print(universe)
    print("== Finish Calculating == " + str(timeNow()))
    return True
