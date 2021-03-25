from general.table_name import get_latest_bot_update_table_name
from general.sql_query import get_active_universe
from bot.data_download import get_bot_option_type, get_data_dividend_daily_rates, get_data_interest_daily, get_holiday_by_day_and_currency_code, get_latest_bot_ranking_data, get_latest_price, get_latest_vol
from threading import Condition
import pandas as pd
import numpy as np
from datetime import datetime
from general.date_process import dateNow, timeNow, datetimeNow
import numpy as np
from dateutil.relativedelta import relativedelta
import bot.black_scholes as  uno
from general.sql_output import upsert_data_to_database
from global_vars import time_to_expiry


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

def get_Classic(data, time_exp):
    # REQUIREMENT FOR CLASSIC
    data["classic_vol"] = np.where(data["classic_vol"].isnull(), 0.2, data["classic_vol"])
    data["classic_vol"] = np.where(data["classic_vol"] == 0, 0.2, data["classic_vol"])
    
    # Code when we use business days
    # month = int(round((time_exp * 256), 0)) / 22
    month = int(round((time_exp * 365), 0)) / 30
    dur = time_exp^0.5 * min((0.75 + (month * 0.25)) , 2)
    data["potential_max_loss"] = - (dur * data["classic_vol"] * 1.25)
    data["targeted_profit"] = (dur * data["classic_vol"])
    return data

def get_trq(data, time_to_exp_str):
    expiry = str(data[f"expiry_{time_to_exp_str}"].iloc[0])

    expiry = datetime.strptime(expiry, "%Y-%m-%d")
    t = (expiry.date() - datetime.now().date()).days
    data["t"] = t
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
    result_r = result_r[["currency_code", "r"]]
    if len(result_r) > 0:
        data = data.merge(result_r, how="left", on="currency_code")
    else :
        data["r"] = 0
    del result_r

    data["q"] = np.where(data["q"].isnull(), 0, data["q"])
    data["r"] = np.where(data["r"].isnull(), 0, data["r"])
    return data

def get_vol(data, time_to_exp):
    data["v0"] = uno.find_vol(1, data["t"], 
        data["atm_volatility_spot"], data["atm_volatility_one_year"],
        data["atm_volatility_infinity"], 12, 
        data["slope"], data["slope_inf"], data["deriv"], 
        data["deriv_inf"], data["r"], data["q"])
    
    data["v0"] = np.where(data["v0"] > 0.5, 0.5, data["v0"])
    data["v0"] = np.where(data["v0"] < 0.2, 0.2, data["v0"])
    for time_exp in time_to_exp:
        # Code when we use business days
        # month = int(round((time_exp * 256), 0)) / 22
        month = int(round((time_exp * 365), 0)) / 30
        time_exp_str = str(time_exp).replace(".", "")
        data[f"vol_t_{time_exp_str}"] = data["v0"] * (month/12)**0.5
    
    return data

def get_strike_barrier_rebate(data, time_to_exp_str, bot_option_type, bot_group):
    vol = f"vol_t_{time_to_exp_str}"

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
        data["strike_2"] = data["spot_price"] * (1 - data[vol] * 1.5)

    return data

def get_v1_v2_delta_option_price(data, bot_type):
    if bot_type == "UNO":
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
    elif bot_type == "UCDC":
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

def get_loss_profit(data, bot_type):
    if bot_type == "UNO":
        data["potential_max_loss"] = -1 * data["option_price"] / data["spot_price"]
        data["targeted_profit"] = (data["barrier"]-data["strike"]) / data["spot_price"]
    elif bot_type == "UCDC":
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
    latest_vol = get_latest_vol()
    universe = universe.merge(latest_vol, how="left", on="ticker")
    universe = universe.drop(columns=["trading_day"])
    del latest_vol
    
    print("== Getting Price Data == " + str(timeNow()))
    #Getting Volatility
    latest_price = get_latest_price()
    latest_price = latest_price[["ticker", "close", "classic_vol"]]
    latest_price = latest_price.rename(columns={"close" : "spot_price"})
    universe = universe.merge(latest_price, how="left", on="ticker")
    del latest_price
    #universe.to_csv("Test.csv")
    print(universe)
    universe = universe.dropna()
    bot_option_type = get_bot_option_type()
    ranking = get_latest_bot_ranking_data()
    ranking = ranking[["uid", "ranking"]]
    for index, row in bot_option_type.iterrows():
        bot_id = row["bot_id"]
        bot_type = row["bot_type"]
        bot_option_type = row["bot_option_type"]
        time_exp = row["time_to_exp"]
        time_to_exp_str = row["time_to_exp_str"]
        
        #CLASSIC BOT
        print(f"== Calculating {bot_type} on Option Type {bot_option_type} == " + str(timeNow()))
        if bot_type == "CLASSIC":
            result = get_Classic(universe, time_exp)
            result = result.drop(columns=["t"])
        else:
            result = get_trq(universe, time_to_exp_str)
            result = get_vol(result, time_to_exp)
            result = get_strike_barrier_rebate(result, time_to_exp_str, bot_option_type, bot_type)
            result = get_v1_v2_delta_option_price(result, bot_type)
            result = get_loss_profit(result, bot_type)
            result = result.drop(columns=["classic_vol"])

        #Make UID
        result = result.drop(columns=["atm_volatility_spot",
                                    "atm_volatility_one_year",
                                    "atm_volatility_infinity",
                                    "slope",
                                    "slope_inf",
                                    "deriv",
                                    "deriv_inf"])
        result["bot_id"] = bot_id
        result["bot_option_type"] = bot_option_type
        result["bot_type"] = bot_type
        result["time_to_exp_str"] = time_to_exp_str
        result["time_to_exp"] = time_exp
        
        result["uid"] = result["ticker"] + "_" + result["bot_id"]
        result["uid"] = result["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        result["uid"] = result["uid"].str.strip()
        result = result.merge(ranking, how="left", on=["uid"])
        print(result)
        #Inserting Data to Database
        upsert_data_to_database(result, get_latest_bot_update_table_name(), "uid", how="update", cpu_count=True, Text=True)
        del result
    #print(universe)
    print("== Finish Calculating == " + str(timeNow()))
    return True
