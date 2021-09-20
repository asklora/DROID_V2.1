from bot.data_download import get_currency_data
from core.djangomodule.general import formatdigit
from general.sql_output import upsert_data_to_database
from general.date_process import dateNow, str_to_date
import math
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from general.data_process import tuple_data, NoneToZero, uid_maker
from general.sql_query import get_active_universe, get_orders_position, get_orders_position_group_by_user_id, get_orders_position_performance, get_user_account_balance, get_user_core, get_user_deposit, read_query
from general.table_name import (
    get_currency_calendar_table_name,
    get_data_dividend_daily_rates_table_name,
    get_data_interest_daily_rates_table_name,
    get_data_vol_surface_inferred_table_name,
    get_data_vol_surface_table_name,
    get_latest_price_table_name,
    get_latest_vol_table_name,
    get_master_tac_table_name,
    get_user_profit_history_table_name)
from datasource.dsws import get_data_static_from_dsws
from bot import uno
from global_vars import large_hedge, small_hedge, buy_UCDC_prem, sell_UCDC_prem, buy_UNO_prem, sell_UNO_prem, max_vol, min_vol, default_vol
import pandas as pd

def check_date(dates):
    if(type(dates) == str and len(dates) > 10):
        dates = pd.to_datetime(dates)
    elif(type(dates) == str and len(dates) == 10):
        dates = datetime.strptime(dates, "%Y-%m-%d")
    return dates


def get_q(ticker, t):
    table_name = get_data_dividend_daily_rates_table_name()
    query = f"select * from {table_name} where t = {t} and ticker='{ticker}'"
    data = read_query(query, table_name, cpu_counts=True, prints=False)
    try:
        q = data.loc[0, "q"]
    except Exception as e:
        q = 0
    return q


def get_r(currency_code, t):
    table_name = get_data_interest_daily_rates_table_name()
    query = f"select * from {table_name} where t = {t} and currency_code='{currency_code}'"
    data = read_query(query, table_name, cpu_counts=True, prints=False)
    try:
        r = data.loc[0, "r"]
    except Exception as e:
        r = 0
    return r


def get_spot_date(spot_date, ticker):
    spot_date = check_date(spot_date)
    table_name = get_master_tac_table_name()
    query = f"select max(trading_day) as max_date from {table_name} where ticker = {ticker} and spot_date>='{spot_date}' and day_status='trading_day'"
    data = read_query(query, table_name, cpu_counts=True, prints=False)
    return data.loc[0, "max_date"]


def get_holiday(non_working_day, currency_code):
    table_name = get_currency_calendar_table_name()
    query = f"select distinct ON (non_working_day) non_working_day, currency_code from {table_name} "
    query += f" where non_working_day='{non_working_day}' and currency_code in {tuple_data(currency_code)}"
    data = read_query(query, table_name, cpu_counts=True, prints=False)
    return data


def get_expiry_date(time_to_exp, spot_date, currency_code, apps=False):
    """
    - Parameters:
        - time_to_exp -> float
        - spot_date -> date
        - currency_code -> str
    - Returns:
        - datetime -> date
    """
    spot_date = check_date(spot_date)
    days = int(round((time_to_exp * 365), 0))
    expiry = spot_date + relativedelta(days=(days))
    if not apps:
        while(expiry.weekday() != 5):
            expiry = expiry - relativedelta(days=1)
    # days = int(round((time_to_exp * 256), 0))
    # expiry = spot_date + BDay(days-1)

    while True:
        holiday = False
        data = get_holiday(expiry.strftime("%Y-%m-%d"), currency_code)
        if(len(data) > 0):
            holiday = True
        if ((holiday == False) and expiry.weekday() < 5):
            break
        else:
            expiry = expiry - relativedelta(days=1)
    return expiry


def get_strike_barrier(price, vol, bot_option_type, bot_group):
    if bot_group == "UNO":
        if bot_option_type == "OTM":
            strike = price * (1 + vol * 0.5)
        elif bot_option_type == "ITM":
            strike = price * (1 - vol * 0.5)

        if bot_option_type == "OTM":
            barrier = price * (1 + vol * 2)
        elif bot_option_type == "ITM":
            barrier = price * (1 + vol * 1.5)
        return float(strike), float(barrier)

    elif bot_group == "UCDC":
        strike = price
        strike_2 = price * (1 - vol * 1.5)
        return float(strike), float(strike_2)
    return False


def get_option_price_uno(price, strike, barrier, rebate, t, r, q, v1, v2):
    return uno.Up_Out_Call(price, strike, barrier, rebate, t/365, r, q, v1, v2)


def get_option_price_ucdc(price, strike, strike_2, t, r, q, v1, v2):
    return uno.Rev_Conv(price, strike, strike_2, t/365, r, q, v1, v2)


def get_v1_v2(ticker, price, trading_day, t, r, q, strike, barrier):
    trading_day = check_date(trading_day)
    status, obj = get_vol_by_date(ticker, trading_day)
    if status:
        v1 = uno.find_vol(strike / price, t/365, obj["atm_volatility_spot"], obj["atm_volatility_one_year"],
                          obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v1 = np.nan_to_num(v1, nan=0)
        v2 = uno.find_vol(barrier / price, t/365, obj["atm_volatility_spot"], obj["atm_volatility_one_year"],
                          obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v2 = np.nan_to_num(v2, nan=0)
    else:
        v1 = default_vol
        v2 = default_vol
    return float(v1), float(v2)


def get_trq(ticker, expiry, spot_date, currency_code):
    expiry = check_date(expiry)
    spot_date = check_date(spot_date)
    t = (expiry - spot_date).days
    if t == 0:
        t = 1
    r = get_r(currency_code, t)
    q = get_q(ticker, t)
    return int(t), float(r), float(q)


def get_vol(ticker, trading_day, t, r, q, time_to_exp):
    trading_day = check_date(trading_day)
    status, obj = get_vol_by_date(ticker, trading_day)
    if status:
        v0 = uno.find_vol(1, t/365, obj["atm_volatility_spot"], obj["atm_volatility_one_year"],
                          obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v0 = np.nan_to_num(v0, nan=0)
        v0 = max(min(v0, max_vol), min_vol)

        # Code when we use business days
        # month = int(round((time_exp * 256), 0)) / 22
        month = int(round((time_to_exp * 365), 0)) / 30
        vol = v0 * (month/12)**0.5

    else:
        vol = default_vol
    return float(vol)


def get_classic(ticker, spot_date, time_to_exp, investment_amount, price, expiry_date,margin:int=1):
    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    classic_vol_data = get_classic_vol_by_date(ticker, spot_date)
    classic_vol = classic_vol_data["classic_vol"]

    month = int(round((time_to_exp * 365), 0)) / 30
    dur = pow(time_to_exp, 0.5) * min((0.75 + (month * 0.25)), 2)

    data = {
        "price": price,
        "performance":{},
        "position":{}
    }
    total_bot_share_num = math.floor((investment_amount * margin) / price)
    bot_cash_balance =round(investment_amount - (total_bot_share_num * price), 2)
    data["performance"]["vol"] = dur
    data["performance"]["last_hedge_delta"] = 1
    data["performance"]["share_num"] = total_bot_share_num
    data['performance']["current_bot_cash_balance"] = bot_cash_balance
    data["position"]["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    data["position"]["total_bot_share_num"] = total_bot_share_num
    data["position"]["max_loss_pct"] = - (dur * classic_vol * 1.25)
    data["position"]["max_loss_price"] = round(price * (1 + data["position"]["max_loss_pct"]), int(digits))
    data["position"]["max_loss_amount"] = round((data["position"]["max_loss_price"] - price) * total_bot_share_num, int(digits))
    data["position"]["target_profit_pct"] = (dur * classic_vol)
    data["position"]["target_profit_price"] = round(price * (1 + data["position"]["target_profit_pct"]), digits)
    data["position"]["target_profit_amount"] = round((data["position"]["target_profit_price"] - price) * total_bot_share_num, digits)
    data["position"]["bot_cash_balance"] = bot_cash_balance
    data["position"]["investment_amount"]=investment_amount
    return data


def get_ucdc_detail(ticker, currency_code, expiry_date, spot_date, time_to_exp, price, bot_option_type, bot_group):
    """
    - ticker -> str
    - currency_code -> str
    - expiry_date -> date
    - spot_date -> str
    - time_to_exp -> float
    - price -> float
    - bot_option_type -> str
    - bot_group -> str
    """
    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        "price": price,
    }
    data["t"], data["r"], data["q"] = get_trq(
        ticker, expiry_date, spot_date, currency_code)
    data["vol"] = get_vol(ticker, spot_date, data["t"],
                          data["r"], data["q"], time_to_exp)
    data["strike"], data["strike_2"] = get_strike_barrier(
        price, data["vol"], bot_option_type, bot_group)
    data["v1"], data["v2"] = get_v1_v2(
        ticker, price, spot_date, data["t"], data["r"], data["q"], data["strike"], data["strike_2"])

    option_price = uno.Rev_Conv(
        price, data["strike"], data["strike_2"], data["t"]/365, data["r"], data["q"], data["v1"], data["v2"])
    data["option_price"] = np.nan_to_num(option_price, nan=0)
    data["potential_loss"] = (data["strike_2"]-data["strike"]) / price
    data["targeted_profit"] = -1 * option_price / price
    delta = uno.deltaRC(price, data["strike"], data["strike_2"],
                        data["t"]/365, data["r"], data["q"], data["v1"], data["v2"])
    data["delta"] = np.nan_to_num(delta, nan=0)
    return data


def get_ucdc(ticker, currency_code, expiry_date, spot_date, time_to_exp, investment_amount, price, bot_option_type, bot_group, margin:int=1):
    """
    - ticker -> str
    - currency_code -> str
    - expiry_date -> date
    - spot_date -> str
    - time_to_exp -> float
    - price -> float
    - bot_option_type -> str
    - bot_group -> str
    - investment_amount -> str
    """
    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        "price": price,
        'performance':{},
        'position':{}
    }
    
    t, r, q = get_trq(ticker, expiry_date, spot_date, currency_code)
    vol = get_vol(ticker, spot_date, t, r, q, time_to_exp)
    strike, strike_2 = get_strike_barrier(
        price, vol, bot_option_type, bot_group)
    v1, v2 = get_v1_v2(ticker, price, spot_date, t, r, q, strike, strike_2)

    option_price = uno.Rev_Conv(price, strike, strike_2, t/365, r, q, v1, v2)
    option_price = np.nan_to_num(option_price, nan=0)
    potential_loss = (strike_2-strike) / price
    targeted_profit = -1 * option_price / price
    delta = uno.deltaRC(price, strike, strike_2, t/365, r, q, v1, v2)
    delta = np.nan_to_num(delta, nan=0)
    total_bot_share_num = math.floor((investment_amount * margin) / price)
    bot_hedge_share = math.floor(delta *total_bot_share_num)
    bot_cash_balance = round(investment_amount - (bot_hedge_share * price), digits)
    data['performance']["last_hedge_delta"] = delta
    data['performance']["option_price"] = option_price
    data['performance']["t"] = t
    data['performance']["r"] = r
    data['performance']["q"] = q
    data['performance']["strike"] = strike
    data['performance']["strike_2"] = strike_2
    data['performance']["v1"] = v1
    data['performance']["v2"] = v2
    data['performance']["share_num"] =bot_hedge_share
    data['performance']["vol"] = vol
    data['performance']["current_bot_cash_balance"] = bot_cash_balance
    data['position']["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    data['position']["vol"] = vol
    data['position']["total_bot_share_num"] = total_bot_share_num
    data['position']["max_loss_pct"] = potential_loss
    data['position']["max_loss_amount"] = round((strike_2 - strike) * total_bot_share_num, int(digits))
    data['position']["max_loss_price"] = round(strike_2, int(digits))
    data['position']["target_profit_pct"] = targeted_profit
    data['position']["target_profit_price"] = round(((-1 * option_price) + price), int(digits))
    data['position']["target_profit_amount"] = round(option_price * total_bot_share_num, int(digits)) * -1
    data['position']["bot_cash_balance"] = bot_cash_balance
    data['position']["investment_amount"]=investment_amount
    return data


def get_uno_detail(ticker, currency_code, expiry_date, spot_date, time_to_exp, price, bot_option_type, bot_group):
    """
    - ticker -> str
    - currency_code -> str
    - expiry_date -> date
    - spot_date -> str
    - time_to_exp -> float
    - price -> float
    - bot_option_type -> str
    - bot_group -> str
    """
    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        "price": price,
    }
    data["t"], data["r"], data["q"] = get_trq(
        ticker, expiry_date, spot_date, currency_code)
    data["vol"] = get_vol(ticker, spot_date, data["t"],
                          data["r"], data["q"], time_to_exp)
    data["strike"], data["barrier"] = get_strike_barrier(
        price,  data["vol"], bot_option_type, bot_group)
    data["rebate"] = data["barrier"] - data["strike"]
    data["v1"], data["v2"] = get_v1_v2(
        ticker, price, spot_date, data["t"], data["r"], data["q"], data["strike"], data["barrier"])
    delta = uno.deltaUnOC(price, data["strike"], data["barrier"], data["rebate"],
                          data["t"]/365, data["r"], data["q"], data["v1"], data["v2"])
    data["delta"] = np.nan_to_num(delta, nan=0)
    option_price = uno.Up_Out_Call(price, data["strike"], data["barrier"],
                                   data["rebate"], data["t"]/365, data["r"], data["q"], data["v1"], data["v2"])
    data["option_price"] = np.nan_to_num(option_price, nan=0)
    data["potential_loss"] = -1 * option_price / price
    data["targeted_profit"] = (data["rebate"]) / price
    return data


def get_uno(ticker, currency_code, expiry_date, spot_date, time_to_exp, investment_amount, price, bot_option_type, bot_group, margin:int=1):
    """
    - ticker -> str
    - currency_code -> str
    - expiry_date -> date
    - spot_date -> str
    - time_to_exp -> float
    - price -> float
    - bot_option_type -> str
    - bot_group -> str
    - investment_amount -> float
    """

    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        "price": price,
        'performance':{},
        'position':{}
    }

    t, r, q = get_trq(ticker, expiry_date, spot_date, currency_code)
    vol = get_vol(ticker, spot_date, t, r, q, time_to_exp)
    strike, barrier = get_strike_barrier(
        price, vol, bot_option_type, bot_group)
    rebate = barrier - strike
    v1, v2 = get_v1_v2(ticker, price, spot_date, t, r, q, strike, barrier)
    delta = uno.deltaUnOC(price, strike, barrier, rebate, t/365, r, q, v1, v2)
    delta = np.nan_to_num(delta, nan=0)
    option_price = uno.Up_Out_Call(
        price, strike, barrier, rebate, t/365, r, q, v1, v2)
    option_price = np.nan_to_num(option_price, nan=0)
    potential_loss = -1 * option_price / price
    targeted_profit = (barrier-strike) / price
    total_bot_share_num = math.floor((investment_amount * margin) / price)
    bot_hedge_share = math.floor(delta *total_bot_share_num)
    bot_cash_balance = round(investment_amount - (bot_hedge_share * price), digits)
    data['performance']["option_price"] = option_price
    data['performance']["t"] = t
    data['performance']["last_hedge_delta"] = delta
    data['performance']["r"] = r
    data['performance']["q"] = q
    data['performance']["strike"] = strike
    data['performance']["barrier"] = barrier
    data['performance']["v1"] = v1
    data['performance']["v2"] = v2
    data['performance']["share_num"] = bot_hedge_share
    data['performance']["vol"] = vol
    data['performance']["current_bot_cash_balance"] = bot_cash_balance
    data['position']["total_bot_share_num"] = total_bot_share_num
    data['position']["vol"] = vol
    data['position']["max_loss_pct"] = potential_loss
    data['position']["max_loss_amount"] = round(option_price * total_bot_share_num, int(digits)) * -1
    data['position']["max_loss_price"] = round(price - option_price, int(digits))
    data['position']["target_profit_pct"] = targeted_profit
    data['position']["target_profit_price"] = round(barrier, int(digits))
    data['position']["target_profit_amount"] = round(rebate * total_bot_share_num, int(digits))
    data['position']["bot_cash_balance"] = bot_cash_balance
    data['position']["investment_amount"]=investment_amount
    data['position']["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    
    
    return data


def get_vol_by_date(ticker, trading_day):
    trading_day = check_date(trading_day)
    vol_table = get_data_vol_surface_table_name()
    vol_inferred_table = get_data_vol_surface_inferred_table_name()
    latest_vol_table = get_latest_vol_table_name()
    query = f"select * "
    query += f"from {vol_table} vol "
    query += f"where vol.ticker = '{ticker}' and "
    query += f"vol.trading_day <= '{trading_day}' "
    query += f"order by trading_day DESC limit 1;"
    data = read_query(query, vol_table, cpu_counts=True, prints=False)
    if(len(data) != 1):
        query = f"select * "
        query += f"from {vol_inferred_table} vol "
        query += f"where vol.ticker = '{ticker}' and "
        query += f"vol.trading_day <= '{trading_day}' "
        query += f"order by trading_day DESC limit 1;"
        data = read_query(query, vol_inferred_table,
                          cpu_counts=True, prints=False)
        if(len(data) != 1):
            query = f"select * "
            query += f"from {latest_vol_table} vol "
            query += f"where vol.ticker = '{ticker}' limit 1;"
            data = read_query(query, latest_vol_table,
                              cpu_counts=True, prints=False)
            if(len(data) != 1):
                data = {
                    "ticker": ticker,
                    "trading_day": trading_day
                }
                return False, data
    data = {
        "ticker": ticker,
        "trading_day": trading_day,
        "atm_volatility_spot": data.loc[0, "atm_volatility_spot"],
        "atm_volatility_one_year": data.loc[0, "atm_volatility_one_year"],
        "atm_volatility_infinity": data.loc[0, "atm_volatility_infinity"],
        "slope": data.loc[0, "slope"],
        "slope_inf": data.loc[0, "slope_inf"],
        "deriv": data.loc[0, "deriv"],
        "deriv_inf": data.loc[0, "deriv_inf"]
    }
    return True, data


def get_classic_vol_by_date(ticker, trading_day):
    trading_day = check_date(trading_day)
    vol_table = "classic_vol_history"
    latest_price_table = get_latest_price_table_name()
    query = f"select * "
    query += f"from {latest_price_table} vol "
    query += f"where vol.ticker = '{ticker}';"
    data = read_query(query, latest_price_table,
                        cpu_counts=True, prints=False)
    if(len(data) != 1):
        classic_vol = default_vol
    else:
        classic_vol = data.loc[0, "classic_vol"]
    if classic_vol == None:
        classic_vol = default_vol
    if classic_vol == np.NaN:
        classic_vol = default_vol
    data = {
        "ticker": ticker,
        "trading_day": trading_day,
        "classic_vol": classic_vol
    }
    return data


def get_uno_hedge(latest_spot_price, strike, delta, last_hedge_delta):
    hedge = True
    if latest_spot_price > strike:
        # hedge threhold for OTM
        if abs(delta - last_hedge_delta) < large_hedge:
            # if delta change too small then reset new delta to old delta (last_hedge_delta)
            delta = last_hedge_delta
            hedge = False

    if latest_spot_price <= strike:
        # hedge threhold for ITM
        if abs(delta - last_hedge_delta) < small_hedge:
            # if delta changetoo small then reset new delta to old delta (last_hedge_delta)
            delta = last_hedge_delta
            hedge = False
    return delta, hedge


def get_ucdc_hedge(currency_code, delta, last_hedge_delta):
    hedge = True
    if currency_code in ["EUR", "USD", "0#.ETF", "0#.SPX", "0#.SXXE"]:
        # hedge threshold for DM
        if abs(delta - last_hedge_delta) < large_hedge:
            # if delta change too small then reset new delta to old delta (last_hedge_delta)
            delta = last_hedge_delta
            hedge = False
    else:
        # hedge threshold for EM
        if abs(delta - last_hedge_delta) < small_hedge:
            # if delta change too small then reset new delta to old delta (last_hedge_delta)
            delta = last_hedge_delta
            hedge = False
    return delta, hedge


def get_hedge_detail(live_price, bot_cash_balance, ask_price, bid_price, last_share_num, bot_share_num, delta, last_hedge_delta, hedge=False, uno=False, ucdc=False, margin=1):
    #err
    if(hedge):
        hedge_shares = round((delta - last_hedge_delta) * bot_share_num, 0)
        if(hedge_shares > 0):
            hedge_shares = min(hedge_shares, math.floor(bot_cash_balance/live_price))
        if(hedge_shares < 0):
            hedge_shares = max(hedge_shares, last_share_num * -1)
        share_num = last_share_num + hedge_shares
    else:
        hedge_shares = 0
        share_num = last_share_num
    if(hedge_shares > 0):
        status = "buy"
    elif(hedge_shares < 0):
        status = "sell"
    else:
        status = "hold"
    if(uno):
        # buy above offer and sell below bid (chase MORE - short gamma)
        buy_prem = buy_UNO_prem
        sell_prem = sell_UNO_prem
    else:
        # buy above ofer and sell below bid (chase LESS - long gamma)
        buy_prem = buy_UCDC_prem
        sell_prem = sell_UCDC_prem
    if(status == "buy"):
        hedge_price = ask_price
        if(hedge_shares > 0):
            hedge_price = buy_prem * ask_price
    elif(status == "sell"):
        hedge_price = bid_price
        if(hedge_shares < 0):
            hedge_price = sell_prem * bid_price
    else:
        hedge_price = 0
    return share_num, hedge_shares, status, hedge_price

def get_dividend_paid_date(ticker=None, currency_code=None):
    universe = get_active_universe(ticker=ticker, currency_code=currency_code)
    universe = universe.drop(columns=["entity_type"])
    filter_field = ["IDTDDXDDE", "DPS"]
    identifier="ticker"
    result, error_ticker = get_data_static_from_dsws(universe[["ticker"]], identifier, filter_field, use_ticker=True, split_number=min(len(universe), 40))
    result = result.rename(columns={"IDTDDXDDE": "dividend_ex_date", "index":"ticker", "DPS" : "dividend_per_share"})
    return result


def check_dividend_paid(ticker, trading_day, share_num, bot_cash_dividend):
    result = get_dividend_paid_date(ticker=[ticker])
    if(len(result) > 0):
        print(str(result.loc[0, "dividend_ex_date"]))
        print(str(trading_day))
        if(str(result.loc[0, "dividend_ex_date"]) == str(trading_day)):
            return bot_cash_dividend + (result.loc[0, "dividend_per_share"] * share_num)
    return bot_cash_dividend

def populate_daily_profit(currency_code=None, user_id=None):
    user_core = get_user_core(currency_code=currency_code, user_id=user_id, field="id as user_id, username, is_joined")[["user_id", "is_joined"]]
    user_balance = get_user_account_balance(currency_code=currency_code, user_id=user_id, field="user_id, currency_code, amount as balance, balance_uid")
    user_deposit = get_user_deposit(user_id=user_id, field="balance_uid, sum(amount) as deposit")

    currency = get_currency_data(currency_code=currency_code)
    currency = currency[["currency_code", "is_decimal"]]
    user_core = user_core.merge(user_balance, how="left", on=["user_id"])
    user_core = user_core.merge(user_deposit, how="left", on=["balance_uid"])
    user_core = user_core.merge(currency, how="left", on=["currency_code"])
    user_core["balance"] = np.where(user_core["balance"].isnull(), 0, user_core["balance"])
    user_core["deposit"] = np.where(user_core["deposit"].isnull(), 0, user_core["deposit"])

    bot_order_pending = get_orders_position_group_by_user_id(user_id=user_core["user_id"].to_list(), stock=False)
    user_core = user_core.merge(bot_order_pending, how="left", on=["user_id"])
    user_core["bot_pending_amount"] = np.where(user_core["bot_pending_amount"].isnull(), 0, user_core["bot_pending_amount"])
    stock_order_pending = get_orders_position_group_by_user_id(user_id=user_core["user_id"].to_list(), stock=True)
    user_core = user_core.merge(stock_order_pending, how="left", on=["user_id"])
    user_core["stock_pending_amount"] = np.where(user_core["stock_pending_amount"].isnull(), 0, user_core["stock_pending_amount"])

    user_core["pending_amount"] = user_core["stock_pending_amount"] + user_core["bot_pending_amount"]
    
    orders_position_field = "position_uid, user_id, investment_amount, margin"
    orders_position = get_orders_position(user_id=user_core["user_id"].to_list(), active=True, field=orders_position_field)
    if(len(orders_position)):
        orders_performance_field = "distinct created, position_uid, current_bot_cash_balance, current_investment_amount"
        orders_performance = get_orders_position_performance(position_uid=orders_position["position_uid"].to_list(), field=orders_performance_field, latest=True)
        orders_performance["created"] = orders_performance["created"].dt.date
        orders_performance = orders_performance.drop_duplicates(subset=["created", "position_uid"], keep="first")
        orders_performance = orders_performance.drop(columns=["created"])

        orders_position = orders_position.merge(orders_performance, how="left", on=["position_uid"])
    for index, row in user_core.iterrows():
        rounded = 0
        if(row["is_decimal"]):
            rounded = 2
        user = row["user_id"]
        position = orders_position.loc[orders_position["user_id"] == user]
        if(len(position)):
            position["margin_invested_amount"] = position["investment_amount"] * position["margin"]
            position["crr_ivt_amt"] = (position["current_investment_amount"] + position["current_bot_cash_balance"])
            position["daily_profit"] = position["crr_ivt_amt"] - position["investment_amount"]
            profit = formatdigit(NoneToZero(np.nansum(position["daily_profit"].to_list())), currency_decimal=row["is_decimal"])
            daily_profit_pct = round(profit / NoneToZero(np.nansum(position["crr_ivt_amt"].to_list())) * 100, 4)
            daily_invested_amount = formatdigit(NoneToZero(np.nansum(position["crr_ivt_amt"].to_list())) + user_core.loc[index, "pending_amount"], currency_decimal=row["is_decimal"])
        else:
            profit = 0
            daily_profit_pct = 0
            daily_invested_amount = 0
        user_core.loc[index, "daily_profit"] = profit
        user_core.loc[index, "daily_profit_pct"] = daily_profit_pct
        user_core.loc[index, "daily_invested_amount"] = daily_invested_amount
        user_core.loc[index, "total_profit"] = (user_core.loc[index, "daily_invested_amount"] + user_core.loc[index, "balance"] - user_core.loc[index, "deposit"])
        user_core.loc[index, "total_profit_pct"] = (user_core.loc[index, "total_profit"] / user_core.loc[index, "deposit"]) * 100
    # print(user_core[["user_id", "balance", "deposit", "daily_invested_amount", "total_profit", "total_profit_pct"]])
    user_core["trading_day"] =  str_to_date(dateNow())
    user_core["user_id"] = user_core["user_id"].astype(str)
    user_core = uid_maker(user_core, uid="uid", ticker="user_id", trading_day="trading_day", date=True)
    user_core["user_id"] = user_core["user_id"].astype(int)
    user_core = user_core.drop(columns=["currency_code", "is_decimal", "bot_pending_amount", "stock_pending_amount", "pending_amount", "balance_uid", "deposit", "balance"])
    joined = user_core.loc[user_core["is_joined"] == True]
    joined = joined.sort_values(by=["total_profit_pct"], ascending=[False])
    joined = joined.reset_index(inplace=False, drop=True)
    joined = joined.reset_index(inplace=False)
    joined = joined.rename(columns={"index" : "rank"})
    joined["total_profit_pct"] = joined["total_profit_pct"].round(4)
    joined["rank"] = joined["rank"] + 1
    joined = joined.drop(columns=["is_joined"])
    upsert_data_to_database(joined, get_user_profit_history_table_name(), "uid", how="update", cpu_count=False, Text=True)

    not_joined = user_core.loc[user_core["is_joined"] == False]
    not_joined = not_joined.drop(columns=["is_joined"])
    not_joined["rank"] = None
    not_joined["total_profit_pct"] = not_joined["total_profit_pct"].round(4)
    upsert_data_to_database(not_joined, get_user_profit_history_table_name(), "uid", how="update", cpu_count=False, Text=True)
