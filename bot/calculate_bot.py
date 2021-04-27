import math
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from general.data_process import tuple_data
from general.sql_query import read_query
from general.table_name import (
    get_currency_calendar_table_name,
    get_data_dividend_daily_rates_table_name,
    get_data_interest_daily_rates_table_name,
    get_data_vol_surface_inferred_table_name,
    get_data_vol_surface_table_name,
    get_latest_price_table_name,
    get_latest_vol_table_name,
    get_master_tac_table_name)
from bot import uno
from global_vars import large_hedge, small_hedge, buy_UCDC_prem, sell_UCDC_prem, buy_UNO_prem, sell_UNO_prem, max_vol, min_vol, default_vol
import pandas as pd
# comment22


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
    query = f"select * from {table_name} "
    query += f" where non_working_day='{non_working_day}' and currency_code in {tuple_data(currency_code)}"
    data = read_query(query, table_name, cpu_counts=True, prints=False)
    return data


def get_expiry_date(time_to_exp, spot_date, currency_code):
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
    expiry = spot_date + relativedelta(days=(days-1))
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


def get_classic(ticker, spot_date, time_to_exp, investment_amount, price, expiry_date):
    spot_date = check_date(spot_date)
    expiry_date = check_date(expiry_date)
    digits = max(min(4-len(str(int(price))), 2), -1)
    classic_vol_data = get_classic_vol_by_date(ticker, spot_date)
    classic_vol = classic_vol_data["classic_vol"]

    month = int(round((time_to_exp * 365), 0)) / 30
    dur = pow(time_to_exp, 0.5) * min((0.75 + (month * 0.25)), 2)

    data = {
        "price": price,
    }
    data["vol"] = dur
    data["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    data["share_num"] = math.floor(investment_amount / price)
    data["max_loss_pct"] = - (dur * classic_vol * 1.25)
    data["max_loss_price"] = round(
        price * (1 + data["max_loss_pct"]), int(digits))
    data["max_loss_amount"] = round(
        (data["max_loss_price"] - price) * data["share_num"], int(digits))
    data["target_profit_pct"] = (dur * classic_vol)
    data["target_profit_price"] = round(
        price * (1 + data["target_profit_pct"]), digits)
    data["target_profit_amount"] = round(
        (data["target_profit_price"] - price) * data["share_num"], digits)
    data["bot_cash_balance"] = round(
        investment_amount - (data["share_num"] * price), 2)

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


def get_ucdc(ticker, currency_code, expiry_date, spot_date, time_to_exp, investment_amount, price, bot_option_type, bot_group):
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
    share_num = investment_amount / price
    data["last_hedge_delta"] = delta
    data["option_price"] = option_price
    data["t"] = t
    data["r"] = r
    data["q"] = q
    data["strike"] = strike
    data["strike_2"] = strike_2
    data["v1"] = v1
    data["v2"] = v2
    data["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    data["vol"] = vol
    data["share_num"] = math.floor(delta * share_num)
    data["max_loss_pct"] = potential_loss
    data["max_loss_pct_display"] = round(data["max_loss_pct"] * 100, 2)
    data["max_loss_amount"] = round(
        (strike_2 - strike) * data["share_num"], int(digits))
    data["max_loss_price"] = round(strike_2, int(digits))
    data["target_profit_pct"] = targeted_profit
    data["target_profit_pct_display"] = round(
        data["target_profit_pct"] * 100, 2)
    data["target_profit_price"] = round(
        ((-1 * option_price) + price), int(digits))
    data["target_profit_amount"] = round(
        option_price * data["share_num"], int(digits)) * -1
    data["bot_cash_balance"] = round(
        investment_amount - (data["share_num"] * price), 2)
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


def get_uno(ticker, currency_code, expiry_date, spot_date, time_to_exp, investment_amount, price, bot_option_type, bot_group):
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
    share_num = investment_amount / price
    data["option_price"] = option_price
    data["t"] = t
    data["last_hedge_delta"] = delta
    data["r"] = r
    data["q"] = q
    data["strike"] = strike
    data["barrier"] = barrier
    data["v1"] = v1
    data["v2"] = v2
    data["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    data["share_num"] = math.floor(delta * share_num)
    data["max_loss_pct"] = potential_loss
    data["vol"] = vol
    data["max_loss_pct_display"] = round(data["max_loss_pct"] * 100, 2)
    data["max_loss_amount"] = round(
        option_price * data["share_num"], int(digits)) * -1
    data["max_loss_price"] = round(price - option_price, int(digits))
    data["target_profit_pct"] = targeted_profit
    data["target_profit_pct_display"] = round(
        data["target_profit_pct"] * 100, 2)
    data["target_profit_price"] = round(barrier, int(digits))
    data["target_profit_amount"] = round(
        rebate * data["share_num"], int(digits))
    data["bot_cash_balance"] = round(
        investment_amount - (data["share_num"] * price), 2)
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
    query += f"from {vol_table} vol "
    query += f"where vol.ticker = '{ticker}' and "
    query += f"vol.spot_date <= '{trading_day}' "
    query += f"order by spot_date DESC limit 1;"
    data = read_query(query, vol_table, cpu_counts=True,
                      droid1=True, prints=False)
    if(len(data) != 1):
        query = f"select * "
        query += f"from {latest_price_table} vol "
        query += f"where vol.ticker = '{ticker}';"
        data = read_query(query, latest_price_table,
                          cpu_counts=True, prints=False)
        if(len(data) != 1):
            classic_vol = default_vol
        else:
            classic_vol = data.loc[0, "classic_vol"]
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
    hedge = False
    if latest_spot_price > strike:
        if abs(delta - last_hedge_delta) > large_hedge:
            last_hedge_delta = delta
            hedge = True

    if latest_spot_price <= strike:
        if abs(delta - last_hedge_delta) > small_hedge:
            last_hedge_delta = delta
    return last_hedge_delta, hedge


def get_ucdc_hedge(currency_code, delta, last_hedge_delta):
    hedge = False
    if currency_code in ["EUR", "USD", "0#.ETF", "0#.SPX", "0#.SXXE"]:
        if abs(delta - last_hedge_delta) > large_hedge:
            last_hedge_delta = delta
            hedge = True
    else:
        if abs(delta - last_hedge_delta) > small_hedge:
            last_hedge_delta = delta
    return last_hedge_delta, hedge


def get_hedge_detail(ask_price, bid_price, last_share_num, bot_share_num, delta, last_hedge_delta, hedge=False, uno=False, ucdc=False):
    if(hedge):
        hedge_shares = round((delta - last_hedge_delta) * bot_share_num, 0)
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
        buy_prem = buy_UNO_prem
        sell_prem = sell_UNO_prem
    else:
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
