import math
import json
import base64
import numpy as np
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_q(identyfier, t):
    result = {'ticker': identyfier, 't': t}
    encoded_result = base64.b64encode(json.dumps(result).encode()).decode()
    try:
        value = DailyDivRates.objects.get(uid=encoded_result)
    except DailyDivRates.DoesNotExist:
        result = {'ticker': identyfier, 't': 0.0027397260273972603}
        encoded_result = base64.b64encode(json.dumps(result).encode()).decode()
        try:
            value = DailyDivRates.objects.get(uid=encoded_result)
            return value.q
        except DailyDivRates.DoesNotExist:
            val = 0
            return val
    return value.q


def get_r(identyfier, t):
    result = {'currency_code': identyfier, 't': t}
    encoded_result = base64.b64encode(json.dumps(result).encode()).decode()
    try :
        value = DailyInterestRates.objects.get(uid=encoded_result)
    except DailyInterestRates.DoesNotExist:
        result = {'currency_code': identyfier, 't': 0.0027397260273972603}
        encoded_result = base64.b64encode(json.dumps(result).encode()).decode()
        try :
            value = DailyInterestRates.objects.get(uid=encoded_result)
        except DailyDivRates.DoesNotExist:
            value = 0
            return value
    return value.r

def get_expiry_date(spot_date, country_code, months):
    if (months < 0 or months == 4):
        if months == 4:
            months = 1
        expiry = datetime.strptime(spot_date, "%Y-%m-%d").date() + relativedelta(weeks=float(months) * 4) - relativedelta(days=1)
    else:
        expiry = datetime.strptime(spot_date, "%Y-%m-%d").date() + relativedelta(months=int(months)) - relativedelta(days=1)
    while True:
        holiday = False
        data = BussinessCalendars.objects.filter(country_code=country_code, non_working_day=expiry.strftime("%Y-%m-%d"))
        if(len(data) > 0):
            holiday = True
        if ((holiday == False) and expiry.weekday() < 5):
            break
        else:
            expiry = expiry - relativedelta(days=1)
    return expiry

def get_spot_date(spot_date, ticker):
    price_data = MasterOhlctr.objects.filter(ticker=ticker, trading_day__lte=spot_date, day_status="trading_day").order_by("-trading_day")
    spot_date = price_data[0].trading_day
    return spot_date

def get_strike_barrier(price, vol, bot_option_type, bot_group):
    if bot_group == "UNO":
        if bot_option_type == 'OTM':
            strike = price * (1 + vol * 0.5)
        elif bot_option_type == 'ITM':
            strike = price * (1 - vol * 0.5)

        if bot_option_type == 'OTM':
            barrier = price * (1 + vol * 2)
        elif bot_option_type == 'ITM':
            barrier = price * (1 + vol * 1.5)
        return float(strike), float(barrier)

    elif bot_group == "UCDC":
        strike = price
        strike_2 = price* (1 - vol * 1.5)
        return float(strike), float(strike_2)

    return False

def get_v1_v2(ticker, price, trading_day, t, r, q, strike, barrier):
    status, obj = get_vol_by_date(ticker, trading_day)
    if status:
        v1 = uno.find_vol(strike / price, t, obj["atm_volatility_spot"], obj["atm_volatility_one_year"], obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v1 = np.nan_to_num(v1, nan=0)
        v2 = uno.find_vol(barrier / price, t, obj["atm_volatility_spot"], obj["atm_volatility_one_year"], obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v2 = np.nan_to_num(v2, nan=0)
    else :
        v1 = 0.2
        v2 = 0.2
    return float(v1), float(v2)

def get_trq(ticker, expiry, spot_date, currency_code):
    expiry = datetime.strptime(expiry, "%Y-%m-%d")
    spot_date = datetime.strptime(spot_date, "%Y-%m-%d")
    daydiff = (expiry.date() - spot_date.date()).days
    t = daydiff / 365
    if t == 0:
        t = 0.0027397260273972603
    r = get_r(currency_code, t)
    q = get_q(ticker, t)
    return float(t), float(r), float(q)

def get_vol(ticker, trading_day, t, r, q, months):
    status, obj = get_vol_by_date(ticker, trading_day)
    if status:
        v0 = uno.find_vol(1, t, obj["atm_volatility_spot"], obj["atm_volatility_one_year"],
                          obj["atm_volatility_infinity"], 12, obj["slope"], obj["slope_inf"], obj["deriv"], obj["deriv_inf"], r, q)
        v0 = np.nan_to_num(v0, nan=0)
        v0 = max(min(v0, 0.50), 0.20)
        vol = v0 * (int(months)/12)**0.5
    else:
        vol = 0.2
    return float(vol)

def get_classic(ticker, spot_date, bot_horizon_month, investment_amount, price):
    bot_horizon_month = int(bot_horizon_month)
    digits = max(min(4-len(str(int(price))), 2), -1)
    classic_vol_data = get_classic_vol_by_date(ticker, spot_date)
    classic_vol = classic_vol_data["classic_vol"]
    if bot_horizon_month == 1:
        dur = 0.288675 * 1
    elif bot_horizon_month == 3:
        dur = 0.5 * 1.75
    data = {
        'price': price,
    }
    data['vol'] = dur
    data["share_num"] = math.floor(investment_amount / price)
    data["max_loss_pct"] = - (dur * classic_vol * 1.25)
    data["max_loss_price"] = round(price * (1 + data["max_loss_pct"]), int(digits))
    data["max_loss_amount"] = round((data["max_loss_price"] - price) * data["share_num"], int(digits))
    data["target_profit_pct"] = (dur * classic_vol)
    data["target_profit_price"] = round(price * (1 + data["target_profit_pct"]), digits)
    data["target_profit_amount"] = round((data["target_profit_price"] - price) * data["share_num"], digits)
    data["bot_cash_balance"] = round(investment_amount - (data["share_num"] * price), 2)
    return data

def get_ucdc_detail(ticker, currency_code, expiry_date, spot_date, bot_horizon_month, investment_amount, price, bot_option_type, bot_group):
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        "price": price,
    }
    data["t"], data["r"], data["q"] = get_trq(ticker, expiry_date, spot_date, currency_code)
    data["vol"] = get_vol(ticker, spot_date, data["t"], data["r"], data["q"], bot_horizon_month)
    data["strike"], data["strike_2"] = get_strike_barrier(price, data["vol"], bot_option_type, bot_group)
    data["v1"], data["v2"] = get_v1_v2(ticker, price, spot_date, data["t"], data["r"], data["q"], data["strike"], data["strike_2"])

    option_price = uno.Rev_Conv(price, data["strike"], data["strike_2"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    data["option_price"] = np.nan_to_num(option_price, nan=0)
    data["potential_loss"] = (data["strike_2"]-data["strike"]) / price
    data["targeted_profit"] = -1 * option_price / price
    delta = uno.deltaRC(price, data["strike"], data["strike_2"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    data["delta"] = np.nan_to_num(data["delta"], nan=0)
    return data

def get_ucdc(ticker, currency_code, expiry_date, spot_date, bot_horizon_month, investment_amount, price, bot_option_type, bot_group):
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        'price': price,
    }
    t, r, q = get_trq(ticker, expiry_date, spot_date, currency_code)
    vol = get_vol(ticker, spot_date, t, r, q, bot_horizon_month)
    strike, strike_2 = get_strike_barrier(price, vol, bot_option_type, bot_group)
    v1, v2 = get_v1_v2(ticker, price, spot_date, t, r, q, strike, strike_2)

    option_price = uno.Rev_Conv(price, strike, strike_2, t, r, q, v1, v2)
    option_price = np.nan_to_num(option_price, nan=0)
    potential_loss = (strike_2-strike) / price
    targeted_profit = -1 * option_price / price
    delta = uno.deltaRC(price, strike, strike_2, t, r, q, v1, v2)
    delta = np.nan_to_num(delta, nan=0)
    share_num = investment_amount / price
    data['vol'] = vol
    data['share_num'] = math.floor(delta * share_num)
    data['max_loss_pct'] = potential_loss
    data['max_loss_pct_display'] = round(data['max_loss_pct'] * 100, 2)
    data['max_loss_amount'] = round((strike_2 - strike) * data['share_num'], int(digits))
    data['max_loss_price'] = round(strike_2, int(digits))
    data['target_profit_pct'] = targeted_profit
    data['target_profit_pct_display'] = round(data['target_profit_pct'] * 100, 2)
    data['target_profit_price'] = round(((-1 * option_price) + price), int(digits))
    data['target_profit_amount'] = round(option_price * data['share_num'], int(digits)) * -1
    data['bot_cash_balance'] = round(investment_amount - (data['share_num'] * price), 2)
    return data

def get_uno_detail(ticker, currency_code, expiry_date, spot_date, bot_horizon_month, investment_amount, price, bot_option_type, bot_group):
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        'price': price,
    }
    data["t"], data["r"], data["q"] = get_trq(ticker, expiry_date, spot_date, currency_code)
    data["vol"] = get_vol(ticker, spot_date, data["t"], data["r"], data["q"], bot_horizon_month)
    data["strike"], data["barrier"] = get_strike_barrier(price,  data["vol"], bot_option_type, bot_group)
    data["rebate"] = data["barrier"] - data["strike"]
    data["v1"], data["v2"] = get_v1_v2(ticker, price, spot_date, data["t"], data["r"], data["q"], data["strike"], data["barrier"])
    delta = uno.deltaUnOC(price, data["strike"], data["barrier"], data["rebate"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    data["delta"] = np.nan_to_num(delta, nan=0)
    option_price = uno.Up_Out_Call(price, data["strike"], data["barrier"], data["rebate"], data["t"], data["r"], data["q"], data["v1"], data["v2"])
    data["option_price"] = np.nan_to_num(option_price, nan=0)
    data["potential_loss"] = -1 * option_price / price
    data["targeted_profit"] = (data["rebate"]) / price
    return data

def get_uno(ticker, currency_code, expiry_date, spot_date, bot_horizon_month, investment_amount, price, bot_option_type, bot_group):
    digits = max(min(4-len(str(int(price))), 2), -1)
    data = {
        'price': price,
    }

    t, r, q = get_trq(ticker, expiry_date, spot_date, currency_code)
    vol = get_vol(ticker, spot_date, t, r, q, bot_horizon_month)
    strike, barrier = get_strike_barrier(price, vol, bot_option_type, bot_group)
    rebate = barrier - strike
    v1, v2 = get_v1_v2(ticker, price, spot_date, t, r, q, strike, barrier)
    delta = uno.deltaUnOC(price, strike, barrier, rebate, t, r, q, v1, v2)
    delta = np.nan_to_num(delta, nan=0)
    option_price = uno.Up_Out_Call(price, strike, barrier, rebate, t, r, q, v1, v2)
    option_price = np.nan_to_num(option_price, nan=0)
    potential_loss = -1 * option_price / price
    targeted_profit = (barrier-strike) / price
    share_num = investment_amount / price
    data['share_num'] = math.floor(delta * share_num)
    data['max_loss_pct'] = potential_loss
    data['vol'] = vol
    data['max_loss_pct_display'] = round(data['max_loss_pct'] * 100, 2)
    data['max_loss_amount'] = round(option_price * data['share_num'], int(digits)) * -1
    data['max_loss_price'] = round(price - option_price, int(digits))
    data['target_profit_pct'] = targeted_profit
    data['target_profit_pct_display'] = round(data['target_profit_pct'] * 100, 2)
    data['target_profit_price'] = round(barrier, int(digits))
    data['target_profit_amount'] = round(rebate * data['share_num'], int(digits))
    data['bot_cash_balance'] = round(investment_amount - (data['share_num'] * price), 2)
    return data

def get_vol_by_date(ticker, trading_day):
    vol_table = "droid_vol_surface_parameters"
    vol_inferred_table = "droid_vol_surface_parameters_inferred"
    latest_vol_table = "latest_vol_updates"
    db_url = "postgres://postgres:ml2021#LORA@droid-v1-cluster.cluster-ro-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"

    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * "
        query += f"from {vol_table} vol "
        query += f"where vol.ticker = '{ticker}' and "
        query += f"vol.trading_day <= '{trading_day}' "
        query += f"order by trading_day DESC limit 1;"
        data = pd.read_sql(query, con=conn)
        data = pd.DataFrame(data)
        if(len(data) != 1):
            query = f"select * "
            query += f"from {vol_inferred_table} vol "
            query += f"where vol.ticker = '{ticker}' and "
            query += f"vol.trading_day <= '{trading_day}' "
            query += f"order by trading_day DESC limit 1;"
            data = pd.read_sql(query, con=conn)
            data = pd.DataFrame(data)
            if(len(data) != 1):
                query = f"select * "
                query += f"from {latest_vol_table} vol "
                query += f"where vol.ticker = '{ticker}' limit 1;"
                data = pd.read_sql(query, con=conn)
                data = pd.DataFrame(data)
                if(len(data) != 1):
                    data = {
                        "ticker": ticker,
                        "trading_day": trading_day
                    }
                    return False, data
    engine.dispose()
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
    vol_table = "classic_vol_history"
    latest_price_updates_table = "latest_price_updates"
    db_url = "postgres://postgres:ml2021#LORA@droid-v1-cluster.cluster-ro-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"

    engine = create_engine(db_url, max_overflow=-1, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        metadata = db.MetaData()
        query = f"select * "
        query += f"from {vol_table} vol "
        query += f"where vol.ticker = '{ticker}' and "
        query += f"vol.spot_date <= '{trading_day}' "
        query += f"order by spot_date DESC limit 1;"
        data = pd.read_sql(query, con=conn)
        data = pd.DataFrame(data)
        if(len(data) != 1):
            query = f"select * "
            query += f"from {latest_price_updates_table} vol "
            query += f"where vol.ticker = '{ticker}';"
            data = pd.read_sql(query, con=conn)
            data = pd.DataFrame(data)
            if(len(data) != 1):
                classic_vol = 0.2
            else:
                classic_vol = data.loc[0, "classic_vol"]
        else:
            classic_vol = data.loc[0, "classic_vol"]
    engine.dispose()
    if classic_vol == None:
        classic_vol = 0.2
    if classic_vol == np.NaN:
        classic_vol = 0.2
    data = {
        "ticker": ticker,
        "trading_day": trading_day,
        "classic_vol": classic_vol
    }
    return data
