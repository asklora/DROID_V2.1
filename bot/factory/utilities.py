from datetime import datetime

import numpy as np
import pandas as pd
from general.data_process import NoneToZero


class BotUtilities:
    @staticmethod
    def check_date(dates):
        if type(dates) == str and len(dates) > 10:
            dates = pd.to_datetime(dates)
        elif type(dates) == str and len(dates) == 10:
            dates = datetime.strptime(dates, "%Y-%m-%d")
        return dates

    # FIXME: modify calls to database
    @classmethod
    def get_holiday(cls, non_working_day, currency_code):
        table_name = get_currency_calendar_table_name()
        query = f"select distinct ON (non_working_day) non_working_day, currency_code from {table_name} "
        query += f" where non_working_day='{non_working_day}' and currency_code in {tuple_data(currency_code)}"
        data = read_query(query, table_name, cpu_counts=True, prints=False)
        return data

    @classmethod
    def get_expiry_date(
        cls,
        time_to_exp,
        spot_date,
        currency_code,
        apps=False,
    ):
        """
        - Parameters:
            - time_to_exp -> float
            - spot_date -> date
            - currency_code -> str
        - Returns:
            - datetime -> date
        """
        spot_date = cls.check_date(spot_date)
        days = int(round((time_to_exp * 365), 0))
        expiry = spot_date + relativedelta(days=(days))
        if not apps:
            while expiry.weekday() != 5:
                expiry = expiry - relativedelta(days=1)
        # days = int(round((time_to_exp * 256), 0))
        # expiry = spot_date + BDay(days-1)

        while True:
            holiday = False
            data = get_holiday(expiry.strftime("%Y-%m-%d"), currency_code)
            if len(data) > 0:
                holiday = True
            if (holiday == False) and expiry.weekday() < 5:
                break
            else:
                expiry = expiry - relativedelta(days=1)
        return expiry

    @staticmethod
    def get_strike_barrier(price, vol, bot_option_type, bot_group):
        price = NoneToZero(price)
        vol = NoneToZero(vol)
        if bot_group == "UNO":
            if bot_option_type == "OTM":
                strike = price * (1 + vol * 0.5)
            elif bot_option_type == "ITM":
                strike = price * (1 - vol * 0.5)

            if bot_option_type == "OTM":
                barrier = price * (1 + vol * 2)
            elif bot_option_type == "ITM":
                barrier = price * (1 + vol * 1.5)
            return float(NoneToZero(strike)), float(NoneToZero(barrier))

        elif bot_group == "UCDC":
            strike = price
            strike_2 = price * (1 - vol * 1.5)
            return float(NoneToZero(strike)), float(NoneToZero(strike_2))
        return False

    def get_trq(self, ticker, expiry, spot_date, currency_code):
        expiry = self.check_date(expiry)
        spot_date = self.check_date(spot_date)
        t = (expiry - spot_date).days
        if t == 0:
            t = 1
        r = get_r(currency_code, t)
        q = get_q(ticker, t)
        return int(NoneToZero(t)), float(NoneToZero(r)), float(NoneToZero(q))

    def get_vol(self, ticker, trading_day, t, r, q, time_to_exp):
        t = NoneToZero(t)
        r = NoneToZero(r)
        q = NoneToZero(q)
        trading_day = self.check_date(trading_day)
        # FIXME: get vol_by_data from estimator
        status, obj = self.estimator.get_vol_by_date(ticker, trading_day)
        if status:
            v0 = uno.find_vol(
                1,
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v0 = np.nan_to_num(v0, nan=0)
            v0 = max(min(v0, max_vol), min_vol)

            # Code when we use business days
            # month = int(round((time_exp * 256), 0)) / 22
            month = int(round((time_to_exp * 365), 0)) / 30
            vol = v0 * (month / 12) ** 0.5

        else:
            vol = default_vol
        return float(NoneToZero(vol))

    def get_v1_v2(self, ticker, price, trading_day, t, r, q, strike, barrier):
        trading_day = self.check_date(trading_day)
        status, obj = self.get_vol_by_date(ticker, trading_day)
        price = NoneToZero(price)
        t = NoneToZero(t)
        r = NoneToZero(r)
        q = NoneToZero(q)
        strike = NoneToZero(strike)
        barrier = NoneToZero(barrier)
        if status:
            v1 = uno.find_vol(
                NoneToZero(strike / price),
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v1 = np.nan_to_num(v1, nan=0)
            v2 = uno.find_vol(
                NoneToZero(barrier / price),
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v2 = np.nan_to_num(v2, nan=0)
        else:
            v1 = default_vol
            v2 = default_vol
        return float(NoneToZero(v1)), float(NoneToZero(v2))
