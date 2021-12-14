from datetime import datetime

import numpy as np
import pandas as pd
from core.master.models import DataDividendDailyRates, DataInterestDailyRates
from general.data_process import NoneToZero


class BotUtilities:
    
    @property
    def _get_q(self, ticker: str, t: int) -> int:
        try:
            q = DataDividendDailyRates.objects.get(ticker=ticker, t=t).q
            if q:
                return q
            return 0
        except DataDividendDailyRates.DoesNotExist:
            return 0

    @property
    def _get_r(self, currency_code: str, t: int) -> int:
        try:
            r = DataInterestDailyRates.objects.get(
                currency_code=currency_code, t=t
            ).r
            if r:
                return r
            return 0
        except DataInterestDailyRates.DoesNotexist:
            return 0

    def get_trq(
        self,
        expiry: datetime,
        spot_date: datetime,
        ticker: str,
        currency_code: str,
    ) -> tuple(int, float, float):
        t = max(1, (expiry - spot_date).days)
        return t, self._get_r(currency_code, t), self._get_q(ticker, t)


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
