from datetime import datetime
from typing import Tuple

from core.master.models import DataDividendDailyRates, DataInterestDailyRates
from general.data_process import NoneToZero
from core.djangomodule.general import logging

class BotUtilities:
    def _get_q(self, ticker: str, t: int) -> int:
        try:
            q = DataDividendDailyRates.objects.get(ticker=ticker, t=t).q
            if q:
                return q
            return 0
        except DataDividendDailyRates.DoesNotExist:
            logging.warning(f"No q for {ticker} at t={t}")
            return 0

    def _get_r(self, currency_code: str, t: int) -> int:
        try:
            r = DataInterestDailyRates.objects.get(
                currency_code=currency_code, t=t
            ).r
            if r:
                return r
            return 0
        except DataInterestDailyRates.DoesNotexist:
            logging.warning(f"No r for {currency_code} at t={t}")
            return 0

    def get_trq(
        self,
        expiry: datetime.date,
        spot_date: datetime.date,
        ticker: str,
        currency_code: str,
    ) -> Tuple[int, float, float]:
        t = max(1, (expiry - spot_date).days)
        return t, self._get_r(currency_code, t), self._get_q(ticker, t)

    def get_strike_barrier(self, price, vol, bot_option_type, bot_group):
        price = NoneToZero(price)
        vol = NoneToZero(vol)
        if bot_group == "UNO":
            if bot_option_type == "OTM":
                strike = price * (1 + vol * 0.5)
                barrier = price * (1 + vol * 2)
            elif bot_option_type == "ITM":
                strike = price * (1 - vol * 0.5)
                barrier = price * (1 + vol * 1.5)

            return float(NoneToZero(strike)), float(NoneToZero(barrier))

        elif bot_group == "UCDC":
            strike = price
            strike_2 = price * (1 - vol * 1.5)
            return float(NoneToZero(strike)), float(NoneToZero(strike_2))
        return False
