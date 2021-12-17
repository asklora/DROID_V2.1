from datetime import datetime
from typing import Tuple

from core.djangomodule.general import logging
from core.master.models import DataDividendDailyRates, DataInterestDailyRates
from general.data_process import NoneToZero


class BotUtilities:
    def _digits(self, price):
        digit = max(min(4 - len(str(int(price))), 2), -1)
        return int(digit)

    def _round(self, value: float):
        return round(value, self._digits(value))

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

    def get_current_investment_amount(
        self, live_price: float, share_num: float
    ) -> float:
        return self._round(live_price * share_num)

    def get_current_pnl_amount(
        self,
        live_price: float,
        last_live_price: float,
        last_pnl_amt: float,
        share_num: float,
    ) -> float:
        current_pnl_amt = (
            last_pnl_amt + (live_price - last_live_price) * share_num
        )

        return self._round(current_pnl_amt)

    def get_current_pnl_pct(
        self,
        live_price: float,
        last_live_price: float,
        last_pnl_amt: float,
        share_num: float,
        investment_amount: float,
    ) -> float:
        current_pnl_amt: float = self.get_current_pnl_amount(
            live_price=live_price,
            last_live_price=last_live_price,
            last_pnl_amt=last_pnl_amt,
            share_num=share_num,
        )
        return self._round(current_pnl_amt / investment_amount)

    def get_current_bot_cash_balance(
        self,
        last_bot_cash_balance: float,
        live_price: float,
        share_num: float,
        last_share_num: float,
    ) -> float:
        bot_cash_balance: float = (
            last_bot_cash_balance - (share_num - last_share_num) * live_price
        )

        return self._round(bot_cash_balance)

    def get_current_value(
        self,
        current_price: float,
        entry_price: float,
        last_pnl_amt: float,
        share_num: float,
    ) -> float:
        current_investment: float = self.get_current_investment_amount(
            live_price=current_price,
            share_num=share_num,
        )
        current_pnl_amount: float = self.get_current_pnl_amount(
            live_price=current_price,
            last_live_price=entry_price,
            last_pnl_amt=last_pnl_amt,
            share_num=share_num,
        )

        return current_investment + current_pnl_amount
