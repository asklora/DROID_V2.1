import math
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
import scipy.stats as si
from bot.factory.bot_protocols import ValidatorProtocol
from core.master.models import LatestPrice
from scipy.optimize import newton

from general.data_process import NoneToZero

from ..botproperties import BaseProperties, ClassicProperties, UnoProperties


class Creator(ABC):
    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def _construct(self):
        pass

    @abstractmethod
    def last_hedge_delta(self):
        pass

    @abstractmethod
    def get_bot_cash_balance(self):
        pass

    @abstractmethod
    def max_loss_pct(self):
        pass

    @abstractmethod
    def max_loss_price(self):
        pass

    @abstractmethod
    def max_loss_amount(self):
        pass

    @abstractmethod
    def target_profit_pct(self):
        pass

    @abstractmethod
    def target_profit_price(self):
        pass

    @abstractmethod
    def target_profit_amount(self):
        pass

    @abstractmethod
    def get_result(self):
        pass

    @abstractmethod
    def get_result_as_dict(self):
        pass


class BaseCreator(Creator):
    validated_data: ValidatorProtocol
    _default_properties: BaseProperties

    def __init__(self, validated_data, estimator):
        self.validated_data = validated_data
        self.estimator = estimator

    def _digits(self, price):
        digit = max(min(4 - len(str(int(price))), 2), -1)
        return int(digit)

    def get_total_bot_share_num(self):
        inv_amt = self.validated_data.investment_amount
        margin = self.validated_data.margin
        price = self.validated_data.price
        return math.floor((inv_amt * margin) / price)

    def _construct(self):
        self._default_properties = BaseProperties(
            ticker=self.validated_data.ticker,
            last_hedge_delta=self.last_hedge_delta(),
            share_num=self.get_total_bot_share_num(),
            current_bot_cash_balance=self.get_bot_cash_balance(),
            expiry=self.validated_data.expiry,
            spot_date=self.validated_data.spot_date,
            total_bot_share_num=self.get_total_bot_share_num(),
            max_loss_pct=self.max_loss_amount(),
            max_loss_price=self.max_loss_price(),
            max_loss_amount=self.max_loss_amount(),
            target_profit_pct=self.target_profit_pct(),
            target_profit_price=self.target_profit_price(),
            target_profit_amount=self.target_profit_amount(),
            bot_cash_balance=self.get_bot_cash_balance(),
            investment_amount=self.validated_data.investment_amount,
            price=self.validated_data.price,
            margin=self.validated_data.margin,
        )


class ClassicCreator(BaseCreator):
    def get_classic_vol(self):
        try:
            return LatestPrice.objects.get(
                ticker=self.validated_data.ticker
            ).classic_vol
        except LatestPrice.DoesNotExist:
            raise ValueError("Ticker not found in latest price")

    def get_bot_cash_balance(self):
        return round(
            self.validated_data.investment_amount
            - (self.get_total_bot_share_num() * self.validated_data.price),
            2,
        )

    def _month(self) -> int:
        """
        private function to get month
        """
        return int(round((self.validated_data.time_to_exp * 365), 0)) / 30

    def get_vol(self) -> float:
        return pow(self.validated_data.time_to_exp, 0.5) * min(
            (0.75 + (self._month() * 0.25)), 2
        )

    def last_hedge_delta(self):
        return 1

    def max_loss_pct(self) -> float:
        return self.get_vol() * self.get_classic_vol() * 1.25

    def max_loss_price(self) -> float:
        return round(
            self.validated_data.price * (1 + self.max_loss_pct()),
            self._digits(self.validated_data.price),
        )

    def max_loss_amount(self):
        return round(
            (self.max_loss_price() - self.validated_data.price)
            * self.get_total_bot_share_num(),
            self._digits(self.validated_data.price),
        )

    def target_profit_pct(self):
        return self.get_vol() * self.get_classic_vol()

    def target_profit_price(self):
        return round(
            self.validated_data.price * (1 + self.target_profit_pct()),
            self._digits(self.validated_data.price),
        )

    def target_profit_amount(self):
        return round(
            (self.target_profit_price() - self.validated_data.price)
            * self.get_total_bot_share_num(),
            self._digits(self.validated_data.price),
        )

    def process(self):
        self._construct()
        self.properties = ClassicProperties(
            **self._default_properties.__dict__,
            vol=self.get_vol(),
            classic_vol=self.get_classic_vol()
        )

    def get_result(self):
        return self.properties

    def get_result_as_dict(self):
        return self.properties.__dict__


class UnoCreator(BaseCreator):
    
    
    def _uno_itm(self):
        return

    def _uno_otm(self):
        return

    def _get_strike_barrier(self, price, vol, bot_option_type, bot_group):
        pass

    def last_hedge_delta(self):
        delta = self.estimator.get_delta()
        return np.nan_to_num(delta, nan=0)

    def _bot_hedge_share(self):
        delta = self.estimator.get_delta()
        math.floor(delta * self.validated_data.total_bot_share_num)

    def get_bot_cash_balance(self):
        return round(
            self.validated_data.investment_amount
            - (self._bot_hedge_share() * self.validated_data.price),
            self._digits(self.validated_data.price),
        )

    def max_loss_pct(self):
        option_price = self.estimator.Up_Out_Call(
            price, strike, barrier, rebate, t / 365, r, q, v1, v2
        )
        option_price = np.nan_to_num(option_price, nan=0)
        return -1 * option_price / price

    def max_loss_price(self):
        return round(price - option_price, int(digits))

    def max_loss_amount(self):
        return round(option_price * total_bot_share_num, int(digits)) * -1

    def target_profit_pct(self):
        return (barrier - strike) / price

    def target_profit_price(self):
        round(barrier, int(digits))

    def target_profit_amount(self):
        round(rebate * total_bot_share_num, int(digits))

    def process(self):
        self._construct()
        self.properties = UnoProperties(
            **self._default_properties.__dict__,
            vol=self.get_vol(),
            classic_vol=self.get_classic_vol()
        )

    def get_result(self):
        pass

    def get_result_as_dict(self):
        pass
