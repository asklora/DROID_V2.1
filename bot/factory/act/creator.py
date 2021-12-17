import math
from abc import ABC, abstractmethod
from bot.factory.bot_protocols import ValidatorProtocol
from core.master.models import LatestPrice
from ..botproperties import (
    BaseProperties,
    ClassicProperties,
    EstimatorUcdcResult,
    UnoProperties,
    UcdcProperties,
    EstimatorUnoResult,
)


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
    def get_result(self) -> BaseProperties:
        pass

    @abstractmethod
    def get_result_as_dict(self) -> dict:
        pass


class BaseCreator(Creator):
    validated_data: ValidatorProtocol
    _default_properties: BaseProperties
    properties: BaseProperties

    def __init__(self, validated_data, estimator):
        self.validated_data = validated_data
        self.estimator = estimator

    def get_total_bot_share_num(self):
        inv_amt = self.validated_data.investment_amount
        margin = self.validated_data.margin
        price = self.validated_data.price
        return math.floor((inv_amt * margin) / price)

    def get_hedge_share(self):
        return self.get_total_bot_share_num()

    def _construct(self):
        self._default_properties = BaseProperties(
            ticker=self.validated_data.ticker,
            last_hedge_delta=self.last_hedge_delta(),
            share_num=self.get_hedge_share(),
            current_bot_cash_balance=self.get_bot_cash_balance(),
            expiry=self.validated_data.expiry,
            created=self.validated_data.created,
            spot_date=self.validated_data.spot_date,
            total_bot_share_num=self.get_total_bot_share_num(),
            max_loss_pct=self.max_loss_pct(),
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

    def _properties_check(self):
        if self.properties:
            return
        else:
            raise ValueError("No result found, need process to be trigered")

    def get_result(self):
        self._properties_check()
        return self.properties

    def _result_date_to_sting(self, data: dict) -> dict:
        if not data.get("created"):
            raise ValueError("error key created not found")
        data["created"] = str(data["created"])
        if not data.get("spot_date"):
            raise ValueError("error key spot_date not found")
        data["spot_date"] = str(data["spot_date"])

        if not data.get("expiry"):
            raise ValueError("error key expiry not found")
        data["expiry"] = str(data["expiry"])

        return data

    def get_result_as_dict(self):
        self._properties_check()
        return self._result_date_to_sting(self.properties.__dict__)


class ClassicCreator(BaseCreator):
    def get_classic_vol(self):
        try:
            return LatestPrice.objects.get(
                ticker=self.validated_data.ticker
            ).classic_vol
        except LatestPrice.DoesNotExist:
            raise ValueError("Ticker not found in latest price")

    def get_bot_cash_balance(self):
        return self.estimator._round(
            self.validated_data.investment_amount
            - (self.get_total_bot_share_num() * self.validated_data.price)
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
        return self.estimator._round(
            self.validated_data.price * (1 + self.max_loss_pct())
        )

    def max_loss_amount(self):
        return self.estimator._round(
            (self.max_loss_price() - self.validated_data.price)
            * self.get_total_bot_share_num()
        )

    def target_profit_pct(self):
        return self.get_vol() * self.get_classic_vol()

    def target_profit_price(self):
        return self.estimator._round(
            self.validated_data.price * (1 + self.target_profit_pct())
        )

    def target_profit_amount(self):
        return self.estimator._round(
            (self.target_profit_price() - self.validated_data.price)
            * self.get_total_bot_share_num()
        )

    def process(self):
        self._construct()
        self.properties = ClassicProperties(
            **self._default_properties.__dict__,
            vol=self.get_vol(),
            classic_vol=self.get_classic_vol()
        )


class UnoCreator(BaseCreator):
    est: EstimatorUnoResult

    def last_hedge_delta(self):
        return self.est.delta

    def _bot_hedge_share(self):
        return math.floor(self.est.delta * self.get_total_bot_share_num())

    def get_hedge_share(self):
        return self._bot_hedge_share()

    def get_bot_cash_balance(self):
        return self.estimator._round(
            self.validated_data.investment_amount
            - (self._bot_hedge_share() * self.validated_data.price)
        )

    def max_loss_pct(self):
        return -1 * self.est.option_price / self.validated_data.price

    def max_loss_price(self):
        return self.estimator._round(
            self.validated_data.price - self.est.option_price
        )

    def max_loss_amount(self):
        return (
            self.estimator._round(
                self.est.option_price * self.get_total_bot_share_num()
            )
            * -1
        )

    def target_profit_pct(self):
        return (self.est.barrier - self.est.barrier) / self.validated_data.price

    def target_profit_price(self):
        return self.estimator._round(self.est.barrier)

    def target_profit_amount(self):
        return self.estimator._round(
            self.est.rebate * self.get_total_bot_share_num()
        )

    def process(self):
        self.est = self.estimator.calculate(self.validated_data)
        self._construct()
        result_dict = self.est.__dict__
        result_dict.pop("rebate")
        self.properties = UnoProperties(
            **self._default_properties.__dict__, **result_dict
        )


class UcdcCreator(UnoCreator):
    est: EstimatorUcdcResult

    def target_profit_pct(self):
        return -1 * self.est.option_price / self.validated_data.price

    def target_profit_price(self):
        return self.estimator._round(
            ((-1 * self.est.option_price) + self.validated_data.price)
        )

    def target_profit_amount(self):
        return (
            self.estimator._round(
                self.est.option_price * self.get_total_bot_share_num()
            )
            * -1
        )

    def max_loss_pct(self):
        return (self.est.strike_2 - self.est.strike) / self.validated_data.price

    def max_loss_price(self):
        return self.estimator._round(self.est.strike_2)

    def max_loss_amount(self):
        return self.estimator._round(
            (self.est.strike_2 - self.est.strike)
            * self.get_total_bot_share_num()
        )

    def process(self):
        self.est = self.estimator.calculate(self.validated_data)
        self._construct()
        result_dict = self.est.__dict__
        self.properties = UcdcProperties(
            **self._default_properties.__dict__, **result_dict
        )
