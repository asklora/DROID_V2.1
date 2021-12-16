from dataclasses import dataclass
from datetime import datetime


@dataclass
class BaseProperties:
    ticker: str
    last_hedge_delta: float
    share_num: float
    current_bot_cash_balance: float
    expiry: datetime.date
    spot_date: datetime.date
    created: datetime
    total_bot_share_num: float
    max_loss_pct: float
    max_loss_price: float
    max_loss_amount: float
    target_profit_pct: float
    target_profit_price: float
    target_profit_amount: float
    bot_cash_balance: float
    investment_amount: float
    price: float
    margin: int


@dataclass
class BaseResultProperties:
    q: float
    r: float
    t: float


@dataclass
class ClassicProperties(BaseProperties):
    vol: float
    classic_vol: float


@dataclass
class UnoProperties(BaseProperties):
    barrier: float
    delta: float
    option_price: float
    q: float
    r: float
    strike: float
    t: float
    v1: float
    v2: float
    vol: float


@dataclass
class UcdcProperties(BaseProperties):
    strike_2: float
    delta: float
    option_price: float
    q: float
    r: float
    strike: float
    t: float
    v1: float
    v2: float
    vol: float


@dataclass
class EstimatorUnoResult(BaseResultProperties):
    barrier: float
    delta: float
    option_price: float
    rebate: float
    strike: float
    v1: float
    v2: float
    vol: float


@dataclass
class EstimatorUcdcResult(BaseResultProperties):
    delta: float
    option_price: float
    strike: float
    strike_2: float
    v1: float
    v2: float
    vol: float
