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
class ClassicProperties(BaseProperties):
    vol: float
    classic_vol: float


class UnoProperties(BaseProperties):
    pass


class UcdcProperties(BaseProperties):
    pass
