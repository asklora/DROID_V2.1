from dataclasses import  dataclass
from core.universe.models import Universe
from core.user.models import User
from core.user.convert import ConvertMoney




@dataclass
class ActionPayload:
    order_uid:str
    firebase_token:str or None
    status:str
    

  
@dataclass
class SellPayload:
    setup: dict
    side: str
    ticker: Universe
    user_id: User
    margin:int


@dataclass
class BuyPayload:
    amount: float
    bot_id: str
    price: float
    side: str
    ticker: Universe
    user_id: User
    margin: int

    @property
    def c_amount(self):
        converter = ConvertMoney(
            self.user_id.user_balance.currency_code, self.ticker.currency_code)
        return converter.convert(self.amount)