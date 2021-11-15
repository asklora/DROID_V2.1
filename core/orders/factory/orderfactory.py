from dataclasses import dataclass
from core.universe.models import Universe
from core.user.models import User
from core.user.convert import ConvertMoney

from core.orders.models import Order,OrderPosition
from core.bot.models import BotOptionType
from .order_protocol import ValidatorProtocol, OrderProtocol
from rest_framework import exceptions


@dataclass
class SellPayload:
    bot_id:str
    amount:str
    ticker:Universe
    setup:dict
    user_id:User
    
    
@dataclass
class BuyPayload:
    bot_id:str
    amount:str
    price:float
    side:str
    ticker:Universe
    user_id:User
    margin:int
    
    @property
    def c_amount(self):
        converter = ConvertMoney(self.user_id.user_balance.currency_code,self.ticker.currency_code)
        return converter.convert(self.amount)



class SellValidator:
    
    
    
    
    def __init__(self, payload:SellPayload):
        self.payload = payload
    
    def validate(self):
        print(self.payload)



class BuyValidator:
    
    def __init__(self, payload:BuyPayload):
        self.payload = payload
        

    
    def is_order_exist(self):
        orders = Order.objects.filter(user_id=self.payload.user_id,ticker=self.payload.ticker,bot_id=self.payload.bot_id,status='pending',side='buy')
        if orders.exists():
            raise exceptions.NotAcceptable({"detail": f"you already has order for {self.payload.ticker.ticker} in current options"})
        
    def is_portfolio_exist(self):
        portfolios = OrderPosition.objects.filter(user_id=self.payload.user_id,ticker=self.payload.ticker,bot_id__in=self.payload.bot_id,is_live=True).prefetch_related('ticker')
        if portfolios.exists():
            raise exceptions.NotAcceptable({"detail": f"cannot have multiple position for {self.payload.ticker.ticker} in current options"})
    
    
    def is_below_one(self):
        return (self.payload.c_amount / self.payload.price) < 1
            
    
    def is_insufficient_funds(self):
        if self.payload.amount > self.payload.user_id.user_balance.amount or self.is_below_one():
            raise exceptions.NotAcceptable({"detail": "insufficient funds"})
    
    def is_zero_amount(self):
        if self.payload.amount <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})
        
    def validate(self):
        self.is_order_exist()
        self.is_portfolio_exist()
        self.is_zero_amount()
        self.is_below_one()
        self.is_insufficient_funds()

        
        print(self.payload)



class SellOrderProcessor:
    
    def __init__(self,payload:SellPayload):
        self.payload = payload
        self.validator:ValidatorProtocol = SellValidator(SellPayload(**payload))
    
    
    
    def execute(self):
        pass
        
        


class BuyOrderProcessor:
    
    
    
    def __init__(self,payload:BuyPayload):
        self.payload = payload
        self.validator:ValidatorProtocol = BuyValidator(BuyPayload(**payload))
        
    def execute(self):
        pass


class OrderController:
    
    def process(self,processor:OrderProtocol):
        processor.validator.validate()
        processor.execute()