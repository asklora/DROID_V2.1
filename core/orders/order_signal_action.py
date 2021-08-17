from core.bot.models import BotOptionType
from core.user.models import TransactionHistory
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from abc import ABC,abstractmethod

"""
user/bot
broker/simulation
init/hedge
    hedge expired,finish / hedge daily

buy/sell

"""

class AbstracOrderConnector(ABC):
    
    @abstractmethod
    def on_placed(self):
        pass
    
    @abstractmethod
    def on_pending(self):
        pass
    
    
    @abstractmethod
    def on_filled(self):
        pass
    
    @abstractmethod
    def on_cancel(self):
        pass
    

class BuyActionBase(ABC):

    @abstractmethod
    def process_buy(self):
        pass


class SellActionBase(ABC):

    @abstractmethod
    def process_sell(self):
        pass



class LiveOrderConnector(AbstracOrderConnector):
    
    
    
    def __init__(self,*args,**kwargs):
       for key, value in kwargs.items():
            setattr(self, key, value)

class SimulationOrderConnector(AbstracOrderConnector):
    
    
    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)



class SellOrderSimulation(SellActionBase,SimulationOrderConnector):
    
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

        
    










class OrderServices:
    
    
    
    def __init__(self,instance:Order):
        self.instance = instance
        self.user_wallet = instance.user_id.user_balance
        self.user_wallet_amount = instance.user_id.amount
        self.user_wallet_currency = instance.user_id.user_balance.currency_code
        self.bot = BotOptionType.objects.get(bot_id=instance.bot_id)
        order_property = self.__dict__
        self.bot_handler = BotOrder(**order_property)
        self.user_handler = UserOrder(**order_property)

    
    
    def process_transaction(self):
        if self.bot.is_stock():
            self.user_handler
        else:
            self.bot_handler
    
