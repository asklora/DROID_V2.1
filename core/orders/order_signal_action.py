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

class AbstracBaseOrder(ABC):
    
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
    
    


class OrderActorActionBase(AbstracBaseOrder):
    
    
    
    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def begin_action(self):
        if self.instance.status == 'pending':
            self.on_pending()
        elif self.instance.status == 'filled':
            self.on_filled()
        elif self.instance.status == 'cancel':
            self.on_cancel()

class SimulationOrder(OrderActorActionBase):

    def on_placed(self):
        pass
    
    def on_pending(self):
        if self.instance.is_init:
            """
            take user balance into Order
            """
            inv_amt = self.instance.setup['investment_amount']
            digits = max(min(5-len(str(int(self.instance.price))), 2), -1)
            TransactionHistory.objects.create(
                balance_uid=self.instance.user_id.wallet,
                side="debit",
                amount=round(inv_amt, digits),
                transaction_detail={
                    "last_amount" : self.user_wallet_amount,
                    "description": "bot order",
                    "event": "create",
                    "order_uid": str(self.instance.order_uid)
                },
            )


    def on_filled(self):
        pass

    def on_cancel(self):
        pass
    


class LiveOrder(OrderActorActionBase):

    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def on_placed(self):
        pass
    
    def on_pending(self):
        pass

    def on_filled(self):
        pass

    def on_cancel(self):
        pass


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
    
