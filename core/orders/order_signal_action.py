from core.bot.models import BotOptionType
from core.user.models import TransactionHistory
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from abc import ABC,abstractmethod



class OrderActorActionBase(ABC):
    
    
    @abstractmethod
    def on_pending(self):
        pass
    
    
    @abstractmethod
    def on_filled(self):
        pass
    
    @abstractmethod
    def on_cancel(self):
        pass
    
    def begin_action(self):
        if self.instance.status == 'pending':
            self.on_pending()
        elif self.instance.status == 'filled':
            self.on_filled()
        elif self.instance.status == 'cancel':
            self.on_cancel()

class BotOrder(OrderActorActionBase):

    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    



    def on_pending(self):
        if self.instance.is_init:
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
        # if not instance.order_type:
        #     commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
        #         instance.amount, instance.side, instance.user_id)
        #     fee = OrderFee.objects.create(
        #         order_uid=instance,
        #         fee_type=f"{instance.side} commissions fee",
        #         amount=commissions_fee
        #     )
        #     balance = Accountbalance.objects.get(user_id=instance.user_id)
        #     TransactionHistory.objects.create(
        #         balance_uid=instance.user_id.wallet,
        #         side="debit",
        #         amount=total_fee,
        #         transaction_detail={
        #             "description": f"{instance.side} fee",
        #             # "position": f"{order.position_uid}",
        #             "event": "fee",
        #             "fee_id": fee.id
        #         },
        #     )
        #     if stamp_duty_fee > 0:
        #         stamp = OrderFee.objects.create(
        #             order_uid=instance,
        #             fee_type=f"{instance.side} stamp_duty fee",
        #             amount=stamp_duty_fee
        #         )
        #         balance = Accountbalance.objects.get(user_id=instance.user_id)
        #         TransactionHistory.objects.create(
        #             balance_uid=instance.user_id.wallet,
        #             side="debit",
        #             amount=stamp_duty_fee,
        #             transaction_detail={
        #                 "last_amount" : balance.amount,
        #                 "description": f"{instance.side} fee",
        #                 # "position": f"{order.position_uid}",
        #                 "event": "stamp_duty",
        #                 "stamp_duty_id": stamp.id
        #             },
        #         )


    def on_filled(self):
        pass

    def on_cancel(self):
        pass
    


class UserOrder(OrderActorActionBase):

    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


    
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

    

    def bot_hedge(self):
        pass

    
    def calculate_fee(self,amount, side, user_id):
        pass
    
    
    def create_fee(self, user_id, balance_uid, position_uid, status):
        pass

    def common_order_method(self):
        pass

    def execute_sell(self):
        pass

    def execute_buy(self):
        pass
    
    def init_order_method(self):
        if self.bot.is_stock():
            self.init_user_order()
        else:
            self.init_bot_order()
    
    def init_bot_order(self):
        pass
    
    def init_user_order(self):
        if self.instance.status == 'pending':
            self.on_pending()
        elif self.instance.status == 'filled':
            self.on_filled()
        elif self.instance.status == 'cancel':
            self.on_cancel()

    
   
    def order_identity(self):
        if self.instance.init:
            self.init_order_method()
        else:
            self.common_order_method()