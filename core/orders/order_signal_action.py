from core.bot.models import BotOptionType
from core.user.models import TransactionHistory
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from abc import ABC,abstractmethod
from core.djangomodule.general import formatdigit
from core.Clients.models import UserClient


"""
user/bot
broker/simulation
init/hedge
    hedge expired,finish / hedge daily

buy/sell

"""

class AbstracOrderConnector(ABC):
    
    @abstractmethod
    def on_buy_placed(self):
        pass
    
    @abstractmethod
    def on_buy_pending(self):
        pass
    
    
    @abstractmethod
    def on_buy_filled(self):
        pass
    
    @abstractmethod
    def on_buy_cancel(self):
        pass

    @abstractmethod
    def on_sell_placed(self):
        pass
    
    @abstractmethod
    def on_sell_pending(self):
        pass
    
    
    @abstractmethod
    def on_sell_filled(self):
        pass
    
    @abstractmethod
    def on_sell_cancel(self):
        pass
    

class BaseOrderConnector(AbstracOrderConnector):
    
    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.digits = max(min(5-len(str(int(self.instance.price))), 2), -1)
    
    def run(self):
        func_name =f'on_{self.instance.side}_{self.instance.status}'
        if hasattr(self,func_name):
            """
            SKIP REVIEW STATUS
            """
            function = getattr(self, func_name)
            function()

    
    def on_buy_placed(self):
        pass
    
    def on_buy_pending(self):
        """
        Only Take amount off order from wallet if order initialized
        """
        if self.instance.is_init:
            TransactionHistory.objects.create(
                balance_uid=self.user_wallet,
                side="debit",
                amount=round(self.instance.amount, self.digits),
                transaction_detail={
                    "last_amount" : self.user_wallet_amount,
                    "description": "bot order",
                    # "position": f"{order.position_uid}",
                    "event": "create",
                    "order_uid": str(self.instance.order_uid)
                },
            )
    
    
    def on_buy_filled(self):
        if self.instance.is_init:
            """
            Can be BOT or USER
            """
            # create position and performance
            position_uid,performance_uid = self.create_position_perfomance()
        
        else:
            """
            Only Bot hedge behaviour
            """
            position_uid,performance_uid =self.hedge_position_performance()
        
        # create fee
        self.create_fee(position_uid)
            
        # update last pending transaction 
        self.update_initial_transaction_position(position_uid)

        # update to connect performance and prevent triger signal
        Order.objects.filter(pk=self.instance.order_uid).update(performance_uid=performance_uid)
    
    def on_buy_cancel(self):
        if self.instance.is_init and self.instance.status == 'pending':
            trans = TransactionHistory.objects.filter(
                side='debit', transaction_detail__description='bot order', transaction_detail__order_uid=str(self.instance.order_uid))
            if trans.exists():
                trans.delete()

    def on_sell_placed(self):
        pass
    
    def on_sell_pending(self):
        pass
    
    
    def on_sell_filled(self):
        pass
    
    def on_sell_cancel(self):
        pass

    def hedge_position_performance(self):
        
        """
        Only Bot hedge behaviour
        UPDATE the position and CREATE performance
        """
        
        order = OrderPosition.objects.get(
                        position_uid=self.instance.setup["position"]["position_uid"])
        key_list = list(self.instance.setup["position"])

        # UPDATE the position
        for key in key_list:
            if not key in ["created", "updated", "share_num"]:
                if hasattr(order, key):
                    field = order._meta.get_field(key)
                    if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                        raw_key = f"{key}_id"
                        setattr(
                            order, raw_key, self.instance.setup["position"].pop(key))
                    else:
                        setattr(order, key,
                                self.instance.setup["position"].pop(key))
        
        # remove field position_uid, cause it will double
        self.instance.setup["performance"].pop("position_uid")
        
        # CREATE performance from hedge setup
        perf = PositionPerformance.objects.create(
            position_uid=order,  # replace with self.instance
            # the rest is value from setup
            **self.instance.setup["performance"]
        )

        perf.order_uid = self.instance
        order.save()
        perf.save()

        
        return order.position_uid,perf.performance_uid 
    
    def create_position_perfomance(self):
        """
        execute Every init order
        BOT / USER
        CREATE position and CREATE performance
        """
        margin=1
        order = OrderPosition.objects.create(
                    user_id=self.instance.user_id,
                    ticker=self.instance.ticker,
                    bot_id=self.instance.bot_id,
                    spot_date=self.instance.filled_at.date(),
                    entry_price=self.instance.price,
                    is_live=True,
                    margin=margin
                )
        perf = PositionPerformance.objects.create(
            created=self.instance.filled_at.date(),
            position_uid=order,
            last_spot_price=self.instance.price,
            last_live_price=self.instance.price,
            order_uid=self.instance,
            status='Populate'
        )
        
        # if bot
        if not self.bot.is_stock():

            for key, val in self.instance.setup.items():
                if hasattr(perf, key):
                    setattr(perf, key, val)
                if hasattr(order, key):
                    if key == "share_num":
                        continue
                    else:
                        setattr(order, key, val)
                else:
                    if key == "total_bot_share_num":
                        setattr(order, "share_num", val)
        else:
            # without bot
            order.investment_amount = self.instance.amount
            order.bot_cash_balance = 0
            order.share_num = self.instance.qty
            perf.share_num = self.instance.qty
            # start creating position
        perf.current_pnl_amt = 0  # need to calculate with fee
        perf.current_bot_cash_balance = order.bot_cash_balance

        perf.current_investment_amount = round(
            perf.last_live_price * perf.share_num, self.digits)
        perf.current_pnl_ret = (perf.current_bot_cash_balance + perf.current_investment_amount -
                                order.investment_amount) / order.investment_amount
        perf.order_id = self.instance

        perf.save()
        order.save()
        return order.position_uid,perf.performance_uid
    
    
    
    
    def update_initial_transaction_position(self,position_uid:str):
        # update the transaction
        trans = TransactionHistory.objects.filter(side="debit", transaction_detail__description="bot order", transaction_detail__order_uid=str(self.instance.order_uid))
        if trans.exists():
            trans = trans.get()
            trans.transaction_detail["position"] = position_uid
            trans.save()
        # update fee transaction
        fee_trans = TransactionHistory.objects.filter(side="debit", transaction_detail__event="fee", transaction_detail__order_uid=str(self.instance.order_uid))
        if fee_trans.exists():
            fee_trans = fee_trans.get()
            fee_trans.transaction_detail["position"] = position_uid
            fee_trans.save()
        # update stamp_duty transaction
        stamp_trans = TransactionHistory.objects.filter(side="debit", transaction_detail__event="stamp_duty", transaction_detail__order_uid=str(self.instance.order_uid))
        if stamp_trans.exists():
            stamp_trans = stamp_trans.get()
            stamp_trans.transaction_detail["position"] = position_uid
            stamp_trans.save()




    def calculate_fee(self):
        user_client = UserClient.objects.get(user_id=self.instance.user_id)
        if(self.instance.side == "sell"):
            commissions = user_client.client.commissions_sell
            stamp_duty = user_client.stamp_duty_sell
        else:
            commissions = user_client.client.commissions_buy
            stamp_duty = user_client.stamp_duty_buy

        if(user_client.client.commissions_type == "percent"):
            commissions_fee = self.instance.amount * commissions / 100
        else:
            commissions_fee = commissions

        if(user_client.stamp_duty_type == "percent"):
            stamp_duty_fee = self.instance.amount * stamp_duty / 100
        else:
            stamp_duty_fee = stamp_duty
        total_fee = commissions_fee + stamp_duty_fee
        return formatdigit(commissions_fee), formatdigit(stamp_duty_fee), formatdigit(total_fee)



    def create_fee(self,  position_uid,):
        commissions_fee, stamp_duty_fee, total_fee = self.calculate_fee()
        
        if commissions_fee:
            fee = OrderFee.objects.create(
                order_uid=self.instance,
                fee_type=f"{self.instance.side} commissions fee",
                amount=commissions_fee
            )
        if total_fee:
            TransactionHistory.objects.create(
                balance_uid=self.user_wallet,
                side="debit",
                amount=commissions_fee,
                transaction_detail={
                    "last_amount" : self.user_wallet_amount,
                    "description": f"{self.instance.side} fee",
                    "position": f"{position_uid}",
                    "event": "fee",
                    "fee_id": fee.id,
                    "order_uid": str(self.instance.order_uid)
                },
            )
        if stamp_duty_fee:
            stamp = OrderFee.objects.create(
                order_uid=self.instance,
                fee_type=f"{self.instance.side} stamp_duty fee",
                amount=stamp_duty_fee
            )
            TransactionHistory.objects.create(
                balance_uid=self.user_wallet,
                side="debit",
                amount=stamp_duty_fee,
                transaction_detail={
                    "last_amount" : self.user_wallet_amount,
                    "description": f"{self.instance.side} fee",
                    "position": f"{position_uid}",
                    "event": "stamp_duty",
                    "stamp_duty_id": stamp.id,
                    "order_uid": str(self.instance.order_uid)
                },
            )

class LiveOrderConnector(BaseOrderConnector):
    """
    different behaviour in order placed.
    will start sent request to broker

    """
  
    def __init__(self,*args,**kwargs):
        super().__init__(*args, **kwargs)

    def on_buy_placed(self):
        print('sent order to broker')

class SimulationOrderConnector(BaseOrderConnector):
    """
    different behaviour in order placed.
    will do nothing in here

    """
    
    def __init__(self,*args,**kwargs):
        super().__init__(*args, **kwargs)
    
    def on_buy_placed(self):
        """do nothing"""
        pass
        
        
    






class OrderServices:
    
    
    
    def __init__(self,instance:Order):
        self.instance = instance
        self.user_wallet = instance.user_id.user_balance
        self.user_wallet_amount = instance.user_id.amount
        self.user_wallet_currency = instance.user_id.user_balance.currency_code
        self.bot = BotOptionType.objects.get(bot_id=instance.bot_id)
        self.order_property = self.__dict__

    
    
    def process_transaction(self):
        if self.live():
            handler = LiveOrderConnector(**self.order_property)
        else:
            handler = SimulationOrderConnector(**self.order_property)
        
        handler.run()

    
