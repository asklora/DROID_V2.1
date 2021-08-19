from core.bot.models import BotOptionType
from core.user.models import TransactionHistory
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from abc import ABC,abstractmethod
from core.djangomodule.general import formatdigit
from core.Clients.models import UserClient
from django.db import transaction as db_transaction
from datasource.rkd import RkdData
from portfolio import classic_sell_position

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
        self.is_decimal = self.instance.ticker.currency_code.is_decimal
    
    def run(self):
        """
        in here we delete the long if else statement

        will trigger the function inside this class with prefix name and invoke
        """
        func_name =f'on_{self.instance.side}_{self.instance.status}'
        
        if hasattr(self,func_name):
            """
            only invoke this function.
            hasattr will check the function name is exist or not 

           - on_buy_placed
           - on_buy_pending
           - on_buy_filled
           - on_buy_cancel
           - on_sell_placed
           - on_sell_pending
           - on_sell_filled
           - on_sell_cancel

            we SKIP REVIEW STATUS because the handler is before save


            gettattr => make string function name into a function variable and ready to be invoked
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
                amount=formatdigit(self.instance.amount, self.is_decimal),
                transaction_detail={
                    "last_amount" : self.user_wallet_amount,
                    "description": "bot order",
                    # "position": f"{order.position_uid}",
                    "event": "create",
                    "order_uid": str(self.instance.order_uid)
                },
            )
        else:
            """Must be BOT buy here"""
            pass
    
    
    def on_buy_filled(self):
        if self.instance.is_init:
            """
            Can be BOT or USER
            """
            # create position and performance
            position,performance = self.create_position_perfomance()
        
        else:
            """
            Only Bot hedge behaviour
            """
            position,performance =self.update_position_performance()
        
        # create fee

        if self.instance.order_type != 'apps':
            self.create_fee(position.position_uid)
            
        # update last pending transaction 
        self.update_initial_transaction_position(position.position_uid)

        # update to connect performance and prevent triger signal
        Order.objects.filter(pk=self.instance.order_uid).update(performance_uid=performance.performance_uid)
    
    def on_buy_cancel(self):
        if self.instance.is_init and self.instance.status == 'pending':
            trans = TransactionHistory.objects.filter(
                side='debit', transaction_detail__description='bot order', transaction_detail__order_uid=str(self.instance.order_uid))
            if trans.exists():
                trans.delete()
    
    
    def on_sell_pending(self):
        """
        STILL DO NOTHING
        
        are we gonna to take position bot_cash_balance / real position values here before move to wallet?
        problems:
            we dont store current values of the position as a balance position,
            current_values should not change in position when sell order is pending
            surely not bot cash balance we take/hold in here
        """
        pass
    
    
    
    
    
    def on_sell_cancel(self):
        pass
    

    def transfer_to_wallet(self,position:OrderPosition):

        amt = position.investment_amount + position.final_pnl_amount
        return_amt = amt + position.bot_cash_dividend
        TransactionHistory.objects.create(
            balance_uid=self.user_wallet,
            side="credit",
            amount=formatdigit(return_amt,self.is_decimal) ,
            transaction_detail={
                "last_amount" : self.user_wallet_amount,
                "description": "bot return",
                "position": f"{position.position_uid}",
                "event": "return",
                "order_uid": str(self.instance.order_uid)
            },
        )


    def update_position_performance(self):
        
        """
        Only Bot hedge behaviour and sell
        UPDATE the position and CREATE performance
        """
        
        position = OrderPosition.objects.get(
                        position_uid=self.instance.setup["position"]["position_uid"])
        key_list = list(self.instance.setup["position"])

        # UPDATE the position
        for key in key_list:
            if not key in ["created", "updated", "share_num"]:
                if hasattr(position, key):
                    field = position._meta.get_field(key)
                    if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                        raw_key = f"{key}_id"
                        setattr(
                            position, raw_key, self.instance.setup["position"].pop(key))
                    else:
                        setattr(position, key,
                                self.instance.setup["position"].pop(key))
        
        # remove field position_uid, cause it will double
        self.instance.setup["performance"].pop("position_uid")
        
        # CREATE performance from hedge setup
        performance = PositionPerformance.objects.create(
            position_uid=position,  # replace with self.instance
            # the rest is value from setup
            **self.instance.setup["performance"]
        )

        performance.order_uid = self.instance
        position.save()
        performance.save()

        
        return position,performance 
    
    def create_position_perfomance(self):
        """
        execute Every init order
        BOT / USER
        CREATE position and CREATE performance
        """
        margin=1
        position = OrderPosition.objects.create(
                    user_id=self.instance.user_id,
                    ticker=self.instance.ticker,
                    bot_id=self.instance.bot_id,
                    spot_date=self.instance.filled_at.date(),
                    entry_price=self.instance.price,
                    is_live=True,
                    margin=margin
                )
        performance = PositionPerformance.objects.create(
            created=self.instance.filled_at.date(),
            position_uid=position,
            last_spot_price=self.instance.price,
            last_live_price=self.instance.price,
            order_uid=self.instance,
            status='Populate'
        )
        
        # if bot
        if not self.bot.is_stock():

            for key, val in self.instance.setup.items():
                if hasattr(performance, key):
                    setattr(performance, key, val)
                if hasattr(position, key):
                    if key == "share_num":
                        continue
                    else:
                        setattr(position, key, val)
                else:
                    if key == "total_bot_share_num":
                        setattr(position, "share_num", val)
        else:
            # without bot
            position.investment_amount = self.instance.amount
            position.bot_cash_balance = 0
            position.share_num = self.instance.qty
            performance.share_num = self.instance.qty
            # start creating position
        performance.current_pnl_amt = 0  # need to calculate with fee
        performance.current_bot_cash_balance = position.bot_cash_balance

        performance.current_investment_amount = formatdigit(
            performance.last_live_price * performance.share_num, self.is_decimal)
        performance.current_pnl_ret = (performance.current_bot_cash_balance + performance.current_investment_amount -
                                position.investment_amount) / position.investment_amount
        performance.order_id = self.instance

        performance.save()
        position.save()
        
        return position,performance
    
    
    
    
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
        return formatdigit(commissions_fee, self.is_decimal), formatdigit(stamp_duty_fee, self.is_decimal), formatdigit(total_fee, self.is_decimal)



    def create_fee(self,  position_uid):
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
    
    # TODO: #51 INTERACTIVE BROKER ORDER REQUEST
    def on_buy_placed(self):
        print('sent buy order to broker')
        
    def on_sell_placed(self):
        print('sent sell order to broker')

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

    def on_sell_placed(self):
        position_uid = self.instance.setup.get('position',{}).get('position_uid',None)
        if not position_uid:
            raise Exception('position_uid not found')
    
    def on_sell_filled(self):
        """
        if sell is successfully filled. we will unpack the setup
        then update the position and creating performances
        
        - Only if position stop live we will transfer the money back to wallet
        """
        # TODO: #49 Please Check the logic here

        # update position and create performance from setup 
        position,performance = self.update_position_performance()
        
        if not position.is_live:
            # transfer to wallet
            self.transfer_to_wallet(position)
            if self.instance.order_type == 'apps':
                self.create_fee(position.position_uid)
        
        
    






class OrderServices:
    
    
    
    def __init__(self,instance:Order):
        self.instance = instance
        self.user_wallet = instance.user_id.wallet
        self.user_wallet_amount = instance.user_id.balance
        self.user_wallet_currency = instance.user_id.currency
        self.bot = BotOptionType.objects.get(bot_id=instance.bot_id)
        self.order_property = self.__dict__

    
    
    def process_transaction(self):
        if self.instance.order_type == 'live':
            handler = LiveOrderConnector(**self.order_property)
        else:
            handler = SimulationOrderConnector(**self.order_property)
        
        handler.run()

    
