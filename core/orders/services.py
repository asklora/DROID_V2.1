from .models import OrderPosition,Order
from rest_framework import exceptions
from core.bot.models import BotOptionType
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
    )
from typing import Union,Optional,Tuple
from datetime import datetime

class OrderPositionValidation:
    def __init__(self,ticker:str,bot_id:Union[str,list,None],user_id:Union[str,int]):
        self.ticker =ticker
        self.bot_id =bot_id
        self.user_id =user_id
        if isinstance(bot_id,str):
            bot_type = BotOptionType.objects.get(bot_id=bot_id).bot_type
            self.bots = [bot['bot_id'] for bot in BotOptionType.objects.filter(bot_type=bot_type).values('bot_id')]
        elif isinstance(bot_id,list):
            self.bots= self.bot_id+["STOCK_stock_0"]
            

    def allowed_bot(self):
        
        portfolios = OrderPosition.objects.filter(user_id=self.user_id,ticker=self.ticker,is_live=True).prefetch_related('ticker').distinct('bot_id')
        orders = Order.objects.filter(user_id=self.user_id,ticker=self.ticker,status='pending',side='buy').distinct('bot_id')
        bot_portfolios = [bot.bot_id for bot in portfolios]
        bot_orders = [bot.bot_id for bot in orders]
        used_bot = bot_portfolios+bot_orders
        used_bot_type = BotOptionType.objects.filter(bot_id__in=used_bot).distinct('bot_type').values('bot_type')
        unused_bot_list = [bot['bot_id']for bot in BotOptionType.objects.exclude(bot_type__in=used_bot_type).values('bot_id')]
        response =[]
        for unused in self.bots:
            data ={}
            if  unused in unused_bot_list:
                data[unused] = True
            else:
                data[unused]=False
            response.append(data)
        return {"allowed_bot":response}


    def is_order_exist(self,side):
        orders = Order.objects.filter(user_id=self.user_id,ticker=self.ticker,bot_id__in=self.bots,status='pending',side=side)
        if orders.exists():
            try:
                order = orders.latest('created')
            except OrderPosition.MultipleObjectsReturned:
                order = orders[0]
            return order
        return None

        

    def is_portfolio_exist(self):
        portfolios = OrderPosition.objects.filter(user_id=self.user_id,ticker=self.ticker,bot_id__in=self.bots,is_live=True).prefetch_related('ticker')
        if portfolios.exists():
            try:
                portfolio = portfolios.latest('created')
            except OrderPosition.MultipleObjectsReturned:
                portfolio = portfolios[0]

            return portfolio
        return None
    
    def validate_buy(self):
        valid_position = self.is_portfolio_exist()
        valid_order = self.is_order_exist('buy')
        if valid_position or valid_order:
            
            if valid_position:
                return {"position":f"{valid_position.position_uid}"}
            elif valid_order :
                return {"position":f"{valid_order.order_uid}"}
        else:
            return None
    
    def validate_sell(self):
        has_order = self.is_order_exist('sell')
        if has_order:
            return {"order_uid":f"{has_order.order_uid}"}
        else:
            return None



def sell_position_service(price:float, trading_day:datetime, position_uid:str)->Tuple[OrderPosition,Optional[Union[Order,None]]]:
    position  = OrderPosition.objects.select_related('ticker').get(position_uid=position_uid)
    if not position.is_live:
        raise exceptions.NotAcceptable(f"position, has been closed")
    bot = position.bot
    if bot.is_ucdc():
       positions, order= ucdc_sell_position(price, trading_day, position,apps=True)
    elif bot.is_uno():
        positions, order=uno_sell_position(price, trading_day, position,apps=True)
    elif bot.is_classic():
        positions, order=classic_sell_position(price, trading_day, position,apps=True)
    elif bot.is_stock():
        positions, order=user_sell_position(price, trading_day, position, apps=True)
    pending_order = Order.objects.prefetch_related("ticker").filter(
        user_id=order.user_id,
        status='pending',
        bot_id=order.bot_id,
        ticker=order.ticker
        )
    if pending_order.exists():
        last_order = pending_order.first()
        orderId = last_order.order_uid.hex
        order.delete()
        raise exceptions.NotAcceptable(f"sell order already exists for this position, order id : {orderId}, current status pending")
    return positions, order


def side_validation(validated_data):
    
    
    
    
    if validated_data["side"] == "sell":
        init = False
        position = validated_data.get("setup",{}).get("position",None)
        if not position:
            raise exceptions.NotAcceptable({"detail":"must provided the position uid for sell side"})
    else:
        
        validation=OrderPositionValidation(validated_data["ticker"],validated_data.get("bot_id",None),validated_data["user_id"].id)
        init = True
        if validated_data["amount"] <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})
        #TODO currency convert here
        if validated_data["amount"] > validated_data["user_id"].user_balance.amount:
            raise exceptions.NotAcceptable({"detail": "insufficient funds"})
        if validated_data["amount"] / validated_data["price"] < 1:
            raise exceptions.NotAcceptable({"detail":"share should not below one"})
        if validation.validate_buy():
            raise exceptions.NotAcceptable({"detail": f"user already has position for {validated_data['ticker']} in current options"})

    return init