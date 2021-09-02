from .models import OrderPosition,Order
from rest_framework import exceptions
from core.bot.models import BotOptionType
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
    )
from typing import Union


class OrderPositionValidation:
    def __init__(self,ticker:str,bot_id:str,user_id:Union[str,int]):
        self.ticker =ticker
        self.bot_id =bot_id
        self.user_id =user_id
        bot_type = BotOptionType.objects.get(bot_id=bot_id).bot_type
        self.bots = [bot['bot_id'] for bot in BotOptionType.objects.filter(bot_type=bot_type).values('bot_id')]

    def is_order_exist(self):
        orders = Order.objects.filter(user_id=self.user_id,ticker=self.ticker,bot_id__in=self.bots,status='pending')
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
    
    def validate(self):
        valid_position = self.is_portfolio_exist()
        valid_order = self.is_order_exist()
        print(valid_position,valid_order)
        if valid_position or valid_order:
            
            if valid_position:
                return {"position":f"{valid_position.position_uid}"}
            elif valid_order :
                return {"position":f"{valid_order.order_uid}"}
        else:
            return None



def sell_position_service(price, trading_day, position_uid):
    position  = OrderPosition.objects.get(position_uid=position_uid)
    bot = position.bot
    if bot.is_ucdc():
       positions, order= ucdc_sell_position(price, trading_day, position_uid,apps=True)
    elif bot.is_uno():
        positions, order=uno_sell_position(price, trading_day, position_uid,apps=True)
    elif bot.is_classic():
        positions, order=classic_sell_position(price, trading_day, position_uid,apps=True)
    elif bot.is_stock():
        positions, order=user_sell_position(price, trading_day, position_uid, apps=True)
    return positions, order


def side_validation(validated_data):
    
    if validated_data["side"] == "sell":
        init = False
        position = validated_data.get("setup",{}).get("position",None)
        if not position:
            raise exceptions.NotAcceptable({"detail":"must provided the position uid for sell side"})
    else:
        validation=OrderPositionValidation(validated_data["ticker"],validated_data["bot_id"],validated_data["user_id"].id)
        init = True
        if validated_data["amount"] > validated_data["user_id"].user_balance.amount:
            raise exceptions.NotAcceptable({"detail": "insuficent balance"})
        if validated_data["amount"] / validated_data["price"] < 1:
            raise exceptions.NotAcceptable({"detail":"share should not below one"})
        if validated_data["amount"] <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})
        if validation.validate():
            raise exceptions.NotAcceptable({"detail": f"user already has position for {validated_data['ticker']} in current options"})

    return init