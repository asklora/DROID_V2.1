from .models import OrderPosition
from rest_framework import exceptions
from core.bot.models import BotOptionType
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
    )

def is_portfolio_exist(ticker,bot_id,user_id):
    bot_type = BotOptionType.objects.get(bot_id=bot_id).bot_type
    bots = [bot['bot_id'] for bot in BotOptionType.objects.filter(bot_type=bot_type).values('bot_id')]
    portfolios = OrderPosition.objects.filter(user_id=user_id,ticker=ticker,bot_id__in=bots,is_live=True).prefetch_related('ticker')
    if portfolios.exists():
        portfolio = portfolios.latest('created')
        return portfolio
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
        init = True
        if validated_data["amount"] > validated_data["user_id"].user_balance.amount:
            raise exceptions.NotAcceptable({"detail": "insuficent balance"})
        if validated_data["amount"] / validated_data["price"] < 1:
            raise exceptions.NotAcceptable({"detail":"share should not below one"})
        if validated_data["amount"] <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})
        if is_portfolio_exist(validated_data["ticker"],validated_data["bot_id"],validated_data["user_id"].id):
            raise exceptions.NotAcceptable({"detail": f"user already has position for {validated_data['ticker']} in current options"})

    return init