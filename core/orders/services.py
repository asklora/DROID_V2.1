from .models import OrderPosition
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
    )

def is_portfolio_exist(ticker,bot_id,user_id):
    portfolios = OrderPosition.objects.filter(user_id=user_id,ticker=ticker,bot_id=bot_id,is_live=True).prefetch_related('ticker')
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