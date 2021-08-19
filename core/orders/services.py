from .models import OrderPosition
from portfolio.daily_hedge_classic import classic_sell_position
from portfolio.daily_hedge_ucdc import ucdc_sell_position
from portfolio.daily_hedge_uno import uno_sell_position
from portfolio.daily_hedge_user import user_sell_position

def is_portfolio_exist(ticker,bot_id,user_id):
    portfolios = OrderPosition.objects.filter(user_id=user_id,ticker=ticker,bot_id=bot_id,is_live=True).prefetch_related('ticker')
    if portfolios.exists():
        portfolio = portfolios.latest('created')
        return portfolio
    return None

# FIXME: return order
def sell_position_service(price, trading_day, position_uid):
    position  = OrderPosition.objects.get(position_uid=position_uid)
    if position.bot.is_ucdc():
       positions,order= ucdc_sell_position(price, trading_day, position_uid)
    elif position.bot.is_uno():
        positions,order=uno_sell_position(price, trading_day, position_uid)
    elif position.bot.is_classic():
        positions,order=classic_sell_position(price, trading_day, position_uid)
    elif position.bot.is_stock():
        positions,order=user_sell_position(price, trading_day, position_uid)
    return order