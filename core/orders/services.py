from .models import OrderPosition


def is_portfolio_exist(ticker,bot_id,user_id):
    portfolios = OrderPosition.objects.filter(user_id=user_id,ticker=ticker,bot_id=bot_id,is_live=True).prefetch_related('ticker')
    if portfolios.exists():
        portfolio = portfolios.latest('created')
        return portfolio
    return None


# TODO: FUNGSI WRAPPERNYA BUAT DISINI
# example


# def sell_position_service(price,trading_day,position_uid):
#     position  = OrderPosition.objects.get(position_uid=position_uid)
#     if position.bot.is_ucdc():
#         sellclassic()
#     elif position.bot.is_uno():
#         selluno()
#     elif position.bot.is_classic():
#         sellclassic()
#     elif position.bot.is_stock():
#         sellstock()