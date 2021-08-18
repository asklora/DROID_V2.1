from .models import OrderPosition


def is_portfolio_exist(ticker,bot_id,user_id):
    portfolios = OrderPosition.objects.filter(user_id=user_id,ticker=ticker,bot_id=bot_id,is_live=True).prefetch_related('ticker')
    if portfolios.exists():
        portfolio = portfolios.latest('created')
        return portfolio
    return None
