from datetime import datetime
from django.core.management.base import BaseCommand
from core.services import *
from core.orders.models import Order,OrderPosition,PositionPerformance
from core.master.models import LatestPrice
from bot.calculate_bot import get_ucdc_detail,get_uno_detail,get_classic


class Command(BaseCommand):
    
    def handle(self, *args, **options):
        orders = OrderPosition.objects.filter(is_live=True)
        for order in orders:
            bot = order.bot
            price = LatestPrice.objects.get(ticker=order.symbol)
            today = datetime.now().date()
            intraday_price = (price.intraday_ask + price.intraday_bid) * 0.5
            try:
                prev_performance = PositionPerformance.objects.filter(position=order).latest('created')
            except PositionPerformance.DoesNotExist:
                prev_performance = False
            # create error log here
            if prev_performance:
                if bot.is_uno():
                    detail = get_uno_detail(order.symbol.ticker,order.symbol.currency_code,
                                order.expiry,today,
                                    bot.time_to_exp,
                                intraday_price,bot.bot_option_type,bot.bot_type.bot_type)
                    bot_cash_balance = prev_performance.current_bot_cash_balance + ((prev_performance.share_num - order.share_num) * intraday_price)
                    current_pnl_amt = prev_performance.current_pnl_amt + (intraday_price - prev_performance.last_live_price) * prev_performance.share_num
                elif order.bot.is_ucdc():
                    detail = get_ucdc_detail(order.symbol.ticker,order.symbol.currency_code,
                                             order.expiry,today,bot.time_to_exp,intraday_price,
                                             bot.bot_option_type,bot.bot_type.bot_type)
                elif order.bot.is_classic():
                    detail =''
                    pass
                
                print(detail)
            