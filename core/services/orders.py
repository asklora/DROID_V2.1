# from core.orders.models import OrderHistory,OrderPositon
# from core.master.models import LatestPrice
# from core.bot.models import BotOptionType
# from bot.calculate_bot import *
# from core import services


# @services.celery_app.task
# def classic_growth(position_id):
#     position = OrderPositon.objects.get(uid=position_id)
#     try:
#         latest_price = LatestPrice.objects.get(ticker=position.stock_selected)
#         last_live_price = latest_price.close

#         try:
#             last_perf = OrderHistory.objects.get(order_id=position.order_id,init=True)
#         except OrderHistory.DoesNotExist:
#             last_perf = False
#         # New Changes
        
#         if last_perf:
#             share_num = last_perf.share_num
#             current_pnl_amt = last_perf.current_pnl_amt + \
#                 (last_live_price - last_perf.last_live_price) * \
#                 last_perf.share_num
#             side = 'buy'
#         elif not last_perf:
#             last_live_price = position.entry_price
#             current_pnl_amt = 0  # initial value
#             share_num = position.share_num
#             current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
#             side = 'hold'
#         # New Changes
#         current_investment_amount = last_live_price * position.share_num
#         digits = max(min(5-len(str(int(position.entry_price))), 2), -1)
#         OrderHistory.objects.create(
#             position=position,
#             share_num=share_num,
#             last_live_price=round(last_live_price, 2),
#             last_spot_price=position.entry_price,
#             current_pnl_ret=round(current_pnl_ret, 4),
#             current_pnl_amt=round(current_pnl_amt, digits),
#             current_investment_amount=round(current_investment_amount, 2),
#             current_bot_cash_balance=position.bot_cash_balance,
#             timestamp=datetime.now(),
#             updated=datetime.today().strftime('%Y-%m-%d'),
#             last_hedge_delta = 100,
#             entry_price=None,
#             side=side,
#             filled_avg_price=None,
#             status=None,
#             filled_at=None,
#             canceled_at=None,
#             amount=None,
#             option_price=None,
#             strike=None,
#             barrier=None,
#             r=None,
#             q=None,
#             v1=None,
#             v2=None,
#             delta=None,
#             strike_2=None,
#         )
#         position.save()
#         timestamp = datetime.now()       
#     except LatestPrice.DoesNotExist:
#         timestamp = datetime.now()
#         data = {'message': f'Fail check for classic {position.stock_selected.index.index}',
#                 'time': f'{timestamp}'}




# @services.celery_app.task
# def order_daily_growth():
#     positions = OrderPositon.objects.filter(is_live=True,use_bot=True)
#     for position in positions:
#         bot = BotOptionType.objects.get(bot_id=position.bot_id)
#         if bot.bot_type.bot_type == 'CLASSIC':
#             classic_growth(position.uid)
#         elif bot.bot_type.bot_type == 'UNO':
#             classic_growth(position.uid)
#         elif bot.bot_type.bot_type == 'UCDC':
#             classic_growth(position.uid)