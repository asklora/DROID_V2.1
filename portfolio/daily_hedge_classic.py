from datetime import datetime,timezone,time,timedelta,date
from core.universe.models import Currency
from core.master.models import LatestPrice, MasterTac
from core.orders.models import Order, OrderPosition, PositionPerformance
from bot.calculate_bot import get_spot_date
# from core.classic_droid.celery import app

def final(price_data, position):
    performance = PositionPerformance.objects.filter(position_id=position.position_id).latest("timestamp")
    today = price_data.trading_day
    last_live_price = price_data.tri_adj_close

    high = price_data.tri_adj_high
    if high == 0 or high == None:
        high = last_live_price
    
    low = price_data.tri_adj_low
    if low == 0 or low == None:
        low = last_live_price

    if high > position.target_profit_price or low < position.max_loss_price or today >= position.expiry:
        current_investment_amount=last_live_price * position.share_num
        current_pnl_amt = performance.current_pnl_amt + ((last_live_price - performance.last_live_price) * performance.share_num)
        current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_price = last_live_price
        position.current_inv_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_return = position.current_inv_ret
        position.current_inv_amt = current_investment_amount
        position.final_pnl_amount =  current_pnl_amt
        position.event_date = today
        position.status = True
        if high > position.target_profit_price:
            position.event = "Targeted Profit"
        elif low < position.max_loss_price:
            position.event = "Maximum Loss"
        elif today >= position.expiry:
            if last_live_price < position.entry_price:
                position.event = "Loss"
            elif last_live_price > position.entry_price:
                position.event = "Profit"
            else:
                position.event = "Bot Expired"
        position.save()
        return True
    else:
        return False

def performance(master, position):
    latest_price = master
    last_live_price = latest_price.tri_adjusted_price
    
    try:
        last_perf = PositionPerformance.objects.filter(order_id=position.order_id).latest("timestamp")
    except PositionPerformance.DoesNotExist:
        last_perf = False
    #New Changes
    
    if last_perf:
        share_num = last_perf.share_num
        current_pnl_amt = last_perf.current_pnl_amt + (last_live_price - last_perf.last_live_price ) * last_perf.share_num
    elif not last_perf:
        last_live_price = position.entry_price
        current_pnl_amt = 0 # initial value
        share_num = position.share_num
    current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
    #New Changes
    current_investment_amount = last_live_price * position.share_num
    digits = max(min(5-len(str(int(position.entry_price))),2),-1)
    start_date = (latest_price.trading_day).strftime("%Y-%m-%d")
    timestamps = datetime.strptime(start_date, "%Y-%m-%d")
    perf =PositionPerformance.objects.create(
        order=position,
        share_num=share_num,
        last_live_price=round(last_live_price,2),
        last_spot_price=position.entry_price,
        current_pnl_ret=round(current_pnl_ret,4),
        current_pnl_amt=round(current_pnl_amt,digits),
        current_investment_amount=round(current_investment_amount,2),
        current_bot_cash_balance=  round(position.bot_cash_balance,2),
        timestamp=timestamps,
        updated= latest_price.trading_day,
        created= latest_price.trading_day,
        last_hedge_delta = 100
    )
    perf.save()
                  
def classic_position_check(position_id, history=False):
    try:
        position = OrderPosition.objects.get(position_id=position_id, is_live=True)
        performance_data = PositionPerformance.objects.filter(position_id=position_id)
        trading_day = get_spot_date(position.spot_date, position.stock_selected).strftime("%Y-%m-%d")
        tac_data = MasterTac.objects.filter(ticker=position.stock_selected, trading_day__gte=trading_day).order_by("trading_day")
        lastest_price_data = LatestPrice.objects.get(ticker=position.stock_selected)
        for tac in tac_data:
            print(f"{tac.trading_day} done")
            performance(tac, position)
            position.save()
            status = final(tac, position)
            if status :
                print(f"position end")
                break
        return True
    except OrderPosition.DoesNotExist:
        return False

