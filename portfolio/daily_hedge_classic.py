from datetime import datetime
from core.master.models import LatestPrice, MasterTac
from core.orders.models import OrderPosition, PositionPerformance
# from core.classic_droid.celery import app

def final(price_data, position, latest_price=False, force_sell=False):
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid).latest("created")
    if(latest_price):
        today = price_data.last_date
        live_price = price_data.close
        high = price_data.high
        low = price_data.low
    else:
        today = price_data.trading_day
        live_price = price_data.tri_adj_close
        high = price_data.tri_adj_high
        low = price_data.tri_adj_low

    if high == 0 or high == None:
        high = live_price
    if low == 0 or low == None:
        low = live_price

    status_expiry = high > position.target_profit_price or low < position.max_loss_price or today >= position.expiry
    if(force_sell):
        status_expiry = True

    if status_expiry:
        current_investment_amount=live_price * position.share_num
        current_pnl_amt = performance.current_pnl_amt + ((live_price - performance.last_live_price) * performance.share_num)
        # current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_price = live_price
        position.current_inv_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_return = position.current_inv_ret
        position.current_inv_amt = current_investment_amount
        position.final_pnl_amount =  current_pnl_amt
        position.event_date = today
        position.is_live = False
        if high > position.target_profit_price:
            position.event = "Targeted Profit"
        elif low < position.max_loss_price:
            position.event = "Maximum Loss"
        elif today >= position.expiry or force_sell:
            if live_price < position.entry_price:
                position.event = "Loss"
            elif live_price > position.entry_price:
                position.event = "Profit"
            else:
                position.event = "Bot Expired"
        position.save()
        return True
    else:
        return False

def create_performance(price_data, position, latest_price=False):
    bot = position.bot
    
    if(latest_price):
        live_price = price_data.close
        trading_day = price_data.last_date
    else:
        live_price = price_data.tri_adj_close
        trading_day = price_data.trading_day

    try:
        last_performance = PositionPerformance.objects.filter(position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False

    if last_performance:
        share_num = last_performance.share_num
        current_pnl_amt = last_performance.current_pnl_amt + (live_price - last_performance.last_live_price ) * last_performance.share_num
    else:
        live_price = position.entry_price
        current_pnl_amt = 0
        share_num = position.share_num
    current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
    current_investment_amount = live_price * position.share_num
    digits = max(min(5-len(str(int(position.entry_price))),2),-1)
    performance =PositionPerformance.objects.create(
        position_uid=position,
        share_num=share_num,
        last_live_price=round(live_price,2),
        last_spot_price=position.entry_price,
        current_pnl_ret=round(current_pnl_ret,4),
        current_pnl_amt=round(current_pnl_amt,digits),
        current_investment_amount=round(current_investment_amount,2),
        current_bot_cash_balance=  round(position.bot_cash_balance,2),
        updated= trading_day,
        created= trading_day,
        last_hedge_delta = 100
        # order_summary
        # position_uid
        # order_uid
        # performance_uid
    )
    performance.save()
                  
def classic_position_check(position_uid):
    try:
        position = OrderPosition.objects.get(position_uid=position_uid, is_live=True)
        try:
            performance = PositionPerformance.objects.filter(position_uid=position.position_uid).latest("created")
            trading_day = performance.created
        except PositionPerformance.DoesNotExist:
            performance = False
            trading_day = position.spot_date
        tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__gt=trading_day).order_by("trading_day")
        lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
        status = False
        for tac in tac_data:
            trading_day = tac.trading_day
            print(f"{trading_day} done")
            create_performance(tac, position)
            position.save()
            status = final(tac, position)
            if status :
                print(f"position end")
                break
        if(not status and trading_day.date() < lastest_price_data.last_date and position.expiry >= lastest_price_data.last_date):
            trading_day = lastest_price_data.last_date
            print(f"latest price {trading_day} done")
            create_performance(lastest_price_data, position, latest_price=True)
            position.save()
            status = final(lastest_price_data, position, latest_price=True)
            if status :
                print(f"position end")
        
        tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__gte=position.expiry).order_by("trading_day")
        if(not status and tac_data.count() > 0):
            tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__lte=position.expiry).latest("-trading_day")
            print(f"force {tac_data.trading_day} done")
            status = final(tac_data, position, force_sell=True)
            if status :
                print(f"position end")
        return True
    except OrderPosition.DoesNotExist:
        return False