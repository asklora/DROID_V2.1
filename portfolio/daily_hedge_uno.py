import math
from bot import uno
from core.master.models import LatestPrice, MasterTac
from core.orders.models import OrderPosition, PositionPerformance
from bot.calculate_bot import (
    get_hedge_detail,
    get_trq, 
    get_strike_barrier,
    get_uno_hedge, 
    get_v1_v2, 
    get_vol)

def final(price_data, position, latest_price=False):
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid).latest("created")
    if(latest_price):
        today = price_data.last_date
        last_live_price = price_data.close
        high = price_data.high
    else:
        today = price_data.trading_day
        last_live_price = price_data.tri_adj_close
        high = price_data.tri_adj_high

    if high == 0 or high == None:
        high = last_live_price

    if high > position.target_profit_price or today >= position.expiry:
        current_investment_amount=last_live_price * performance.share_num
        current_pnl_amt = performance.current_pnl_amt + ((last_live_price - performance.last_live_price) * performance.share_num)
        # current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_price = last_live_price
        position.current_inv_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_return = position.current_inv_ret
        position.final_pnl_amount =  current_pnl_amt
        position.current_inv_amt = current_investment_amount
        position.event_date =today
        position.status = True
        if high > position.target_profit_price:
            position.event = "KO"
        elif today >= position.expiry:
            if current_pnl_amt < 0:
                position.event = "Loss"
            elif current_pnl_amt >= 0:
                position.event = "Profit"
            else:
                position.event = "Bot Expired"
        position.save()
        return True
    else:
        return False

def create_performance(price_data, position, latest_price=False):
    # new access bot reference
    bot = position.bot
    expiry = position.expiry.strftime("%Y-%m-%d")

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

    t, r, q = get_trq(position.symbol.symbol, expiry, trading_day, position.currency.currency_code)

    if last_performance:
        strike = last_performance.strike
        barrier = last_performance.barrier
        option_price = last_performance.option_price
        rebate = barrier - strike

        v1, v2 = get_v1_v2(position.symbol.symbol, live_price, trading_day, t, r, q, strike, barrier)
        delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t, r, q, v1, v2)
        # share_num = math.floor(delta * position.share_num)
        last_hedge_delta = get_uno_hedge(live_price, strike, delta, last_performance.last_hedge_delta)
        share_num, hedge_shares, status, hedge_price = get_hedge_detail(live_price, live_price, last_performance.share_num, delta, last_hedge_delta, hedge=(last_hedge_delta==delta), uno=True)

        bot_cash_balance = last_performance.current_bot_cash_balance + ((last_performance.share_num - share_num) * live_price)
        current_pnl_amt = last_performance.current_pnl_amt + (live_price - last_performance.last_live_price) * last_performance.share_num
    elif not last_performance:
        current_pnl_amt = 0 # initial value
        share_num = round((position.investment_amount / live_price), 1)
        bot_cash_balance = position.bot_cash_balance
        vol = get_vol(position.symbol.symbol, trading_day, t, r, q, bot.bot_type.bot_horizon_month)
        strike, barrier = get_strike_barrier(live_price, vol, bot.option_type, bot.bot_type.bot_group)
        rebate = barrier - strike
        v1, v2 = get_v1_v2(position.symbol.symbol, live_price, trading_day, t, r, q, strike, barrier)
        delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t, r, q, v1, v2)
        share_num = math.floor(delta * share_num)
        bot_cash_balance = position.investment_amount - (share_num * live_price)
        last_hedge_delta = delta
            
    current_pnl_ret = (current_pnl_amt + bot_cash_balance) / position.investment_amount
            #New Changes
    current_investment_amount = live_price * share_num
    position.bot_cash_balance = round(bot_cash_balance,2)
    position.share_num = round((position.investment_amount / live_price), 1)
    position.save()
    digits = max(min(5-len(str(int(position.entry_price))),2),-1)
    perf = PositionPerformance.objects.create(
        order=position,
        share_num=share_num,
        last_live_price=round(live_price,2),
        last_spot_price=position.entry_price,
        current_pnl_ret=round(current_pnl_ret,4),
        current_pnl_amt=round(current_pnl_amt,digits),
        current_investment_amount=round(current_investment_amount,2),
        current_bot_cash_balance=  round(bot_cash_balance,2),
        updated= trading_day,
        created= trading_day,
        last_hedge_delta = last_hedge_delta,
        v1 = v1,
        v2 = v2,
        r = r,
        q = q,
        strike = strike,
        barrier = barrier,
        delta = delta,
        option_price = option_price
        # order_summary,
        # position_uid,
        # order_uid,
        # performance_uid,
        )
    perf.save()

# @app.task
def uno_position_check(position_uid):
    try:
        position = OrderPosition.objects.get(position_uid=position_uid, is_live=True)
        try:
            performance = PositionPerformance.objects.filter(order_id=position.order_id).latest("created")
            trading_day = performance.created
        except PositionPerformance.DoesNotExist:
            performance = False
            trading_day = position.spot_date
        tac_data = MasterTac.objects.filter(symbol=position.symbol, trading_day__gt=trading_day).order_by("trading_day")
        lastest_price_data = LatestPrice.objects.get(symbol=position.symbol)
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
        if(not status and trading_day < lastest_price_data.last_date):
            trading_day = lastest_price_data.last_date
            print(f"{trading_day} done")
            create_performance(lastest_price_data, position, latest_price=True)
            position.save()
            status = final(lastest_price_data, position, latest_price=True)
            if status :
                print(f"position end")
        return True
    except OrderPosition.DoesNotExist:
        return False