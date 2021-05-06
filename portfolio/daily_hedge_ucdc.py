from datetime import datetime
from general.date_process import datetimeNow
import sys
import math
from bot import uno
from core.master.models import LatestPrice, MasterTac, MasterOhlcvtr
from core.orders.models import OrderPosition, PositionPerformance, Order
from bot.calculate_bot import (
    get_hedge_detail,
    get_option_price_ucdc,
    get_trq, get_strike_barrier,
    get_ucdc_hedge,
    get_v1_v2,
    get_vol)
from config.celery import app
import pandas as pd
from core.djangomodule.serializers import OrderPositionSerializer


def create_performance(price_data, position, latest_price=False):
    # new access bot reference
    bot = position.bot
    expiry = position.expiry
    # ============ prepare to be replaced with third party latest price ========
    if(latest_price):
        live_price = price_data.close
        trading_day = price_data.last_date
        bid_price = price_data.intraday_bid
        ask_price = price_data.intraday_ask
    else:
        live_price = price_data.close
        trading_day = price_data.trading_day
        bid_price = price_data.close
        ask_price = price_data.close

    status_expiry = trading_day >= position.expiry

    try:
        last_performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False

    t, r, q = get_trq(position.ticker, expiry, trading_day,
                      position.ticker.currency_code)
    if last_performance:
        currency_code = str(position.ticker.currency_code)
        strike = last_performance.strike
        strike_2 = last_performance.strike_2
        option_price = last_performance.option_price
        current_pnl_amt = last_performance.current_pnl_amt + \
            (live_price - last_performance.last_live_price) * \
            last_performance.share_num

        v1, v2 = get_v1_v2(position.ticker, live_price,
                           trading_day, t, r, q, strike, strike_2)
        if(status_expiry):
            delta = last_performance.last_hedge_delta
            hedge = False
            share_num = 0
            hedge_shares = last_performance.share_num * -1
            status = "sell"
        else:
            delta = uno.deltaRC(live_price, strike,
                                strike_2, t/365, r, q, v1, v2)
            delta, hedge = get_ucdc_hedge(
                currency_code, delta, last_performance.last_hedge_delta)
            share_num, hedge_shares, status, hedge_price = get_hedge_detail(
                ask_price, bid_price, last_performance.share_num, position.share_num, delta, last_performance.last_hedge_delta,
                hedge=hedge, ucdc=True, margin=position.user_id.is_large_margin)
        bot_cash_balance = last_performance.current_bot_cash_balance + \
            ((last_performance.share_num - share_num) * live_price)
    else:
        current_pnl_amt = 0  # initial value
        vol = get_vol(position.ticker, trading_day, t, r,
                      q, bot.bot_type.bot_horizon_month)
        strike, strike_2 = get_strike_barrier(
            live_price, vol, bot.option_type, bot.bot_type.bot_group)
        v1, v2 = get_v1_v2(position.ticker, live_price,
                           trading_day, t, r, q, strike, strike_2)
        option_price = get_option_price_ucdc(
            live_price, strike, strike_2, t, r, q, v1, v2)
        delta = uno.deltaRC(live_price, strike, strike_2, t/365, r, q, v1, v2)
        if(position.user_id.is_large_margin):
            share_num = position.share_num * 1.5
        else:
            share_num = position.share_num
        share_num = math.floor(delta * share_num)
        bot_cash_balance = position.investment_amount - \
            (share_num * live_price)

    current_investment_amount = live_price * share_num
    current_pnl_ret = (bot_cash_balance + current_investment_amount -
                       position.investment_amount) / position.investment_amount
    position.bot_cash_balance = round(bot_cash_balance, 2)
    digits = max(min(5 - len(str(int(position.entry_price))), 2), -1)
    log_time = pd.Timestamp(trading_day)
    if log_time == datetime.now():
        log_time = datetime.now()
    # not creating performance first, value stored at dict and placed in setup order we can use it later after the order filled
    # see below
    performance = dict(
        position_uid=str(position.position_uid),
        share_num=share_num,
        last_live_price=round(live_price, 2),
        last_spot_price=position.entry_price,
        current_pnl_ret=round(current_pnl_ret, 4),
        current_pnl_amt=round(current_pnl_amt, digits),
        current_investment_amount=round(current_investment_amount, 2),
        current_bot_cash_balance=round(bot_cash_balance, 2),
        updated=str(log_time),
        created=str(log_time),
        last_hedge_delta=delta,
        v1=v1,
        v2=v2,
        r=r,
        q=q,
        strike=strike,
        strike_2=strike_2,
        option_price=option_price
    )

    if status_expiry:
        position.final_price = live_price
        position.current_inv_ret = performance["current_pnl_ret"]
        position.final_return = position.current_inv_ret
        position.final_pnl_amount = performance["current_pnl_amt"]
        position.current_inv_amt = live_price * performance["share_num"]
        position.event_date = trading_day
        position.is_live = False
        if trading_day >= position.expiry:
            position.event = "Bot Expired"
        # this need to save
        position.save()

    # serializing -> make dictionary position instance
    position_val = OrderPositionSerializer(position).data
    # remove created and updated from position
    [position_val.pop(key) for key in ["created", "updated"]]

    # merge two dict, and save to order setup
    setup = {"performance": performance, "position": position_val}
    if not status == "hold":
        if hedge_shares < 0:
            hedge_shares = hedge_shares * -1  # make it positive in order

        order = Order.objects.create(
            is_init=False,
            ticker_id=position_val["ticker"],
            created=log_time,
            updated=log_time,
            price=live_price,
            bot_id=bot.bot_id,
            amount=(hedge_shares*live_price),
            user_id_id=position_val["user_id"],
            side=status,
            qty=hedge_shares,
            setup=setup
        )
        # only for bot
        if order:
            order.placed = True
            order.placed_at = log_time
            order.save()
        # go to core/orders/signal.py Line 54 and 112
        # this will wait until order filled then creating performance along with it
        if(status_expiry):
            return True, order.order_uid
        else:
            return False, order.order_uid

    # otherwise just create a record below

    # remove position_uid from dict and swap with instance
    performance.pop("position_uid")
    position.save()
    # create the record
    PositionPerformance.objects.create(
        position_uid=position,  # swapped with instance
        **performance  # the dict value
    )
    if(status_expiry):
        return True, None
    else:
        return False, None


@app.task
def ucdc_position_check(position_uid):
    try:
        position = OrderPosition.objects.get(
            position_uid=position_uid, is_live=True)
        try:
            performance = PositionPerformance.objects.filter(
                position_uid=position.position_uid).latest("created")
            trading_day = performance.created
        except PositionPerformance.DoesNotExist:
            performance = False
            trading_day = position.spot_date
        tac_data = MasterOhlcvtr.objects.filter(
            ticker=position.ticker, trading_day__gt=trading_day, trading_day__lte=position.expiry, day_status='trading_day').order_by("trading_day")
        status = False
        for tac in tac_data:
            trading_day = tac.trading_day
            print(f"tac {trading_day} done")
            status, order_id = create_performance(tac, position)
            # position.save()
            if order_id:
                order = Order.objects.get(order_uid=order_id)
                log_time = pd.Timestamp(trading_day)
                if order.status in ["pending", "review"]:
                    order.status = "filled"
                    order.filled_at = log_time
                    order.save()
            if status:
                print(f"position end tac")
                break
        if(type(trading_day) == datetime):
            trading_day = trading_day.date()
        lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
        if(not status and trading_day < lastest_price_data.last_date and position.expiry >= lastest_price_data.last_date):
            trading_day = lastest_price_data.last_date
            print(f"latest price {trading_day} done")
            status, order_id = create_performance(
                lastest_price_data, position, latest_price=True)
            # position.save()
            if order_id:
                order = Order.objects.get(order_uid=order_id)
                log_time = pd.Timestamp(trading_day)
                if order.status in ["pending", "review"]:
                    order.status = "filled"
                    order.filled_at = log_time
                    order.save()
            if status:
                print(f"position end not tac")
        try:
            tac_data = MasterOhlcvtr.objects.filter(
                ticker=position.ticker, trading_day__gte=position.expiry, day_status='trading_day').latest("trading_day")
            if(not status and tac_data):
                position.expiry = tac_data.trading_day
                position.save()
                print(f"force sell {tac_data.trading_day} done")
                status, order_id = create_performance(tac_data, position)
                if order_id:
                    order = Order.objects.get(order_uid=order_id)
                    log_time = pd.Timestamp(trading_day)
                    if order.status in ["pending", "review"]:
                        order.status = "filled"
                        order.filled_at = log_time
                        order.save()
                # position.save()
                if status:
                    print(f"position end moving expiry")
        except MasterOhlcvtr.DoesNotExist:
            status = False
        return True
    except OrderPosition.DoesNotExist:
        return False
