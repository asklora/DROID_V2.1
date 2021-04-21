from datetime import datetime
from general.date_process import datetimeNow
import sys
import math
from bot import uno
from core.master.models import LatestPrice, MasterTac
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


def final(price_data, position, latest_price=False, force_sell=False):
    if(latest_price):
        today = price_data.last_date
        live_price = price_data.close
    else:
        today = price_data.trading_day
        live_price = price_data.tri_adj_close

    status_expiry = today >= position.expiry
    if(force_sell):
        status_expiry = True
    # try catch klo error
    performance = PositionPerformance.objects.filter(
        position_uid=position.position_uid).latest("created")
    if status_expiry:
        current_investment_amount = live_price * performance.share_num
        current_pnl_amt = performance.current_pnl_amt + \
            ((live_price - performance.last_live_price) * performance.share_num)
        # current_pnl_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_price = live_price
        position.current_inv_ret = (
            current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_return = position.current_inv_ret
        position.final_pnl_amount = current_pnl_amt
        position.current_inv_amt = current_investment_amount
        position.event_date = today
        position.is_live = False
        if today >= position.expiry or force_sell:
            position.event = "Bot Expired"
        position.save()
        # must add sell order
        return True
    else:
        return False


def create_performance(price_data, position, latest_price=False):
    # new access bot reference
    bot = position.bot
    expiry = position.expiry

    if(latest_price):
        live_price = price_data.close
        trading_day = price_data.last_date
        bid_price = price_data.intraday_bid
        ask_price = price_data.intraday_ask
    else:
        live_price = price_data.tri_adj_close
        trading_day = price_data.trading_day
        bid_price = price_data.tri_adj_close
        ask_price = price_data.tri_adj_close
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
        if(trading_day >= position.expiry):
            delta =last_performance.last_hedge_delta
            last_hedge_delta = last_performance.last_hedge_delta
            hedge = False
            share_num, hedge_shares, status, hedge_price = get_hedge_detail(
                ask_price, bid_price, last_performance.share_num, position.share_num, delta, last_performance.last_hedge_delta, hedge=hedge, ucdc=True)
        else:
            delta = uno.deltaRC(live_price, strike, strike_2, t, r, q, v1, v2)
            last_hedge_delta, hedge = get_ucdc_hedge(
                currency_code, delta, last_performance.last_hedge_delta)
            share_num, hedge_shares, status, hedge_price = get_hedge_detail(
                ask_price, bid_price, last_performance.share_num, position.share_num, delta, last_performance.last_hedge_delta, hedge=hedge, ucdc=True)
        bot_cash_balance = last_performance.current_bot_cash_balance + ((last_performance.share_num - share_num) * live_price)
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
        delta = uno.deltaRC(live_price, strike, strike_2, t, r, q, v1, v2)
        share_num = round((position.investment_amount / live_price), 1)
        share_num = math.floor(delta * share_num)
        bot_cash_balance = position.investment_amount - \
            (share_num * live_price)
        last_hedge_delta = delta

    current_pnl_ret = (current_pnl_amt + bot_cash_balance) / \
        position.investment_amount
    current_investment_amount = live_price * share_num
    position.bot_cash_balance = round(bot_cash_balance, 2)
    position.share_num = round((position.investment_amount / live_price), 1)
    position.save()
    digits = max(min(5 - len(str(int(position.entry_price))), 2), -1)
    log_time = pd.Timestamp(trading_day)
    performance = PositionPerformance.objects.create(
        position_uid=position,
        share_num=share_num,
        last_live_price=round(live_price, 2),
        last_spot_price=position.entry_price,
        current_pnl_ret=round(current_pnl_ret, 4),
        current_pnl_amt=round(current_pnl_amt, digits),
        current_investment_amount=round(current_investment_amount, 2),
        current_bot_cash_balance=round(bot_cash_balance, 2),
        updated=log_time,
        created=log_time,
        last_hedge_delta=last_hedge_delta,
        v1=v1,
        v2=v2,
        r=r,
        q=q,
        strike=strike,
        strike_2=strike_2,
        option_price=option_price
        # order_summary,
        # position_uid,
        # order_uid,
        # performance_uid,
    )
    if not status == 'hold':
        order = Order.objects.create(
            is_init=False,
            ticker=position.ticker,
            created=log_time,
            updated=log_time,
            price=live_price,
            bot_id=bot.bot_id,
            amount=hedge_shares*live_price,
            user_id=position.user_id,
            side=status,
            performance_uid=performance.performance_uid
        )
        if order:
            order.placed = True
            order.placed_at = log_time
            order.save()
        print(order.status)
        if order.status in ['pending', 'review']:
            order.status = 'filled'
            order.filled_at = log_time
            order.save()
            performance.order_uid = order
            performance.save()


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
        tac_data = MasterTac.objects.filter(
            ticker=position.ticker, trading_day__gt=trading_day, trading_day__lte=position.expiry).order_by("trading_day")
        lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
        status = False
        for tac in tac_data:
            trading_day = tac.trading_day
            print(f"tac {trading_day} done")
            create_performance(tac, position)
            position.save()
            status = final(tac, position)
            if status:
                print(f"position end")
                break
        if(type(trading_day) == datetime):
            trading_day = trading_day.date()
        if(not status and trading_day < lastest_price_data.last_date and position.expiry >= lastest_price_data.last_date):
            trading_day = lastest_price_data.last_date
            print(f"latest price {trading_day} done")
            create_performance(lastest_price_data, position, latest_price=True)
            position.save()
            status = final(lastest_price_data, position, latest_price=True)
            if status:
                print(f"position end")

        try:
            tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__gte=position.expiry).latest("-trading_day")
            if(not status and tac_data):
                position.expiry = tac_data.trading_day
                position.save()
                print(f"force sell {tac_data.trading_day} done")
                create_performance(tac_data, position)
                position.save()
                status = final(tac_data, position)
                if status:
                    print(f"position end")
        except PositionPerformance.DoesNotExist:
            status = False
        return True
    except OrderPosition.DoesNotExist:
        return False
