from datetime import datetime
from general.date_process import to_date

from pandas.io.sql import DatabaseError
from bot import uno
from core.master.models import LatestPrice, MasterOhlcvtr, HedgeLatestPriceHistory
from core.orders.models import OrderPosition, PositionPerformance, Order
from bot.calculate_bot import (
    check_dividend_paid,
    get_option_price_uno,
    get_hedge_detail,
    get_trq,
    get_strike_barrier,
    get_uno_hedge,
    get_v1_v2,
    get_vol)
from config.celery import app
import pandas as pd
from core.djangomodule.serializers import OrderPositionSerializer
from core.djangomodule.general import formatdigit
from core.services.models import ErrorLog
from django.db import transaction
from django.conf import settings
from typing import Optional,Union,Tuple




def uno_sell_position(live_price:float, trading_day:str, position:OrderPosition, apps:bool=False)-> Tuple[OrderPosition,Optional[Union[Order,None]]]:
    bot = position.bot
    latest = LatestPrice.objects.get(ticker=position.ticker)
    ask_price = latest.intraday_ask
    bid_price= latest.intraday_bid
    high= latest.high

    if high == 0 or high == None:
        high = live_price
    if ask_price == 0 or ask_price == None:
        ask_price = live_price
    if bid_price == 0 or bid_price == None:
        bid_price = live_price
        
    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
    trading_day = to_date(trading_day)


    performance, position, status, hedge_shares = populate_performance(live_price, ask_price, bid_price, trading_day, log_time, position, expiry=True)
    current_pnl_amt = performance["current_pnl_amt"]
    position.final_price = live_price
    position.current_inv_ret = performance["current_pnl_ret"]
    position.final_return = position.current_inv_ret
    position.final_pnl_amount = performance["current_pnl_amt"]
    position.current_inv_amt = live_price * performance["share_num"]
    position.event_date = trading_day
    position.is_live = False

    expiry = to_date(position.expiry)
    
    if high > position.target_profit_price:
        position.event = "KO"
    elif trading_day >= expiry:
        if current_pnl_amt < 0:
            position.event = "Loss"
        elif current_pnl_amt >= 0:
            position.event = "Profit"
        else:
            position.event = "Bot Expired"
    order, performance, position = populate_order(status, hedge_shares, log_time, live_price, bot, performance, position, apps=apps)
    return position, order

def populate_order(status, hedge_shares, log_time, live_price, bot, performance, position, apps=False):
    position_val = OrderPositionSerializer(position).data
    [position_val.pop(key) for key in ["created", "updated"]]
    setup = {"performance": performance, "position": position_val}
    if not status == "hold":
        if hedge_shares < 0:
            hedge_shares = hedge_shares * -1  # make it positif in order
        order_type = 'apps' if apps else None

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
            setup=setup,
            order_type=order_type,
            margin=position.margin
        )
        if order and not apps:
            order.status = "placed"
            order.placed = True
            order.placed_at = log_time
            order.save()
        return order, performance, position
    return None, performance, position

def populate_performance(live_price, ask_price, bid_price, trading_day, log_time, position, expiry=False):
    bot = position.bot
    try:
        last_performance = PositionPerformance.objects.filter(position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False

    t, r, q = get_trq(position.ticker.ticker, position.expiry, trading_day, position.ticker.currency_code)
    if last_performance:
        strike = last_performance.strike
        barrier = last_performance.barrier
        option_price = last_performance.option_price
        rebate = barrier - strike
        v1, v2 = get_v1_v2(position.ticker.ticker, live_price, trading_day, t, r, q, strike, barrier)
        if(expiry):
            delta = last_performance.last_hedge_delta
            hedge = False
            share_num = 0
            hedge_shares = last_performance.share_num * -1
            status = "sell"
            bot_cash_balance = last_performance.current_bot_cash_balance + (last_performance.share_num * live_price)
        else:
            delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t/365, r, q, v1, v2)
            delta, hedge = get_uno_hedge(live_price, strike, delta, last_performance.last_hedge_delta)

            margin_amount = (position.margin - 1) * position.investment_amount
            available_balance = last_performance.current_bot_cash_balance + margin_amount
            if(position.margin == 1):
                if(available_balance < 0):
                    available_balance = 0

            share_num, hedge_shares, status, hedge_price = get_hedge_detail(live_price, available_balance, 
                ask_price, bid_price, last_performance.share_num, position.share_num, delta, last_performance.last_hedge_delta, 
                hedge=hedge, uno=True)

        bot_cash_balance = formatdigit(last_performance.current_bot_cash_balance - ((share_num-last_performance.share_num) * live_price))
        current_pnl_amt = last_performance.current_pnl_amt + (live_price - last_performance.last_live_price) * last_performance.share_num
    else:
        current_pnl_amt = 0  # initial value
        share_num = round((position.investment_amount / live_price), 1)
        bot_cash_balance = position.bot_cash_balance
        vol = get_vol(position.ticker.ticker, trading_day, t, r, q, bot.time_to_exp)
        strike, barrier = get_strike_barrier(live_price, vol, bot.bot_option_type, bot.bot_type.bot_type)
        rebate = barrier - strike
        v1, v2 = get_v1_v2(position.ticker.ticker, live_price, trading_day, t, r, q, strike, barrier)
        delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t/365, r, q, v1, v2)
        option_price = get_option_price_uno(live_price, strike, barrier, rebate, t, r, q, v1, v2)
        share_num = position.share_num
        bot_cash_balance = position.investment_amount - (share_num * live_price)
        hedge_shares = share_num
        status = "buy"
        
    current_investment_amount = live_price * share_num
    current_pnl_ret = current_pnl_amt / position.investment_amount
    # position.bot_cash_dividend = check_dividend_paid(position.ticker.ticker, trading_day, share_num, position.bot_cash_dividend)
    position.bot_cash_balance = round(bot_cash_balance, 2)
    position.save()
    digits = max(min(5-len(str(int(position.entry_price))), 2), -1)
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
        barrier=barrier,
        option_price=option_price,
        order_summary={
            "hedge_shares": hedge_shares
        },
        status="Hedge"
    )
    return performance, position, status, hedge_shares
    
def create_performance(price_data, position, latest=False, hedge=False, tac=False):
    bot = position.bot
    apps=False
    if position.user_id.current_status == "verified":
        apps =True
    if(latest):
        live_price = price_data.close
        if price_data.latest_price:
            live_price = price_data.latest_price
        trading_day = price_data.last_date
        bid_price = price_data.intraday_bid
        ask_price = price_data.intraday_ask
        high = price_data.high
    elif(hedge):
        live_price = price_data.latest_price
        trading_day = price_data.last_date
        bid_price = price_data.latest_price
        ask_price = price_data.latest_price
        high = price_data.high
    else:
        live_price = price_data.close
        trading_day = price_data.trading_day
        bid_price = price_data.close
        ask_price = price_data.close
        high = price_data.high

    if high == 0 or high == None:
        high = live_price
    if ask_price == 0 or ask_price == None:
        ask_price = live_price
    if bid_price == 0 or bid_price == None:
        bid_price = live_price

    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
    if hedge:
        log_time = price_data.intraday_time

    trading_day = to_date(trading_day)
    expiry = to_date(position.expiry)
    status_expiry = high > position.target_profit_price or trading_day >= expiry

    if(status_expiry):
        position, order = uno_sell_position(live_price, trading_day, position,apps=apps)
        return True, order.order_uid
    else:
        performance, position, status, hedge_shares = populate_performance(live_price, ask_price, bid_price, trading_day, log_time, position, expiry=False)
        
        order, performance_order, position = populate_order(status, hedge_shares, log_time, live_price, bot, performance, position, apps=apps)
        if (order):
            return False, order.order_uid
        
        #NOTE: only create record, no buy and sell
        performance.pop("position_uid")
        PositionPerformance.objects.create(
            position_uid=position,  # swapped with instance
            **performance  # the dict value
        )
        position.save()

        return False, None



@app.task
def uno_position_check(position_uid, to_date=None, tac=False, hedge=False, latest=False):
    if not settings.TESTDEBUG:
        transaction.set_autocommit(False)

    try:
        position = OrderPosition.objects.get(
            position_uid=position_uid, is_live=True)
        try:
            performance = PositionPerformance.objects.filter(position_uid=position.position_uid, status="Hedge").latest("created")
            trading_day = performance.created
        except PositionPerformance.DoesNotExist:
            performance = False
            trading_day = position.spot_date
        if to_date:
            exp_date = pd.to_datetime(to_date)
        else:
            exp_date = position.expiry
        if(type(trading_day) == datetime):
                trading_day = trading_day.date()
        status = False
        if hedge:
            try:
                lastest_price_data = HedgeLatestPriceHistory.objects.filter(last_date__gt=trading_day, types="hedge", ticker=position.ticker)
            except HedgeLatestPriceHistory.DoesNotExist:
                print("not exist", position.ticker.ticker)
                return None
            for hedge_price in lastest_price_data:
                trading_day = hedge_price.last_date
                status, order_id = create_performance(hedge_price, position, hedge=True)
                if order_id:
                    order = Order.objects.get(order_uid=order_id)
                    log_time = hedge_price.intraday_time
                    if order.status in ["pending", "review"]:
                        order.status = "filled"
                        order.filled_at = log_time
                        order.save()
                print(f"trading_day {trading_day}-{hedge_price.ticker} done")
                if status:
                    break
        elif(tac):
            tac_data = MasterOhlcvtr.objects.filter(
                ticker=position.ticker, trading_day__gt=trading_day, trading_day__lte=exp_date, day_status="trading_day").order_by("trading_day")
            for tac_price in tac_data:
                trading_day = tac_price.trading_day
                status, order_id = create_performance(tac_price, position, tac=True)
                if order_id:
                    order = Order.objects.get(order_uid=order_id)
                    log_time = pd.to_datetime(tac_price.trading_day)
                    if order.status in ["pending", "review"]:
                        order.status = "filled"
                        order.filled_at = log_time
                        order.save()

                        print(f"Position event: {OrderPosition.objects.get(position_uid=position.position_uid).event}")
                if settings.TESTDEBUG:
                    print("\n")
                    print(f"Bot cash balance: {PositionPerformance.objects.filter(position_uid=position.position_uid).latest('created').current_bot_cash_balance}")
                    print(f"Share num: {PositionPerformance.objects.filter(position_uid=position.position_uid).latest('created').share_num}")
                    print(f"PnL amount: {PositionPerformance.objects.filter(position_uid=position.position_uid).latest('created').current_pnl_amt}")
                    print(f"trading_day {trading_day}-{tac_price.ticker} done")
                if status:
                    break
            if(type(trading_day) == datetime):
                trading_day = trading_day.date()
            if trading_day >= position.expiry:
                try:
                    tac_data = MasterOhlcvtr.objects.filter(ticker=position.ticker, trading_day__gte=position.expiry, day_status="trading_day").latest("trading_day")
                    if(not status and tac_data):
                        position.expiry = tac_data.trading_day
                        position.save()
                        print(f"force sell {tac_data.trading_day} done")
                        status, order_id = create_performance(tac_data, position, tac=True)
                        if order_id:
                            order = Order.objects.get(order_uid=order_id)
                            log_time = pd.to_datetime(tac_data.trading_day)
                            if order.status in ["pending", "review"]:
                                order.status = "filled"
                                order.filled_at = log_time
                                order.save()
                        
                        if status:
                            print(f"position end")
                except MasterOhlcvtr.DoesNotExist:
                    status = False
        else:
            lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
            if(not status and trading_day <= lastest_price_data.last_date and exp_date >= lastest_price_data.last_date):
                trading_day = lastest_price_data.last_date
                print(f"latest price {trading_day} done")
                status, order_id = create_performance(lastest_price_data, position, latest=True)
                if order_id:
                    order = Order.objects.get(order_uid=order_id)
                    log_time = lastest_price_data.intraday_time
                    if order.status in ["pending", "review"]:
                        order.status = "filled"
                        order.filled_at = log_time
                        order.save()
                if status:
                    print(f"position end")

        if not settings.TESTDEBUG:
            transaction.commit()
            print("transaction committed")

        return True
    except OrderPosition.DoesNotExist as e:
        err = ErrorLog.objects.create_log(
            error_description=f"{position_uid} not exist", error_message=str(e))
        err.send_report_error()
        return {"err": f"{position.ticker.ticker}"}
    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"error in Position {position_uid}", error_message=str(e))
        err.send_report_error()

        return {"err": f"{position.ticker.ticker}"}
