from core.user.convert import ConvertMoney
from bot.calculate_bot import check_dividend_paid
from datetime import datetime
from general.date_process import to_date

from core.djangomodule.calendar import TradingHours
from core.master.models import LatestPrice, MasterOhlcvtr, HedgeLatestPriceHistory
from core.orders.models import OrderPosition, PositionPerformance, Order
from config.celery import app
import pandas as pd
from core.djangomodule.serializers import OrderPositionSerializer
from core.djangomodule.general import formatdigit
from core.services.models import ErrorLog
from django.db import transaction
from typing import Optional,Union,Tuple
from django.conf import settings

def user_sell_position(  live_price:float, trading_day:datetime, 
                        position:OrderPosition, apps:bool=False) -> Tuple[OrderPosition,Optional[Union[Order,None]]]:

                        
    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
    trading_day = to_date(trading_day)

    performance, position = populate_performance(live_price, trading_day, log_time, position, expiry=True)

    position.final_price = live_price
    position.current_inv_ret = performance["current_pnl_ret"]
    position.final_return = position.current_inv_ret
    position.final_pnl_amount = performance["current_pnl_amt"]
    position.current_inv_amt = live_price * performance["share_num"]
    position.event_date = trading_day
    position.is_live = False
    position.event = "Stopped by User"
    converter = ConvertMoney(position.ticker.currency_code, position.user_id.currency)
    position.exchange_rate = converter.get_exchange_rate()
    position_val = OrderPositionSerializer(position).data
    [position_val.pop(key) for key in ["created", "updated"]]
    setup = {"performance": performance, "position": position_val}
    order_type = 'apps' if apps else None

    order = Order.objects.create(
        is_init=False,
        ticker=position.ticker,
        created=log_time,
        updated=log_time,
        price=live_price,
        bot_id=position.bot_id,
        amount=position.share_num * live_price,
        user_id=position.user_id,
        side="sell",
        qty=position.share_num,
        setup=setup,
        order_type=order_type,
        margin=position.margin,
        exchange_rate = converter.get_exchange_rate()
    )

    return position, order

def populate_performance(live_price, trading_day, log_time, position, expiry=False):
    try:
        last_performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False
    if last_performance:
        share_num = last_performance.share_num
        if(expiry):
            share_num = 0
        bot_cash_balance = formatdigit(last_performance.current_bot_cash_balance-(share_num-last_performance.share_num)*live_price)
        current_pnl_amt = last_performance.current_pnl_amt + (live_price - last_performance.last_live_price) * last_performance.share_num
    else:
        live_price = position.entry_price
        current_pnl_amt = 0
        bot_cash_balance =0
        share_num = position.share_num
    current_investment_amount = live_price * share_num
    current_pnl_ret = current_pnl_amt / position.investment_amount
    # position.bot_cash_dividend = check_dividend_paid(position.ticker.ticker, trading_day, share_num, position.bot_cash_dividend)
    position.bot_cash_balance = round(bot_cash_balance, 2)
    digits = max(min(5 - len(str(int(position.entry_price))), 2), -1)
    converter = ConvertMoney(position.ticker.currency_code, position.user_id.currency)
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
        last_hedge_delta=1,
        status="Hedge",
        exchange_rate = converter.get_exchange_rate()
    )
    return performance, position
    
def create_performance(price_data, position, latest=False, hedge=False, tac=False):
    bot = position.bot
    if(latest):
        live_price = round(price_data.close, 2)
        if price_data.latest_price:
            live_price = round(price_data.latest_price, 2)
        trading_day = price_data.last_date
    elif(hedge):
        live_price = round(price_data.latest_price, 2)
        trading_day = price_data.last_date
    else:
        live_price = round(price_data.close, 2)
        trading_day = price_data.trading_day

    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
    if hedge:
        log_time = price_data.intraday_time
    performance, position = populate_performance(live_price, trading_day, log_time, position, expiry=False)
    position.save()
    performance.pop("position_uid")
    PositionPerformance.objects.create(
        position_uid=position,  # swapped with instance
        **performance  # the dict value
    )
    position.save()
    return True, None
    

@app.task
def user_position_check(position_uid, to_date=None, tac=False, hedge=False, latest=False):
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
                if performance:
                    lastest_price_data = HedgeLatestPriceHistory.objects.filter(last_date__gt=trading_day, types="hedge", ticker=position.ticker)
                else:
                    lastest_price_data = HedgeLatestPriceHistory.objects.filter(last_date__gte=trading_day, types="hedge", ticker=position.ticker)
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
                print(f"trading_day {trading_day}-{tac_price.ticker} done")
                if status:
                    break
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
        transaction.commit()
        print("transaction committed")
        return True
    except OrderPosition.DoesNotExist as e:
        err = ErrorLog.objects.create_log(
            error_description=f"{position_uid} not exist", error_message=str(e))
        err.send_report_error()
        if settings.TESTDEBUG:
            raise Exception('Hedge error position not found')
        return {"err": f"{position.ticker.ticker}"}
    except Exception as e:
        err = ErrorLog.objects.create_log(
            error_description=f"error in Position {position_uid}", error_message=str(e))
        err.send_report_error()
        if settings.TESTDEBUG:
            raise Exception('Hedge error',str(e))
        return {"err": f"{position.ticker.ticker}"}
