from bot.calculate_bot import check_dividend_paid
from datetime import datetime
from core.djangomodule.calendar import TradingHours
from core.master.models import LatestPrice, MasterOhlcvtr
from core.orders.models import OrderPosition, PositionPerformance, Order
from config.celery import app
import pandas as pd
from core.djangomodule.serializers import OrderPositionSerializer
from core.djangomodule.general import formatdigit
from core.services.models import ErrorLog


def create_performance(price_data, position, latest_price=False,rehedge=False,force_stop=False):
    if(latest_price):
        live_price = price_data.close
        if price_data.latest_price:
            live_price = price_data.latest_price
        trading_day = price_data.last_date
    else:
        live_price = price_data.close
        if price_data.latest_price:
            live_price = price_data.latest_price
        trading_day = price_data.last_date

    try:
        last_performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False

    if last_performance:
        share_num = last_performance.share_num
        if(force_stop):
            share_num = 0
        # bot_cash_balance = last_performance.current_bot_cash_balance + \
        #     ((last_performance.share_num - share_num) * live_price)
        bot_cash_balance = formatdigit(
            last_performance.current_bot_cash_balance-(share_num-last_performance.share_num)*live_price)
        current_pnl_amt = last_performance.current_pnl_amt + \
            (live_price - last_performance.last_live_price) * \
            last_performance.share_num

    else:
        live_price = position.entry_price
        current_pnl_amt = 0
        bot_cash_balance =0
        share_num = position.share_num
    current_investment_amount = live_price * share_num
    current_pnl_ret = current_pnl_amt / position.investment_amount
    # position.bot_cash_dividend = check_dividend_paid(position.ticker.ticker, trading_day, share_num, position.bot_cash_dividend)
    position.bot_cash_balance = round(bot_cash_balance, 2)
    position.save()
    digits = max(min(5 - len(str(int(position.entry_price))), 2), -1)
    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
    if rehedge:
        log_time= price_data.intraday_time
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
    )

    if force_stop:
        position.final_price = live_price
        position.current_inv_ret = performance['current_pnl_ret']
        position.final_return = position.current_inv_ret
        position.final_pnl_amount = performance['current_pnl_amt']
        position.current_inv_amt = live_price * performance['share_num']
        position.event_date = trading_day
        position.is_live = False
        position.event = "Stopped by User"
        position.save()
        # serializing -> make dictionary position instance
        position_val = OrderPositionSerializer(position).data
        # remove created and updated from position
        [position_val.pop(key) for key in ["created", "updated"]]

        # merge two dict, and save to order setup
        setup = {"performance": performance, "position": position_val}
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
            setup=setup
        )
        # only for bot
        if order:
            order.placed = True
            order.placed_at = log_time
            order.status = 'pending'
            order.save()
        # go to core/orders/signal.py Line 54 and 112
        # this will wait until order filled then creating performance along with it
        if(force_stop):
            return True, order
        else:
            return False, order
    # remove position_uid from dict and swap with instance
    performance.pop("position_uid")
    # create the record
    PositionPerformance.objects.create(
        position_uid=position,  # swapped with instance
        **performance  # the dict value
    )
    position.save()
    # if(force_stop):
    #     return True, None
    # else:
    #     return False, None


@app.task
def user_position_check(position_uid, to_date=None, lookback=False,rehedge=None):
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
        if to_date:
            exp_date = pd.to_datetime(to_date)
        else:
            exp_date = datetime.now().date()
        if isinstance(trading_day, datetime):
            trading_day = trading_day.date()
       
        status = False

        if rehedge:
            from core.master.models import HedgeLatestPriceHistory
            try:
                lastest_price_data = HedgeLatestPriceHistory.objects.filter(last_date=rehedge['date'],types=rehedge['types'],ticker=position.ticker)
                if lastest_price_data.exists():
                    lastest_price_data = lastest_price_data.get()
                else:
                    return None
            except HedgeLatestPriceHistory.DoesNotExist:
                print('not exist',position.ticker.ticker)
                return None
            except HedgeLatestPriceHistory.MultipleObjectsReturned:
                print('nmulpile error',position.ticker.ticker)
                return None
            if(type(trading_day) == datetime):
                trading_day = trading_day.date()
            if(not status and trading_day <= lastest_price_data.last_date and exp_date >= lastest_price_data.last_date):
                trading_day = lastest_price_data.last_date
                print(f"rehedge latest price {trading_day} done")
                status, order_id = create_performance(
                    lastest_price_data, position, latest_price=True,rehedge=True)
                
        elif(lookback):
            tac_data = MasterOhlcvtr.objects.filter(
            ticker=position.ticker, trading_day__gt=trading_day, trading_day__lte=exp_date, day_status='trading_day').order_by("trading_day")
            for tac in tac_data:
                # trading_day = tac.trading_day
                status, order_id = create_performance(tac, position)
                # this is for debug only, make function this can be on/off
        else:
            if(type(trading_day) == datetime):
                trading_day = trading_day.date()
            lastest_price_data = LatestPrice.objects.get(
                ticker=position.ticker)
            if(not status):
                trading_day = lastest_price_data.last_date
                print(f"latest price {trading_day} done")
                status, order_id = create_performance(
                    lastest_price_data, position, latest_price=True)
                # position.save()
        return True
    except OrderPosition.DoesNotExist as e:
        err = ErrorLog.objects.create_log(error_description=f'{position_uid} not exist',error_message=str(e))
        err.send_report_error()
        return {'err':f'{position.ticker.ticker}'}
    except Exception as e:
        err = ErrorLog.objects.create_log(error_description=f'error in Position {position_uid}',error_message=str(e))
        err.send_report_error()
        return {'err':f'{position.ticker.ticker}'}
