from datetime import datetime
from core.master.models import LatestPrice, MasterTac, MasterOhlcvtr
from core.orders.models import OrderPosition, PositionPerformance, Order
from config.celery import app
import pandas as pd
from core.djangomodule.serializers import OrderPositionSerializer
from core.djangomodule.general import formatdigit


def create_performance(price_data, position, latest_price=False):
    bot = position.bot

    if(latest_price):
        live_price = price_data.close
        trading_day = price_data.last_date
        high = price_data.high
        low = price_data.low
    else:
        live_price = price_data.close
        trading_day = price_data.trading_day
        high = price_data.close
        low = price_data.close

    if high == 0 or high == None:
        high = live_price
    if low == 0 or low == None:
        low = live_price

    status_expiry = high > position.target_profit_price or low < position.max_loss_price or trading_day >= position.expiry

    try:
        last_performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid).latest("created")
    except PositionPerformance.DoesNotExist:
        last_performance = False

    if last_performance:
        share_num = last_performance.share_num
        if(status_expiry):
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
        share_num = position.share_num
    current_investment_amount = live_price * share_num
    current_pnl_ret = current_pnl_amt / position.investment_amount

    position.bot_cash_balance = round(bot_cash_balance, 2)
    position.save()
    digits = max(min(5 - len(str(int(position.entry_price))), 2), -1)
    log_time = pd.Timestamp(trading_day)
    if log_time.date() == datetime.now().date():
        log_time = datetime.now()
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

    if status_expiry:
        position.final_price = live_price
        position.current_inv_ret = performance['current_pnl_ret']
        position.final_return = position.current_inv_ret
        position.final_pnl_amount = performance['current_pnl_amt']
        position.current_inv_amt = live_price * performance['share_num']
        position.event_date = trading_day
        position.is_live = False
        if high > position.target_profit_price:
            position.event = "Targeted Profit"
        elif low < position.max_loss_price:
            position.event = "Maximum Loss"
        elif trading_day >= position.expiry:
            if live_price < position.entry_price:
                position.event = "Loss"
            elif live_price > position.entry_price:
                position.event = "Profit"
            else:
                position.event = "Bot Expired"
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
            bot_id=bot.bot_id,
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
            order.save()
        # go to core/orders/signal.py Line 54 and 112
        # this will wait until order filled then creating performance along with it
        if(status_expiry):
            return True, order.order_uid
        else:
            return False, order.order_uid
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
def classic_position_check(position_uid, to_date=None):
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
            exp_date = position.expiry
        tac_data = MasterOhlcvtr.objects.filter(
            ticker=position.ticker, trading_day__gt=trading_day.date(), trading_day__lte=exp_date, day_status='trading_day').order_by("trading_day")
        status = False
        for tac in tac_data:
            # trading_day = tac.trading_day
            status, order_id = create_performance(tac, position)
            # this is for debug only, make function this can be on/off
            if order_id:
                order = Order.objects.get(order_uid=order_id)
                log_time = pd.Timestamp(tac.trading_day)
                if order.status in ["pending", "review"]:
                    order.status = "filled"
                    order.filled_at = log_time
                    order.save()
            print(f"tac {trading_day}-{tac.ticker} done")
            if status:
                break
        if(type(trading_day) == datetime):
            trading_day = trading_day.date()
        lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
        if(not status and trading_day < lastest_price_data.last_date and exp_date >= lastest_price_data.last_date):
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
                print(f"position end")
        if trading_day >= position.expiry:
            try:
                tac_data = MasterOhlcvtr.objects.filter(
                    ticker=position.ticker, trading_day__gte=position.expiry, day_status='trading_day').latest("trading_day")
                if(not status and tac_data):
                    position.expiry = tac_data.trading_day
                    position.save()
                    print(f"force sell {tac_data.trading_day} done")
                    status, order_id = create_performance(tac_data, position)
                    # position.save()
                    if order_id:
                        order = Order.objects.get(order_uid=order_id)
                        log_time = pd.Timestamp(trading_day)
                        if order.status in ["pending", "review"]:
                            order.status = "filled"
                            order.filled_at = log_time
                            order.save()
                    if status:
                        print(f"position end")
            except MasterOhlcvtr.DoesNotExist:
                status = False
        return True
    except OrderPosition.DoesNotExist:
        return False
