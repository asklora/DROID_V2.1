from datetime import datetime
from core.master.models import LatestPrice, MasterTac
from core.orders.models import OrderPosition, PositionPerformance, Order
from config.celery import app
import pandas as pd

def create_performance(price_data, position, latest_price=False):
    bot = position.bot

    if(latest_price):
        live_price = price_data.close
        trading_day = price_data.last_date
        high = price_data.high
        low = price_data.low
    else:
        live_price = price_data.tri_adj_close
        trading_day = price_data.trading_day
        high = price_data.tri_adj_high
        low = price_data.tri_adj_low

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
        bot_cash_balance = last_performance.current_bot_cash_balance + ((last_performance.share_num - share_num) * live_price)
        current_pnl_amt = last_performance.current_pnl_amt + (live_price - last_performance.last_live_price) * last_performance.share_num
    else:
        live_price = position.entry_price
        current_pnl_amt = 0
        share_num = position.share_num
    current_pnl_ret = (current_pnl_amt + bot_cash_balance) / position.investment_amount
    position.bot_cash_balance = round(bot_cash_balance, 2)
    position.save()
    current_investment_amount = live_price * share_num
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
        last_hedge_delta=100
    )

    if status_expiry:
        current_investment_amount = live_price * performance.share_num
        current_pnl_amt = performance.current_pnl_amt + ((live_price - performance.last_live_price) * performance.share_num)
        position.final_price = live_price
        position.current_inv_ret = (current_pnl_amt + position.bot_cash_balance) / position.investment_amount
        position.final_return = position.current_inv_ret
        position.current_inv_amt = current_investment_amount
        position.final_pnl_amount = current_pnl_amt
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
    
    if status_expiry:
        order = Order.objects.create(
            is_init = False,
            ticker = position.ticker,
            created = log_time,
            updated = log_time,
            price = live_price,
            bot_id = bot.bot_id,
            amount = position.share_num * live_price,
            user_id = position.user_id,
            side = "sell",
            performance_uid = performance.performance_uid,
            qty = position.share_num
        )
        if order:
            order.placed = True
            order.placed_at = log_time
            order.save()
        if order.status in ["pending", "review"]:
            order.status = "filled"
            order.filled_at = log_time
            order.save()
            performance.order_uid = order
            performance.save()
        return True
    else:
        return False


@app.task
def classic_position_check(position_uid):
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
        tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__gt=trading_day, trading_day__lte=position.expiry).order_by("trading_day")
        status = False
        for tac in tac_data:
            trading_day = tac.trading_day
            print(f"tac {trading_day} done")
            status = create_performance(tac, position)
            position.save()
            if status:
                print(f"position end")
                break
        if(type(trading_day) == datetime):
            trading_day = trading_day.date()
        lastest_price_data = LatestPrice.objects.get(ticker=position.ticker)
        if(not status and trading_day < lastest_price_data.last_date and position.expiry >= lastest_price_data.last_date):
            trading_day = lastest_price_data.last_date
            print(f"latest price {trading_day} done")
            status = create_performance(lastest_price_data, position, latest_price=True)
            position.save()
            if status:
                print(f"position end")
        try:
            tac_data = MasterTac.objects.filter(ticker=position.ticker, trading_day__gte=position.expiry).latest("-trading_day")
            if(not status and tac_data):
                position.expiry = tac_data.trading_day
                position.save()
                print(f"force sell {tac_data.trading_day} done")
                status = create_performance(tac_data, position)
                position.save()
                if status:
                    print(f"position end")
        except PositionPerformance.DoesNotExist:
            status = False
        return True
    except OrderPosition.DoesNotExist:
        return False