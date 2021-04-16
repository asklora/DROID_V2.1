from datetime import datetime,timezone,time,timedelta,date
from core.universe.models import Currency
from core.master.models import LatestPrice, MasterTac
from core.orders.models import Order, OrderPosition, PositionPerformance
from bot.calculate_bot import get_spot_date
# from core.classic_droid.celery import app

def final(master, porfolio):
    latest_price = master
    performance = PortfolioPerformance.objects.filter(order_id=porfolio.order_id).latest("timestamp")
    today = latest_price.trading_day
    #print(today)
    last_live_price = latest_price.tri_adjusted_price

    high = latest_price.tri_adj_high
    if high == 0 or high == None:
        high = last_live_price
    
    low = latest_price.tri_adj_low
    if low == 0 or low == None:
        low = last_live_price

    if high > porfolio.target_profit_price or low < porfolio.max_loss_price or today >= porfolio.expiry:
        current_investment_amount=last_live_price * porfolio.share_num
        current_pnl_amt = performance.current_pnl_amt + ((last_live_price - performance.last_live_price) * performance.share_num)
        current_pnl_ret = (current_pnl_amt + porfolio.bot_cash_balance) / porfolio.investment_amount
        porfolio.final_price = last_live_price
        porfolio.current_inv_ret = (current_pnl_amt + porfolio.bot_cash_balance) / porfolio.investment_amount
        porfolio.final_return = porfolio.current_inv_ret
        porfolio.current_inv_amt = current_investment_amount
        porfolio.final_pnl_amount =  current_pnl_amt
        porfolio.event_date =today
        porfolio.status = True
        if high > porfolio.target_profit_price:
            porfolio.event = "Targeted Profit"
        elif low < porfolio.max_loss_price:
            porfolio.event = "Maximum Loss"
        elif today >= porfolio.expiry:
            if last_live_price < porfolio.entry_price:
                porfolio.event = "Loss"
            elif last_live_price > porfolio.entry_price:
                porfolio.event = "Profit"
            else:
                porfolio.event = "Bot Expired"
        porfolio.save()
        return True
    else:
        return False

def performance(master, porfolio):
    latest_price = master
    last_live_price = latest_price.tri_adjusted_price
    
    try:
        last_perf = PortfolioPerformance.objects.filter(order_id=porfolio.order_id).latest("timestamp")
    except PortfolioPerformance.DoesNotExist:
        last_perf = False
    #New Changes
    
    if last_perf:
        share_num = last_perf.share_num
        current_pnl_amt = last_perf.current_pnl_amt + (last_live_price - last_perf.last_live_price ) * last_perf.share_num
    elif not last_perf:
        last_live_price = porfolio.entry_price
        current_pnl_amt = 0 # initial value
        share_num = porfolio.share_num
    current_pnl_ret = (current_pnl_amt + porfolio.bot_cash_balance) / porfolio.investment_amount
    #New Changes
    current_investment_amount = last_live_price * porfolio.share_num
    digits = max(min(5-len(str(int(porfolio.entry_price))),2),-1)
    start_date = (latest_price.trading_day).strftime("%Y-%m-%d")
    timestamps = datetime.strptime(start_date, "%Y-%m-%d")
    perf = PortfolioPerformance.objects.create(
        order=porfolio,
        share_num=share_num,
        last_live_price=round(last_live_price,2),
        last_spot_price=porfolio.entry_price,
        current_pnl_ret=round(current_pnl_ret,4),
        current_pnl_amt=round(current_pnl_amt,digits),
        current_investment_amount=round(current_investment_amount,2),
        current_bot_cash_balance=  round(porfolio.bot_cash_balance,2),
        timestamp=timestamps,
        updated= latest_price.trading_day,
        created= latest_price.trading_day,
        last_hedge_delta = 100
    )
    perf.save()
    
@app.task                    
def start_trace(order_id, history=False):
    try:
        porfolio = OrderPosition.objects.get(uid=order_id, status=False)
        performance_data = PositionPerformance.objects.filter(order_id=order_id)
        if(len(performance_data) > 1):
            return {"status" : "Cannot Trace This Order ID"}
        ticker = porfolio.stock_selected
        spot_date = porfolio.spot_date
        trading_day = get_spot_date(spot_date.strftime("%Y-%m-%d"), ticker).strftime("%Y-%m-%d")
        Master_ohlctr = MasterTac.objects.filter(ticker=ticker, trading_day__gte=trading_day).order_by("trading_day")
        for master in Master_ohlctr:
            #print(master.trading_day)
            performance(master, porfolio)
            porfolio.save()
            status = final(master, porfolio)
            if status :
                break
        return True
    except PortfolioOrder.DoesNotExist:
        return False

