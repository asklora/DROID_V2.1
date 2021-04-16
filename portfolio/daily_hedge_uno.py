from core.universe.models import Currency, Universe
from core.master.models import LatestPrice, MasterTac
from datetime import datetime
from bot import uno
import math
from bot.calculate_bot import get_trq, get_strike_barrier, get_v1_v2, get_vol, get_spot_date

def final(master, porfolio):
    latest_price = master
    performance = PortfolioPerformance.objects.filter(order_id=porfolio.order_id).latest('timestamp')
    #today = datetime.today().strftime('%Y-%m-%d')
    # today = latest_price.trading_day + timedelta(days=1)
    today = latest_price.trading_day
    live_price = latest_price.tri_adjusted_price

    high = latest_price.tri_adj_high
    if high == 0 or high == None:
        high = live_price

    if high > porfolio.target_profit_price or today >= porfolio.expiry:
        current_investment_amount=live_price * performance.share_num
        current_pnl_amt = performance.current_pnl_amt + ((live_price - performance.last_live_price) * performance.share_num)
        current_pnl_ret = (current_pnl_amt + porfolio.bot_cash_balance) / porfolio.investment_amount
        porfolio.final_price = live_price
        porfolio.current_inv_ret = (current_pnl_amt + porfolio.bot_cash_balance) / porfolio.investment_amount
        porfolio.final_return = porfolio.current_inv_ret
        porfolio.final_pnl_amount =  current_pnl_amt
        porfolio.current_inv_amt = current_investment_amount
        porfolio.event_date =today
        porfolio.status = True
        if high > porfolio.target_profit_price:
            porfolio.event = "KO"
        elif today >= porfolio.expiry:
            if current_pnl_amt < 0:
                porfolio.event = "Loss"
            elif current_pnl_amt >= 0:
                porfolio.event = "Profit"
            else:
                porfolio.event = "Bot Expired"
        print("SAVED")
        porfolio.save()
        print("UDAH SAVE?")
        return True
    else:
        return False

def performance(master, porfolio):
    latest_price = master
    
    expiry = porfolio.expiry.strftime('%Y-%m-%d')
    spot = latest_price.trading_day.strftime('%Y-%m-%d')
    try:
        last_perf = PortfolioPerformance.objects.filter(order_id=porfolio.order_id).latest('timestamp')
    except PortfolioPerformance.DoesNotExist:
        last_perf = False
    #New Changes
    live_price = latest_price.tri_adjusted_price

    t, r, q = get_trq(porfolio.stock_selected.ticker, expiry, spot, porfolio.currency.currency_code)

    if last_perf:
        executive_option =UnoOptionsPrice.objects.filter(portfolio_id=porfolio.order_id).first()
        strike=executive_option.strike
        barrier=executive_option.barrier
        rebate=barrier - strike

        v1, v2 = get_v1_v2(porfolio.stock_selected.ticker, live_price, spot, t, r, q, strike, barrier)

        delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t, r, q, v1, v2)

        share_num = math.floor(delta * porfolio.share_num)

        spot_price = live_price
        if spot_price > strike :
            last_hedge_delta = last_perf.last_hedge_delta
            if abs(last_hedge_delta - delta) <= ((v1 + v2)  / 15) :
                share_num = last_perf.share_num
                last_hedge_delta = last_hedge_delta
            else :
                last_hedge_delta = delta
        else :
            last_hedge_delta = delta

        bot_cash_balance = last_perf.current_bot_cash_balance + ((last_perf.share_num - share_num) * live_price)
        current_pnl_amt = last_perf.current_pnl_amt + (live_price - last_perf.last_live_price) * last_perf.share_num
    elif not last_perf:
        current_pnl_amt = 0 # initial value
        share_num = round((porfolio.investment_amount / live_price), 1)
        bot_cash_balance = porfolio.bot_cash_balance
        vol = get_vol(porfolio.stock_selected.ticker, spot, t, r, q, porfolio.bot_type.bot_horizon_month)
        strike, barrier = get_strike_barrier(live_price, vol, porfolio.option_type, porfolio.bot_type.bot_group)
        rebate = barrier - strike
        v1, v2 = get_v1_v2(porfolio.stock_selected.ticker, live_price, spot, t, r, q, strike, barrier)
        delta = uno.deltaUnOC(live_price, strike, barrier, rebate, t, r, q, v1, v2)
        share_num = math.floor(delta * share_num)
        bot_cash_balance = porfolio.investment_amount - (share_num * live_price)
        last_hedge_delta = delta
            
    current_pnl_ret = (current_pnl_amt + bot_cash_balance) / porfolio.investment_amount
            #New Changes
    current_investment_amount = live_price * share_num
    porfolio.bot_cash_balance = round(bot_cash_balance,2)
    porfolio.share_num = round((porfolio.investment_amount / live_price), 1)
    porfolio.save()
    digits = max(min(5-len(str(int(porfolio.entry_price))),2),-1)
    start_date = (latest_price.trading_day).strftime("%Y-%m-%d")
    timestamps = datetime.strptime(start_date, "%Y-%m-%d")
    perf = PortfolioPerformance.objects.create(
        order=porfolio,
        share_num=share_num,
        last_live_price=round(live_price,2),
        last_spot_price=porfolio.entry_price,
        current_pnl_ret=round(current_pnl_ret,4),
        current_pnl_amt=round(current_pnl_amt,digits),
        current_investment_amount=round(current_investment_amount,2),
        current_bot_cash_balance=  round(bot_cash_balance,2),
        timestamp=timestamps,
        updated= latest_price.trading_day,
        created= latest_price.trading_day,
        last_hedge_delta = last_hedge_delta
        )
    perf.save()
    if perf:
        uno_model = UnoOptionsPrice.objects.create(
            portfolio=porfolio,
            v1 = v1,
            v2 = v2,
            r = r,
            q = q,
            strike = strike,
            barrier = barrier,
            delta = delta,
            created = latest_price.trading_day,
            updated = latest_price.trading_day,
            created_time = timestamps
            )
@app.task
def start_uno_trace(order_id):
    try:
        porfolio = PortfolioOrder.objects.get(order_id=order_id, status=False)
        performance_data = PortfolioPerformance.objects.filter(order_id=order_id)
        uno_model = UnoOptionsPrice.objects.filter(portfolio_id=order_id)
        if(len(performance_data) > 1):
            return {'status' : "Cannot Trace This Order ID"}

        ticker = porfolio.stock_selected
        spot_date = porfolio.spot_date
        trading_day = get_spot_date(spot_date.strftime("%Y-%m-%d"), ticker).strftime("%Y-%m-%d")
        Master_ohlctr = MasterOhlctr.objects.filter(ticker=ticker, trading_day__gte=trading_day).order_by('trading_day')
        for master in Master_ohlctr:
            performance(master, porfolio)
            porfolio.save()
            status = final(master, porfolio)
            if status:
                break
        return True
    except PortfolioOrder.DoesNotExist:
        return False