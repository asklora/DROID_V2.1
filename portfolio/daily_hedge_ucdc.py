from backendmodel.universe.models import Indices
from backendmodel.stock_prices.models import LatestPrice, MasterOhlctr
from backendmodel.classic_droid.models import PortfolioPerformance,PortfolioOrder
from backendmodel.helpermodel.models import LatestVolUpdate
import pendulum
from datetime import datetime,timezone,time,timedelta,date
from backendmodel.executive_uno import utils,uno
from backendmodel.executive_uno.models import UnoOptionsPrice
from .calculation import get_trq, get_strike_barrier, get_v1_v2, get_vol, get_spot_date
import math
from backendmodel.classic_droid.celery import app


def final(master, porfolio):
    latest_price = master
    performance = PortfolioPerformance.objects.filter(order_id=porfolio.order_id).latest('timestamp')
    #today = datetime.today().strftime('%Y-%m-%d')
    # today = latest_price.trading_day + timedelta(days=1)
    today = latest_price.trading_day
    live_price = latest_price.tri_adjusted_price
    if today >= porfolio.expiry:
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
        if today >= porfolio.expiry:
            porfolio.event = "Bot Expired"
        porfolio.save()
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
        strike_2=executive_option.strike_2
        v1, v2 = get_v1_v2(porfolio.stock_selected.ticker, live_price, spot, t, r, q, strike, strike_2)
        delta = uno.deltaRC(live_price, strike, strike_2, t, r, q, v1, v2)
        share_num = math.floor(delta * porfolio.share_num)

        index = str(porfolio.stock_selected.index.index)
        if index not in ["0#.SPX", "0#.ETF", "0#.SXXE"]:
            last_hedge_delta = last_perf.last_hedge_delta
            if abs((last_hedge_delta - delta)) <= ((v1 + v2)  / 15) :
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
        vol = get_vol(porfolio.stock_selected.ticker, spot, t, r, q, porfolio.bot_type.bot_horizon_month)
        strike, strike_2 = get_strike_barrier(live_price, vol, porfolio.option_type, porfolio.bot_type.bot_group)
        v1, v2 = get_v1_v2(porfolio.stock_selected.ticker, live_price, spot, t, r, q, strike, strike_2)
        delta = uno.deltaRC(live_price, strike, strike_2, t, r, q, v1, v2)
        share_num = math.floor(delta * share_num)
        bot_cash_balance = porfolio.investment_amount - (share_num * live_price)
        last_hedge_delta = delta

    current_pnl_ret = (current_pnl_amt + bot_cash_balance) / porfolio.investment_amount
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
    if perf:
        uno_model = UnoOptionsPrice.objects.create(
            portfolio=porfolio,
            v1 = v1,
            v2 = v2,
            r = r,
            q = q,
            strike = strike,
            strike_2 = strike_2,
            delta = delta,
            created = latest_price.trading_day,
            updated = latest_price.trading_day,
            created_time = timestamps
            )
@app.task
def start_ucdc_trace(order_id):
    try:
        porfolio = PortfolioOrder.objects.get(order_id=order_id, status=False)
        performance_data = PortfolioPerformance.objects.filter(order_id=order_id)
        uno_model = UnoOptionsPrice.objects.filter(portfolio_id=order_id)
        if(len(performance_data) > 1):
            return {'status' : "Cannot Trace This Order ID"}
        # if(len(performance_data) == 1):
            
        #     #Reupdate Performance
        #     last_perf = performance_data.latest('timestamp')
        #     last_perf.last_live_price = last_perf.last_spot_price
        #     last_perf.current_pnl_amt = 0
        #     last_perf.updated = porfolio.spot_date
        #     last_perf.created = porfolio.spot_date
        #     last_perf.current_investment_amount = round(last_perf.share_num * last_perf.last_spot_price,2)
        #     last_perf.current_bot_cash_balance=  porfolio.bot_cash_balance
        #     start_date = (porfolio.spot_date).strftime("%Y-%m-%d")
        #     timestamps = datetime.strptime(start_date, "%Y-%m-%d")
        #     last_perf.timestamp = timestamps
        #     last_perf.save()
            
        #     #Reupdate OptionPrice
        #     last_option_price = PortfolioPerformance.objects.filter(order_id=order_id).latest('timestamp')
        #     expiry = porfolio.expiry.strftime('%Y-%m-%d')
        #     spot = porfolio.spot_date.strftime('%Y-%m-%d')
        #     last_live_price = last_perf.last_spot_price
        #     t, r, q = get_trq(porfolio.stock_selected.ticker, expiry, spot, porfolio.currency.currency_code)
        #     vol = get_vol(porfolio.stock_selected.ticker, spot, t, r, q, porfolio.bot_type.bot_horizon_month)
        #     strike, strike_2 = get_strike_barrier(last_live_price, vol, porfolio.option_type, porfolio.bot_type.bot_group)
        #     v1, v2 = get_v1_v2(porfolio.stock_selected.ticker, last_live_price, spot, t, r, q, strike, strike_2)
        #     delta = uno.deltaRC(last_live_price, strike, strike_2, t, r, q, v1, v2)

        #     last_option_price.portfolio=porfolio
        #     last_option_price.v1 = v1
        #     last_option_price.v2 = v2
        #     last_option_price.r = r
        #     last_option_price.updated = start_date
        #     last_option_price.created = start_date
        #     last_option_price.created_time = timestamps
        #     last_option_price.q = q
        #     last_option_price.strike = strike
        #     last_option_price.strike_2 = strike_2
        #     last_option_price.delta = delta
        #     last_option_price.save()

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