from math import floor
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
from core.orders.models import Order, PositionPerformance, OrderPosition
from general.date_process import dateNow
from general.data_process import get_uid
from core.user.models import Accountbalance, TransactionHistory, User, UserDepositHistory
from core.orders.models import OrderPosition, PositionPerformance
from core.universe.models import Universe
from core.bot.models import BotOptionType
import sys
from general.sql_query import get_orders_position_performance
from general.api_quant_request import get_single_ticker_close_price
import pandas as pd
from django.core.management.base import BaseCommand
from general.date_process import date_plus_day, date_minus_day, dateNow

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("-create_user", "--create_user", type=bool, help="create_user", default=False)
        parser.add_argument("-save_csv", "--save_csv", type=bool, help="save_csv", default=False)
        parser.add_argument("-ticker", "--ticker", type=str, help="ticker", default='TSLA.O')
        parser.add_argument("-spot_date", "--spot_date", type=str, help="spot_date", default='2022-01-01')
        parser.add_argument("-bot_id", "--bot_id", type=str, help="bot_id", default='UNO_OTM_05')
        parser.add_argument("-user_id", "--user_id", type=int, help="user_id", default=59105)
        parser.add_argument("-amount", "--amount", type=int, help="amount", default=200000)

    def handle(self, *args, **options):

        # Save new order position to excel
        if (options["save_csv"]):

            today = dateNow()   # only get hedging create today

            # get all new position generated today
            position = OrderPosition.objects.filter(user_id_id=59105, created__gt=(today))

            xls = pd.ExcelWriter(f'plot_hedging_data_{today}.xlsx')
            for i in position:
                # get all hedging details
                performance = PositionPerformance.objects.filter(position_uid_id=i.position_uid)
                performance = pd.DataFrame(performance.values('created', 'current_pnl_ret', 'order_summary')
                                           ).rename(columns={"created": "trading_day"})
                performance['hedge_shares'] = [x['hedge_shares'] if x else None for x in performance['order_summary'].to_list()]
                performance = performance.drop(columns=['order_summary'])

                # get close price
                price = get_single_ticker_close_price(i.ticker, date_minus_day(i.spot_date, 15), date_plus_day(i.expiry, 15))
                price = pd.DataFrame(price, index=["close"]).transpose().reset_index().rename(columns={"index": "trading_day"})
                price["trading_day"] = pd.to_datetime(price["trading_day"])

                # merge & save sheet
                performance = performance.merge(price, on=["trading_day"], how="outer").sort_values(by=['trading_day'])
                sheet_name = f"{str(i.ticker).split('.')[0]}_{i.bot_id}_{i.spot_date.strftime('%Y%m%d')}"
                performance.to_excel(xls, sheet_name=sheet_name, index=False)
            xls.save()

        #Create User Part
        elif (options["create_user"]):
            user = User.objects.create_user(
                email="hanwha_hedging@loratechai.com",
                username="HanwhaHedging",
                first_name="Hanwha",
                last_name="Hedging",
                gender="other",
                phone="012345678",
                password="HANWHA",
                is_active=True,
                current_status="verified",
                is_joined=False,
                is_test=True,
            )
            user_balance = Accountbalance.objects.create(
                user=user,
                amount=0,
                currency_code_id="USD",
            )
            transaction = TransactionHistory.objects.create(
                balance_uid=user_balance,
                side="credit",
                amount=1000000,
                transaction_detail={"event": "first deposit"},
            )
            UserDepositHistory.objects.create(
                uid=get_uid(user.id, trading_day=dateNow(), replace=True),
                user_id=user,
                trading_day=dateNow(),
                deposit=transaction.amount,
            )

        else:
            ticker = options["ticker"]
            spot_date = options["spot_date"]
            bot_id = options["bot_id"]
            user_id = options["user_id"]
            amount = options["amount"]
            price = get_single_ticker_close_price(ticker, spot_date, spot_date)[spot_date]

            user = User.objects.get(id=user_id)
            order = Order.objects.create(
                amount=amount,
                bot_id=bot_id,
                created=pd.Timestamp(spot_date),
                margin=2,
                order_type="apps",
                price=price,
                qty=floor(amount / price),
                side="buy",
                ticker_id=ticker,
                user_id_id=user_id,
                user_id=user,
            )
            if order:
                order.placed = True
                order.placed_at = pd.Timestamp(spot_date)
                order.save()
            if order.status == "review":
                order.status = "pending"
                order.save()
            if order.status == "pending":
                order.status = "filled"
                order.filled_at = pd.Timestamp(spot_date)
                order.save()
            performance = PositionPerformance.objects.get(order_uid=order.order_uid)
            position = OrderPosition.objects.get(pk=performance.position_uid_id)
            if("CLASSIC" in bot_id):
                classic_position_check(position.position_uid, tac=True)
            if("UNO" in bot_id):
                uno_position_check(position.position_uid, tac=True)
            if("UCDC" in bot_id):
                ucdc_position_check(position.position_uid, tac=True)
            pass

