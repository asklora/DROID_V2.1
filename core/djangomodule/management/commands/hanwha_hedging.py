from math import floor
from portfolio.daily_hedge_uno import uno_position_check
from core.orders.models import Order, PositionPerformance, OrderPosition
from general.date_process import dateNow
from general.data_process import get_uid
from core.user.models import Accountbalance, TransactionHistory, User, UserDepositHistory
from core.universe.models import Universe
from core.bot.models import BotOptionType
import sys
import pandas as pd
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-run_number", "--run_number", help="run_number", type=int, default=0)

    def handle(self, *args, **options):
        #Create User Part
        # user = User.objects.create_user(
        #     email="hanwha_hedging@loratechai.com",
        #     username="HanwhaHedging",
        #     first_name="Hanwha",
        #     last_name="Hedging",
        #     gender="other",
        #     phone="012345678",
        #     password="HANWHA",
        #     is_active=True,
        #     current_status="verified",
        #     is_joined=False,
        #     is_test=True,
        # )
        # user_balance = Accountbalance.objects.create(
        #     user=user,
        #     amount=0,
        #     currency_code_id="USD",
        # )
        # transaction = TransactionHistory.objects.create(
        #     balance_uid=user_balance,
        #     side="credit",
        #     amount=1000000,
        #     transaction_detail={"event": "first deposit"},
        # )
        # UserDepositHistory.objects.create(
        #     uid=get_uid(user.id, trading_day=dateNow(), replace=True),
        #     user_id=user,
        #     trading_day=dateNow(),
        #     deposit=transaction.amount,
        # )

        ticker = "TSLA.O"
        spot_date = "2021-12-15"
        bot_id="UNO_ITM_007692"
        amount = 200000
        price = 975.99
        user_id = 59105

        # ticker = "TSLA.O"
        # spot_date = "2022-01-03"
        # bot_id="UNO_ITM_007692"
        # amount = 200000
        # price = 1199.78
        # user_id = 59105

        user = User.objects.get(id=user_id)
        order = Order.objects.create(
            amount=amount,
            bot_id=bot_id,
            created=pd.Timestamp(spot_date),
            margin=1,
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
        uno_position_check(position.position_uid, to_date=None, tac=True, hedge=False, latest=False)
        pass