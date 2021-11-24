from django.core.management.base import BaseCommand
from core.orders.models import Order,OrderPosition,PositionPerformance
from core.master.models import MasterOhlcvtr
from portfolio import ucdc_position_check,uno_position_check
from datetime import datetime
from core.user.models import User
import pandas as pd




def create_buy_order(
    created:datetime,
    price: float,
    ticker: str,
    amount: float = 20000,
    bot_id: str = "STOCK_stock_0",
    margin: int = 1,
    # qty: int = 100,
    user_id: int = None,
    user: User = None,
) -> Order:
    return Order.objects.create(
        created=created,
        amount=amount,
        bot_id=bot_id,
        margin=margin,
        order_type="apps",  # to differentiate itself from FELS's orders
        price=price,
        # qty=qty,
        side="buy",
        ticker_id=ticker,
        # user_id_id=user_id,
        user_id=user_id,
    )




class Command(BaseCommand):
    def handle(self, *args, **options):
        user = User.objects.get(id=1509)
        
        ticker = "2331.HK"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-03-31",
        )
        price = master.close
        # log_time = datetime.combine(master.trading_day, datetime.min.time())
        log_time = datetime.now()

        buy_order = create_buy_order(
            created=log_time,
            ticker=ticker,
            price=price,
            user_id=user,
            margin=2,
            bot_id="UNO_OTM_05",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = log_time
        buy_order.save()
        buy_order.status = "pending"
        buy_order.save()



        # buy_order.status = "filled"
        # buy_order.filled_at = log_time
        # buy_order.save()

        # confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        # performance = PositionPerformance.objects.get(
        #     order_uid_id=confirmed_buy_order.order_uid
        # )

        # position: OrderPosition = OrderPosition.objects.get(
        #     pk=performance.position_uid_id
        # )

        # step 2: setup hedge
        # uno_position_check(
        #     position_uid='50666e6827e946009b602fc586c0c957',
        #     tac=True,
        #     to_date='2021-09-29'
        # )
        # # step 3: get hedge positions
        # performance = PositionPerformance.objects.filter(
        #     position_uid=position.position_uid
        # )