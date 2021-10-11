from datetime import datetime

import pytest
from core.master.models import MasterOhlcvtr
from core.orders.models import Order, OrderPosition, PositionPerformance
from portfolio import classic_position_check
from tests.utils.order import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_should_create_hedge_order_for_classic_bot(user) -> None:
    # step 1: create a new order
    ticker = "6606.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        created=log_time,
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    # step 2: setup hedge
    classic_position_check(
        position_uid=position.position_uid,
        tac=True,
    )
    # step 3: get hedge positions
    performance = PositionPerformance.objects.filter(
        position_uid=position.position_uid,
    )

    print(len(performance))

    assert performance.exists()
    assert len(performance) > 1