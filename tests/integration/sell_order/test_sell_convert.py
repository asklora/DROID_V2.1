from datetime import datetime

import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.user.convert import ConvertMoney
from tests.utils.order import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_sell_order_with_conversion(user):
    ticker: str = "XOM"
    price: float = 60.93
    amount: float = 10000

    # converter to usd
    usd_conversion = ConvertMoney(user.currency, "USD")
    usd_conversion_result = usd_conversion.convert(amount)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=usd_conversion_result,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    order.status = "filled"
    order.filled_at = datetime.now()
    order.save()

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid=order.order_uid,
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    # We create the sell order
    sellPosition, sell_order = sell_position_service(
        price,
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert confirmed_sell_order

    # the one we are testing
    hkd_conversion = ConvertMoney(confirmed_sell_order.amount, "HKD")
    hkd_conversion_result = hkd_conversion.convert(amount)

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    print(confirmed_sell_order.setup)

    assert (
        confirmed_sell_order.setup["position"]["bot_cash_balance"]
        == hkd_conversion_result
    )
