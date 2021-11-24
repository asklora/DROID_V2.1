import random
from typing import List

import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.universe.models import Universe
from core.user.convert import ConvertMoney
from tests.utils.order import confirm_order, create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_buy_order_with_conversion(user):
    # We get the tickers
    usd_tickers: List[Universe] = Universe.objects.filter(
        currency_code="USD",
        is_active=True,
    ).values_list("ticker", flat=True)

    # We turn them into list of tickers
    tickers: List[str] = [str(elem) for elem in list(usd_tickers)]

    ticker: str = random.choice(tickers)
    price: float = 60.93
    amount: float = 10000

    # the one we are testing
    conversion = ConvertMoney(user.currency, "USD")
    conversion_result = conversion.convert(amount)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=amount,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    confirm_order(order)

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid_id=order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    print(f"Conversion result: {conversion_result}")
    print(f"Position investment amt: {position.investment_amount}")

    assert conversion_result == position.investment_amount
