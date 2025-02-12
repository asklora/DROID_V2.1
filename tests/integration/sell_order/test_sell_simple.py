from datetime import datetime
from random import choice

import pytest
from core.orders.factory.orderfactory import OrderController, SellOrderProcessor
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import Accountbalance, User
from tests.utils.order import (
    FeatureManager,
    MockGetterPrice,
    confirm_order,
    create_buy_order,
)

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_new_sell_order_for_user(
    same_day_sell_feature,
    tickers,
    user,
) -> None:
    """
    A new SELL order should be created from a buy order
    """

    ticker, price = choice(tickers)

    # We create an order
    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
    )

    # We set it as filled
    confirm_order(buy_order)

    # We get the order to update the data
    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    # Check if it successfully is added to the performance table
    assert confirmed_buy_order.performance_uid is not None

    # We get the position and performance data to update it
    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    # We confirm the above test
    assert performance is not None

    # We then get the position based on the performance data
    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    # We confirm if the position is set
    assert position is not None

    # Before selling, we disable the same-day selling feature
    feature_manager: FeatureManager = FeatureManager(same_day_sell_feature)
    feature_manager.deactivate()

    # We create the sell order
    latest_price: float = buy_order.price + (buy_order.price * 0.25)

    order_payload: dict = {
        "setup": {"position": position.position_uid},
        "side": "sell",
        "ticker": buy_order.ticker,
        "user_id": buy_order.user_id,
        "margin": buy_order.margin,
    }

    controller: OrderController = OrderController()

    sell_order: Order = controller.process(
        SellOrderProcessor(
            order_payload,
            getterprice=MockGetterPrice(
                price=latest_price,
            ),
        ),
    )

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid is not None
    confirm_order(confirmed_sell_order)

    # We confirm that the selling is successfully finished
    # by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount

    feature_manager.activate()

    assert user_balance != previous_user_balance
