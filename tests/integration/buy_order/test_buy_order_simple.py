from datetime import datetime

import pytest
from core.orders.models import Order
from core.user.models import Accountbalance

from tests.utils.order import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_simple_order(user) -> None:
    """
    A new order should be created with default values for is_init, placed,
    status, dates, etc.
    """

    order = create_buy_order(
        user_id=user.id,
        ticker="0780.HK",
        price=1317,
    )

    assert order.is_init is True
    assert order.placed is False
    assert order.status == "review"
    assert order.placed_at is None
    assert order.filled_at is None
    assert order.canceled_at is None
    assert order.amount == 131700
    assert order.price == 1317
    assert order.qty == 100  # from order.amount divided by order.price


def test_create_new_buy_order_for_user(user) -> None:
    """
    A new BUY order should be created with empty setup
    """

    order = create_buy_order(
        ticker="0005.HK",
        price=1317,
        user_id=user.id,
        bot_id="STOCK_stock_0",
    )

    assert order.side == "buy"
    assert order.setup is None  # should be empty


def test_update_new_buy_order_for_user(user) -> None:
    """
    A new BUY order's status will be set to PENDING and the price is deducted
    from USER balance
    #NOTE : after test must clean
    """

    side = "buy"
    ticker = "0005.HK"
    qty = 3
    price = 1317
    bot_id = "STOCK_stock_0"

    order = Order.objects.create(
        amount=price * qty,
        bot_id=bot_id,
        order_type="apps",
        price=price,
        qty=qty,
        side=side,
        ticker_id=ticker,
        user_id=user,
    )

    previous_user_balance = Accountbalance.objects.get(user=user)

    # harusnya masih tetap
    print("User balance before: ", previous_user_balance.amount)

    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    user_balance = Accountbalance.objects.get(user=user)
    print("User balance after: ", user_balance.amount)

    order = Order.objects.get(pk=order.order_uid)

    assert order.amount == price * qty
    assert user_balance.amount == previous_user_balance.amount - order.amount
