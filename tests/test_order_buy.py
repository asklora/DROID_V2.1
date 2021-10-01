from datetime import datetime

import pytest
from core.orders.models import Order
from core.user.models import Accountbalance

from tests.utils import create_buy_order

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
        ticker="3377.HK",
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
    ticker = "3377.HK"
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


def test_check_if_user_balance_is_cut_accordingly_with_margin(user) -> None:
    """
    A new buy order will be created and filled, and user balance is deducted
    with the same nominal as the order amount.
    Margin calculation should not cut the user balance.
    """

    side = "buy"
    ticker = "3377.HK"
    amount = 131700
    price = 1317
    margin = 2
    bot_id = "UNO_OTM_007692"

    # Save initial user balance
    user_balance = Accountbalance.objects.get(user=user)
    initial_user_balance = user_balance.amount

    # Create the order
    order = Order.objects.create(
        amount=amount,
        bot_id=bot_id,
        margin=margin,
        order_type="apps",
        price=price,
        side=side,
        ticker_id=ticker,
        user_id=user,
    )

    # The amount and qty should be calculated correctly
    print(f"Ordered amount: {amount}")
    print(f"Calculated amount: {order.amount}")
    print(f"Ordered qty: {amount / price}")
    print(f"Calculated qty: {order.qty}")

    # The user balance should be untouched
    user_balance = Accountbalance.objects.get(user=user)
    print("User balance before order is filled: ", user_balance.amount)

    # We place the order, deducting the amount from user's balance
    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    # Let's confirm this
    user_balance = Accountbalance.objects.get(user=user)
    print("User balance after order is filled: ", user_balance.amount)

    order = Order.objects.get(pk=order.order_uid)

    assert user_balance.amount == initial_user_balance - amount


def test_create_new_buy_order_for_classic_bot(user) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "CLASSIC_classic_007692"

    order = create_buy_order(
        bot_id=bot_id,
        price=1317,
        ticker="3377.HK",
        user_id=user.id,
    )

    print(order.setup)

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id


def test_create_new_buy_order_for_uno_bot(user) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "UNO_OTM_007692"

    order = create_buy_order(
        bot_id=bot_id,
        price=1317,
        ticker="3377.HK",
        user_id=user.id,
    )

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id


def test_create_new_buy_order_for_ucdc_bot(user) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "UCDC_ATM_007692"

    order = create_buy_order(
        bot_id=bot_id,
        price=1317,
        ticker="3377.HK",
        user_id=user.id,
    )

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id
