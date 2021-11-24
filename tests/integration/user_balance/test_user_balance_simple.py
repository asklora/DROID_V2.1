from datetime import datetime
from random import choice

import pytest
from core.orders.models import Order
from core.user.models import Accountbalance

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_check_if_user_balance_is_cut_accordingly(
    user,
    tickers,
) -> None:
    """
    A new buy order will be created and filled, and user balance is deducted
    with the same nominal as the order amount.
    Margin calculation should not cut the user balance.
    """

    side = "buy"
    ticker, price = choice(tickers).values()
    qty = 20000
    bot_id = "STOCK_stock_0"

    # Save initial user balance
    user_balance = Accountbalance.objects.get(user=user)
    initial_user_balance = user_balance.amount

    # Create the order
    order = Order.objects.create(
        bot_id=bot_id,
        order_type="apps",
        price=price,
        side=side,
        ticker_id=ticker,
        user_id=user,
        qty=qty,
    )

    # The user balance should be untouched
    user_balance = Accountbalance.objects.get(user=user)
    print("User balance before order is filled: ", user_balance.amount)
    assert user_balance.amount == 200000

    # We place the order, deducting the amount from user's balance
    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    # Let's confirm this
    user_balance = Accountbalance.objects.get(user=user)
    print("User balance after order is filled: ", user_balance.amount)

    assert user_balance.amount == round(initial_user_balance - order.amount)


def test_check_if_user_balance_is_cut_accordingly_with_margin(
    user,
    tickers,
) -> None:
    """
    A new buy order will be created and filled, and user balance is deducted
    with the same nominal as the order amount.
    Margin calculation should not cut the user balance.
    """

    side = "buy"
    ticker, price = choice(tickers).values()
    amount = 20000 * price
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
