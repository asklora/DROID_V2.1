from random import choice

import pytest
from core.orders.models import Order
from core.user.models import Accountbalance, User
from tests.utils.order import confirm_order, create_buy_order, create_sell_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_new_sell_order_for_user_with_classic_bot(
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers)

    # We create an order
    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    assert buy_order

    # we confirm the order
    confirm_order(buy_order)

    # create sell order here
    sell_order = create_sell_order(buy_order)
    print(sell_order.price)

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
    assert user_balance != previous_user_balance


def test_create_new_sell_order_for_user_with_uno_bot(
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers)

    # We create an order
    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    # we confirm the order
    confirm_order(buy_order)

    # create sell order here
    sell_order = create_sell_order(buy_order)

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
    assert user_balance != previous_user_balance


def test_create_new_sell_order_for_user_with_ucdc_bot(
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers)

    # We create an order
    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    # we confirm the order
    confirm_order(buy_order)

    # create sell order here
    sell_order = create_sell_order(buy_order)

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
    assert user_balance != previous_user_balance
