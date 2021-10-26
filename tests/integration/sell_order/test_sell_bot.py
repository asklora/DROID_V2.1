from datetime import datetime

import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.user.models import Accountbalance, User
from tests.utils.order import confirm_order, create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_new_sell_order_for_user_with_classic_bot(user) -> None:
    price = 6.11

    # We create an order
    buy_order = create_buy_order(
        ticker="1199.HK",
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    confirm_order(buy_order)

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    sellPosition, sell_order = sell_position_service(
        # Selling in different price point (1317 + 13 = 1330 here)
        price + 3.0,
        datetime.now(),
        position.position_uid,
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
    assert user_balance != previous_user_balance


def test_create_new_sell_order_for_user_with_uno_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    confirm_order(buy_order)

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
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
    assert user_balance != previous_user_balance


def test_create_new_sell_order_for_user_with_ucdc_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    confirm_order(buy_order)

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
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
    assert user_balance != previous_user_balance
