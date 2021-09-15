from datetime import datetime

import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.user.models import Accountbalance, User

from utils import create_buy_order

pytestmark = pytest.mark.django_db


def test_should_create_new_sell_order_for_user(user) -> None:
    """
    A new SELL order should be created from a buy order
    """

    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
    )

    # We set it as filled
    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

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
        pk=performance.position_uid_id)

    # We confirm if the position is set
    assert position is not None

    # We create the sell order
    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid is not None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # We confirm that the selling is successfully finished by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount
    assert user_balance != previous_user_balance


def test_should_create_new_sell_order_for_user_with_classic_bot(user) -> None:
    price = 6.11

    # We create an order
    buy_order = create_buy_order(
        ticker="1199.HK",
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        # Selling in different price point (1317 + 13 = 1330 here)
        price + 3.0,
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid is not None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # We confirm that the selling is successfully finished by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount
    assert user_balance != previous_user_balance


def test_should_create_new_sell_order_for_user_with_uno_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid is not None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # We confirm that the selling is successfully finished by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount
    assert user_balance != previous_user_balance


def test_should_create_new_sell_order_for_user_with_ucdc_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid is not None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # We confirm that the selling is successfully finished by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount
    assert user_balance != previous_user_balance
