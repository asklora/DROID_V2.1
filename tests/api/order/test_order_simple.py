from random import choice

import pytest
from core.orders.models import Order
from tests.utils.order import (
    FeatureManager,
    confirm_order,
    get_position_performance,
)

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_api_create_buy_order(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers)

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "STOCK_stock_0",
        "amount": 10000,
        "user": user.id,
        "side": "buy",
    }

    response = client.post(
        path="/api/order/create/",
        data=data,
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()
    print(order)

    assert order is not None
    assert order["order_uid"] is not None

    # we check it from the backend side
    order = Order.objects.get(pk=order["order_uid"])
    assert order is not None


def test_api_create_sell_order(
    authentication,
    client,
    same_day_sell_feature,
    tickers,
    user,
) -> None:
    ticker, price = choice(tickers)

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "STOCK_stock_0",
        "amount": 10000,
        "user": user.id,
        "side": "buy",
    }

    buy_response = client.post(
        path="/api/order/create/",
        data=data,
        **authentication,
    )

    if (
        buy_response.status_code != 201
        or buy_response.headers["Content-Type"] != "application/json"
    ):
        assert False

    buy_order = buy_response.json()

    assert buy_order is not None
    assert buy_order["order_uid"] is not None

    # we check it from the backend side
    buy_order = Order.objects.get(pk=buy_order["order_uid"])
    assert buy_order is not None

    # And we confirm it
    confirm_order(buy_order)

    # we get the position uid from the database
    position, _ = get_position_performance(buy_order)
    assert position

    # Before selling, we disable the same-day selling feature
    feature_manager: FeatureManager = FeatureManager(same_day_sell_feature)
    feature_manager.deactivate()

    # we create the sell order
    sell_response = client.post(
        path="/api/order/create/",
        data={
            "user": user.id,
            "side": "sell",
            "ticker": buy_order.ticker,
            "setup": '{{"position": "{0}"}}'.format(position.position_uid),
        },
        **authentication,
    )

    if (
        sell_response.status_code != 201
        or sell_response.headers["Content-Type"] != "application/json"
    ):
        assert False

    sell_order = sell_response.json()

    feature_manager.activate()

    assert sell_order is not None
    assert sell_order["order_uid"] is not None
