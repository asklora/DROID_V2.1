from random import choice
from typing import Union

import pytest
from core.orders.models import Order
from tests.utils.market import check_market, close_market, open_market
from tests.utils.mocks import mock_order_serializer
from tests.utils.order import confirm_order, confirm_order_api
from tests.utils.position_performance import get_position_performance

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_api_create_duplicated_buy_orders(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers).values()

    def create_order() -> Union[dict, None]:
        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": ticker,
                "price": price,
                "bot_id": "STOCK_stock_0",
                "amount": 10000,
                "margin": 2,
                "user": user.id,
                "side": "buy",
            },
            **authentication,
        )

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        return response.json()

    order_1 = create_order()
    assert order_1 is not None

    # info
    from django.db import connection

    db_name = connection.settings_dict
    print(db_name)

    # we confirm the order
    buy_order = Order.objects.get(pk=order_1["order_uid"])
    assert buy_order is not None
    confirm_order(buy_order)

    # This should fail and return None
    order_2 = create_order()
    assert order_2 is None


def test_api_create_duplicated_pending_sell_orders(
    authentication,
    client,
    mocker,
    user,
    tickers,
) -> None:
    # mock all the things!
    mocker.patch(
        "core.orders.serializers.OrderActionSerializer.create",
        wraps=mock_order_serializer,
    )
    mocker.patch(
        "core.services.order_services.asyncio.run",
    )

    ticker, price = choice(tickers).values()

    response = client.post(
        path="/api/order/create/",
        data={
            "ticker": ticker,
            "price": price,
            "bot_id": "UCDC_ATM_007692",
            "amount": 10000,
            "margin": 2,
            "user": user.id,
            "side": "buy",
        },
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()
    assert order is not None

    # we confirm the order
    buy_order = Order.objects.get(pk=order["order_uid"])
    assert buy_order is not None
    assert str(buy_order.order_uid).replace("-", "") == order["order_uid"]

    # if the market is closed, the order won't be filled
    market_is_open = check_market(buy_order.ticker.mic)
    if not market_is_open:
        open_market(buy_order.ticker.mic)

    confirm_order_api(
        order["order_uid"],
        client,
        authentication,
    )

    position, _ = get_position_performance(buy_order.order_uid)
    assert position

    # we create the sell order
    sell_order_data = {
        "user": user.id,
        "side": "sell",
        "ticker": buy_order.ticker,
        "setup": '{{"position": "{0}"}}'.format(position.position_uid),
    }

    sell_response_1 = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    if (
        sell_response_1.status_code != 201
        or sell_response_1.headers["Content-Type"] != "application/json"
    ):
        assert False

    sell_order_1 = sell_response_1.json()
    assert sell_order_1 is not None
    assert sell_order_1["order_uid"] is not None

    # We set the market to be closed, if it's opened
    close_market(buy_order.ticker.mic)

    sell_order = Order.objects.get(pk=sell_order_1["order_uid"])
    confirm_order_api(sell_order.order_uid, client, authentication)

    print(f"sell order status: {sell_order.status}")

    # this should fail and return None
    sell_response_2 = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    print(sell_response_2.json())

    if market_is_open:
        open_market(buy_order.ticker.mic)

    sell_order_2 = sell_response_2.json()

    assert sell_response_2.status_code != 201
    assert (
        sell_order_2["detail"]
        == f"sell order already exists for this position, order id : {sell_order_1['order_uid']}, current status pending"
    )


def test_api_create_duplicated_filled_sell_orders(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers).values()

    response = client.post(
        path="/api/order/create/",
        data={
            "ticker": ticker,
            "price": price,
            "bot_id": "UCDC_ATM_007692",
            "amount": 10000,
            "margin": 2,
            "user": user.id,
            "side": "buy",
        },
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()
    assert order is not None

    # we confirm the order
    buy_order = Order.objects.get(pk=order["order_uid"])
    assert buy_order is not None
    assert buy_order.order_uid.hex == order["order_uid"]

    confirm_order(buy_order)

    position, _ = get_position_performance(buy_order.order_uid)
    assert position

    # we create the sell order
    sell_order_data = {
        "user": user.id,
        "side": "sell",
        "ticker": buy_order.ticker,
        "setup": '{{"position": "{0}"}}'.format(position.position_uid),
    }

    sell_response_1 = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    if (
        sell_response_1.status_code != 201
        or sell_response_1.headers["Content-Type"] != "application/json"
    ):
        assert False

    sell_order_1 = sell_response_1.json()
    assert sell_order_1 is not None
    assert sell_order_1["order_uid"] is not None

    sell_order = Order.objects.get(pk=sell_order_1["order_uid"])
    confirm_order(sell_order)

    # this should fail and return None
    sell_response_2 = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    print(sell_response_2.json())

    sell_order_2 = sell_response_2.json()

    assert sell_response_2.status_code != 201
    assert sell_order_2["detail"] == "position, has been closed"


def test_duplicated_pending_buy_order_celery(
    authentication,
    client,
    mocker,
    user,
    tickers,
) -> None:
    # mock all the things!
    mocker.patch(
        "core.orders.serializers.OrderActionSerializer.create",
        wraps=mock_order_serializer,
    )
    mocker.patch(
        "core.services.order_services.asyncio.run",
    )

    # utility function to create a new order
    ticker, price = choice(tickers).values()

    def create_order() -> Union[dict, None]:
        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": ticker,
                "price": price,
                "bot_id": "CLASSIC_classic_003846",
                "amount": 20000,  # 10.000 HKD more than the user can afford
                "margin": 2,
                "user": user.id,
                "side": "buy",
            },
            **authentication,
        )

        print(response.json())

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        return response.json()

    order_1 = create_order()
    assert order_1

    # we confirm the order
    buy_order = Order.objects.get(pk=order_1["order_uid"])
    assert buy_order is not None
    assert str(buy_order.order_uid).replace("-", "") == order_1["order_uid"]

    # if the market is closed, the order won't be filled
    market_is_open = check_market(buy_order.ticker.mic)
    if not market_is_open:
        open_market(buy_order.ticker.mic)

    # we confirm the above order
    confirmed_order = confirm_order_api(
        order_1["order_uid"],
        client,
        authentication,
    )

    # make sure we mocked the function alright
    assert confirmed_order["status"] == "executed in mock"

    # we create a new order while it's still pending
    order_2 = create_order()
    assert not order_2
