import time
from random import choice
from typing import Union

import pytest
from core.orders.models import Order

# from core.orders.factory.orderfactory import
from rest_framework import exceptions
from tests.utils.market import check_market, close_market, open_market
from tests.utils.mocks import (
    mock_buy_validate,
    mock_order_action_serializer,
    mock_sell_validate,
)
from tests.utils.order import confirm_order, confirm_order_api, get_position_performance

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_duplicated_pending_buy_order(
    authentication,
    client,
    mocker,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers).values()
    bot_id: str = "CLASSIC_classic_003846"

    # utility function to create a new order
    def create_order() -> Union[dict, None]:
        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": ticker,
                "price": price,
                "bot_id": bot_id,
                "amount": 20000,
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
    first_order = Order.objects.get(pk=order_1.get("order_uid"))
    assert first_order is not None
    assert first_order.order_uid.hex == order_1.get("order_uid")

    # we close the market to make it pending
    market_is_initially_open: bool = check_market(first_order.ticker.mic)
    if market_is_initially_open:
        close_market(first_order.ticker.mic)

    # we confirm the above order
    mocker.patch(
        "core.orders.serializers.OrderActionSerializer.create",
        wraps=mock_order_action_serializer,
    )
    mock_buy_validator = mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_notification"
    )
    mock_buy_validator = mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_response"
    )
    confirm_order_api(
        order_1.get("order_uid"),
        client,
        authentication,
    )

    time.sleep(30)

    # we see if its really pending
    first_order = Order.objects.get(pk=order_1.get("order_uid"))
    assert first_order.status == "pending"

    with pytest.raises(exceptions.NotAcceptable):
        mock_buy_validator = mocker.patch(
            "core.orders.factory.orderfactory.BuyValidator"
        )
        mock_buy_validator.validate = mock_buy_validate(
            user=user,
            ticker=ticker,
            bot_id=bot_id,
        )

        # we create a new order while it's still pending
        order_2 = create_order()
        assert not order_2

        if market_is_initially_open:
            open_market(first_order.ticker.mic)


def test_duplicated_filled_buy_order(
    authentication,
    client,
    mocker,
    tickers,
    user,
) -> None:
    ticker, price = choice(tickers).values()
    bot_id: str = "STOCK_stock_0"

    def create_order() -> Union[dict, None]:
        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": ticker,
                "price": price,
                "bot_id": bot_id,
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
    print(order_1)
    assert order_1 is not None

    # we confirm the order
    buy_order = Order.objects.get(pk=order_1["order_uid"])
    assert buy_order is not None
    confirm_order(buy_order)

    with pytest.raises(exceptions.NotAcceptable):
        mock_buy_validator = mocker.patch(
            "core.orders.factory.orderfactory.BuyValidator"
        )
        mock_buy_validator.validate = mock_buy_validate(
            user=user,
            ticker=ticker,
            bot_id=bot_id,
        )

        # we create a new order while it's still pending
        order_2 = create_order()
        assert order_2 is None


def test_duplicated_pending_sell_order(
    authentication,
    client,
    mocker,
    tickers,
    user,
) -> None:
    ticker, price = choice(tickers).values()
    bot_id: str = "UCDC_ATM_007692"

    response = client.post(
        path="/api/order/create/",
        data={
            "ticker": ticker,
            "price": price,
            "bot_id": bot_id,
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

    # if the market is closed, the order won't be filled
    market_is_initially_open = check_market(buy_order.ticker.mic)
    if not market_is_initially_open:
        open_market(buy_order.ticker.mic)

    mocker.patch(
        "core.orders.serializers.OrderActionSerializer.create",
        wraps=mock_order_action_serializer,
    )
    mock_buy_validator = mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_notification"
    )
    mock_buy_validator = mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_response"
    )
    confirm_order_api(
        order["order_uid"],
        client,
        authentication,
    )

    position, _ = get_position_performance(buy_order)
    assert position

    # we create the sell order
    sell_order_data = {
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

    confirm_order_api(sell_order_1["order_uid"], client, authentication)

    sell_order = Order.objects.get(pk=sell_order_1["order_uid"])
    print(f"sell order status: {sell_order.status}")
    assert sell_order.status == "pending"

    with pytest.raises(exceptions.NotAcceptable):
        mock_buy_validator = mocker.patch(
            "core.orders.factory.orderfactory.BuyValidator"
        )
        mock_buy_validator.validate = mock_sell_validate(
            user=user,
            ticker=ticker,
            bot_id=bot_id,
        )

        # this should fail and return None
        sell_response_2 = client.post(
            path="/api/order/create/",
            data=sell_order_data,
            **authentication,
        )

        print(sell_response_2.json())

        if market_is_initially_open:
            open_market(buy_order.ticker.mic)

        sell_order_2 = sell_response_2.json()

        assert sell_response_2.status_code != 201
        assert (
            sell_order_2["detail"]
            == f"sell order already exists for this position, order id : {sell_order_1['order_uid']}, current status pending"
        )


def test_duplicated_filled_sell_order(
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

    position, _ = get_position_performance(buy_order)
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
