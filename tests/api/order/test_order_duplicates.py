import time
from random import choice
from typing import Union

import pytest
from core.orders.models import Order

# from core.orders.factory.orderfactory import
from rest_framework import exceptions
from tests.utils.market import MarketManager
from tests.utils.mocks import (
    mock_execute_task,
    mock_buy_validate,
    mock_sell_validate,
)
from tests.utils.order import (
    confirm_order,
    confirm_order_api,
    get_position_performance,
)

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
    ticker, price = choice(tickers)
    bot_id: str = "CLASSIC_classic_003846"

    mocker.patch(
        "core.orders.factory.orderfactory.ActionProcessor.execute_task",
        wraps=mock_execute_task,
    )
    mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_notification",
    )
    mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_response",
    )

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
    market_manager: MarketManager = MarketManager(mic=first_order.ticker.mic)
    market_is_initially_open: bool = market_manager.is_open
    if market_is_initially_open:
        market_manager.close()

    # we confirm the above order
    confirm_order_api(
        order_1.get("order_uid", first_order.order_uid.hex),
        client,
        authentication,
    )

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
            market_manager.open()


def test_duplicated_filled_buy_order(
    authentication,
    client,
    mocker,
    tickers,
    user,
) -> None:
    ticker, price = choice(tickers)
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
    ticker, price = choice(tickers)
    bot_id: str = "UCDC_ATM_007692"

    mocker.patch(
        "core.orders.factory.orderfactory.ActionProcessor.execute_task",
        wraps=mock_execute_task,
    )
    mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_notification",
    )
    mocker.patch(
        "core.orders.factory.orderfactory.BaseAction.send_response",
    )

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
    market_manager: MarketManager = MarketManager(mic=buy_order.ticker.mic)
    market_is_initially_open: bool = market_manager.is_open
    if not market_is_initially_open:
        market_manager.open()

    # we confirm the order
    confirm_order_api(
        order["order_uid"],
        client,
        authentication,
    )

    buy_position, _ = get_position_performance(buy_order)
    assert buy_position

    # we create the sell order
    sell_order_data = {
        "side": "sell",
        "ticker": buy_order.ticker,
        "setup": '{{"position": "{0}"}}'.format(buy_position.position_uid),
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
    market_manager.close()

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
            position=buy_position,
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
            market_manager.open()

        sell_order_2 = sell_response_2.json()

        assert sell_response_2.status_code != 201
        assert sell_order_2["detail"] == (
            "sell order already exists for this position, order id : "
            f"{sell_order_1['order_uid']}, current status pending"
        )


def test_duplicated_filled_sell_order(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers)

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
