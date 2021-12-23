from random import choice
import pytest
from rest_framework import exceptions
from core.orders.models import Order
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


def test_order_no_instant_sell(
    authentication,
    client,
    mocker,
    tickers,
    user,
) -> None:
    ticker, price = choice(tickers)
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
    buy_order: Order = Order.objects.get(pk=order["order_uid"])
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

    # this should fail and return None
    sell_response = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    print(sell_response.json())

    assert sell_response.status_code != 201

    sell_response_body = sell_response.json()
    assert (
        sell_response_body.get("detail")
        == "You can only sell the order in the next trading day"
    )
