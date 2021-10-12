from typing import Union

import pytest
from core.orders.models import Order
from tests.utils.order import confirm_order
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
) -> None:
    def create_order() -> Union[dict, None]:
        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": "3377.HK",
                "price": 1.63,
                "bot_id": "STOCK_stock_0",
                "amount": 100,
                "margin": 1,
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


def test_api_create_duplicated_sell_orders(
    authentication,
    client,
    user,
) -> None:
    response = client.post(
        path="/api/order/create/",
        data={
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "STOCK_stock_0",
            "amount": 100,
            "margin": 1,
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

    # info
    from django.db import connection

    db_name = connection.settings_dict
    print(db_name)

    # we confirm the order
    buy_order = Order.objects.get(pk=order["order_uid"])
    assert buy_order is not None
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

    sell_order = sell_response_1.json()
    assert sell_order is not None
    assert sell_order["order_uid"] is not None

    # this should fail and return None
    sell_response_2 = client.post(
        path="/api/order/create/",
        data=sell_order_data,
        **authentication,
    )

    assert sell_response_2.status_code != 201
    assert sell_response_2.json() is None


def test_duplicated_pending_buy_order_celery(
    authentication,
    client,
    user,
    mocker,
) -> None:
    def mock_order_executor(data):
        print(data)

    order_executor_mock = mocker.patch(
        "core.orders.serializers.OrderActionSerializer.create",
        wraps=mock_order_executor,
    )

    response = client.post(
        path="/api/order/create/",
        data={
            "ticker": "1109.HK",
            "price": 31.9,
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
        assert False

    response_body = response.json()
    assert response_body

    confirmed_order = client.post(
        path="/api/order/action/",
        data={
            "order_uid": response_body["order_uid"],
            "status": "placed",
            "firebase_token": "",
        },
        **authentication,
    )

    if (
        confirmed_order.status_code != 200
        or confirmed_order.headers["Content-Type"] != "application/json"
    ):
        return None

    return confirmed_order.json()

    assert order_executor_mock.assert_called_once()
