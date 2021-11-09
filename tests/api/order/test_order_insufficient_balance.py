import time

from random import choice
from typing import Union

import pytest

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_api_create_order_with_insufficient_balance(
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
            "bot_id": "CLASSIC_classic_003846",
            "amount": 210000,  # 10.000 HKD more than the user can afford
            "margin": 2,
            "user": user.id,
            "side": "buy",
        },
        **authentication,
    )

    if (
        response.status_code != 406
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    response_body = response.json()
    assert response_body["detail"] == "insufficient funds"


def test_api_multiple_order_insufficient_balance(
    authentication,
    client,
    user,
    tickers,
) -> None:
    def create_order() -> Union[dict, None]:
        ticker, price = choice(tickers).values()

        response = client.post(
            path="/api/order/create/",
            data={
                "ticker": ticker,
                "price": price,
                "bot_id": "UCDC_ATM_007692",
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

    def confirm_order(order_uid: str) -> Union[dict, None]:
        response = client.post(
            path="/api/order/action/",
            data={
                "order_uid": order_uid,
                "status": "placed",
                "firebase_token": "",
            },
            **authentication,
        )

        print(response.json())

        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        return response.json()

    # we create 9 orders here
    for i in range(9):
        order = create_order()
        assert order

        placed_order = confirm_order(order["order_uid"])
        assert order["order_uid"] == placed_order["order_uid"]
        assert placed_order["status"] == "executed"
        time.sleep(3)

    ticker, price = choice(tickers).values()
    last_order = client.post(
        path="/api/order/create/",
        data={
            "ticker": ticker,
            "price": price,
            "bot_id": "STOCK_stock_0",
            "amount": 20000,
            "margin": 2,
            "user": user.id,
            "side": "buy",
        },
        **authentication,
    )

    if (
        last_order.status_code != 406
        or last_order.headers["Content-Type"] != "application/json"
    ):
        return None

    last_order_body = last_order.json()
    assert last_order_body["detail"] == "insufficient funds"
