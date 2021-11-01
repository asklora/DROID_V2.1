from datetime import datetime

import pytest
from core.orders.models import Order
from tests.utils.order import confirm_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_api_edit_order_status(
    authentication,
    client,
    order,
) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    data = {
        "order_uid": order["order_uid"],
        "status": "placed",
        "firebase_token": "",
    }

    response = client.post(
        path="/api/order/action/",
        data=data,
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()
    assert response_body["order_uid"] == order["order_uid"]
    assert (
        response_body["status"] == "executed"
    )  # The modification has been... executed


def test_api_get_order_detail(authentication, client, order) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    response = client.get(
        path=f"/api/order/get/{order['order_uid']}/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()
    assert response_body["order_uid"] is not None
    assert response_body["ticker"] == order.ticker
    assert response_body["price"] == order.price
    assert (
        # Amount was cut to fit maximum share number
        response_body["amount"]
        != 100
    )


def test_api_get_user_orders(authentication, client, order) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    currentOrder = Order.objects.get(pk=order["order_uid"])

    assert currentOrder is not None

    confirm_order(currentOrder)

    response = client.get(
        path="/api/order/getall/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()
    assert len(response_body) > 0


def test_api_get_user_positions(
    authentication,
    client,
    order,
    user,
) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    currentOrder = Order.objects.get(pk=order["order_uid"])

    assert currentOrder is not None

    confirm_order(currentOrder)

    response = client.get(
        path=f"/api/order/position/{user.id}/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()

    assert response_body["count"] > 0
    assert len(response_body["results"]) == response_body["count"]


def test_api_get_order_position_details(
    authentication,
    client,
    order,
    user,
) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    currentOrder = Order.objects.get(pk=order["order_uid"])

    assert currentOrder is not None

    confirm_order(currentOrder)

    response = client.get(
        path=f"/api/order/position/{user.id}/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    # We take the first (and only) position record for this user
    position = response.json()["results"][0]

    # We request this position details
    response = client.get(
        path=f"/api/order/position/{position['position_uid']}/details/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()

    assert response_body is not None
    assert response_body["entry_price"] == order["price"]


def test_api_get_order_performance(
    authentication,
    client,
    order,
    user,
) -> None:
    """Order is taken from the fixture so we dont have to create another"""

    currentOrder = Order.objects.get(pk=order["order_uid"])

    assert currentOrder is not None

    confirm_order(currentOrder)

    response = client.get(
        path=f"/api/order/position/{user.id}/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    position = response.json()["results"][0]

    response = client.get(
        path=f"/api/order/performance/{position['position_uid']}/",
        **authentication,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    # We get the first performance record
    response_body = response.json()[0]

    assert response_body is not None
    assert response_body["initial_investment_amt"] == order["amount"]
