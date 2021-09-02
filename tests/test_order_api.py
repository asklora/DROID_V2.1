from datetime import datetime
from typing import Union

import pytest
from core.orders.models import Order


@pytest.mark.django_db
class TestAPI:
    tokens = None
    headers = None

    def authenticate(self, client) -> bool:
        data = {
            "email": "pytest@tests.com",
            "password": "helloworld",
        }

        response = client.post(path="/api/auth/", data=data)

        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            return False

        response_body = response.json()
        self.tokens = response_body
        self.headers = {"HTTP_AUTHORIZATION": "Bearer " + response_body["access"]}
        return True

    def create_order(self, user, client) -> Union[Order, None]:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "STOCK_stock_0",
            "amount": 100,
            "user": user.id,
            "side": "buy",
            "margin": 2,
        }

        response = client.post(path="/api/order/create/", data=data, **self.headers)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        return response.json()

    def test_api_token_verify(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        token = self.headers["HTTP_AUTHORIZATION"].split("Bearer ")[1]

        response = client.post(path="/api/auth/verify/", data={"token": token})

        # API only returns the status code with empty body
        assert response.status_code == 200

    def test_api_token_revoke(self, user, client) -> None:
        assert self.authenticate(client)

        token = self.tokens["refresh"]
        print(token)

        response = client.post(
            path="/api/auth/revoke/",
            data={"token": token},
            **self.headers,
        )

        assert response.status_code == 205
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["message"] == "token revoked"

    def test_api_get_user_data(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        response = client.get(path="/api/user/me/", **self.headers)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["email"] != None

    def test_api_create_order(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        order = self.create_order(user, client)

        assert order != None
        assert order["order_uid"] != None
        assert order["price"] == 1.63

    def test_api_edit_order_status(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order first
        order = self.create_order(user, client)

        assert order != None

        data = {
            "order_uid": order["order_uid"],
            "status": "placed",
            "firebase_token": "",
        }

        response = client.post(
            path="/api/order/action/",
            data=data,
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["order_uid"] == order["order_uid"]
        assert (
            response_body["status"] == "executed"
        )  # The modification has been... executed

    def test_api_get_order_detail(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order
        order = self.create_order(user, client)

        assert order != None

        response = client.get(
            path=f"/api/order/get/{order['order_uid']}/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["order_uid"] != None
        assert response_body["ticker"] == "3377.HK"
        assert response_body["price"] == 1.63
        assert (
            response_body["amount"] != 100
        )  # Amount was cut to fit maximum share number

    def test_api_get_user_orders(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order
        order = self.create_order(user, client)

        assert order != None

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder != None

        currentOrder.status = "placed"
        currentOrder.placed = True
        currentOrder.placed_at = datetime.now()
        currentOrder.save()

        response = client.get(
            path="/api/order/getall/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert len(response_body) > 0

    def test_api_get_user_positions(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order
        order = self.create_order(user, client)

        assert order != None

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder != None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

        response = client.get(
            path=f"/api/order/position/{user.id}/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["count"] > 0
        assert len(response_body["results"]) == response_body["count"]

    def test_api_get_order_performance(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order
        order = self.create_order(user, client)

        assert order != None

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder != None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

        response = client.get(
            path=f"/api/order/position/{user.id}/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        position = response.json()["results"][0]

        response = client.get(
            path=f"/api/order/position/{position['position_uid']}/details/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body != None
        assert response_body["entry_price"] == order["price"]

    def test_api_get_order_position_details(self, user, client) -> None:
        if self.headers is None:
            assert self.authenticate(client)

        # We create the order
        order = self.create_order(user, client)

        assert order != None

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder != None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

        response = client.get(
            path=f"/api/order/position/{user.id}/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        position = response.json()["results"][0]

        response = client.get(
            path=f"/api/order/position/{position['position_uid']}/details/",
            **self.headers,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body != None
        assert response_body["entry_price"] == order["price"]
