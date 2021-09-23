from datetime import datetime
from typing import Union

import pytest
from bot.calculate_bot import check_date, get_expiry_date
from core.bot.models import BotOptionType
from core.orders.models import Order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


class TestAPIAuth:
    def test_api_token_verify(self, client, authentication) -> None:
        token = authentication["HTTP_AUTHORIZATION"].split("Bearer ")[1]

        response = client.post(path="/api/auth/verify/", data={"token": token})

        # API only returns the status code with empty body
        assert response.status_code == 200

    def test_api_token_revoke(self, user, client) -> None:
        response = client.post(
            path="/api/auth/",
            data={
                "email": user.email,
                "password": "everything_is_but_a_test",
            },
        )

        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        response_body = response.json()
        refresh_token = response_body["refresh"]
        headers = {"HTTP_AUTHORIZATION": "Bearer " + response_body["access"]}

        response = client.post(
            path="/api/auth/revoke/",
            data={"token": refresh_token},
            **headers,
        )

        assert response.status_code == 205
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["message"] == "token revoked"


class TestAPIUser:
    def test_api_get_user_data(self, authentication, client, user) -> None:
        response = client.get(path="/api/user/me/", **authentication)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["email"] == user.email
        assert response_body["username"] == user.username
        assert response_body["is_active"]


class TestAPIOrder:
    def test_api_create_order(self, authentication, client, user) -> None:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "STOCK_stock_0",
            "amount": 100,
            "user": user.id,
            "side": "buy",
        }

        response = client.post(path="/api/order/create/", data=data, **authentication)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False

        order = response.json()

        assert order is not None
        assert order["order_uid"] is not None
        assert order["amount"] == 99.43  # adjusted to the maximum purchasable stocks

    def test_api_create_order_with_margin(self, authentication, client, user) -> None:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "UNO_ITM_003846",
            "amount": 100,
            "user": user.id,
            "side": "buy",
            "margin": 2,
        }

        response = client.post(path="/api/order/create/", data=data, **authentication)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False

        order = response.json()

        assert order is not None
        assert order["order_uid"] is not None
        # confirm if the qty is correctly counted with margin
        assert order["qty"] != 100.0

    def test_api_create_order_with_classic_bot(
        self, authentication, client, user
    ) -> None:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "CLASSIC_classic_003846",
            "amount": 100,
            "user": user.id,
            "side": "buy",
        }

        response = client.post(path="/api/order/create/", data=data, **authentication)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False

        order = response.json()

        assert order is not None
        assert order["order_uid"] is not None
        assert order["qty"] == order["setup"]["performance"]["share_num"]
        # confirm if the setup is not empty
        assert order["setup"] is not None

        # confirm if the expiry is set correctly
        bot = BotOptionType.objects.get(bot_id="CLASSIC_classic_003846")

        expiry = get_expiry_date(bot.time_to_exp, order["created"], "HKD", apps=True)
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
        assert order["setup"]["position"]["expiry"] == expiry_date

    def test_api_create_order_with_uno_bot(self, authentication, client, user) -> None:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "UNO_ITM_003846",
            "amount": 100,
            "user": user.id,
            "side": "buy",
            "margin": 2,
        }

        response = client.post(path="/api/order/create/", data=data, **authentication)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False

        order = response.json()

        assert order is not None
        assert order["order_uid"] is not None
        # confirm if the qty is correctly counted with margin
        assert order["qty"] == order["setup"]["performance"]["share_num"]
        # confirm if the setup is not empty
        assert order["setup"] is not None

        # confirm if the expiry is set correctly
        bot = BotOptionType.objects.get(bot_id="UNO_ITM_003846")

        expiry = get_expiry_date(bot.time_to_exp, order["created"], "HKD", apps=True)
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
        assert order["setup"]["position"]["expiry"] == expiry_date

    def test_api_create_order_with_ucdc_bot(self, authentication, client, user) -> None:
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "UCDC_ATM_003846",
            "amount": 100,
            "user": user.id,
            "side": "buy",
            "margin": 2,
        }

        response = client.post(path="/api/order/create/", data=data, **authentication)

        if (
            response.status_code != 201
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False

        order = response.json()

        assert order is not None
        assert order["order_uid"] is not None
        # confirm if the qty is correctly counted with margin
        assert order["qty"] == order["setup"]["performance"]["share_num"]
        # confirm if the setup is not empty
        assert order["setup"] is not None

        # confirm if the expiry is set correctly
        bot = BotOptionType.objects.get(bot_id="UCDC_ATM_003846")

        expiry = get_expiry_date(bot.time_to_exp, order["created"], "HKD", apps=True)
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
        assert order["setup"]["position"]["expiry"] == expiry_date

    def test_api_create_duplicated_orders(self, authentication, client, user) -> None:
        def create_order() -> Union[dict, None]:
            response = client.post(
                path="/api/order/create/",
                data={
                    "ticker": "3377.HK",
                    "price": 1.63,
                    "bot_id": "STOCK_stock_0",
                    "amount": 100,
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

        order1 = create_order()
        assert order1 is not None
        # We then set it to filled

        # First, let's find the order
        from django.db import connection

        db_name = connection.settings_dict
        print(db_name)
        currentOrder = Order.objects.get(pk=order1["order_uid"])
        assert currentOrder is not None

        # And we set it
        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

        # This should fail and return None
        order2 = create_order()
        assert order2 is None

    def test_api_edit_order_status(self, authentication, client, order) -> None:
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

    def test_api_get_order_detail(self, authentication, client, order) -> None:
        """Order is taken from the fixture so we dont have to create another"""

        response = client.get(
            path=f"/api/order/get/{order['order_uid']}/",
            **authentication,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["order_uid"] is not None
        assert response_body["ticker"] == "3377.HK"
        assert response_body["price"] == 1.63
        assert (
            # Amount was cut to fit maximum share number
            response_body["amount"]
            != 100
        )

    def test_api_get_user_orders(self, authentication, client, order) -> None:
        """Order is taken from the fixture so we dont have to create another"""

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder is not None

        currentOrder.status = "placed"
        currentOrder.placed = True
        currentOrder.placed_at = datetime.now()
        currentOrder.save()

        response = client.get(
            path="/api/order/getall/",
            **authentication,
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert len(response_body) > 0

    def test_api_get_user_positions(self, authentication, client, order, user) -> None:
        """Order is taken from the fixture so we dont have to create another"""

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder is not None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

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
        self, authentication, client, order, user
    ) -> None:
        """Order is taken from the fixture so we dont have to create another"""

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder is not None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

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
        self, authentication, client, order, user
    ) -> None:
        """Order is taken from the fixture so we dont have to create another"""

        currentOrder = Order.objects.get(pk=order["order_uid"])

        assert currentOrder is not None

        currentOrder.placed = True
        currentOrder.status = "filled"
        currentOrder.placed_at = datetime.now()
        currentOrder.filled_at = datetime.now()
        currentOrder.save()

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
