import pytest
import requests


class TestAPI:

    pytestmark = pytest.mark.django_db

    headers = None

    def build_url(
        self,
        url: str = "http://0.0.0.0:8000",
        path: str = "/api/order/create/",
    ) -> str:
        return f"{url}{path}"

    def authenticate(
        self,
        email: str = "ata",
        password: str = "123",
    ) -> None:
        url = self.build_url(path="/api/auth/")
        response = requests.post(
            url=url,
            data={
                "email": email,
                "password": password,
            },
        )

        if (
            response.status_code == 200
            and response.headers["Content-Type"] == "application/json"
        ):
            response_body = response.json()
            self.headers = {"Authorization": "Bearer " + response_body["access"]}

    def test_should_get_user_data(self) -> None:
        if self.headers is None:
            self.authenticate()

        url = self.build_url(path="/api/user/me/")
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["email"] != None

    def test_should_create_order(self) -> None:
        if self.headers is None:
            self.authenticate()

        url = self.build_url(path="/api/order/create/")
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "STOCK_stock_0",
            "amount": 100,
            "user": "198",
            "side": "buy",
            "margin": 2,
        }

        response = requests.post(url, data=data, headers=self.headers)

        assert response.status_code == 201
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        print(response_body)

        assert response_body["order_uid"] != None
        assert response_body["price"] == 1.63

    # TODO: Create tests using Client