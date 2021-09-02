import pytest
import requests

@pytest.mark.django_db
class TestAPI:
    tokens = None
    headers = None

    def build_url(
        self,
        url: str = "http://0.0.0.0:8000",
        path: str = "/api/order/create/",
    ) -> str:
        return f"{url}{path}"

    def authenticate(self) -> bool:
        url = self.build_url(path="/api/auth/")
        data = {
            "email": "pytest@tests.com",
            "password": "helloworld",
        }

        response = requests.post(url=url, data=data)

        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            return False

        response_body = response.json()
        self.tokens = response_body
        self.headers = {"Authorization": "Bearer " + response_body["access"]}
        return True

    def test_api_token_verify(self, user) -> None:
        if self.headers is None:
            assert self.authenticate()

        token = self.headers["Authorization"].split("Bearer ")[1]

        url = self.build_url(path="/api/auth/verify/")
        response = requests.post(url, data={"token": token})

        # API only returns the status code with empty body
        assert response.status_code == 200

    def test_api_token_revoke(self, user) -> None:
        assert self.authenticate()

        token = self.tokens["refresh"]
        print(token)

        url = self.build_url(path="/api/auth/revoke/")
        response = requests.post(url, data={"token": token}, headers=self.headers)

        assert response.status_code == 205
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["message"] == "token revoked"

    def test_api_get_user_data(self, user) -> None:
        if self.headers is None:
            assert self.authenticate()

        url = self.build_url(path="/api/user/me/")
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["email"] != None

    def test_api_create_order(self, user) -> None:
        if self.headers is None:
            assert self.authenticate()

        url = self.build_url(path="/api/order/create/")
        data = {
            "ticker": "3377.HK",
            "price": 1.63,
            "bot_id": "STOCK_stock_0",
            "amount": 100,
            "user": user.id,
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
