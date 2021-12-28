import pytest

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_healthcheck_api(client) -> None:
    headers: dict = {
        "HTTP_CHECK_KEY": "runhealthcheck",
        "HTTP_ASKLORA_CHECK": True,
        "HTTP_API_CHECK": True,
        "HTTP_MARKET_CHECK": True,
        # "HTTP_TESTPROJECT_CHECK": True,
    }

    response = client.get(
        path="/api/service/healthcheck/",
        **headers,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body: dict = response.json()
    print(response_body)
    assert response_body.get("message") == "ok"
