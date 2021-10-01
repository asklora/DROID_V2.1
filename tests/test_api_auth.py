"""
The user has been created before testing (consult `conftest.py` for code)
we are only going to check the users' token validity
"""


def test_api_token_verify(client, authentication) -> None:
    token = authentication["HTTP_AUTHORIZATION"].split("Bearer ")[1]

    response = client.post(path="/api/auth/verify/", data={"token": token})

    # API only returns the status code with empty body
    assert response.status_code == 200


def test_api_token_revoke(user, client) -> None:
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
