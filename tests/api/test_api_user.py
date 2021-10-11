"""
The user has been created before testing (consult `conftest.py` for code)
we are only going to check if the api would return the user's data correctly
"""


def test_api_get_user_data(authentication, client, user) -> None:
    response = client.get(path="/api/user/me/", **authentication)

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    response_body = response.json()
    assert response_body["email"] == user.email
    assert response_body["username"] == user.username
    assert response_body["is_active"]
