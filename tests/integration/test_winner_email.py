from typing import List

import pytest
from core.user.models import User
from schema import Or, Schema
from core.services.notification import send_winner_email

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_sending_winner_email(mocker) -> None:
    def send_to_asklora(payload):
        assert Schema(
            {
                "session": str,
                "winner": Or(
                    [
                        {
                            "email": str,
                            "first_name": Or(str, None),
                            "last_name": Or(str, None),
                            "ranks": Or(int, float),
                            "username": str,
                        },
                    ],
                    [],
                ),
            }
        ).validate(payload)

        users: List[User] = User.objects.filter(is_joined=True)
        assert len(users) == len(payload["winner"])
    
    # we mock the payload sending to asklora
    payload_sent = mocker.patch(
        "core.services.notification.send_to_asklora",
        wraps=send_to_asklora,
    )

    # we try sending the emails
    send_winner_email.apply()

    # check if the payload is correct
    payload_sent.assert_called()

