from typing import List

import pytest
from core.user.models import Season, SeasonHistory
from schema import Or, Schema
from core.services.notification import send_winner_email

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


# def test_sending_winner_email(mocker) -> None:
#     def send_to_asklora(payload):
#         # check if the schema of the payload's data is correct
#         assert Schema(
#             {
#                 "session": str,
#                 "winner": Or(
#                     [
#                         {
#                             "email": str,
#                             "first_name": Or(str, None),
#                             "last_name": Or(str, None),
#                             "ranks": Or(int, float),
#                             "username": str,
#                         },
#                     ],
#                     [],
#                 ),
#             }
#         ).validate(payload["payload"])
#         try:
#             last_season: Season = Season.objects.latest("end_date")
#         except Season.DoesNotExist:
#             return
#         winners: List[SeasonHistory] = list(
#             SeasonHistory.objects.filter(season_id=last_season, rank__gt=0)
#         )

#         # check if the number of the winners matches the payload data
#         assert len(winners) == len(payload["payload"]["winner"])

#     # we mock the payload sending to asklora
#     payload_sent = mocker.patch(
#         "core.services.notification.send_to_asklora",
#         wraps=send_to_asklora,
#     )

#     # we try sending the emails
#     send_winner_email()

#     # check if the payload is correct
#     payload_sent.assert_called()
