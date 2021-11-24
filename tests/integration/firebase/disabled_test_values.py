from random import choice
from typing import List

import pytest
from core.user.models import User
from django.conf import settings
from django.utils import timezone

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_user_values(firestore_client):
    def get_firestore_doc(id):
        return firestore_client.collection(
            settings.FIREBASE_COLLECTION["portfolio"],
        ).document(
            str(id),
        )

    def calculate_pct(current_assets: float):
        previous_assets: float = 100000.0  # for new users, the initial deposit

        percentage: float = (
            (current_assets - previous_assets) / previous_assets * 100
        )

        return percentage

    # we filter users that just joined
    last_month: int = timezone.now().month - 1
    new_users: List[User] = User.objects.filter(
        date_joined__month=last_month,
        is_joined=True,
    ).exclude(user_balance__amount=100000)
    assert new_users

    # we pick random user
    user = choice(new_users)
    print(user.user_balance.__dict__)

    # we check if the user data has been added to firebase
    doc_ref = get_firestore_doc(user.id)
    doc = doc_ref.get()
    assert doc.exists

    # we then get the data
    doc_dict = doc.to_dict()
    print(doc_dict)

    # assert the daily profit percentage
    firebase_percentage: float = round(doc_dict["daily_profit_pct"], 2)
    calculated_percentage: float = round(
        calculate_pct(doc_dict["current_asset"]),
        2,
    )
    assert firebase_percentage == calculated_percentage

    # TODO: assert mtd

    # TODO: assert pnl
