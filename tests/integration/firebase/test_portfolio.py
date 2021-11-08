import pytest

from ingestion.firestore_migration import firebase_user_update
from tests.utils.firebase_schema import FIREBASE_SCHEMA

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_firebase_data():
    result = firebase_user_update(
        currency_code=["HKD"],
        update_firebase=False,
    )

    assert type(result) is not str

    records = result.to_dict("records")

    for portfolio in records:
        assert FIREBASE_SCHEMA.validate(portfolio)
