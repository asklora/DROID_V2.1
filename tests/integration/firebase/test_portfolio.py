import pytest
from ingestion.firestore_migration import firebase_user_update
from schema import SchemaError
from tests.utils.firebase_schema import FIREBASE_PORTFOLIO_SCHEMA

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_firebase_data() -> None:
    result = firebase_user_update(
        currency_code=["HKD"],
        update_firebase=False,
    )

    assert result is not None
    assert not isinstance(result, str)

    records = result.to_dict("records")
    failed: int = 0

    for portfolio in records:
        try:
            FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)
        except SchemaError as e:
            failed += 1
            print(e)

    assert failed == 0
