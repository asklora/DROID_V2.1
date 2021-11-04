import pytest
import pandera as pa
from ingestion.firestore_migration import firebase_user_update

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

    schema = pa.DataFrameSchema(
        columns={
            "user_id": pa.Column(int),
            "balance": pa.Column(float),
            "currency": pa.Column(str),
            "daily_profit": pa.Column(float),
            "pct_total_bot_invested_amount": pa.Column(float),
            "total_invested_amount": pa.Column(float),
            "total_portfolio": pa.Column(float),
            "is_decimal": pa.Column(bool),
            "total_user_invested_amount": pa.Column(float),
            "daily_invested_amount": pa.Column(float),
            "pct_total_user_invested_amount": pa.Column(float),
            "current_asset": pa.Column(float),
            "daily_profit_pct": pa.Column(float),
            "rank": pa.Column(float),
            "total_profit": pa.Column(float),
            "total_profit_amount": pa.Column(float),
            "total_profit_pct": pa.Column(float),
            "pending_amount": pa.Column(float),
            "stock_pending_amount": pa.Column(float),
            "bot_pending_amount": pa.Column(float),
            "profile": pa.Column(float),
            "total_bot_invested_amount": pa.Column(float),
            "daily_live_profit": pa.Column(float),
            "active_portfolio": pa.Column(dict),
        },
        strict=True,
        ordered=True,
    )

    schema.validate(result, lazy=True)
