import pytest
import pandas as pd
import pandera as pa
from schema import Or, Schema
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

    profile_schema = Schema(
        {
            "birth_date": Or(str, None),
            "is_joined": bool,
            "phone": Or(str, None),
            "gender": Or(str, None),
            "first_name": Or(str, None),
            "username": Or(str, None),
            "email": str,
            "last_name": Or(str, None),
        }
    )

    portfolio_schema = Schema(
        {
            "barrier": Or(float, 0),
            "bot_cash_balance": Or(float, 0),
            "bot_details": {
                "duration": str,
                "bot_id": str,
                "bot_option_type": str,
                "bot_apps_name": str,
                "bot_apps_description": Or(str, None),
            },
            "currency_code": str,
            "current_ivt_amt": Or(float, 0),
            "current_values": Or(float, 0),
            "entry_price": Or(float, 0),
            "exchange_rate": Or(float, 0),
            "expiry": str,
            "investment_amount": Or(float, 0),
            "margin_amount": Or(float, 0),
            "margin": int,
            "order_uid": str,
            "pct_cash": Or(float, 0),
            "pct_profit": Or(float, 0),
            "pct_stock": Or(float, 0),
            "position_uid": str,
            "price": Or(float, 0),
            "profit": Or(float, 0),
            "schi_name": Or(str, None),
            "share_num": Or(float, 0),
            "spot_date": str,
            "status": str,
            "stop_loss": Or(float, 0),
            "take_profit": Or(float, 0),
            "tchi_name": Or(str, None),
            "threshold": Or(float, 0),
            "ticker_fullname": Or(str, None),
            "ticker_name": str,
            "ticker": str,
            "trading_day": str,
            "user_id": int,
        }
    )

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
            "rank": pa.Column(None, nullable=True),
            "total_profit": pa.Column(float),
            "total_profit_amount": pa.Column(float),
            "total_profit_pct": pa.Column(float),
            "pending_amount": pa.Column(float),
            "stock_pending_amount": pa.Column(float),
            "bot_pending_amount": pa.Column(float),
            "profile": {
                "birth_date": pa.Column(str, nullable=True),
                "is_joined": pa.Column(bool),
                "phone": pa.Column(str, nullable=True),
                "gender": pa.Column(str, nullable=True),
                "first_name": pa.Column(str, nullable=True),
                "username": pa.Column(str, nullable=True),
                "email": pa.Column(str),
                "last_name": pa.Column(str, nullable=True),
            },
            "total_bot_invested_amount": pa.Column(float),
            "daily_live_profit": pa.Column(float),
            "active_portfolio": pa.Column(
                checks=pa.Check(
                    lambda p: all(
                        portfolio_schema.validate(por) if len(por) > 0 else True
                        for por in p
                    ),
                    element_wise=True,
                ),
            ),
        },
        strict=True,
    )

    try:
        schema.validate(result, lazy=True)
        assert True
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        assert False
