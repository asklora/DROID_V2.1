from typing import Any

from config.celery import app
from django.conf import settings
from firebase_admin import firestore
from general.slack import report_to_slack
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
)

from .base import HealthCheck, Market
from .checks import ApiCheck, FirebaseCheck, MarketCheck, TestProjectCheck


@app.task(ignore_result=True)
def run_healthcheck() -> None:
    # variables
    tradinghours_token: str = (
        "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
    )
    firebase_database: Any = firestore.client()
    portfolio_collection: str = settings.FIREBASE_COLLECTION["portfolio"]
    universe_collection: str = settings.FIREBASE_COLLECTION["universe"]
    testproject_token: str = "okSRWQSYYFAr7LZvVkczgvyEpm5h1TkYWvSAEm-GAz41"

    # initialize healthcheck
    healthcheck: HealthCheck = HealthCheck(
        checks=[
            # api checks
            ApiCheck(
                name="droid staging",
                url="https://dev-services.asklora.ai",
            ),
            ApiCheck(
                name="droid production",
                url="https://services.asklora.ai",
            ),
            # market check
            MarketCheck(
                tradinghours_token=tradinghours_token,
                markets=[
                    Market("US market", "USD", "US.NASDAQ"),
                    Market("HK market", "HKD", "HK.HKEX"),
                ],
            ),
            # Firebase check
            FirebaseCheck(
                database=firebase_database,
                collection=portfolio_collection,
                id_field="user_id",
                schema=FIREBASE_PORTFOLIO_SCHEMA,
            ),
            FirebaseCheck(
                database=firebase_database,
                collection=universe_collection,
                id_field="ticker",
                schema=FIREBASE_UNIVERSE_SCHEMA,
            ),
            # TestProject check
            TestProjectCheck(
                api_key=testproject_token,
                project_id="GGUk2NYP4k2YZVYIVSVlUg",
                job_id="YaTSnhGMxESupCgfkNO08g",
            ),
        ]
    )

    result: str = healthcheck.execute()

    # print(result)
    report_to_slack(result, channel="#healthcheck")
