from typing import Any

from config.celery import app
from core.universe.models import Universe
from core.user.models import User
from django.conf import settings
from firebase_admin import firestore
from general.slack import report_to_slack
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
)

from .base import Endpoint, HealthCheck, Market
from .checks import (
    ApiCheck,
    AskloraCheck,
    FirebaseCheck,
    MarketCheck,
    TestUsersCheck,
    # TestProjectCheck,
)


def send_report(result: Any):
    report_to_slack(str(result), channel="#healthcheck")


@app.task(ignore_result=True)
def run_healthcheck() -> None:
    # variables
    firebase_app: Any = firestore.client()
    portfolio_collection: str = settings.FIREBASE_COLLECTION["portfolio"]
    universe_collection: str = settings.FIREBASE_COLLECTION["universe"]
    # testproject_token: str = "okSRWQSYYFAr7LZvVkczgvyEpm5h1TkYWvSAEm-GAz41"
    tradinghours_token: str = (
        "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
    )

    # initialize healthcheck
    healthcheck: HealthCheck = HealthCheck(
        checks=[
            # Celery and Asklora check
            AskloraCheck(
                check_key="asklora_check",
                module="core.djangomodule.crudlib.healthcheck.check_asklora",
                payload=None,
                queue=settings.ASKLORA_QUEUE,
            ),
            # api checks
            ApiCheck(
                check_key="api_check",
                endpoints=[
                    Endpoint(
                        name="droid production",
                        url="https://services.asklora.ai",
                    ),
                    Endpoint(
                        name="droid staging",
                        url="https://dev-services.asklora.ai",
                    ),
                ],
            ),
            # market check
            MarketCheck(
                check_key="market_check",
                tradinghours_token=tradinghours_token,
                markets=[
                    Market("US market", "USD", "US.NASDAQ"),
                    Market("HK market", "HKD", "HK.HKEX"),
                ],
            ),
            # Firebase check
            FirebaseCheck(
                check_key="firebase_portfolio_check",
                firebase_app=firebase_app,
                model=User,
                collection=portfolio_collection,
                schema=FIREBASE_PORTFOLIO_SCHEMA,
            ),
            FirebaseCheck(
                check_key="firebase_universe_check",
                firebase_app=firebase_app,
                model=Universe,
                collection=universe_collection,
                schema=FIREBASE_UNIVERSE_SCHEMA,
            ),
            TestUsersCheck(
                check_key="test_users_check",
                firebase_app=firebase_app,
                model=User,
                collection=portfolio_collection,
            ),
            # TestProject check
            # TestProjectCheck(
            #     check_key="testproject_check",
            #     api_key=testproject_token,
            #     project_id="GGUk2NYP4k2YZVYIVSVlUg",
            #     job_id="YaTSnhGMxESupCgfkNO08g",
            # ),
        ]
    )

    success: bool = healthcheck.execute()
    print(healthcheck.get_result())

    if not success:
        send_report(healthcheck)
