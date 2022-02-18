from typing import Any, List

from core.services.healthcheck.base import Check, Endpoint, HealthCheck, Market
from core.services.healthcheck.checks import (
    ApiCheck,
    AskloraCheck,
    FirebaseCheck,
    MarketCheck,
    TestProjectCheck,
    TestUsersCheck,
)
from core.services.healthcheck.run import send_report
from core.services.serializers import BroadcastSenderSerializer
from core.universe.models import Universe
from core.user.models import User
from django.conf import settings
from firebase_admin import firestore
from rest_framework import response, status
from rest_framework.views import APIView
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
)


class BroadcastSender(APIView):
    serializer_class = BroadcastSenderSerializer
    authentication_classes = []

    def post(self, request):
        serializer = BroadcastSenderSerializer(data=request.data)
        key = request.META.get("HTTP_CHECK_KEY")
        if serializer.is_valid():
            try:
                if key == "sendmessage":
                    serializer.save()
                else:
                    return response.Response(
                        {"message": "Check your key"},
                        status=status.HTTP_403_FORBIDDEN,
                    )
            except Exception as e:
                return response.Response(
                    {"message": str(e)},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            return response.Response(
                {"message": "Message sent successfully"},
                status=status.HTTP_200_OK,
            )
        return response.Response(
            {"message": "Invalid value"},
            status=status.HTTP_400_BAD_REQUEST,
        )


class Healthcheck(APIView):
    authentication_classes = []

    def get_checks(self, keys: List[str]) -> List[Check]:
        # Variables
        firebase_app: Any = firestore.client()
        portfolio_collection: str = settings.FIREBASE_COLLECTION["portfolio"]
        universe_collection: str = settings.FIREBASE_COLLECTION["universe"]
        testproject_token: str = "okSRWQSYYFAr7LZvVkczgvyEpm5h1TkYWvSAEm-GAz41"
        tradinghours_token: str = (
            "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
        )

        # Dictionary containing all checks
        all_checks: dict[str, Check] = {
            "asklora": AskloraCheck(
                check_key="asklora_check",
                module="core.djangomodule.crudlib.healthcheck.check_asklora",
                payload=None,
                queue=settings.ASKLORA_QUEUE,
            ),
            "api": ApiCheck(
                check_key="api_check",
                endpoints=[
                    Endpoint(
                        name="droid staging",
                        url="https://dev-services.asklora.ai",
                    ),
                ],
            ),
            "portfolio": FirebaseCheck(
                check_key="firebase_portfolio_check",
                firebase_app=firebase_app,
                model=User,
                collection=portfolio_collection,
                schema=FIREBASE_PORTFOLIO_SCHEMA,
            ),
            "universe": FirebaseCheck(
                check_key="firebase_universe_check",
                firebase_app=firebase_app,
                model=Universe,
                collection=universe_collection,
                schema=FIREBASE_UNIVERSE_SCHEMA,
            ),
            "market": MarketCheck(
                check_key="market_check",
                tradinghours_token=tradinghours_token,
                markets=[
                    Market("US market", "USD", "US.NASDAQ"),
                    Market("HK market", "HKD", "HK.HKEX"),
                ],
            ),
            "testproject": TestProjectCheck(
                check_key="testproject_test",
                api_key=testproject_token,
                project_id="GGUk2NYP4k2YZVYIVSVlUg",
                job_id="YaTSnhGMxESupCgfkNO08g",
            ),
            "testusers": TestUsersCheck(
                check_key="test_users_check",
                firebase_app=firebase_app,
                model=User,
                collection=portfolio_collection,
            ),
        }

        checks: List[Check] = [all_checks[key] for key in keys]

        return checks

    def run_check(self, items: List) -> dict:
        if items:
            healthcheck: HealthCheck = HealthCheck(checks=items)
            success: bool = healthcheck.execute()

            if not success:
                print(healthcheck)
                send_report(healthcheck)

            return {
                "message": "ok" if success else "error",
                "result": healthcheck.get_result(),
            }
        else:
            return {"message": "no tests are run"}

    def get(self, request):
        key: str = request.META.get("HTTP_CHECK_KEY")
        check_list: List[str] = request.META.get("HTTP_CHECK_LIST").split(",")

        if key == "runhealthcheck":
            checks: List[Check] = self.get_checks(check_list)
            result: dict = self.run_check(checks)

            return response.Response(
                result,
                status=status.HTTP_200_OK,
            )
        else:
            return response.Response(
                {"message": "Check your key"},
                status=status.HTTP_403_FORBIDDEN,
            )
