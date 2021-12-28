from typing import List

from core.services.healthcheck.base import HealthCheck, Market
from core.services.healthcheck.checks import (
    ApiCheck,
    AskloraCheck,
    MarketCheck,
    TestProjectCheck,
)
from core.services.serializers import BroadcastSenderSerializer
from django.conf import settings
from rest_framework import response, status
from rest_framework.views import APIView


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

    def run_check(self, items: List) -> dict:
        if items:
            healthcheck: HealthCheck = HealthCheck(checks=items)
            success: bool = healthcheck.execute()

            print(healthcheck)

            return {
                "message": "ok" if success else "error",
                "result": healthcheck.get_result(),
            }
        else:
            return {"message": "no tests are run"}

    def get(self, request):
        key = request.META.get("HTTP_CHECK_KEY")
        tradinghours_token: str = (
            "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
        )
        # firebase_app: Any = firestore.client()
        # portfolio_collection: str = settings.FIREBASE_COLLECTION["portfolio"]
        # universe_collection: str = settings.FIREBASE_COLLECTION["universe"]
        testproject_token: str = "okSRWQSYYFAr7LZvVkczgvyEpm5h1TkYWvSAEm-GAz41"

        asklora_check = (
            AskloraCheck(
                check_key="asklora_check",
                module="core.djangomodule.crudlib.healthcheck.check_asklora",
                payload=None,
                queue=settings.ASKLORA_QUEUE,
            ),
        )
        api_check = (
            ApiCheck(
                check_key="api_check_production",
                name="droid production",
                url="https://services.asklora.ai",
            ),
            ApiCheck(
                check_key="api_check_staging",
                name="droid staging",
                url="https://dev-services.asklora.ai",
            ),
        )
        # firebase_check = request.META.get("HTTP_FIREBASE_CHECK")
        # test_users_check = request.META.get("HTTP_TEST_USERS_CHECK")
        market_check = (
            MarketCheck(
                check_key="market_check",
                tradinghours_token=tradinghours_token,
                markets=[
                    Market("US market", "USD", "US.NASDAQ"),
                    Market("HK market", "HKD", "HK.HKEX"),
                ],
            ),
        )
        testproject_check = (
            TestProjectCheck(
                check_key="testproject_test",
                api_key=testproject_token,
                project_id="GGUk2NYP4k2YZVYIVSVlUg",
                job_id="YaTSnhGMxESupCgfkNO08g",
            ),
        )

        if key == "runhealthcheck":
            run_asklora_check = (
                asklora_check if request.META.get("HTTP_ASKLORA_CHECK") else ()
            )
            run_api_check = (
                api_check if request.META.get("HTTP_API_CHECK") else ()
            )
            run_market_check = (
                market_check if request.META.get("HTTP_MARKET_CHECK") else ()
            )
            run_testproject_check = (
                testproject_check
                if request.META.get("HTTP_TESTPROJECT_CHECK")
                else ()
            )

            checks: List = [
                *run_asklora_check,
                *run_api_check,
                *run_market_check,
                *run_testproject_check,
            ]

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
