from typing import List

from core.services.healthcheck.base import HealthCheck
from core.services.healthcheck.checks import ApiCheck
from core.services.serializers import BroadcastSenderSerializer
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

            _, failure = healthcheck.execute()

            return {
                "message": "ok" if not failure else "failed",
                "result": healthcheck.get_result_as_dict(),
            }
        else:
            return {"message": "no tests are run"}

    def get(self, request):
        key = request.META.get("HTTP_CHECK_KEY")

        # asklora_check = request.META.get("HTTP_ASKLORA_CHECK")
        api_check = request.META.get("HTTP_API_CHECK")
        # market_check = request.META.get("HTTP_MARKET_CHECK")
        # firebase_check = request.META.get("HTTP_FIREBASE_CHECK")
        # test_users_check = request.META.get("HTTP_TEST_USERS_CHECK")
        # testproject_check = request.META.get("HTTP_TESTPROJECT_CHECK")

        if key == "runhealthcheck":
            run_api_check = (
                (
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
                if api_check
                else ()
            )

            checks = [
                *run_api_check,
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
