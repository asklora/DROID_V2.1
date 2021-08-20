from rest_framework import fields, serializers
from .models import Client, UserClient, ClientTopStock
from drf_spectacular.utils import OpenApiExample, extend_schema_serializer


class ClientSerializers(serializers.ModelSerializer):
    class Meta:
        model = Client
        fields = ("client_uid", "client_name")


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            "Return List of Users",
            description="Return List of Users",
            value=[
                {
                    "email": "string",
                    "user_id": 0,
                    "extra_data": {"property1": "string", "property2": "string"},
                }
            ],
            response_only=True,  # signal that example only applies to responses
        ),
    ]
)
class UserClientSerializers(serializers.ModelSerializer):
    email = serializers.CharField(read_only=True, source="user.email")

    class Meta:
        model = UserClient
        fields = ("email", "user_id", "extra_data")


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            "Return List of unexecuted Top stock",
            description="Return List of unexecuted Top stock",
            value=[
                {
                    "uid": "ffokeu",
                    "client": "SwxyYhs",
                    "ticker": "AAPL.O",
                    "spot_date": "2021-06-20",
                    "expiry_date": "2021-06-20",
                    "has_position": False,
                    "bot_id": "classic_0987_093",
                    "currency_code": "HKD",
                    "rank": 1,
                    "service_type": "bot_advisor",
                    "capital": "small",
                    "bot": "UNO",
                    "week_of_year": 2,
                }
            ],
            response_only=True,  # signal that example only applies to responses
        ),
    ]
)
class ClientTopStockSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientTopStock
        fields = [
            "uid",
            "client",
            "ticker",
            "spot_date",
            "expiry_date",
            "has_position",
            "bot_id",
            "currency_code",
            "rank",
            "service_type",
            "capital",
            "bot",
            "week_of_year",
        ]


class ResponseSerializer(serializers.Serializer):
    order_uid = serializers.CharField(required=True)
