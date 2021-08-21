from rest_framework import fields, serializers
from .models import Client, UserClient, ClientTopStock
from drf_spectacular.utils import OpenApiExample, extend_schema_serializer


class ClientSerializers(serializers.ModelSerializer):
    class Meta:
        model = Client
        fields = ("client_uid", "client_name")


class UserClientSerializers(serializers.ModelSerializer):
    email = serializers.CharField(read_only=True, source="user.email")

    class Meta:
        model = UserClient
        fields = ("email", "user_id", "extra_data")


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
