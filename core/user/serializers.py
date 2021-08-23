from django.contrib.auth import get_user_model, authenticate
from drf_spectacular.utils import (
    OpenApiExample,
    OpenApiResponse,
    extend_schema_serializer,
)
from rest_framework import serializers, exceptions
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.serializers import (
    TokenObtainSerializer,
    PasswordField,
    api_settings,
    update_last_login,
)
from .models import User


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            "Log in with example account",
            description="Try to log in using an example account",
            value={"email": "ata", "password": "123"},
            request_only=True,
        ),
        OpenApiExample(
            "Example response for user with email 'ata'",
            value={
                "refresh": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTYyOTUyNTAyOCwianRpIjoiZWQ4NzhjZGQxOGZkNGUyNWIyNjcwNzVjMjk2N2E2NTUiLCJ1c2VybmFtZSI6ImF0YSJ9.oNxgmcsQwEy6MEkKs_7EO5DEjK_QAQGnU15uGOtmo8Y",
                "access": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjI5NDM5ODI4LCJqdGkiOiIzMWQ0MzUzNDFmNGU0NThiODBiYmMzYzgzZmYxMTc0ZSIsInVzZXJuYW1lIjoiYXRhIn0.8ToPjwJj3ZlQ7mlCsPaV-Fhqq2NV2tyxLeNgi1mk5Nk",
            },
            response_only=True,
        ),
    ],
)
class PairTokenSerializer(TokenObtainSerializer):
    username_field = get_user_model().AUTH_FIELD_NAME

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields[self.username_field] = serializers.CharField()
        self.fields["password"] = PasswordField()

    @classmethod
    def get_token(cls, user):
        return RefreshToken.for_user(user)

    def validate(self, attrs):
        authenticate_kwargs = {
            "username": attrs[self.username_field],
            "password": attrs["password"],
        }
        try:
            authenticate_kwargs["request"] = self.context["request"]
        except KeyError:
            pass

        self.user = authenticate(**authenticate_kwargs)

        if not api_settings.USER_AUTHENTICATION_RULE(self.user):
            raise exceptions.AuthenticationFailed(
                self.error_messages["no_active_account"],
                "no_active_account",
            )
        data = {}
        refresh = self.get_token(self.user)

        data["refresh"] = str(refresh)
        data["access"] = str(refresh.access_token)

        if api_settings.UPDATE_LAST_LOGIN:
            update_last_login(None, self.user)

        return data


class TokenRevokeSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)


class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)
    current_assets = serializers.FloatField(read_only=True)
    balance = serializers.FloatField(read_only=True)
    position_live = serializers.FloatField(read_only=True)
    position_expired = serializers.FloatField(read_only=True)
    total_invested_amount = serializers.FloatField(read_only=True)
    starting_amount = serializers.FloatField(read_only=True)
    current_total_investment_value = serializers.FloatField(read_only=True)
    total_amount = serializers.FloatField(read_only=True)
    total_profit_amount = serializers.FloatField(read_only=True)
    total_profit_return = serializers.FloatField(read_only=True)
    total_fee_amount = serializers.FloatField(read_only=True)
    total_stock_amount = serializers.FloatField(read_only=True)
    total_stamp_amount = serializers.FloatField(read_only=True)
    total_commission_amount = serializers.FloatField(read_only=True)
    currency = serializers.CharField(read_only=True)
    total_user_invested_amount = serializers.FloatField(read_only=True)
    total_bot_invested_amount = serializers.FloatField(read_only=True)
    total_pending_amount = serializers.FloatField(read_only=True)

    class Meta:
        model = User
        # fields = '__all__'o
        exclude = ("groups", "user_permissions")


class UserSummarySerializer(serializers.ModelSerializer):
    current_assets = serializers.FloatField(read_only=True)
    balance = serializers.FloatField(read_only=True)
    position_live = serializers.FloatField(read_only=True)
    position_expired = serializers.FloatField(read_only=True)
    total_invested_amount = serializers.FloatField(read_only=True)
    starting_amount = serializers.FloatField(read_only=True)
    current_total_investment_value = serializers.FloatField(read_only=True)
    total_amount = serializers.FloatField(read_only=True)
    total_profit_amount = serializers.FloatField(read_only=True)
    total_profit_return = serializers.FloatField(read_only=True)
    total_fee_amount = serializers.FloatField(read_only=True)
    total_stock_amount = serializers.FloatField(read_only=True)
    total_stamp_amount = serializers.FloatField(read_only=True)
    total_commission_amount = serializers.FloatField(read_only=True)
    currency = serializers.CharField(read_only=True)
    total_user_invested_amount = serializers.FloatField(read_only=True)
    total_bot_invested_amount = serializers.FloatField(read_only=True)
    total_pending_amount = serializers.FloatField(read_only=True)

    class Meta:
        model = User
        fields = (
            "id",
            "email",
            "current_assets",
            "balance",
            "position_live",
            "position_expired",
            "total_invested_amount",
            "starting_amount",
            "current_total_investment_value",
            "total_amount",
            "total_profit_amount",
            "total_profit_return",
            "total_stock_amount",
            "currency",
            "total_fee_amount",
            "total_stamp_amount",
            "total_commission_amount",
            "total_user_invested_amount",
            "total_bot_invested_amount",
            "total_pending_amount"
        )
