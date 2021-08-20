from abc import get_cache_token
from rest_framework.views import APIView
from rest_framework import permissions, response, status, serializers, exceptions
from .serializers import UserSerializer, TokenRevokeSerializer, UserSummarySerializer
from rest_framework_simplejwt.tokens import RefreshToken
from drf_spectacular.utils import extend_schema, OpenApiTypes, OpenApiExample
from core.user.models import User
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.serializers import (
    TokenObtainSerializer,
    PasswordField,
    api_settings,
    update_last_login,
    RefreshToken,
)
from django.contrib.auth import get_user_model, authenticate
from core.djangomodule.general import set_cache_data, get_cached_data, IsRegisteredUser


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


class PairTokenView(TokenObtainPairView):
    serializer_class = PairTokenSerializer


class UserSummaryView(APIView):
    serializer_class = UserSummarySerializer
    permission_classes = [IsRegisteredUser]

    @extend_schema(operation_id="Get user summary")
    def get(self, request, pk, format=None):
        """
        Get user summary by id
        """

        cache_key = f"usersummary{pk}"
        cached_data = get_cached_data(cache_key)
        # if cached_data:
        #     return response.Response(cached_data,status=status.HTTP_200_OK)
        user = User.objects.get(id=pk)
        res = UserSummarySerializer(user).data
        set_cache_data(cache_key, data=res, interval=(60 * 60) * 4)
        return response.Response(res, status=status.HTTP_200_OK)


class UserProfile(APIView):
    serializer_class = UserSerializer
    permission_classes = [IsRegisteredUser]

    @extend_schema(operation_id="Get user information")
    def get(self, request, format=None):
        """
        Get the profile of currently logged-in user
        """
        user = request.user
        if str(user) == "AnonymousUser":
            return response.Response(
                {"error": "User is not logged in"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        else:
            return response.Response(UserSerializer(user).data, status=status.HTTP_200_OK)


class RevokeToken(APIView):

    serializer_class = TokenRevokeSerializer

    @extend_schema(
        examples=[OpenApiExample("message", {"message": "string"}, response_only=True)],
        operation_id="Destroy token",
    )
    def post(self, request):
        """
        Revoke active token
        """
        serializer = TokenRevokeSerializer(data=request.data)
        if serializer.is_valid():
            try:
                token = RefreshToken(serializer.data["token"])
                token.blacklist()
            except Exception as e:
                return response.Response(
                    {"message": str(e)}, status=status.HTTP_400_BAD_REQUEST
                )
            return response.Response(
                {"message": "token revoked"}, status=status.HTTP_205_RESET_CONTENT
            )
        return response.Response(
            {"message": "invalid value"}, status=status.HTTP_400_BAD_REQUEST
        )
