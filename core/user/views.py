from rest_framework.views import APIView
from rest_framework import response, status
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView
from drf_spectacular.utils import extend_schema, OpenApiExample
from core.djangomodule.general import set_cache_data, get_cached_data, IsRegisteredUser
from core.user.models import User
from .serializers import (
    PairTokenSerializer,
    UserSerializer,
    TokenRevokeSerializer,
    UserSummarySerializer,
)


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

    @extend_schema(operation_id="Get user profile")
    def get(self, request, format=None):
        """
        Get the profile of currently logged-in user
        """
        user = request.user
        if str(user) == "AnonymousUser":
            return response.Response(
                {"message": "User is not logged in"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        else:
            return response.Response(
                UserSerializer(user).data, status=status.HTTP_200_OK
            )


class RevokeToken(APIView):
    serializer_class = TokenRevokeSerializer

    @extend_schema(
        examples=[OpenApiExample("message", {"message": "string"}, response_only=True)],
        operation_id="Destroy token",
    )
    def post(self, request):
        """
        revoke token
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
