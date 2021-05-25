from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import UserSerializer,TokenRevokeSerializer
from rest_framework_simplejwt.tokens import RefreshToken
from drf_spectacular.utils import extend_schema,OpenApiTypes,OpenApiExample





class UserProfile(APIView):
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]
    
    @extend_schema(
        operation_id='Get user information'
    )
    def get(self, request, format=None):
        """
        Return a user profile.
        """
        user = request.user
        return response.Response(UserSerializer(user).data,status=status.HTTP_200_OK)
    
    
class RevokeToken(APIView):
    
    serializer_class=TokenRevokeSerializer
    @extend_schema(
        examples=[
            OpenApiExample('message',{'message':'string'},response_only=True)
        ],
        operation_id='Destroy token'
    )
    def post(self, request):
        """
        revoke token
        """
        serializer =TokenRevokeSerializer(data=request.data)
        if serializer.is_valid():
            try:
                token = RefreshToken(serializer.data['token'])
                token.blacklist()
            except Exception as e:
                return response.Response({'message':str(e)},status=status.HTTP_400_BAD_REQUEST)
            return response.Response({'message':'token revoked'},status=status.HTTP_205_RESET_CONTENT)
        return response.Response({'message':'invalid value'},status=status.HTTP_400_BAD_REQUEST)
        
