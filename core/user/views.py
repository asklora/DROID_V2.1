from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import UserSerializer

class UserProfile(APIView):
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]
    
    
    def get(self, request, format=None):
        """
        Return a user profile.
        """
        user = request.user
        return response.Response(UserSerializer(user).data,status=status.HTTP_200_OK)
