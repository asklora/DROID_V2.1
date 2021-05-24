from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import ClientSerializers
from .models import Client


class ClientView(APIView):
    serializer_class = ClientSerializers
    permission_classes = [permissions.IsAdminUser]
    
    
    def get(self, request, format=None):
        """
        Return a list of client.
        """
        user = request.user
        if user.email == 'asklora@loratechai.com':
            return response.Response(ClientSerializers(Client.objects.all(),many=True).data,status=status.HTTP_200_OK)
        data = Client.objects.filter(client_related__user_id=user)
        if data.exists():
            return response.Response(ClientSerializers(data, many=True).data,status=status.HTTP_200_OK)
        else:
            return response.Response({'message':'data not found'}, status=status.HTTP_404_NOT_FOUND)
        
        
