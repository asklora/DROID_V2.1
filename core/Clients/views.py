from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import ClientSerializers, UserClientSerializers
from .models import Client,UserClient
from core.user.models import User
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample


class ClientView(APIView):
    serializer_class = ClientSerializers
    permission_classes = [permissions.IsAdminUser]
    
    @extend_schema(
        operation_id='Get available client',
    )
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

class UserClientView(APIView):
    serializer_class = UserClientSerializers
    permission_classes = [permissions.IsAdminUser]
    
    @extend_schema(
        operation_id='Retrive users of client',
    )
    def get(self, request,client_id, format=None):
        """
        Return a list user of client.
        """
        users = UserClient.objects.filter(client_uid=client_id)
        res = UserClientSerializers(users,many=True).data
        return response.Response(res,status=status.HTTP_200_OK)
        
        
