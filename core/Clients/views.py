from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import ClientSerializers, UserClientSerializers
from .models import Client,UserClient
from drf_spectacular.utils import extend_schema
from core.djangomodule.general import  set_cache_data,get_cached_data

class ClientView(APIView):
    serializer_class = ClientSerializers
    permission_classes = [permissions.IsAdminUser]
    
    @extend_schema(
        operation_id='Get available client'
    )
    def get(self, request, format=None):
        """
        Return a list of client
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
        cache_key = f'users_client_{client_id}'
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return response.Response(cached_data,status=status.HTTP_200_OK)
        users = UserClient.objects.filter(client_id=client_id,user__is_superuser=False,extra_data__service_type__in=['bot_advisor','bot_tester'])
        res = UserClientSerializers(users,many=True).data
        set_cache_data(cache_key,data=res,interval=(60*60)*4)
        return response.Response(res,status=status.HTTP_200_OK)
        
        
