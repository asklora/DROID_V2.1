from rest_framework.views import APIView
from rest_framework import permissions, response,status
from .serializers import ClientSerializers, UserClientSerializers,ClientTopStockSerializers,ResponseSerializer
from .models import Client,UserClient,ClientTopStock
from core.user.models import TransactionHistory
from drf_spectacular.utils import extend_schema,OpenApiExample
from datetime import datetime
from core.djangomodule.general import  set_cache_data,get_cached_data,errserializer

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
        # if cached_data:
        #     return response.Response(cached_data,status=status.HTTP_200_OK)
        users = UserClient.objects.filter(client_id=client_id,user__is_superuser=False,extra_data__service_type__in=['bot_advisor','bot_tester'])
        res = UserClientSerializers(users,many=True).data
        set_cache_data(cache_key,data=res,interval=(60*60)*4)
        return response.Response(res,status=status.HTTP_200_OK)


class TopStockClientView(APIView):

    serializer_class = ClientTopStockSerializers
    permission_classes = [permissions.IsAdminUser]

    @extend_schema(
        operation_id='Retrive unexecuted top stock client'
    )
    def get(self, request,client_id):
        
        
        data = ClientTopStock.objects.filter(has_position=False,client=client_id).order_by('-spot_date','rank')

        return response.Response(ClientTopStockSerializers(data,many=True).data,status=status.HTTP_200_OK)
    
class TopStockAction(APIView):
    permission_classes = [permissions.IsAdminUser]
    
    
    @extend_schema(
        responses=errserializer
     )
    def delete(self,request,uid):
        try:
            topstock = ClientTopStock.objects.get(uid=uid)
        except ClientTopStock.DoesNotExist:
            return response.Response({'detail':f'{uid} not found'}, status=status.HTTP_404_NOT_FOUND)
        
        topstock.delete()
        return response.Response({'detail':'OK'},status=status.HTTP_204_NO_CONTENT)
    
    
    
    
    @extend_schema(
        request=ResponseSerializer,
        responses=errserializer
     )
    def put(self, request,uid):
        try:
            topstock = ClientTopStock.objects.get(uid=uid)
        except ClientTopStock.DoesNotExist:
            return response.Response({'detail':f'{uid} not found'}, status=status.HTTP_404_NOT_FOUND)
        try:
         trans = TransactionHistory.objects.filter(transaction_detail__order_uid=request.data['order_uid'],transaction_detail__description='bot order')
         trans = trans.get()
        except TransactionHistory.DoesNotExist:
            return response.Response({'detail':f'transaction not found'}, status=status.HTTP_404_NOT_FOUND)
        except TransactionHistory.MultipleObjectsReturned:
            return response.Response({'detail':f'there is duplicate transaction found'}, status=status.HTTP_400_BAD_REQUEST)
        topstock.position_uid = trans.transaction_detail['position']
        topstock.has_position = True
        topstock.execution_date = datetime.now().date()
        topstock.save()
        return response.Response({'detail':'OK'},status=status.HTTP_200_OK)
