from .serializers import PositionSerializer, PerformanceSerializer
from rest_framework import viewsets, views, permissions, response, status, serializers
from .models import OrderPosition, PositionPerformance
from core.Clients.models import UserClient
from drf_spectacular.utils import extend_schema_view,extend_schema, OpenApiResponse


class errserializer(serializers.Serializer):
    detail = serializers.CharField()

@extend_schema_view(
    list=extend_schema(
        operation_id='Get positions by Client'
    )
)
class PositionViews(viewsets.ReadOnlyModelViewSet):
    """
    get positions by Client
    """
    
    serializer_class = PositionSerializer
    queryset = OrderPosition.objects.all()
    permission_classes =(permissions.IsAdminUser,)
    
    


    def get_queryset(self):
        if self.request.user.email == 'asklora@loratechai.com':
            return self.queryset
           
        user =  self.request.user
        client = user.client_user.all().first()
        user_id = [user.user_id for user in UserClient.objects.filter(client=client.client)]

        return OrderPosition.objects.filter(user_id__in=user_id)
            
@extend_schema_view(
    list=extend_schema(
        operation_id='Get positions by Account'
    )
)
class PositionUserViews(viewsets.ReadOnlyModelViewSet):
    """
    get positions by account
    """
    serializer_class = PositionSerializer
    queryset = OrderPosition.objects.all()
    permission_classes =(permissions.IsAdminUser,)

    
    def get_queryset(self):
        if self.kwargs:
            return OrderPosition.objects.filter(user_id=self.kwargs['user_id'])
        else:
            return OrderPosition.objects.filter(user_id=None)
        
class BotPerformanceViews(views.APIView):
    """
    get bot Performance by positions
    """
    serializer_class = PerformanceSerializer
    permission_classes =(permissions.IsAdminUser,)

    @extend_schema(
        operation_id='Get bot Performance by positions',
        responses={
            200: OpenApiResponse(response=PerformanceSerializer,),
            404: OpenApiResponse(description='Bad request (position not found)',response=errserializer),
            401: OpenApiResponse(description='Unauthorized request',response=errserializer),
        }
    )
    def get(self,request,position_uid):
        perf = PositionPerformance.objects.filter(position_uid=position_uid).order_by('created')
        if not perf.exists():
            return response.Response({'message':f'{position_uid} doesnt exist'},status=status.HTTP_404_NOT_FOUND)
        return response.Response(PerformanceSerializer(perf,many=True).data,status=status.HTTP_200_OK)