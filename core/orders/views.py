from .serializers import PositionSerializer
from rest_framework import viewsets,views,permissions
from .models import OrderPosition
from core.Clients.models import UserClient
from drf_spectacular.utils import extend_schema_view,extend_schema


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
        if self.request.user.email == 'asklora@loratechai.com':
            return self.queryset
        if self.kwargs:
            return OrderPosition.objects.filter(user_id=self.kwargs['user_id'])  