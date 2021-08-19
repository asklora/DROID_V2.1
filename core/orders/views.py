from .serializers import (
    PositionSerializer,
    PerformanceSerializer,
    OrderCreateSerializer,
    OrderUpdateSerializer,
    OrderDetailsSerializers,
    OrderListSerializers,
    OrderActionSerializer,
    OrderPortfolioCheckSerializer
)
from rest_framework import viewsets, views, response, status, serializers
from rest_framework.decorators import action
from .models import OrderPosition, PositionPerformance, Order
from core.Clients.models import UserClient
from drf_spectacular.utils import extend_schema_view, extend_schema, OpenApiResponse, OpenApiParameter, OpenApiTypes
from core.djangomodule.general import IsRegisteredUser,errserializer





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
    permission_classes = (IsRegisteredUser,)

    def get_queryset(self):
        if self.request.user.email == 'asklora@loratechai.com':
            return self.queryset

        user = self.request.user
        client = user.client_user.all().first()
        user_id = [user.user_id for user in UserClient.objects.filter(
            client=client.client)]

        return OrderPosition.objects.filter(user_id__in=user_id)


@extend_schema_view(
    list=extend_schema(
        operation_id='Get positions by Account',
        parameters=[
            OpenApiParameter(name='live',required=False,type=int,description='1 is true (still live), 0 is false (completed)')
        ]
    )
)
class PositionUserViews(viewsets.ReadOnlyModelViewSet):
    """
    get positions by accounts
    params to sort live/completed
    - live positions
        - url/?live
    - complete positions
        - url/?complete
    """
    serializer_class = PositionSerializer
    queryset = OrderPosition.objects.all()
    permission_classes = (IsRegisteredUser,)

    def get_queryset(self):
        if self.kwargs:
            if 'live' in self.request.query_params:
                params = self.request.query_params.get('live',None)
                if params not in ['0','1']:
                    return []
                if params:
                    return OrderPosition.objects.prefetch_related('ticker').filter(user_id=self.kwargs['user_id'], is_live=params)
                else:
                    return OrderPosition.objects.prefetch_related('ticker').filter(user_id=self.kwargs['user_id'], is_live=True)
            elif 'complete' in self.request.query_params:
                return OrderPosition.objects.prefetch_related('ticker').filter(user_id=self.kwargs['user_id'], is_live=False)
            return OrderPosition.objects.prefetch_related('ticker').filter(user_id=self.kwargs['user_id'])
        else:
            return OrderPosition.objects.prefetch_related('ticker').filter(user_id=None)

class PositionDetailViews(views.APIView):
    """
    get Detail positions
    """
    serializer_class = PositionSerializer
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id='Get bot Performance by positions',
        responses={
            200: OpenApiResponse(response=PositionSerializer,),
            404: OpenApiResponse(description='Bad request (position not found)', response=errserializer),
            401: OpenApiResponse(description='Unauthorized request', response=errserializer),
        }
    )
    def get(self, request, position_uid):
        user = request.user
        position = OrderPosition.objects.filter(
            position_uid=position_uid)
        if position.exists():
            position = position.get()
            if position.user_id == user or user.is_superuser:
                return response.Response(PositionSerializer(position).data, status=status.HTTP_200_OK)
            else:
                return response.Response({'detail': f'this position not belong to current user'}, status=status.HTTP_403_FORBIDDEN)

        return response.Response({'detail': f'{position_uid} doesnt exist'}, status=status.HTTP_404_NOT_FOUND)

class BotPerformanceViews(views.APIView):
    """
    get bot Performance by positions
    """
    serializer_class = PerformanceSerializer
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id='Get bot Performance by positions',
        responses={
            200: OpenApiResponse(response=PerformanceSerializer,),
            404: OpenApiResponse(description='Bad request (position not found)', response=errserializer),
            401: OpenApiResponse(description='Unauthorized request', response=errserializer),
        }
    )
    def get(self, request, position_uid):
        perf = PositionPerformance.objects.filter(
            position_uid=position_uid).order_by('created')
        if not perf.exists():
            return response.Response({'message': f'{position_uid} doesnt exist'}, status=status.HTTP_404_NOT_FOUND)
        return response.Response(PerformanceSerializer(perf, many=True).data, status=status.HTTP_200_OK)


class OrderViews(views.APIView):
    serializer_class = OrderCreateSerializer
    permission_classes = (IsRegisteredUser,)

    def post(self, request):
        # TODO: #56 create documentation API for Order create @hendika
        # refer to https://drf-spectacular.readthedocs.io/en/latest/
        # two type example requests for SELL and BUY
        """
        - Buy request:
                {
                "ticker": "string",
                "price": 0,
                "bot_id": "string",
                "amount": 0,
                "user": "string",
                "side": "string",
                }
        - Sell request:
            {
                "user": "string",
                "side": "string",
                "ticker":"string",
                "setup": {
                "position":"string"
                }
            }
        """
                
        serializer = OrderCreateSerializer(
            data=request.data, context={'request': request})
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_201_CREATED)
        return response.Response(serializer.errors,status=status.HTTP_400_BAD_REQUEST)

class OrderPortfolioCheckView(views.APIView):
    """
    check user positions
    """
    serializer_class = OrderPortfolioCheckSerializer
    permission_classes = (IsRegisteredUser,)
    @extend_schema(
        operation_id='check positions',
        responses={
            200: OpenApiResponse(response=OrderPortfolioCheckSerializer,),
            404: OpenApiResponse(description='Bad request (position not found)', response=errserializer),
            401: OpenApiResponse(description='Unauthorized request', response=errserializer),
        },
        description="check user positions"
    )
    def post(self, request):

        serializer = OrderPortfolioCheckSerializer(
            data=request.data, context={'request': request})
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)

class OrderUpdateViews(views.APIView):
    serializer_class = OrderUpdateSerializer
    permission_classes = (IsRegisteredUser,)

    def post(self, request):
        try:
            instance = OrderUpdateSerializer.Meta.model.objects.get(
                order_uid=request.data['order_uid'])
        except OrderUpdateSerializer.Meta.model.DoesNotExist:
            return response.Response({'detail': 'order not found'}, status=status.HTTP_404_NOT_FOUND)
        # ignore if fels account
        if not instance.user_id.id == 135:
            if instance.user_id.username != request.user.username:
                return response.Response({'detail': 'credentials not allowed to change this order'}, status=status.HTTP_403_FORBIDDEN)
        serializer = OrderUpdateSerializer(instance, data=request.data,context={'request': request})
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)


class OrderGetViews(viewsets.ViewSet):
    permission_classes =(IsRegisteredUser,)
    @extend_schema(
        responses=OrderListSerializers,
        # more customizations
    )
    def list(self, request):
        instances = Order.objects.filter(user_id=request.user)
        self.serialzer_class = OrderListSerializers
        return response.Response(OrderListSerializers(instances, many=True).data, status=status.HTTP_200_OK)
    permission_classes =(IsRegisteredUser,)
    @extend_schema(
        responses=OrderDetailsSerializers,
        # more customizations
    )
    def retrieve(self, request, order_uid=None):
        try:
            instance = Order.objects.get(order_uid=order_uid)
            self.serialzer_class = OrderDetailsSerializers
            return response.Response(OrderDetailsSerializers(instance).data, status=status.HTTP_200_OK)
        except Order.DoesNotExist:
            return response.Response({'detail': 'order not forund'}, status=status.HTTP_404_NOT_FOUND)


class OrderActionViews(views.APIView):
    serializer_class = OrderActionSerializer
    permission_classes = (IsRegisteredUser,)

    def post(self, request):
        try:
            instance = OrderActionSerializer.Meta.model.objects.get(
                order_uid=request.data['order_uid'])
        except OrderActionSerializer.Meta.model.DoesNotExist:
            return response.Response({'detail': 'order not found'}, status=status.HTTP_404_NOT_FOUND)
        except KeyError as e:
            err = str(e)
            return response.Response({'detail': f'error key {err}'}, status=status.HTTP_400_BAD_REQUEST)
       # ignore if fels account
        if not instance.user_id.id == 135:
            if instance.user_id.username != request.user.username:
                return response.Response({'detail': 'credentials not allowed to change this order'}, status=status.HTTP_403_FORBIDDEN)
        serializer = OrderActionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)
