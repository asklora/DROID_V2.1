from core.djangomodule.general import (
    IsRegisteredUser, 
    errserializer,
    OrderThrottle,
    OrderActionThrottle
    )
from rest_framework import viewsets, views, response, status
from drf_spectacular.utils import (
    extend_schema_view,
    extend_schema,
    OpenApiResponse,
    OpenApiParameter,
)
from core.Clients.models import UserClient
from core.user.models import TransactionHistory
from .models import OrderPosition, PositionPerformance, Order
from .serializers import (
    PositionSerializer,
    PerformanceSerializer,
    OrderCreateSerializer,
    OrderUpdateSerializer,
    OrderDetailsSerializers,
    OrderListSerializers,
    OrderActionSerializer,
    OrderPortfolioCheckSerializer,
)
from django.utils.translation import gettext as _


class BotPerformanceViews(views.APIView):
    """
    Get bot performance by positions
    """

    serializer_class = PerformanceSerializer
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id="Get bot Performance by positions",
        responses={
            200: OpenApiResponse(
                response=PerformanceSerializer,
            ),
            404: OpenApiResponse(
                description="Bad request (position not found)", response=errserializer
            ),
            401: OpenApiResponse(
                description="Unauthorized request", response=errserializer
            ),
        },
    )
    def get(self, request, position_uid):
        perf = PositionPerformance.objects.filter(position_uid=position_uid).exclude(order_uid=None).order_by(
            "created"
        )
        if not perf.exists():
            return response.Response(
                {"message": _("position %(position)s does not exist") % {'position': position_uid}},
                status=status.HTTP_404_NOT_FOUND,
            )
        return response.Response(
            PerformanceSerializer(perf, many=True).data, status=status.HTTP_200_OK
        )


@extend_schema_view(list=extend_schema(operation_id="Get positions by Client"))
class PositionViews(viewsets.ReadOnlyModelViewSet):
    """
    Get positions for a client
    """

    serializer_class = PositionSerializer
    queryset = OrderPosition.objects.all()
    permission_classes = (IsRegisteredUser,)

    def get_queryset(self):
        if self.request.user.email == "asklora@loratechai.com":
            return self.queryset

        user = self.request.user
        client = user.client_user.all().first()
        user_id = [
            user.user_id for user in UserClient.objects.filter(client=client.client)
        ]

        return OrderPosition.objects.filter(user_id__in=user_id)


@extend_schema_view(
    list=extend_schema(
        operation_id="Get positions by Account",
        parameters=[
            OpenApiParameter(
                name="live",
                required=False,
                type=int,
                description="1 is true (still live), 0 is false (completed)",
            )
        ],
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
            if "live" in self.request.query_params:
                params = self.request.query_params.get("live", None)
                if params not in ["0", "1"]:
                    return []
                if params:
                    return OrderPosition.objects.prefetch_related("ticker").filter(
                        user_id=self.kwargs["user_id"], is_live=params
                    )
                else:
                    return OrderPosition.objects.prefetch_related("ticker").filter(
                        user_id=self.kwargs["user_id"], is_live=True
                    )
            elif "complete" in self.request.query_params:
                return OrderPosition.objects.prefetch_related("ticker").filter(
                    user_id=self.kwargs["user_id"], is_live=False
                )
            else:
                return OrderPosition.objects.prefetch_related("ticker").filter(
                    user_id=self.kwargs["user_id"]
                )
        else:
            return OrderPosition.objects.prefetch_related("ticker").filter(user_id=None)


class PositionDetailViews(views.APIView):
    """
    View detailed information for a given position of a user
    """

    serializer_class = PositionSerializer
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id="Get bot Performance by positions",
        responses={
            200: OpenApiResponse(
                response=PositionSerializer,
            ),
            404: OpenApiResponse(
                description="Bad request: position not found", response=errserializer
            ),
            403: OpenApiResponse(
                description="Bad request: position does not belong to current user", response=errserializer
            ),
        },
    )
    def get(self, request, position_uid):
        user = request.user
        position = OrderPosition.objects.filter(position_uid=position_uid)
        if position.exists():
            position = position.get()
            if position.user_id == user or user.is_superuser:
                return response.Response(
                    PositionSerializer(position).data, status=status.HTTP_200_OK
                )
            else:
                return response.Response(
                    {"detail": _("this position not belong to current user")},
                    status=status.HTTP_403_FORBIDDEN,
                )

        return response.Response(
            {"detail": _("position %(position)s does not exist") % {"position": position_uid}}, status=status.HTTP_404_NOT_FOUND
        )


class OrderViews(views.APIView):
    """
    Create a new order position
    """
    throttle_classes = [OrderThrottle]
    serializer_class = OrderCreateSerializer
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id="Create new orders",
        responses={
            201: OpenApiResponse(response=OrderCreateSerializer),
            404: OpenApiResponse(description="User not found", response=errserializer),
            400: OpenApiResponse(
                description="Bad request (check your parameters)",
                response=errserializer,
            ),
        },
    )
    def post(self, request):
        serializer = OrderCreateSerializer(
            data=request.data, context={"request": request}
        )
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_201_CREATED)
        return response.Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class OrderPortfolioCheckView(views.APIView):
    """
    check user positions
    """

    serializer_class = OrderPortfolioCheckSerializer
    # permission_classes = (IsRegisteredUser,)

    @extend_schema(
        operation_id="check positions",
        responses={
            200: OpenApiResponse(
                response=OrderPortfolioCheckSerializer,
            ),
            404: OpenApiResponse(
                description="Bad request (position not found)", response=errserializer
            ),
            403: OpenApiResponse(
                description="Unauthorized request", response=errserializer
            ),
        },
        description="check user positions",
    )
    def post(self, request):

        serializer = OrderPortfolioCheckSerializer(
            data=request.data, context={"request": request}
        )
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)


class OrderUpdateViews(views.APIView):
    """
    Update positions
    """

    serializer_class = OrderUpdateSerializer
    permission_classes = (IsRegisteredUser,)

    def post(self, request):
        try:
            instance = OrderUpdateSerializer.Meta.model.objects.get(
                order_uid=request.data["order_uid"]
            )
        except OrderUpdateSerializer.Meta.model.DoesNotExist:
            return response.Response(
                {"detail": _("order not found")}, status=status.HTTP_404_NOT_FOUND
            )
        # ignore if fels account
        if not instance.user_id.id == 135:
            if instance.user_id.username != request.user.username:
                return response.Response(
                    {"detail": _("credentials not allowed to change this order")},
                    status=status.HTTP_403_FORBIDDEN,
                )
        serializer = OrderUpdateSerializer(
            instance, data=request.data, context={"request": request}
        )
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)


class OrderGetViews(viewsets.ViewSet):
    """
    Get details of an order
    """
    
    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        responses=OrderListSerializers,
        # more customizations
    )
    def list(self, request):
        user =request.user
        trans= [ order_uid.transaction_detail.get("order_uid",None) for 
                order_uid in TransactionHistory.objects.filter(
                    balance_uid=user.user_balance,transaction_detail__event__in=["return","create"])]
        pendings = [order.order_uid for order in Order.objects.filter(user_id=user,status='pending')]
        filtered_order = trans+pendings
        instances = Order.objects.prefetch_related('ticker').filter(pk__in=filtered_order).order_by('-created')
        self.serialzer_class = OrderListSerializers
        return response.Response(
            OrderListSerializers(instances, many=True).data, status=status.HTTP_200_OK
        )

    permission_classes = (IsRegisteredUser,)

    @extend_schema(
        responses=OrderDetailsSerializers,
        # more customizations
    )
    def retrieve(self, request, order_uid=None):
        try:
            instance = Order.objects.get(order_uid=order_uid)
            self.serialzer_class = OrderDetailsSerializers
            return response.Response(
                OrderDetailsSerializers(instance).data, status=status.HTTP_200_OK
            )
        except Order.DoesNotExist:
            return response.Response(
                {"detail": _("order not found")}, status=status.HTTP_404_NOT_FOUND
            )


class OrderActionViews(views.APIView):
    """
    Order actions
    """
    throttle_classes = [OrderActionThrottle]
    serializer_class = OrderActionSerializer
    permission_classes = (IsRegisteredUser,)

    def post(self, request):
        try:
            instance = OrderActionSerializer.Meta.model.objects.get(
                order_uid=request.data["order_uid"]
            )
        except OrderActionSerializer.Meta.model.DoesNotExist:
            return response.Response(
                {"detail": _("order not found")}, status=status.HTTP_404_NOT_FOUND
            )
        except KeyError as e:
            err = str(e)
            return response.Response(
                {"detail": _("error key %(err)s") % {err: err}}, status=status.HTTP_400_BAD_REQUEST
            )
        # ignore if fels account
        if not instance.user_id.id == 135:
            if instance.user_id.username != request.user.username:
                return response.Response(
                    {"detail": _("credentials not allowed to change this order")},
                    status=status.HTTP_403_FORBIDDEN,
                )
        serializer = OrderActionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_200_OK)
        return response.Response(serializer.errors)
