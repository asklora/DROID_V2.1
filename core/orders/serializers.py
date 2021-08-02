from rest_framework import serializers, exceptions
from .models import OrderPosition, PositionPerformance, OrderFee, Order
from drf_spectacular.utils import OpenApiExample, extend_schema_serializer
from django.db.models import Sum, F
from core.user.models import TransactionHistory, User
from django.utils import timezone
from django.apps import apps
from datasource.rkd import RkdData
from django.db import transaction as db_transaction
from core.djangomodule.general import UnixEpochDateField
import json


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            'Get bot Performance by positions',
            description='Get bot Performance by positions',
            value=[{
                "created": "2021-05-27T05:25:55.776Z",
                "prev_bot_share_num": 0,
                "share_num": 0,
                "current_investment_amount": 0,
                "side": "string",
                "price": 0,
                "hedge_share": "string",
                'stamp': 0,
                'commission': 0

            }],
            response_only=True,  # signal that example only applies to responses
            status_codes=[200]
        ),
    ]
)
class PerformanceSerializer(serializers.ModelSerializer):
    prev_bot_share_num = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    price = serializers.FloatField(source='last_live_price')
    hedge_share = serializers.SerializerMethodField()
    stamp = serializers.SerializerMethodField()
    commission = serializers.SerializerMethodField()

    class Meta:
        model = PositionPerformance
        fields = ('created', 'prev_bot_share_num', 'share_num', 'current_investment_amount',
                  'side', 'price', 'hedge_share', 'stamp', 'commission')

    def get_hedge_share(self, obj) -> int:
        if obj.order_summary:
            if 'hedge_shares' in obj.order_summary:
                return int(obj.order_summary['hedge_shares'])
        return 0

    def get_side(self, obj) -> str:
        if obj.order_uid:
            return obj.order_uid.side
        return "hold"

    def get_stamp(self, obj) -> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(
                order_uid=obj.order_uid, fee_type=f'{obj.order_uid.side} stamp_duty fee')
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    def get_commission(self, obj) -> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(
                order_uid=obj.order_uid, fee_type=f'{obj.order_uid.side} commissions fee')
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    def get_prev_bot_share_num(self, obj) -> int:
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
        if prev:
            return int(prev.share_num)
        return 0


class PositionSerializer(serializers.ModelSerializer):
    option_type = serializers.SerializerMethodField()
    stock_name = serializers.SerializerMethodField()
    last_price = serializers.SerializerMethodField()
    stamp = serializers.SerializerMethodField()
    commission = serializers.SerializerMethodField()
    total_fee = serializers.SerializerMethodField()
    turnover = serializers.SerializerMethodField()

    class Meta:
        model = OrderPosition
        exclude = ("commision_fee", "commision_fee_sell")

    def get_turnover(self, obj) -> float:
        total = 0
        perf = obj.order_position.all().order_by(
            'created').values('last_live_price', 'share_num')
        for index, item in enumerate(perf):
            if index == 0:
                prev_share = 0
            else:
                prev_share = perf[index-1]['share_num']
            turn_over = item['last_live_price'] * \
                abs(item['share_num'] - prev_share)
            total += turn_over
        return total

    def get_stamp(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event='stamp_duty', transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0

    def get_commission(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event='fee', transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0

    def get_total_fee(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event__in=['fee', 'stamp_duty'], transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0

    def get_option_type(self, obj) -> str:
        return obj.bot.bot_option_type

    def get_stock_name(self, obj) -> str:
        if obj.ticker.ticker_name:
            return obj.ticker.ticker_name
        return obj.ticker.ticker_fullname

    def get_last_price(self, obj) -> float:
        return obj.ticker.latest_price_ticker.close


class OrderCreateSerializer(serializers.ModelSerializer):
    user = serializers.CharField(required=False)
    price = serializers.FloatField(required=False)
    status = serializers.CharField(read_only=True)
    qty = serializers.FloatField(read_only=True)
    setup = serializers.JSONField(read_only=True)

    class Meta:
        model = Order
        fields = ['ticker', 'price', 'bot_id', 'amount', 'user',
                  'side', 'status', 'order_uid', 'qty', 'setup']

    def create(self, validated_data):
        if not 'user' in validated_data:
            request = self.context.get('request', None)
            if request:
                validated_data['user_id'] = request.user
                user = request.user
            else:
                error = {'detail': 'missing user'}
                raise serializers.ValidationError(error)
        else:
            usermodel = apps.get_model('user', 'User')
            try:
                user = usermodel.objects.get(id=validated_data.pop('user'))
            except usermodel.DoesNotExist:
                error = {'detail': 'user not found with the given payload user'}
                raise exceptions.NotFound(error)
            validated_data['user_id'] = user
        if validated_data['amount'] > user.user_balance.amount:
            raise exceptions.NotAcceptable({'detail': 'insuficent balance'})

        if not 'price' in validated_data:
            rkd = RkdData()
            df = rkd.get_quote([validated_data['ticker'].ticker], df=True)
            df['latest_price'] = df['latest_price'].astype(float)
            ticker = df.loc[df["ticker"] == validated_data['ticker'].ticker]
            validated_data['price'] = ticker.iloc[0]['latest_price']
        with db_transaction.atomic():
            order = Order.objects.create(**validated_data, order_type='apps')
        return order


class OrderUpdateSerializer(serializers.ModelSerializer):
    status = serializers.CharField(read_only=True)
    setup = serializers.JSONField(read_only=True)
    qty = serializers.FloatField(read_only=True)
    order_uid = serializers.UUIDField()

    class Meta:
        model = Order
        fields = ['price', 'bot_id', 'amount',
                  'order_uid', 'status', 'qty', 'setup']

    def update(self, instance, validated_data):
        for keys, value in validated_data.items():
            setattr(instance, keys, value)

        with db_transaction.atomic():
            try:
                instance.save()
            except Exception as e:
                error = {'detail': 'something went wrong'}
                raise serializers.ValidationError(error)
        return instance


class OrderDetailsSerializers(serializers.ModelSerializer):

    # created = UnixEpochDateField(source='created')
    # filled_at = UnixEpochDateField(source='filled_at')
    # placed_at = UnixEpochDateField(source='placed_at')
    # canceled_at = UnixEpochDateField(source='canceled_at')

    class Meta:
        model = Order
        fields = ['ticker', 'price', 'bot_id', 'amount', 'side',
                  'order_uid', 'status', 'setup', 'created', 'filled_at',
                  'placed', 'placed_at', 'canceled_at', 'qty']


class OrderListSerializers(serializers.ModelSerializer):
    # created = UnixEpochDateField(source='created')
    # filled_at = UnixEpochDateField(source='filled_at')
    # placed_at = UnixEpochDateField(source='placed_at')

    class Meta:
        model = Order
        fields = ['ticker', 'side',
                  'order_uid', 'status', 'created', 'filled_at',
                  'placed', 'placed_at', 'qty']


class OrderActionSerializer(serializers.ModelSerializer):
    action_id = serializers.CharField(read_only=True)
    order_uid = serializers.CharField(required=True)
    firebase_token = serializers.CharField(required=False, write_only=True)

    class Meta:
        model = Order
        fields = ['order_uid', 'status', 'action_id', 'firebase_token']

    def create(self, validated_data):
        instance = OrderActionSerializer.Meta.model.objects.get(
            order_uid=validated_data['order_uid'])
        if instance.status == 'filled':
            raise exceptions.MethodNotAllowed(
                {'detail': 'order already filled, you cannot cancel / confirm'})
        from core.services.order_services import order_executor
        payload = json.dumps(validated_data)
        print(payload)
        task = order_executor.delay(payload)
        data = {'action_id': task.id, 'status': 'executed',
                'order_uid': validated_data['order_uid']}
        return data
