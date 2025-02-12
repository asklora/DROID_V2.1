from rest_framework import serializers, exceptions
from .factory import *
from core.djangomodule.general import formatdigit
from .models import OrderPosition, PositionPerformance, OrderFee, Order
from core.bot.serializers import BotDetailSerializer
from core.bot.models import BotOptionType
from drf_spectacular.utils import OpenApiExample, extend_schema_serializer
from django.db.models import Sum
from core.Clients.models import UserClient, Client
from core.user.models import TransactionHistory
from core.user.convert import ConvertMoney
from django.apps import apps
from django.db import transaction as db_transaction
from .services import OrderPositionValidation
from django.utils.translation import gettext as _


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            "Get bot Performance by positions",
            description="Get bot Performance by positions",
            value=[
                {
                    "created": "2021-05-27T05:25:55.776Z",
                    "prev_bot_share_num": 0,
                    "share_num": 0,
                    "current_investment_amount": 0,
                    "side": "string",
                    "price": 0,
                    "hedge_share": "string",
                    "stamp": 0,
                    "commission": 0,
                    "current_pnl_ret": 0,
                    "current_pnl_amt": 0,
                    "initial_investment_amt": 0,
                    "current_value": 0,
                }
            ],
            response_only=True,  # signal that example only applies to responses
            status_codes=[200],
        ),
    ]
)
class PerformanceSerializer(serializers.ModelSerializer):
    prev_bot_share_num = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    price = serializers.FloatField(source="last_live_price")
    hedge_share = serializers.SerializerMethodField()
    stamp = serializers.SerializerMethodField()
    commission = serializers.SerializerMethodField()
    initial_investment_amt = serializers.SerializerMethodField()
    current_value = serializers.SerializerMethodField()
    current_exchange_rate = serializers.SerializerMethodField()
    amount = serializers.SerializerMethodField()

    class Meta:
        model = PositionPerformance
        fields = (
            "created",
            "updated",
            "prev_bot_share_num",
            "share_num",
            "current_investment_amount",
            "side",
            "price",
            "hedge_share",
            "stamp",
            "commission",
            "current_pnl_ret",
            "current_pnl_amt",
            "initial_investment_amt",
            "current_value",
            "current_exchange_rate",
            "amount",
        )

    def get_current_exchange_rate(self, obj) -> float:
        return ConvertMoney(
            obj.position_uid.ticker.currency_code,
            obj.position_uid.user_id.currency,
        ).get_exchange_rate()

    def get_current_value(self, obj) -> float:
        return obj.current_bot_cash_balance + obj.current_investment_amount

    def get_initial_investment_amt(self, obj) -> float:
        return obj.position_uid.investment_amount

    def get_hedge_share(self, obj) -> int:
        if obj.order_summary:
            if "hedge_shares" in obj.order_summary:
                return int(obj.order_summary["hedge_shares"])
        return 0

    def get_side(self, obj) -> str:
        if obj.order_uid:
            return obj.order_uid.side
        return "hold"

    def get_amount(self, obj) -> float:
        if obj.order_uid:
            return obj.order_uid.amount
        return 0

    def get_stamp(self, obj) -> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(
                order_uid=obj.order_uid,
                fee_type=f"{obj.order_uid.side} stamp_duty fee",
            )
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    def get_commission(self, obj) -> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(
                order_uid=obj.order_uid,
                fee_type=f"{obj.order_uid.side} commissions fee",
            )
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    def get_prev_bot_share_num(self, obj) -> int:
        prev = (
            PositionPerformance.objects.filter(
                position_uid=obj.position_uid, created__lt=obj.created
            )
            .order_by("created")
            .last()
        )
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
    bot_details = serializers.SerializerMethodField()
    currency = serializers.SerializerMethodField()
    total_share_num = serializers.FloatField(source="share_num")
    current_share_num = serializers.SerializerMethodField()
    current_exchange_rate = serializers.SerializerMethodField()
    current_values = serializers.SerializerMethodField()
    current_returns = serializers.SerializerMethodField()
    current_inv_ret = serializers.SerializerMethodField()
    sellable = serializers.BooleanField()

    class Meta:
        model = OrderPosition
        exclude = ("commision_fee", "commision_fee_sell", "share_num")

    def get_current_values(self, obj):
        latest_perf = obj.order_position.latest("created")
        return formatdigit(
            obj.current_values * latest_perf.exchange_rate,
            obj.user_id.user_balance.currency_code.is_decimal,
        )

    def get_current_returns(self, obj):
        latest_perf = obj.order_position.latest("created")
        return formatdigit(
            obj.current_returns * latest_perf.exchange_rate,
            obj.user_id.user_balance.currency_code.is_decimal,
        )

    def get_current_exchange_rate(self, obj) -> float:
        return ConvertMoney(
            obj.ticker.currency_code, obj.user_id.currency
        ).get_exchange_rate()

    def get_current_share_num(self, obj) -> float:
        return obj.order_position.latest("created").share_num

    def get_current_inv_ret(self, obj) -> float:
        return round(obj.current_inv_ret * 100, 2)

    def get_bot_details(self, obj) -> BotDetailSerializer:
        """add detail bot"""
        return BotDetailSerializer(obj.bot).data

    def get_turnover(self, obj) -> float:
        total = 0
        perf = (
            obj.order_position.all()
            .order_by("created")
            .values("last_live_price", "share_num")
        )
        for index, item in enumerate(perf):
            if index == 0:
                prev_share = 0
            else:
                prev_share = perf[index - 1]["share_num"]
            turn_over = item["last_live_price"] * abs(
                item["share_num"] - prev_share
            )
            total += turn_over
        return total

    def get_stamp(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event="stamp_duty",
            transaction_detail__position=obj.position_uid,
        ).aggregate(total=Sum("amount"))
        if transaction["total"]:
            result = round(transaction["total"], 2)
            return result
        return 0

    def get_commission(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event="fee",
            transaction_detail__position=obj.position_uid,
        ).aggregate(total=Sum("amount"))
        if transaction["total"]:
            result = round(transaction["total"], 2)
            return result
        return 0

    def get_total_fee(self, obj) -> float:
        transaction = TransactionHistory.objects.filter(
            transaction_detail__event__in=["fee"],
            transaction_detail__position=obj.position_uid,
        ).aggregate(total=Sum("amount"))
        transaction2 = TransactionHistory.objects.filter(
            transaction_detail__event__in=["stamp_duty"],
            transaction_detail__position=obj.position_uid,
        ).aggregate(total=Sum("amount"))
        if transaction["total"]:
            if transaction2["total"]:
                total2 = transaction2["total"]
            else:
                total2 = 0
            result = round(transaction["total"] - total2, 2)

            return result
        return 0

    def get_option_type(self, obj) -> str:
        return obj.bot.bot_option_type

    def get_stock_name(self, obj) -> str:
        if obj.ticker.ticker_name:
            return obj.ticker.ticker_name
        return obj.ticker.ticker_fullname

    def get_currency(self, obj) -> str:
        return obj.user_id.user_balance.currency_code.currency_code

    def get_last_price(self, obj) -> float:
        return obj.ticker.latest_price_ticker.close


@extend_schema_serializer(
    examples=[
        OpenApiExample(
            "Create new buy order",
            description="Create new buy order",
            value={
                "ticker": "0005.HKD",
                "price": 2.1,
                "bot_id": "string",
                "amount": 10000,
                "user": "198",
                "side": "buy",
                "margin": 1,
            },
            request_only=True,
        ),
        OpenApiExample(
            "Create new sell order",
            description="Create new sell order",
            value={
                "user": "198",
                "side": "sell",
                "ticker": "0005.HKD",
                "setup": {"position": "40fd100387464d1892dabdde291aa2cb"},
            },
            request_only=True,
        ),
    ]
)
class OrderCreateSerializer(serializers.ModelSerializer):
    user = serializers.CharField(required=False)
    price = serializers.FloatField(required=False)
    status = serializers.CharField(read_only=True)
    qty = serializers.FloatField(read_only=True)
    setup = serializers.JSONField(required=False)
    created = serializers.DateTimeField(required=False, read_only=True)
    margin = serializers.IntegerField(required=False, default=1)
    currency = serializers.SerializerMethodField(read_only=True)
    exchange_rate = serializers.FloatField(read_only=True)
    user_currency = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Order
        fields = [
            "ticker",
            "price",
            "bot_id",
            "amount",
            "user",
            "exchange_rate",
            "currency",
            "side",
            "status",
            "order_uid",
            "qty",
            "setup",
            "created",
            "margin",
            "user_currency",
        ]

    def to_internal_value(self, data):
        if self.initial_data["side"] == "sell":
            self.fields["bot_id"].required = False
            self.fields["amount"].required = False
            self.fields["ticker"].required = True
            self.fields["setup"].required = True
        else:
            self.fields["bot_id"].required = True
            self.fields["amount"].required = True
            self.fields["ticker"].required = True

        return super(OrderCreateSerializer, self).to_internal_value(data)

    def create(self, validated_data):
        if not "user" in validated_data:
            request = self.context.get("request", None)
            if request:
                validated_data["user_id"] = request.user
                user = request.user
            else:
                error = {"detail": _("missing user")}
                raise serializers.ValidationError(error)
        else:
            usermodel = apps.get_model("user", "User")
            try:
                user = usermodel.objects.get(id=validated_data.pop("user"))
            except usermodel.DoesNotExist:
                error = {
                    "detail": _("user not found with the given payload user")
                }
                raise exceptions.NotFound(error)
            validated_data["user_id"] = user

        controller = OrderController()
        processor = OrderProcessor[validated_data["side"]]

        return controller.process(processor(validated_data))

    def get_currency(self, obj) -> str:
        return obj.ticker.currency_code.currency_code

    def get_user_currency(self, obj) -> str:
        return obj.user_id.user_balance.currency_code.currency_code


@extend_schema_serializer(
    exclude_fields=("user",),  # schema ignore these field
)
class OrderPortfolioCheckSerializer(serializers.Serializer):

    ticker = serializers.CharField(required=True, write_only=True)
    bot_id = serializers.ListField(required=True, write_only=True)
    user = serializers.CharField(required=False, write_only=True)
    allowed_bot = serializers.ListField(required=False, read_only=True)

    def create(self, validated_data):
        request = self.context.get("request", None)
        user = validated_data.get("user", None)
        if user:
            user_id = user
        else:
            if request:
                user = request.user
                if user.is_anonymous:
                    raise exceptions.NotAcceptable()
                user_id = user.id
            else:
                raise exceptions.NotAcceptable()
        validation = OrderPositionValidation(
            validated_data["ticker"], validated_data["bot_id"], user_id
        )
        data = validation.allowed_bot()
        if data:
            return data
        else:
            raise exceptions.NotFound(
                {"detail": _("no position / order exist")}
            )


class OrderUpdateSerializer(serializers.ModelSerializer):
    user = serializers.CharField(required=False)
    status = serializers.CharField(read_only=True)
    setup = serializers.JSONField(read_only=True)
    qty = serializers.FloatField(read_only=True)
    order_uid = serializers.UUIDField()
    fee = serializers.FloatField(write_only=True, required=False)

    class Meta:
        model = Order
        fields = [
            "price",
            "bot_id",
            "amount",
            "order_uid",
            "status",
            "qty",
            "setup",
            "fee",
            "user",
        ]

    def update(self, instance, validated_data):
        request = self.context.get("request", None)
        if validated_data.get("user", None):
            usermodel = apps.get_model("user", "User")
            user = usermodel.objects.get(id=validated_data.pop("user"))
        else:
            user = request.user
        validated_data["side"] = instance.side
        if not validated_data.get("bot_id", None):
            validated_data["bot_id"] = instance.bot_id

        for keys, value in validated_data.items():
            setattr(instance, keys, value)
        if user.id == 135:
            fee = validated_data.get("fee", None)
            if fee:
                user_client = UserClient.objects.get(user_id=user.id)
                client = Client.objects.get(
                    client_uid=user_client.client.client_uid
                )
                client.commissions_buy = fee
                client.save()
        with db_transaction.atomic():
            try:
                instance.save()
            except Exception as e:
                error = {"detail": _("something went wrong")}
                raise serializers.ValidationError(error)
        return instance


class OrderDetailsSerializers(serializers.ModelSerializer):

    bot_name = serializers.SerializerMethodField()
    currency = serializers.SerializerMethodField()
    bot_range = serializers.SerializerMethodField()
    ticker_name = serializers.SerializerMethodField()

    exchange_rate = serializers.FloatField(read_only=True)
    user_currency = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Order
        fields = [
            "ticker",
            "price",
            "bot_id",
            "amount",
            "side",
            "exchange_rate",
            "order_uid",
            "status",
            "setup",
            "created",
            "filled_at",
            "placed",
            "placed_at",
            "canceled_at",
            "qty",
            "bot_name",
            "currency",
            "bot_range",
            "ticker_name",
            "user_currency",
        ]

    def get_ticker_name(self, obj) -> str:
        return obj.ticker.ticker_name

    def get_bot_name(self, obj) -> str:
        bot = BotOptionType.objects.get(bot_id=obj.bot_id)
        if not bot.is_stock():
            return bot.bot_type.bot_apps_name
        return "Unassisted Trade"

    def get_currency(self, obj) -> str:
        return obj.ticker.currency_code.currency_code

    def get_user_currency(self, obj) -> str:
        return obj.user_id.user_balance.currency_code.currency_code

    def get_bot_range(self, obj) -> str:
        bot = BotOptionType.objects.get(bot_id=obj.bot_id)
        return bot.duration


class OrderListSerializers(serializers.ModelSerializer):
    bot_name = serializers.SerializerMethodField()
    currency = serializers.SerializerMethodField()
    bot_range = serializers.SerializerMethodField()
    ticker_name = serializers.SerializerMethodField()
    exchange_rate = serializers.FloatField(read_only=True)
    user_currency = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Order
        fields = [
            "ticker",
            "side",
            "order_uid",
            "status",
            "created",
            "filled_at",
            "exchange_rate",
            "placed",
            "placed_at",
            "qty",
            "amount",
            "bot_name",
            "currency",
            "bot_range",
            "ticker_name",
            "setup",
            "user_currency",
        ]

    def get_ticker_name(self, obj) -> str:
        return obj.ticker.ticker_name

    def get_bot_name(self, obj) -> str:
        bot = BotOptionType.objects.get(bot_id=obj.bot_id)
        if not bot.is_stock():
            return bot.bot_type.bot_apps_name
        return "Unassisted Trade"

    def get_user_currency(self, obj) -> str:
        return obj.user_id.user_balance.currency_code.currency_code

    def get_currency(self, obj) -> str:
        return obj.ticker.currency_code.currency_code

    def get_bot_range(self, obj) -> str:
        bot = BotOptionType.objects.get(bot_id=obj.bot_id)
        return bot.duration


class OrderActionSerializer(serializers.ModelSerializer):
    action_id = serializers.CharField(read_only=True)
    order_uid = serializers.CharField(required=True)
    firebase_token = serializers.CharField(required=False, write_only=True)

    class Meta:
        model = Order
        fields = ["order_uid", "status", "action_id", "firebase_token"]

    def create(self, validated_data):
        processor = OrderProcessor["action"]
        controller = OrderController()
        return controller.process(processor(validated_data))
