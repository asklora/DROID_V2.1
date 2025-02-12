from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from core.universe.models import Universe
from core.user.models import User
from core.bot.models import BotOptionType
from django.db import IntegrityError
import uuid
from core.djangomodule.general import generate_id, formatdigit
from simple_history.models import HistoricalRecords
from core.services.notification import send_notification
from bot.calculate_bot import populate_daily_profit
from ingestion.firestore_migration import firebase_user_update
from core.user.convert import ConvertMoney
from django.utils import timezone
from datetime import datetime, timedelta







class Feature(BaseTimeStampModel):
    name = models.CharField(max_length=255, unique=True)
    active = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = "Features"

    def __str__(self):
        return f"{self.name}"

class Order(BaseTimeStampModel):
    """
    Orders created by the users
    """

    order_uid = models.UUIDField(primary_key=True, editable=False)
    user_id = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="user_order",
        db_column="user_id",
    )
    ticker = models.ForeignKey(
        Universe,
        on_delete=models.CASCADE,
        related_name="symbol_order",
        db_column="ticker",
    )
    bot_id = models.CharField(max_length=255, null=True, blank=True)
    setup = models.JSONField(blank=True, null=True, default=dict)
    order_type = models.CharField(max_length=75, null=True, blank=True)
    placed = models.BooleanField(default=False)
    status = models.CharField(
        max_length=10, null=True, blank=True, default="review"
    )
    side = models.CharField(max_length=10, default="buy")
    amount = models.FloatField()  # default
    placed_at = models.DateTimeField(null=True, blank=True)
    filled_at = models.DateTimeField(null=True, blank=True)
    canceled_at = models.DateTimeField(null=True, blank=True)
    order_summary = models.JSONField(blank=True, null=True, default=dict)
    is_init = models.BooleanField(default=True)
    price = models.FloatField()
    performance_uid = models.CharField(null=True, blank=True, max_length=255)
    qty = models.FloatField(null=True, blank=True)  # disini x margin
    history = HistoricalRecords(table_name="order_changes_history")
    margin = models.FloatField(null=True, blank=True, default=1)
    exchange_rate = models.FloatField(null=True, blank=True, default=1)

    class Meta:
        managed = True
        db_table = "orders"

    def insufficient_balance(self):
        return (
            self.user_amount / self.margin
        ) > self.user_id.user_balance.amount and self.side == "buy"

    @property
    def is_bot_order(self):
        return self.bot_id != "STOCK_stock_0"

    @property
    def is_app_order(self):
        return self.order_type == "apps"

    def populate_to_firebase(self):
        populate_daily_profit()
        firebase_user_update(user_id=[self.user_id.id])

    @property
    def userconverter(self):
        return ConvertMoney(self.ticker.currency_code, self.user_id.currency)

    @property
    def converted_amount(self):
        converter = ConvertMoney(
            self.user_id.currency, self.ticker.currency_code
        )
        return converter.convert(self.amount)

    @property
    def user_amount(self):
        return formatdigit(
            self.amount * self.exchange_rate,
            self.user_id.user_balance.currency_code.is_decimal,
        )

    def save(self, *args, **kwargs):

        if not self.order_uid:
            self.order_uid = uuid.uuid4().hex
            # using your function as above or anything else
            success = False
            failures = 0
            while not success:
                try:
                    super(Order, self).save(*args, **kwargs)
                except IntegrityError:
                    failures += 1
                    if failures > 5:
                        raise KeyError
                    else:
                        self.order_uid = uuid.uuid4().hex
                else:
                    success = True
        else:
            super().save(*args, **kwargs)


class OrderPosition(BaseTimeStampModel):
    """
    Confirmed orders with status field equals to "filled"
    """

    position_uid = models.CharField(
        primary_key=True, editable=False, max_length=500
    )
    user_id = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="user_position",
        db_column="user_id",
    )
    ticker = models.ForeignKey(
        Universe,
        on_delete=models.CASCADE,
        related_name="ticker_ordered",
        db_column="ticker",
    )
    bot_id = models.CharField(
        max_length=255, null=True, blank=True
    )  # user = stock
    expiry = models.DateField(null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    entry_price = models.FloatField(null=True, blank=True)
    investment_amount = models.FloatField(default=0)
    max_loss_pct = models.FloatField(null=True, blank=True)
    max_loss_price = models.FloatField(null=True, blank=True)
    max_loss_amount = models.FloatField(null=True, blank=True)
    target_profit_pct = models.FloatField(null=True, blank=True)
    target_profit_price = models.FloatField(null=True, blank=True)
    target_profit_amount = models.FloatField(null=True, blank=True)
    bot_cash_balance = models.FloatField(null=True, blank=True)
    event = models.CharField(max_length=75, null=True, blank=True)
    event_date = models.DateField(null=True, blank=True)
    final_price = models.FloatField(null=True, blank=True)
    final_return = models.FloatField(null=True, blank=True)
    final_pnl_amount = models.FloatField(null=True, blank=True)
    current_inv_ret = models.FloatField(null=True, blank=True, default=0)
    current_inv_amt = models.FloatField(null=True, blank=True, default=0)
    is_live = models.BooleanField(default=False)
    share_num = models.FloatField(null=True, blank=True, default=0)
    current_returns = models.FloatField(null=True, blank=True, default=0)
    current_values = models.FloatField(null=True, blank=True, default=0)
    commision_fee = models.FloatField(null=True, blank=True, default=0)
    commision_fee_sell = models.FloatField(null=True, blank=True, default=0)
    vol = models.FloatField(null=True, blank=True)
    margin = models.FloatField(default=1)
    bot_cash_dividend = models.FloatField(null=True, blank=True, default=0)
    history = HistoricalRecords(table_name="order_position_history")
    exchange_rate = models.FloatField(null=True, blank=True, default=1)

    class Meta:
        managed = True
        db_table = "orders_position"
        indexes = [models.Index(fields=["user_id", "ticker"])]

    def current_return(self):
        performance = self.order_position.filter(position_uid=self.position_uid)
        if performance.exists():
            perf = performance.latest("created")
            ret = (
                perf.current_bot_cash_balance + perf.current_investment_amount
            ) - self.investment_amount
            # if self.user_currency != self.currency:
            #     ret = ret * self.currency_rate
            ret = round(ret, 2)
            return ret
        return 0

    def current_value(self):
        # CURRENT ASSETS portfolio
        performance = self.order_position.filter(position_uid=self.position_uid)
        if performance.exists():
            perf = performance.latest("created")
            return formatdigit(
                perf.current_investment_amount + perf.current_bot_cash_balance,
                self.ticker.currency_code.is_decimal,
            )
        return 0

    def stock_amount(self):
        performance = self.order_position.filter(position_uid=self.position_uid)
        if performance.exists():
            perf = performance.latest("created")
            return formatdigit(
                perf.current_investment_amount,
                self.ticker.currency_code.is_decimal,
            )
        return 0

    @property
    def sellable(self):
        feature = Feature.objects.get(name='prevent_instant_sell')
        if feature.active:
            return timezone.now() > self.created + timedelta(hours=9)
        return True

    @property
    def bot(self):
        _bot = BotOptionType.objects.get(bot_id=self.bot_id)
        return _bot

    def save(self, *args, **kwargs):
        self.current_returns = self.current_return()
        self.current_values = self.current_value()
        self.current_inv_amt = self.stock_amount()
        if not self.position_uid:
            self.position_uid = uuid.uuid4().hex
            # using your function as above or anything else
            success = False
            failures = 0
            while not success:
                try:
                    super(OrderPosition, self).save(*args, **kwargs)
                except IntegrityError:
                    failures += 1
                    if (
                        failures > 5
                    ):  # or some other arbitrary cutoff point at which things are clearly wrong
                        raise KeyError
                    else:
                        # looks like a collision, try another random value
                        self.position_uid = uuid.uuid4().hex
                else:
                    success = True
        else:
            super().save(*args, **kwargs)

    def notify_bot_done(self):
        if not self.bot.is_stock():
            return send_notification(
                self.user_id.username,
                f"Bot {self.bot.bot_type.bot_apps_name} completed",
                f"{self.ticker} - {self.event}",
            )


class PositionPerformance(BaseTimeStampModel):
    """
    Tracking the changes in the position of the orders and also AI's performance
    """

    performance_uid = models.CharField(
        max_length=255, primary_key=True, editable=False
    )
    position_uid = models.ForeignKey(
        OrderPosition,
        on_delete=models.CASCADE,
        related_name="order_position",
        db_column="position_uid",
    )
    last_spot_price = models.FloatField(null=True, blank=True)
    last_live_price = models.FloatField(null=True, blank=True)
    current_pnl_ret = models.FloatField(null=True, blank=True)
    current_pnl_amt = models.FloatField(null=True, blank=True)
    current_bot_cash_balance = models.FloatField(null=True, blank=True)
    share_num = models.FloatField(null=True, blank=True)
    current_investment_amount = models.FloatField(null=True, blank=True)
    last_hedge_delta = models.FloatField(null=True, blank=True)
    option_price = models.FloatField(null=True, blank=True)
    strike = models.FloatField(blank=True, null=True)
    barrier = models.FloatField(blank=True, null=True)
    r = models.FloatField(blank=True, null=True)
    q = models.FloatField(blank=True, null=True)
    v1 = models.FloatField(blank=True, null=True)
    v2 = models.FloatField(blank=True, null=True)
    # delta = models.FloatField(blank=True, null=True)
    strike_2 = models.FloatField(blank=True, null=True)
    # order response from third party
    order_summary = models.JSONField(null=True, blank=True)
    order_uid = models.ForeignKey(
        "Order",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        db_column="order_uid",
    )
    status = models.CharField(null=True, blank=True, max_length=200)
    exchange_rate = models.FloatField(null=True, blank=True, default=1)

    def save(self, *args, **kwargs):
        if not self.performance_uid:
            self.performance_uid = generate_id(9)
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(PositionPerformance, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if (
                    failures > 5
                ):  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.performance_uid = generate_id(9)
            else:
                success = True

    class Meta:
        managed = True
        db_table = "orders_position_performance"
        indexes = [
            models.Index(fields=["created", "position_uid", "order_uid"])
        ]

    def __str__(self):
        return str(self.created)


class OrderFee(BaseTimeStampModel):
    """
    Order fees according to their types
    """

    order_uid = models.ForeignKey(
        Order,
        on_delete=models.CASCADE,
        related_name="orders_fee_orders",
        db_column="order_uid",
    )
    fee_type = models.TextField(null=True, blank=True)
    amount = models.FloatField(default=0)

    class Meta:
        managed = True
        db_table = "orders_fee"

    def __str__(self):
        return f"{self.order_uid.order_uid}-{self.created}-{self.fee_type}"


