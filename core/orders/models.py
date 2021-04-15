from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from core.universe.models import Universe
from core.user.models import User
from core.bot.models import BotOptionType
from django.db import IntegrityError
import uuid
# Create your models here.


class Order(BaseTimeStampModel):
    uid = models.UUIDField(primary_key=True, editable=False)
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='user_order')
    symbol = models.ForeignKey(
        Universe, on_delete=models.CASCADE, related_name='symbol_order')
    bot_id = models.CharField(max_length=255, null=True, blank=True)
    setup = models.JSONField(blank=True, null=True, default=dict)
    order_type = models.CharField(max_length=75, null=True, blank=True)
    placed = models.BooleanField(default=False)
    status = models.CharField(max_length=10, null=True,
                              blank=True, default='review')
    side = models.CharField(max_length=10, default='buy')
    amount = models.FloatField()
    placed_at = models.DateTimeField(null=True, blank=True)
    filled_at = models.DateTimeField(null=True, blank=True)
    canceled_at = models.DateTimeField(null=True, blank=True)
    order_summary = models.JSONField(blank=True, null=True, default=dict)
    is_init = models.BooleanField(default=True)
    price = models.FloatField()
    signal_id = models.CharField(null=True, blank=True, max_length=255)

    def save(self, *args, **kwargs):

        if not self.uid:
            self.uid = uuid.uuid4().hex
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
                        self.uid = uuid.uuid4().hex
                else:
                    success = True
        else:
            super().save(*args, **kwargs)


class OrderPosition(BaseTimeStampModel):

    uid = models.UUIDField(primary_key=True, editable=False)
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='user_position')
    symbol = models.ForeignKey(
        Universe, on_delete=models.CASCADE, related_name='ticker_ordered')
    bot_id = models.CharField(
        max_length=255, null=True, blank=True)  # user = stock
    expiry = models.DateField(null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    entry_price = models.FloatField(null=True, blank=True)
    investment_amount = models.FloatField(null=True, blank=True)
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
    current_inv_ret = models.FloatField(null=True, blank=True)
    current_inv_amt = models.FloatField(null=True, blank=True)
    is_live = models.BooleanField(default=False)
    share_num = models.FloatField(null=True, blank=True)
    current_returns = models.FloatField(null=True, blank=True, default=0)
    current_values = models.FloatField(null=True, blank=True, default=0)
    commision_fee = models.FloatField(null=True, blank=True, default=0)
    commision_fee_sell = models.FloatField(null=True, blank=True, default=0)
    vol = models.FloatField(null=True, blank=True)


    class Meta:
        managed = True
        db_table = "order_position"
    
    @property
    def bot(self):
        _bot = BotOptionType.objects.get(bot_id=self.bot_id)
        return _bot

    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = uuid.uuid4().hex
            # using your function as above or anything else
            success = False
            failures = 0
            while not success:
                try:
                    super(OrderPosition, self).save(*args, **kwargs)
                except IntegrityError:
                    failures += 1
                    if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                        raise KeyError
                    else:
                        # looks like a collision, try another random value
                        self.uid = uuid.uuid4().hex
                else:
                    success = True
        else:
            super().save(*args, **kwargs)


class PositionPerformance(BaseTimeStampModel):
    position = models.ForeignKey(
        OrderPosition, on_delete=models.CASCADE, related_name='order_position')
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
    order_id = models.ForeignKey(
        'Order', null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        managed = True
        db_table = "order_position_performance"

    def __str__(self):
        return str(self.created)
