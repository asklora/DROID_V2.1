from django.db import models,IntegrityError
from core.universe.models import Currency,Universe
from core.user.models import User
from core.djangomodule.general import generate_id
from core.djangomodule.models import BaseTimeStampModel


class Client(BaseTimeStampModel):
    uid = models.CharField(max_length=255,primary_key=True,editable=False)
    client_name = models.CharField(max_length=255)
    client_base_currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name="client_currency")
    client_base_commision = models.FloatField(null=True,blank=True)
    use_currency = models.BooleanField(default=True)
    client_credentials = models.JSONField(null=True,blank=True)

    class Meta:
        managed = True
        db_table = "client"

    def __str__(self):
        return self.client_name

    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = generate_id(6)
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(Client, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.uid = generate_id(6)
            else:
                success = True
    
class UserClient(BaseTimeStampModel):
    uid = models.CharField(max_length=255,primary_key=True,editable=False)
    user =models.OneToOneField(User, on_delete=models.CASCADE, related_name="client_user")
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="client_related")
    extra_data = models.JSONField(null=True,blank=True)

    def __str__(self):
        return self.uid
    class Meta:
        managed = True
        db_table = "user_clients"
    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = generate_id(12)
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(UserClient, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.uid = generate_id(12)
            else:
                success = True

class UniverseClient(BaseTimeStampModel):
    ticker = models.ForeignKey(Universe,on_delete=models.CASCADE, related_name="client_universe",db_column="ticker")
    client = models.ForeignKey(Client,on_delete=models.CASCADE, related_name="universe_client",db_column="client")

    class Meta:
        managed = True
        db_table = "universe_client"
        get_latest_by = "created"
        unique_together = ["ticker", "client"]

    
class ClientTopStock(BaseTimeStampModel):
    WAIT="Inactive"
    ACTIVE="Active"
    FINISHED="Completed"
    status_choices = (
        (WAIT, "Inactive"),
        (ACTIVE, "Active"),
        (FINISHED, "Completed"),

    )
    uid = models.CharField(max_length=255,primary_key=True,editable=False)
    client = models.ForeignKey(Client,on_delete=models.CASCADE, related_name="client_top_stock",db_column="client")
    ticker =models.ForeignKey(Universe,on_delete=models.CASCADE, related_name="universe_top_stock",db_column="ticker")
    use_signal= models.BooleanField(default=False)
    spot_date=models.DateField(null=True,blank=True)
    expiry_date=models.DateField(null=True,blank=True)
    has_order = models.BooleanField(default=False)
    order_number = models.CharField(max_length=255,editable=False,null=True,blank=True)
    current_status = models.CharField(max_length=25,default=WAIT,choices=status_choices)
    execution_date=models.DateField(null=True,blank=True)
    completed_date=models.DateField(null=True,blank=True)
    event = models.CharField(max_length=50)
    last_closing_price = models.FloatField(null=True, blank=True)
    investment_amount = models.FloatField(null=True, blank=True)
    vol_t = models.FloatField(null=True, blank=True)
    max_loss_pct = models.FloatField(null=True, blank=True)
    max_loss_price = models.FloatField(null=True, blank=True)
    max_loss_amount = models.FloatField(null=True, blank=True)
    target_profit_pct = models.FloatField(null=True, blank=True)
    target_profit_price = models.FloatField(null=True, blank=True)
    target_profit_amount = models.FloatField(null=True, blank=True)
    execution_price = models.FloatField(null=True, blank=True)
    rank = models.IntegerField(null=True, blank=True)

    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = generate_id(15)
            # using your function as above or anything else
            success = False
            failures = 0
            while not success:
                try:
                    super(ClientTopStock, self).save(*args, **kwargs)
                except IntegrityError:
                    failures += 1
                    if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                        raise KeyError
                    else:
                        # looks like a collision, try another random value
                        self.uid = generate_id(15)
                else:
                    success = True
        else:
            super().save(*args, **kwargs)
    
    class Meta:
        managed = True
        db_table = "client_top_stock"
        verbose_name_plural = "Client Generated Top stock"

    
class ClientBotPerformance(BaseTimeStampModel):
    order = models.ForeignKey(
        ClientTopStock, on_delete=models.CASCADE, related_name="portfolio_perfomance")
    last_live_price = models.FloatField(null=True, blank=True)
    current_pnl_ret = models.FloatField(null=True, blank=True)
    current_pnl_amt = models.FloatField(null=True, blank=True)
    current_bot_cash_balance = models.FloatField(null=True, blank=True)
    share = models.FloatField(null=True, blank=True)
    current_investment_amount = models.FloatField(null=True, blank=True)
    trading_day = models.DateField(null=True, blank=True)
    last_hedge_delta = models.FloatField(null=True, blank=True)

    class Meta:
        managed = True
        db_table = "client_bot_history"
        verbose_name_plural = "Client Bot History"