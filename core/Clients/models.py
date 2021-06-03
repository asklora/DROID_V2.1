from django.db import models, IntegrityError
from core.universe.models import Currency, Universe
from core.user.models import User
from core.djangomodule.general import generate_id
from core.djangomodule.models import BaseTimeStampModel


class Client(BaseTimeStampModel):
    client_uid = models.CharField(
        max_length=255, primary_key=True, editable=False)
    client_name = models.CharField(max_length=255)
    client_credentials = models.JSONField(null=True, blank=True)
    commissions_buy = models.FloatField(null=True, blank=True)
    commissions_sell = models.FloatField(null=True, blank=True)
    commissions_type = models.TextField(null=True, blank=True)

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
    uid = models.CharField(max_length=255, primary_key=True, editable=False)
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name="client_user", db_column="user_id")
    client = models.ForeignKey(Client, on_delete=models.CASCADE,
                               related_name="client_related", db_column="client_uid")
    currency_code = models.ForeignKey(
        Currency, on_delete=models.CASCADE, related_name="client_currency", db_column="currency_code", null=True, blank=True)
    use_currency = models.BooleanField(default=True)
    extra_data = models.JSONField(null=True, blank=True)
    stamp_duty_buy = models.FloatField(null=True, blank=True)
    stamp_duty_sell = models.FloatField(null=True, blank=True)
    stamp_duty_type = models.TextField(null=True, blank=True)

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
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               related_name="client_universe", db_column="ticker")
    client = models.ForeignKey(Client, on_delete=models.CASCADE,
                               related_name="universe_client", db_column="client_uid")

    class Meta:
        managed = True
        db_table = "universe_client"
        get_latest_by = "created"
        unique_together = ["ticker", "client"]

    def __str__(self):
        return f"{self.ticker.ticker}-{self.client.client_name}"


class ClientTopStock(BaseTimeStampModel):
    uid = models.CharField(max_length=255, primary_key=True, editable=False)
    client = models.ForeignKey(Client, on_delete=models.CASCADE,
                               related_name="client_top_stock", db_column="client_uid")
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               related_name="universe_top_stock", db_column="ticker")
    spot_date = models.DateField(null=True, blank=True)
    expiry_date = models.DateField(null=True, blank=True)
    has_position = models.BooleanField(default=False)
    position_uid = models.CharField(
        max_length=255, editable=False, null=True, blank=True)
    execution_date = models.DateField(null=True, blank=True)
    completed_date = models.DateField(null=True, blank=True)
    event = models.CharField(max_length=50, null=True, blank=True)
    bot_id = models.CharField(max_length=80, null=True, blank=True)
    currency_code = models.ForeignKey(
        Currency, on_delete=models.CASCADE, related_name="topstock_currency", db_column="currency_code")
    rank = models.IntegerField(null=True, blank=True)
    service_type = models.CharField(max_length=80, null=True, blank=True)
    capital = models.CharField(max_length=80, null=True, blank=True)
    bot = models.CharField(max_length=80, null=True, blank=True)
    week_of_year = models.IntegerField(null=True, blank=True)

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

    def __str__(self):
        return f"{self.ticker.ticker}-{self.client.client_name}"
