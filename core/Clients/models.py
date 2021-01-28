from django.db import models
from core.universe.models import Currency,Universe
from core.user.models import User
from core.djangomodule.general import generate_id
from core.djangomodule.models import BaseTimeStampModel
from django.db import IntegrityError


class Client(BaseTimeStampModel):
    uid = models.CharField(max_length=255,primary_key=True)
    client_name = models.CharField(max_length=255)
    client_base_currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='client_currency')
    client_base_commision = models.FloatField(null=True,blank=True)
    use_currency = models.BooleanField(default=True)
    client_credentials = models.JSONField(null=True,blank=True)

    class Meta:
        managed = True
        db_table = 'client'

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
    uid = models.CharField(max_length=255,primary_key=True)
    user =models.ForeignKey(User, on_delete=models.CASCADE, related_name='client_user')
    client = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='client_related')
    extra_data = models.JSONField(null=True,blank=True)

    def __str__(self):
        return self.uid
    class Meta:
        managed = True
        db_table = 'user_clients'
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
        unique_together = ['ticker', 'client']