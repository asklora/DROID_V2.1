from django.db import models
from core.universe.models import Currency
from core.djangomodule.general import generate_id
from django.db import IntegrityError


class ClientAsklora(models.Model):
    uid = models.CharField(max_length=255)
    client_name = models.CharField(max_length=255)
    client_base_currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='client_currency')
    client_base_commision = models.FloatField(null=True,blank=True)
    use_currency = models.BooleanField(default=True)
    client_credentials = models.JSONField()

    def __str__(self):
        return self.client_name

    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = generate_id()
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(ClientAsklora, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.uid = generate_id()
            else:
                success = True
    

