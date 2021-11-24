from django.db import models
from django.utils import timezone
import pandas as pd


class BaseTimeStampModel(models.Model):
    """
    Base model for timestamp support, related models: :model:`Clients.Client` and its related models, :model:`orders.Order` etc.
    """
    created = models.DateTimeField(editable=True)
    updated = models.DateTimeField(editable=False)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = timezone.now()

        self.updated = timezone.now()
        return super(BaseTimeStampModel, self).save(*args, **kwargs)
