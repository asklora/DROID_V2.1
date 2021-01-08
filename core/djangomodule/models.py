from django.db import models
from django.utils import timezone

class BaseTimeStampModel(models.Model):
    created = models.DateField(editable=False)
    updated = models.DateField(editable=False)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = timezone.now()

        self.updated = timezone.now()
        return super(BaseTimeStampModel, self).save(*args, **kwargs)
