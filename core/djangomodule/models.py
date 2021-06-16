from django.db import models
from django.utils import timezone
import pandas as pd


class BaseTimeStampModel(models.Model):
    created = models.DateTimeField(editable=True)
    updated = models.DateTimeField(editable=False)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = timezone.now()

        self.updated = timezone.now()
        return super(BaseTimeStampModel, self).save(*args, **kwargs)


class DataFrameModels(pd.DataFrame):

    def save(self,app,model,filter_prefix=None):
        """
        default base on ticker
        """
        if not filter_prefix:
            filter_prefix = 'ticker'

        from django.apps import apps
        Model = apps.get_model(app, model)
        pk_list = getattr(self, filter_prefix).to_list()
        filterset = {f'{filter_prefix}__in':pk_list}
        for item in Model.objects.filter(**filterset):
            self.loc[self[item.pk._meta.get_field().name] == item.pk]
            print(self)
