from django.db import models
from core.djangomodule.models import BaseTimeStampModel

class ThirdpartyCredentials(BaseTimeStampModel):
    services = models.CharField(max_length=255, primary_key=True)
    token = models.CharField(max_length=255,null=True,blank=True)
    base_url = models.CharField(max_length=255,null=True,blank=True)
    username = models.CharField(max_length=255,null=True,blank=True)
    password = models.CharField(max_length=255,null=True,blank=True)
    extra_data = models.JSONField(null=True, blank=True,default=dict)

    def __str__(self):
        self.services

    class Meta:
        db_table = "services"
