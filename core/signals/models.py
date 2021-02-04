from core.djangomodule.models import BaseTimeStampModel
from django.db import models
from core.Clients.models import ClientTopStock
from django.db.models import Value




class LoggerSignal(BaseTimeStampModel):
    logger_id = models.ForeignKey(ClientTopStock,on_delete=models.CASCADE,related_name='logger')
    market_status = models.BooleanField(default=True)
    response = models.JSONField(null=True,blank=True,default=dict)
    status_code = models.IntegerField(null=True,blank=True)
    logger_client_id = models.CharField(max_length=300,null=True,blank=True)
    endpoint = models.CharField(max_length=300,null=True,blank=True)
    header = models.JSONField(null=True,blank=True,default=dict)
    body = models.JSONField(null=True,blank=True,default=dict)
    event = models.CharField(max_length=250,null=True,blank=True)

    def __str__(self):
        return self.created
    
    class Meta:
        managed = True
        db_table = 'signal_logger'
