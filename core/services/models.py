from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from general.slack import report_to_slack
import traceback as trace
import sys

from django.urls import reverse
from django.contrib.contenttypes.models import ContentType



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

class LogManager(models.Manager):
    

    
    def identify_error_function(self):
        tb = sys.exc_info()[-1]
        stk = trace.extract_tb(tb, 1)
        return stk[0][2]
    
    def create_log(self, **kwargs):
        kwargs['error_function'] = self.identify_error_function()
        kwargs['error_traceback'] = trace.format_exc()
        log = self.create(**kwargs)
        # save the error log
        return log





class ErrorLog(BaseTimeStampModel):
    error_description = models.TextField(null=True,blank=True)
    error_traceback = models.TextField(null=True,blank=True)
    error_message = models.TextField(null=True,blank=True)
    error_function = models.TextField(null=True,blank=True)
    objects= LogManager()

    def __str__(self):
        return f'error - {self.created}'

    class Meta:
        db_table = "log_error"
        ordering = ['created']
    
    def get_admin_url(self):
        return reverse("admin:%s_%s_change" % (self._meta.app_label, self._meta.model_name), args=(self.id,))

    def send_report_error(self):
        
        report_to_slack(f'\n*Found Error*\nError desc: {self.error_description}\nError message: {self.error_message}\nError function: {self.error_function}\nErorr Logs: https://services.asklora.ai{self.get_admin_url()}',
        channel='#error-log')
        report_to_slack(f'*Error Traceback:*\n {self.error_traceback}',channel='#error-log')

    

    

