from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from general.slack import report_to_slack
import traceback as trace
import sys
from django.utils.timezone import now
from datetime import timedelta
from django.urls import reverse
from channels_presence.models import Room,Presence,get_channel_layer
from django.conf import settings
import asyncio
from django.contrib.postgres.fields import ArrayField
channel_layer = get_channel_layer()


class CustomRoomManager(models.Manager):

    def prune_presences(self, channel_layer=None, age=None):
        for room in ChannelPresence.objects.all():
            room.custom_prune_presences(age)
class ChannelPresence(Room):
    objects =CustomRoomManager()
    class Meta:
        proxy =True
    
    
    
    def remove_presence_inactive(self, channel_name=None, presence=None):
        if presence is None:
            try:
                presence = Presence.objects.get(room=self, channel_name=channel_name)
            except Presence.DoesNotExist:
                return

        asyncio.run(channel_layer.send(
               presence.channel_name,
                {
                    'type':'broadcastmessage',
                    'message': f'no active connection, connection closed',
                    'status':400
                }
                ))
        asyncio.run(channel_layer.send(
               presence.channel_name,
                {
                    'type':'force_close_connection'
                    
                }
                ))
        asyncio.run(channel_layer.group_discard(
            self.channel_name, presence.channel_name
        ))
        presence.delete()
        self.broadcast_changed(removed=presence)



    def custom_prune_presences(self, age_in_seconds=None):
        if age_in_seconds is None:
            age_in_seconds = getattr(settings, "CHANNELS_PRESENCE_MAX_AGE", 60)
        presence = Presence.objects.filter(
            room=self, last_seen__lt=now() - timedelta(seconds=age_in_seconds)
        )
        if presence.count() > 0:
            for inactive in presence:
                self.remove_presence_inactive(channel_name=inactive.channel_name)
        presence.delete()


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



    

