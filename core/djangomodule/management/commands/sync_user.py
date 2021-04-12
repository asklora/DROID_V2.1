from django.core.management.base import BaseCommand
from core.user.models import User
from core.user.serializers import UserSerializer
from core import services
from rest_framework import serializers
import json
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        serialize = UserSerializer(User.objects.all(), many=True).data
        for user in serialize:
            data ={
            'type':'function',
            'module':'core.djangomodule.crudlib.user.sync_user',
            'payload':dict(user)
        }
            services.celery_app.send_task('config.celery.listener',args=(data,),queue='asklora')