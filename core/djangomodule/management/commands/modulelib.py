from django.core.management.base import BaseCommand
from core.services import *
from core.orders.models import Order
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        # data = {
        #     'task':'hellow'
        # }
        # celery_app.send_task('config.celery.listener',args=(data,),queue='batch')
        # order = Order.objects.get(uid='8224d7d1-0f04-4429-83cc-002a0461dc2c')
        fields= Order._meta.get_fields()
        for field in fields:
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                print(field.get_attname())