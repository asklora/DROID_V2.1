from django.core.management.base import BaseCommand
from core.services import *
from core.orders.models import Order,OrderPosition




class Command(BaseCommand):
    
    def handle(self, *args, **options):
        orders = OrderPosition.objects.filter(is_live=True)
        # for order in orders:
        #     pass