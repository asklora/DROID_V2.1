from django.core.management.base import BaseCommand, CommandError

from core.orders.models import Order, PositionPerformance, OrderPosition
from core.orders.models import Order, PositionPerformance, OrderPosition


class Command(BaseCommand):

    def handle(self, *args, **options):

        PositionPerformance.objects.filter(position_uid__user_id__in=[])
