from django.core.management.base import BaseCommand, CommandError
from core.user.models import TransactionHistory, Accountbalance,User
from core.orders.models import OrderPosition,PositionPerformance, Order



class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "-u", "--username",type=str, help="for backup")


    def handle(self, *args, **options):
        user = User.objects.get(username=options['username'])
        PositionPerformance.objects.filter(position_uid__user_id=user).delete()
        Order.objects.filter(user_id=user).delete()
        OrderPosition.objects.filter(user_id=user).delete()
        TransactionHistory.objects.filter(balance_uid__user=user).delete()
        Accountbalance.objects.filter(user=user).delete()
        user.delete()
