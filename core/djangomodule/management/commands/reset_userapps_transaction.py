from django.core.management.base import BaseCommand
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import TransactionHistory, Accountbalance,User,UserProfitHistory,UserDepositHistory
from ingestion.firestore_migration import firebase_user_update
from bot.calculate_bot import populate_daily_profit
from django.db import transaction

class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        users = User.objects.filter(current_status='verified')
        user_ids = [user.id for user in users]
        transaction.set_autocommit(False)
        for user in users:
            print(user.id)
            PositionPerformance.objects.filter(position_uid__user_id=user).delete()
            Order.objects.filter(user_id=user).delete()
            OrderPosition.objects.filter(user_id=user).delete()
            TransactionHistory.objects.filter(balance_uid__user=user).exclude(transaction_detail__event='first deposit').delete()
            Accountbalance.objects.filter(user=user).update(amount=100000)
            UserProfitHistory.objects.filter(user_id=user).delete()
            UserDepositHistory.objects.filter(user_id=user).delete()
        transaction.commit()
        print(user_ids)
        firebase_user_update(user_id=user_ids)
        populate_daily_profit(user_id=user_ids)
        