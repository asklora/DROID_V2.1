from django.core.management.base import BaseCommand, CommandError
from core.Clients.models import ClientTopStock
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import TransactionHistory, Accountbalance
import json


class Command(BaseCommand):

    def handle(self, *args, **options):
        print("deleting performance . . .")
        PositionPerformance.objects.all().delete()
        print("deleting position . . .")
        OrderPosition.objects.all().delete()
        print("deleting order . . .")
        Order.objects.all().delete()
        print("rollback top stock value . . .")
        ClientTopStock.objects.all().update(position_uid=None, has_position=False)
        print("rollback transaction . . .")
        Accountbalance.objects.all().delete()
        TransactionHistory.objects.all().delete()
        with open("files/balance.json") as f:
            balancedata = json.load(f)
            if balancedata:
                for data in balancedata:
                    user_id = data.pop("user")
                    currency = data.pop("currency_code")
                    amount = data.pop("amount")
                    Accountbalance.objects.create(
                        user_id=user_id,
                        currency_code_id=currency,
                        **data
                    )
            f.close()
        with open("files/balance.json") as ftr:
            transdata = json.load(ftr)
            if transdata:
                for data in transdata:
                    balance_uid = data.pop("balance_uid")
                    TransactionHistory.objects.create(
                        balance_uid_id=balance_uid,
                        amount=data["amount"],
                        side="credit",
                        transaction_detail={
                        "description":"first deposit",
                        }
                        
                    )
            ftr.close()
