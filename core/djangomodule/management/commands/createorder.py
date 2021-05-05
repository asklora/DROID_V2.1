from django.core.management.base import BaseCommand, CommandError
from core.orders.models import Order
from core.bot.models import BotOptionType
from core.user.models import User
import pandas as pd


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-a", "--account", type=str, help="email")
        parser.add_argument("-t", "--ticker", type=str, help="ticker")
        parser.add_argument("-p", "--price", type=float, help="price")
        parser.add_argument("-d", "--date", type=str, help="spot date")
        parser.add_argument("-b", "--bot", type=str, help="bot id")
        parser.add_argument("-amt", "--amount", type=float, help="amount")
        # parser.add_argument("-f4w", "--fels4w", action="store_true", help="only use test account")

    def handle(self, *args, **options):
        user = User.objects.get(email=options["account"])
        spot_date = pd.Timestamp(options["date"])
        # for bot in BotOptionType.objects.all():
        #     print(f"execute order bot {bot.bot_option_name}")
        order = Order.objects.create(
            ticker_id=options["ticker"],
            created=spot_date,
            price=options["price"],
            bot_id=options["bot"],
            amount=options["amount"],
            user_id=user
        )
        if order:
            order.placed = True
            order.placed_at = spot_date
            order.save()
        if order.status == "pending":
            order.status = "filled"
            order.filled_at = spot_date
            order.save()
        print(f"order bot  created")
