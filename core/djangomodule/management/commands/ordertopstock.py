from django.core.management.base import BaseCommand, CommandError
from core.orders.models import Order, PositionPerformance
from core.bot.models import BotOptionType
from core.master.models import MasterOhlcvtr
from core.Clients.models import UserClient, Client
import pandas as pd
from datetime import datetime, timedelta


class Command(BaseCommand):
    def add_arguments(self, parser):

        #     parser.add_argument("-a", "--account", type=str, help="email")
        #     parser.add_argument("-t", "--ticker", type=str, help="ticker")
        #     parser.add_argument("-p", "--price", type=float, help="price")
        #     parser.add_argument("-d", "--date", type=str, help="spot date")
        #     parser.add_argument("-b", "--bot", type=str, help="bot id")
        #     parser.add_argument("-amt", "--amount", type=float, help="amount")
        parser.add_argument(
            "-d", "--debug", action="store_true", help="for celery")

    def handle(self, *args, **options):
        client = Client.objects.get(client_name="HANWHA")
        topstock = client.client_top_stock.filter(
            has_position=False, spot_date='2021-04-05', currency_code='CNY', capital='large_margin', service_type='bot_advisor',uid='8EaWUeN0_C_000001SZ_20210405_bot_advisor_large_margin').order_by("service_type", "spot_date", "currency_code", "capital", "rank")

        for queue in topstock:
            user = UserClient.objects.get(
                currency_code=queue.currency_code,
                extra_data__service_type=queue.service_type,
                extra_data__capital=queue.capital,
                extra_data__type=queue.bot
            )
            bot = BotOptionType.objects.get(bot_id=queue.bot_id)
            if options["debug"]:
                last_spot_date = queue.spot_date - timedelta(days=1)
                if last_spot_date.weekday() == 6:
                    last_spot_date = last_spot_date - \
                        timedelta(days=2)
                elif last_spot_date.weekday() == 5:
                    last_spot_date = last_spot_date - \
                        timedelta(days=1)
                price = MasterOhlcvtr.objects.get(
                    ticker=queue.ticker, trading_day=last_spot_date).close
                print(price, last_spot_date)
                while not price:
                    dates = last_spot_date - timedelta(days=1)
                    if dates.weekday() == 6:
                        dates = dates - timedelta(days=2)
                    elif dates.weekday() == 5:
                        dates = dates - timedelta(days=1)
                    print(dates, "go back")
                    price = MasterOhlcvtr.objects.get(
                        ticker=queue.ticker, trading_day=dates).close

                spot_date = pd.Timestamp(queue.spot_date)
            else:
                price = queue.ticker.latest_price_ticker.close
                spot_date = datetime.now()
            if user.extra_data["service_type"] == "bot_advisor":
                portnum = 8*1.04
            elif user.extra_data["service_type"] == "bot_tester":
                if user.extra_data["capital"] == "small":
                    portnum = 4*1.04
                else:
                    portnum = 8*1.04
            investment_amount = min(
                user.user.current_assets / portnum, user.user.balance / 3)

            digits = max(min(5-len(str(int(price))), 2), -1)
            order = Order.objects.create(
                ticker=queue.ticker,
                created=spot_date,
                price=price,
                bot_id=queue.bot_id,
                amount=round(investment_amount, digits),
                user_id=user.user
            )
            if order:
                order.placed = True
                order.placed_at = spot_date
                order.save()
            if order.status == "pending":
                order.status = "filled"
                order.filled_at = spot_date
                order.save()
                position_uid = PositionPerformance.objects.get(
                    performance_uid=order.performance_uid).position_uid.position_uid
                queue.position_uid = position_uid
                queue.has_position = True
                queue.save()
                print(user.user_id, user.extra_data["service_type"],
                      user.extra_data["capital"], queue.ticker, "created")
