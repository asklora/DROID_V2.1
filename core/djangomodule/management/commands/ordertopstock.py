from django.core.management.base import BaseCommand, CommandError
from core.orders.models import Order, PositionPerformance
from core.Clients.models import UserClient, Client
import pandas as pd


class Command(BaseCommand):
    # def add_arguments(self, parser):

    #     parser.add_argument('-a', '--account', type=str, help='email')
    #     parser.add_argument('-t', '--ticker', type=str, help='ticker')
    #     parser.add_argument('-p', '--price', type=float, help='price')
    #     parser.add_argument('-d', '--date', type=str, help='spot date')
    #     parser.add_argument('-b', '--bot', type=str, help='bot id')
    #     parser.add_argument('-amt', '--amount', type=float, help='amount')

    def handle(self, *args, **options):
        client = Client.objects.get(client_name='HANWHA')
        topstock = client.client_top_stock.filter(has_position=False)

        for queue in topstock:
            user = UserClient.objects.get(
                currency_code=queue.currency_code,
                extra_data__service_type=queue.service_type,
                extra_data__capital=queue.capital,
                extra_data__type=queue.bot
            )

            price = queue.ticker.latest_price_ticker.close
            investment_amount = min(
                user.user.current_assets/user.user.position_count, user.user.balance/3)
            spot_date = pd.Timestamp(queue.spot_date)
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
            if order.status == 'pending':
                order.status = 'filled'
                order.filled_at = spot_date
                order.save()
                position_uid = PositionPerformance.objects.get(
                    performance_uid=order.performance_uid).position_uid.position_uid
                queue.position_uid = position_uid
                queue.has_position = True
                queue.save()
                print(user.user_id, user.extra_data['service_type'],
                      user.extra_data['capital'], queue.ticker, 'created')
