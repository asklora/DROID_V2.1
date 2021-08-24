# from django.test import TestCase

# from core.universe.models import Universe
from core.orders.models import Order

# from core.user.models import User


# class OrderTest(TestCase):
#     fixtures = ["order.json"]

#     def amount_is_correct(self):
#         # ticker, created = Universe.objects.get_or_create(ticker="0780.HK")
#         # user, created = User.objects.get_or_create(id=135)
#         # order = Order.objects.create(
#         #     ticker=ticker,
#         #     price=1317,
#         #     bot_id="STOCK_stock_0",
#         #     amount=1,
#         #     user_id=user,
#         #     side="buy",
#         # )
#         order = Order.objects.get(pk="ce7159ce-a73b-42e2-ae57-c3156ef1bfc5")
#         self.assertEqual(order.price, 1318)


def test_should_create_order(db) -> None:
    order = Order.objects.create(
        ticker="0780.HK",
        price=1317,
        bot_id="STOCK_stock_0",
        amount=1,
        user_id=135,
        side="buy",
    )
    assert order.price == 13.0
