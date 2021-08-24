# from django.test import TestCase

# from core.universe.models import Universe
from core.orders.models import Order
from django.conf import settings
from core.djangomodule.network.cloud import DroidDb

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
import pytest


@pytest.fixture(scope='session')
def django_db_setup():
    db =DroidDb()
    read_endpoint, write_endpoint, port = db.test_url

    DB_ENGINE = "psqlextra.backend"
    settings.DATABASES['default'] = {
        'ENGINE': DB_ENGINE,
        'HOST': write_endpoint,
        'NAME': 'postgres',
        "USER": 'postgres',
        "PASSWORD": 'ml2021#LORA',
        "PORT": port,
    }

def test_should_create_order(db) -> None:
    order = Order.objects.create(
        ticker_id="0780.HK",
        price=1317,
        bot_id="STOCK_stock_0",
        amount=1,
        user_id_id=135,
        side="buy",
    )
    assert order.price == 1317