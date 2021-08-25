# import uuid
import pytest

# from datetime import datetime, time

from django.conf import settings
# from django.core.management import call_command
from core.djangomodule.network.cloud import DroidDb

from core.orders.models import Order
# from core.user.models import Accountbalance, User, TransactionHistory


class TestSimple:
    pytestmark = pytest.mark.django_db

    @pytest.fixture(scope="class")
    def django_db_setup(self):
        db = DroidDb()
        read_endpoint, write_endpoint, port = db.test_url

        DB_ENGINE = "psqlextra.backend"
        settings.DATABASES["default"] = {
            "ENGINE": DB_ENGINE,
            "HOST": write_endpoint,
            "NAME": "postgres",
            "USER": "postgres",
            "PASSWORD": "ml2021#LORA",
            "PORT": port,
        }

    def test_should_create_order(self) -> None:
        """
        A new order should be created with default values for is_init, placed, status, amount, etc.
        """

        ticker = "0780.HK"
        user_id = 135
        bot_id = "STOCK_stock_0"

        order = Order.objects.create(
            amount=1,
            price=1317,
            side="buy",
            bot_id=bot_id,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.is_init == True
        assert order.placed == False
        assert order.status == "review"
        assert order.placed_at == None
        assert order.filled_at == None
        assert order.canceled_at == None
        assert order.amount == 0
        assert order.price == 1317
        assert order.qty == 0

    def test_should_create_new_buy_order_for_user(self) -> None:
        """
        A new BUY order should be created with empty setup
        """

        ticker = "3377.HK"
        user_id = 197
        bot_id = "STOCK_stock_0"

        order = Order.objects.create(
            amount=1,
            price=1317,
            side="buy",
            bot_id=bot_id,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.side == "buy"
        assert order.setup == None # should be empty

    def test_should_create_new_buy_order_for_classic_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        ticker = "3377.HK"
        user_id = 197
        bot_id = "CLASSIC_classic_007692"

        order = Order.objects.create(
            amount=1,
            price=1317,
            side="buy",
            bot_id=bot_id,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id

    def test_should_create_new_buy_order_for_uno_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        ticker = "3377.HK"
        user_id = 197
        bot_id = "UNO_OTM_007692"

        order = Order.objects.create(
            amount=1,
            price=1317,
            side="buy",
            bot_id=bot_id,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id

    def test_should_create_new_buy_order_for_ucdc_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        ticker = "3377.HK"
        user_id = 197
        bot_id = "UCDC_ATM_007692"

        order = Order.objects.create(
            amount=1,
            price=1317,
            side="buy",
            bot_id=bot_id,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id


# class TestComplex:
#     @pytest.fixture(scope="function")
#     def django_db_setup(self, django_db_setup, django_db_blocker):
#         with django_db_blocker.unblock():
#             call_command("loaddata", "core/orders/fixtures/*.json")

#     @pytest.mark.django_db(transaction=True)
#     def test_should_update_new_buy_order_for_user(self) -> None:
#         """
#         A new BUY order's status will be set to PENDING and the price is deducted from USER balance
#         """

#         ticker = "3377.HK"
#         user_id = 197
#         bot_id = "STOCK_stock_0"

#         user_balance = Accountbalance.objects.create(
#             balance_uid=uuid.uuid4().hex,
#             user_id=user_id,
#             amount=100000,
#             currency_code_id="HKD",
#         )

#         order = Order.objects.create(
#             qty=1,
#             price=1317,
#             side="buy",
#             bot_id=bot_id,
#             ticker_id=ticker,
#             user_id_id=user_id,
#         )

#         time.sleep(3)

#         order.status = "pending"
#         order.placed = True
#         order.placed_at = datetime.now()
#         order.save()

#         # transaction = TransactionHistory.objects.get(amount=order.amount)

#         assert order.amount == (order.price * order.qty + 1)
#         assert user_balance.amount == user_balance - order.amount


# def test_should_create_new_sell_order_for_user() -> None:
#     """
#     A new SELL order should be created with empty setup
#     """

#     ticker = "6606.HK"
#     user_id = 198
#     bot_id = "STOCK_stock_0"

#     buyOrder = Order.objects.create(
#         amount=1,
#         price=1317,
#         side="buy",
#         bot_id=bot_id,
#         ticker_id=ticker,
#         user_id_id=user_id,
#     )
#     assert order.side == "buy"
#     assert order.setup == None

