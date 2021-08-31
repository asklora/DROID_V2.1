from bot.calculate_bot import check_date, get_expiry_date
from core.bot.models import BotOptionType
from datetime import datetime
from typing import List, Union
from boto3 import client
from django.test.client import Client

import pytest
import requests
from core.djangomodule.network.cloud import DroidDb
from core.master.models import MasterOhlcvtr
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.serializers import OrderCreateSerializer
from core.orders.services import sell_position_service
from core.user.models import Accountbalance, TransactionHistory, User
from django.conf import settings
from portfolio import (
    classic_position_check,
    ucdc_position_check,
    uno_position_check,
)
from rest_framework import exceptions


def create_buy_order(
    price: float,
    ticker: str,
    amount: float = None,
    bot_id: str = "STOCK_stock_0",
    margin: int = 1,
    qty: int = 100,
    user_id: int = None,
    user: User = None,
) -> Order:
    return Order.objects.create(
        amount=amount if amount != None else price * qty,
        bot_id=bot_id,
        margin=margin,
        order_type="apps",  # to differentiate itself from FELS's orders
        price=price,
        qty=qty,
        side="buy",
        ticker_id=ticker,
        user_id_id=user_id,
        user_id=user,
    )


class TestBuy:
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
        A new order should be created with default values for is_init, placed, status, dates, etc.
        """

        order = create_buy_order(
            user_id=135,
            ticker="0780.HK",
            price=1317,
        )

        assert order.is_init == True
        assert order.placed == False
        assert order.status == "review"
        assert order.placed_at == None
        assert order.filled_at == None
        assert order.canceled_at == None
        assert order.amount == 131700
        assert order.price == 1317
        assert order.qty == 100  # from order.amount divided by order.price

    def test_should_create_new_buy_order_for_user(self) -> None:
        """
        A new BUY order should be created with empty setup
        """

        order = create_buy_order(
            ticker="3377.HK",
            price=1317,
            user_id=197,
            bot_id="STOCK_stock_0",
        )

        assert order.side == "buy"
        assert order.setup == None  # should be empty

    def test_should_update_new_buy_order_for_user(self) -> None:
        """
        A new BUY order's status will be set to PENDING and the price is deducted from USER balance
        #NOTE : after test must clean
        """

        side = "buy"
        ticker = "3377.HK"
        qty = 3
        price = 1317
        bot_id = "STOCK_stock_0"

        """
        nyoba dg kode kemarin error pada signal, balance g mau berubah
        """
        user = User.objects.create_user(
            email="pytest@tests.com",
            username="pikachu_icikiwiw",
            password="helloworld",
            is_active=True,
            current_status="verified",
        )

        user_balance = Accountbalance.objects.create(
            user=user,
            amount=0,
            currency_code_id="HKD",
        )

        print("\n")
        print("saldo awal", user_balance.amount)

        trans = TransactionHistory.objects.create(
            balance_uid=user_balance,
            side="credit",
            amount=100000,
            transaction_detail={"event": "first deposit"},
        )
        # NOTE: setelah top up pertama harus di get balance, kenapa? alasan nya di bawah ada
        user_balance = Accountbalance.objects.get(user=user)
        # simpan saldo sebelum terpotong di variable
        saldo_blm_terpotong = user_balance.amount
        print("saldo top up", user_balance.amount)

        order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side=side,
            ticker_id=ticker,
            user_id=user,
        )

        user_balance = Accountbalance.objects.get(user=user)

        # harusnya masih tetap
        print("saldo order review", user_balance.amount)

        order.status = "placed"
        order.placed = True
        order.placed_at = datetime.now()
        order.save()

        # ketika pending terpotong, harus di get lagi
        # karena perubahan balance ikut signal
        # COBA AJA COMMENT BAWAH INI HASILNYA PASTI FAIL!!
        # GA ADA PERUBAHAN MESKIPUN SIGNAL NYA KE TRIGER. HARUS DI GET LAGI BUAT LIHAT PERUBAHAN
        user_balance = Accountbalance.objects.get(user=user)
        print("saldo pending terpotong", user_balance.amount)

        order = Order.objects.get(pk=order.order_uid)

        assert order.amount == price * qty
        assert user_balance.amount == saldo_blm_terpotong - order.amount

    def test_should_create_new_buy_order_for_user_with_margin(self) -> None:
        """
        A new order should be created for user with margin applied to the amount
        """

        order = create_buy_order(
            user_id=198,
            ticker="0535.HK",
            amount=85,
            price=0.85,
            margin=2,
            qty=None,
        )

        assert (
            order.qty == 200
        )  # order.qty (default to 100 for this test) multiplied by margin
        assert (
            order.amount != 170
        )  # from order.price times order.qty, excluding margin calculation
        assert order.amount == 85  # the correct amount

    def test_should_check_if_user_balance_is_cut_accordingly_with_margin(self) -> None:
        """
        A new buy order will be created and filled, and user balance is deducted buy the order amount.
        Margin calculation should not cut the user balance.
        """

        side = "buy"
        ticker = "3377.HK"
        amount = 131700
        price = 1317
        margin = 2
        bot_id = "STOCK_stock_0"

        # Create sample user
        user = User.objects.create_user(
            email="pytest@tests.com",
            username="pikachu_icikiwiw",
            password="helloworld",
            is_active=True,
            current_status="verified",
        )

        # Create user wallet
        user_balance = Accountbalance.objects.create(
            user=user,
            amount=0,
            currency_code_id="HKD",
        )

        # Add balance
        trans = TransactionHistory.objects.create(
            balance_uid=user_balance,
            side="credit",
            amount=200000,
            transaction_detail={"event": "first deposit"},
        )

        # Save initial user balance
        user_balance = Accountbalance.objects.get(user=user)
        initial_user_balance = user_balance.amount

        # Create the order
        order = Order.objects.create(
            amount=amount,
            bot_id=bot_id,
            margin=2,
            order_type="apps",
            price=price,
            side=side,
            ticker_id=ticker,
            user_id=user,
        )

        # The amount and qty should be calculated correctly
        print(f"Ordered amount: {amount}")
        print(f"Calculated amount: {order.amount}")
        print(f"Ordered qty: {amount / price}")
        print(f"Calculated qty: {order.qty}")

        # The user balance should be untouched
        user_balance = Accountbalance.objects.get(user=user)
        print("User balance before order is filled: ", user_balance.amount)

        # We place the order, deducting the amount from user's balance
        order.status = "placed"
        order.placed = True
        order.placed_at = datetime.now()
        order.save()

        # Let's confirm this
        user_balance = Accountbalance.objects.get(user=user)
        print("User balance after order is filled: ", user_balance.amount)

        order = Order.objects.get(pk=order.order_uid)

        assert (
            order.qty == 200
        )  # order.qty (default to 100 for this test) multiplied by margin
        assert user_balance.amount == initial_user_balance - order.amount

    def test_should_create_new_buy_order_for_classic_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        bot_id = "CLASSIC_classic_007692"

        order = create_buy_order(
            bot_id=bot_id,
            price=1317,
            ticker="3377.HK",
            user_id=197,
        )

        print(order.setup)

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id

    def test_should_create_new_buy_order_for_uno_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        bot_id = "UNO_OTM_007692"

        order = create_buy_order(
            bot_id=bot_id,
            price=1317,
            ticker="3377.HK",
            user_id=197,
        )

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id

    def test_should_create_new_buy_order_for_ucdc_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        bot_id = "UCDC_ATM_007692"

        order = create_buy_order(
            bot_id=bot_id,
            price=1317,
            ticker="3377.HK",
            user_id=197,
        )

        assert order.side == "buy"
        assert order.setup != None  # Setup will be populated with bot information
        assert order.bot_id == bot_id


class TestSell:
    pytestmark = pytest.mark.django_db

    @pytest.fixture(scope="session")
    def django_db_setup(self):
        db = DroidDb()
        read_endpoint, write_endpoint, port = db.test_url

        DB_ENGINE = "psqlextra.backend"
        settings.DATABASES["default"] = {
            "ENGINE": DB_ENGINE,
            "HOST": read_endpoint,
            "NAME": "postgres",
            "USER": "postgres",
            "PASSWORD": "ml2021#LORA",
            "PORT": port,
        }

    def test_should_create_new_sell_order_for_user(self) -> None:
        """
        A new SELL order should be created from a buy order
        """

        price = 1317
        user_id = 198

        # We create an order
        buy_order = create_buy_order(
            ticker="6606.HK",
            price=price,
            user_id=user_id,
        )

        # We set it as filled
        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = datetime.now()
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = datetime.now()
        buy_order.save()

        # We get the order to update the data
        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        # Check if it successfully is added to the performance table
        assert confirmed_buy_order.performance_uid != None

        # We get the position and performance data to update it
        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        # We confirm the above test
        assert performance != None

        # We then get the position based on the performance data
        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        # We confirm if the position is set
        assert position != None

        # We create the sell order
        sellPosition, sell_order = sell_position_service(
            price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
            datetime.now(),
            position.position_uid,
        )

        confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
        assert sell_order.order_uid != None

        confirmed_sell_order.status = "placed"
        confirmed_sell_order.placed = True
        confirmed_sell_order.placed_at = datetime.now()
        confirmed_sell_order.save()

        # We get previous user balance
        user = User.objects.get(pk=user_id)
        previous_user_balance = Accountbalance.objects.get(user=user).amount

        # We accept the order and set it as filled
        confirmed_sell_order.status = "filled"
        confirmed_sell_order.filled_at = datetime.now()
        confirmed_sell_order.save()

        # We confirm that the selling is successfully finished by checking the user balance
        user_balance = Accountbalance.objects.get(user=user).amount
        assert user_balance != previous_user_balance

    def test_should_create_new_sell_order_for_user_with_classic_bot(self) -> None:
        price = 1317
        user_id = 198

        # We create an order
        buy_order = create_buy_order(
            ticker="6606.HK",
            price=price,
            user_id=user_id,
            bot_id="CLASSIC_classic_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = datetime.now()
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = datetime.now()
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        sellPosition, sell_order = sell_position_service(
            price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
            datetime.now(),
            position.position_uid,
        )

        confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
        assert sell_order.order_uid != None

        confirmed_sell_order.status = "placed"
        confirmed_sell_order.placed = True
        confirmed_sell_order.placed_at = datetime.now()
        confirmed_sell_order.save()

        # We get previous user balance
        user = User.objects.get(pk=user_id)
        previous_user_balance = Accountbalance.objects.get(user=user).amount

        # We accept the order and set it as filled
        confirmed_sell_order.status = "filled"
        confirmed_sell_order.filled_at = datetime.now()
        confirmed_sell_order.save()

    def test_should_create_new_sell_order_for_user_with_uno_bot(self) -> None:
        price = 1317
        user_id = 198

        # We create an order
        buy_order = create_buy_order(
            ticker="6606.HK",
            price=price,
            user_id=user_id,
            bot_id="UNO_OTM_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = datetime.now()
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = datetime.now()
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        sellPosition, sell_order = sell_position_service(
            price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
            datetime.now(),
            position.position_uid,
        )

        confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
        assert sell_order.order_uid != None

        confirmed_sell_order.status = "placed"
        confirmed_sell_order.placed = True
        confirmed_sell_order.placed_at = datetime.now()
        confirmed_sell_order.save()

        # We get previous user balance
        user = User.objects.get(pk=user_id)
        previous_user_balance = Accountbalance.objects.get(user=user).amount

        # We accept the order and set it as filled
        confirmed_sell_order.status = "filled"
        confirmed_sell_order.filled_at = datetime.now()
        confirmed_sell_order.save()

    def test_should_create_new_sell_order_for_user_with_ucdc_bot(self) -> None:
        price = 1317
        user_id = 198

        # We create an order
        buy_order = create_buy_order(
            ticker="6606.HK",
            price=price,
            user_id=user_id,
            bot_id="UCDC_ATM_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = datetime.now()
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = datetime.now()
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        sellPosition, sell_order = sell_position_service(
            price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
            datetime.now(),
            position.position_uid,
        )

        confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
        assert sell_order.order_uid != None

        confirmed_sell_order.status = "placed"
        confirmed_sell_order.placed = True
        confirmed_sell_order.placed_at = datetime.now()
        confirmed_sell_order.save()

        # We get previous user balance
        user = User.objects.get(pk=user_id)
        previous_user_balance = Accountbalance.objects.get(user=user).amount

        # We accept the order and set it as filled
        confirmed_sell_order.status = "filled"
        confirmed_sell_order.filled_at = datetime.now()
        confirmed_sell_order.save()


class TestHedge:
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

    def test_should_create_hedge_order_for_classic_bot(self) -> None:
        # step 1: create a new order
        ticker = "6606.HK"
        user_id = 198
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user_id,
            bot_id="CLASSIC_classic_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = log_time
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = log_time
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        # step 2: setup hedge
        classic_position_check(
            position_uid=position.position_uid,
            tac=True,
        )
        # step 3: get hedge positions
        performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid
        )

        print(performance.count())

        assert performance.exists() == True
        assert performance.count() > 1

    def test_should_create_hedge_order_for_uno_bot(self) -> None:
        # step 1: create a new order
        ticker = "3690.HK"
        user_id = 198
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user_id,
            bot_id="UNO_OTM_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = log_time
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = log_time
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        # step 2: setup hedge
        uno_position_check(
            position_uid=position.position_uid,
            tac=True,
        )
        # step 3: get hedge positions
        performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid
        )

        print(performance.count())

        assert performance.exists() == True
        assert performance.count() > 1

    def test_should_create_hedge_order_for_ucdc_bot(self) -> None:
        # step 1: create a new order
        ticker = "2282.HK"
        user_id = 198
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user_id,
            bot_id="UCDC_ATM_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = log_time
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = log_time
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        # step 2: setup hedge
        ucdc_position_check(
            position_uid=position.position_uid,
            tac=True,
        )
        # step 3: get hedge positions
        performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid
        )

        print(performance.count())
        print(performance[-1].expiry)

        assert performance.exists() == True
        assert performance.count() > 1

    def test_should_create_hedge_order_for_ucdc_bot_with_margin(self) -> None:
        # step 1: create a new order
        ticker = "2282.HK"
        user_id = 198
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user_id,
            margin=2,
            bot_id="UCDC_ATM_007692",
        )

        buy_order.status = "placed"
        buy_order.placed = True
        buy_order.placed_at = log_time
        buy_order.save()

        buy_order.status = "filled"
        buy_order.filled_at = log_time
        buy_order.save()

        confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

        performance = PositionPerformance.objects.get(
            order_uid_id=confirmed_buy_order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id
        )

        # step 2: setup hedge
        ucdc_position_check(
            position_uid=position.position_uid,
            tac=True,
        )
        # step 3: get hedge positions
        performance = PositionPerformance.objects.filter(
            position_uid=position.position_uid
        )

        print(performance.count())

        assert performance.exists() == True
        assert performance.count() > 1


class TestBotExpiry:
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

    def test_should_confirm_bot_expiry_for_classic(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="CLASSIC"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date

    def test_should_confirm_bot_expiry_for_uno(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="UNO"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date

    def test_should_confirm_bot_expiry_for_ucdc(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="UCDC"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date


class TestSerializer:
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

    def test_should_create_new_buy_order_from_API(self) -> None:
        side = "buy"
        ticker = "0008.HK"
        qty = 2
        price = 1317
        user_id = 198
        bot_id = "STOCK_stock_0"

        user = User.objects.get(pk=user_id)

        # We create an order
        order_request = {
            "amount": price * qty,
            "bot_id": bot_id,
            "price": price,
            "qty": qty,
            "side": side,
            "ticker": ticker,
            "user": user_id,
        }
        serializer = OrderCreateSerializer(data=order_request)
        if serializer.is_valid(raise_exception=True):
            buy_order = serializer.save()

        assert buy_order.order_uid != None

    def test_should_fail_on_new_buy_order_from_API(self) -> None:
        with pytest.raises(exceptions.NotAcceptable):
            side = "buy"
            ticker = "0780.HK"
            price = 1317
            user_id = 198
            bot_id = "STOCK_stock_0"

            user = User.objects.get(pk=user_id)

            # We create an order
            order_request = {
                "amount": 300000,
                "bot_id": bot_id,
                "price": price,
                "side": side,
                "ticker": ticker,
                "user": user_id,
            }
            serializer = OrderCreateSerializer(data=order_request)
            if serializer.is_valid(raise_exception=True):
                serializer.save()


class TestAPI:
    """
    Unfinished
    """

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

    def build_url(
        self,
        url: str = "http://0.0.0.0:8000",
        path: str = "/api/order/create/",
    ) -> str:
        return f"{url}{path}"

    def log_in(
        self,
        email: str,
        password: str,
    ) -> Union[str, None]:

        return

    def authenticate(
        self,
        email: str = "aga",
        password: str = "123",
    ) -> Union[dict, None]:
        url = self.build_url(path="/api/auth/")
        response = requests.post(
            url=url,
            data={
                "email": email,
                "password": password,
            },
        )

        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            return None

        response_body = response.json()
        return {"Authorization": "Bearer " + response_body["access"]}

    def test_should_get_user_data(self) -> None:
        headers = self.authenticate()

        url = self.build_url(path="/api/user/me/")
        response = requests.get(url, **headers)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        response_body = response.json()
        assert response_body["email"] != None

    def test_with_authenticated_client(self):
        client = Client(raise_request_exception=True)
        username = "aga"
        password = "123"
        status = client.login(username=username, password=password)
        print(status)
        response = client.get("/api/user/me/")
        print(response)
        if (
            response.status_code != 200
            or response.headers["Content-Type"] != "application/json"
        ):
            assert False
        response_body = response.json()
        assert response_body["email"] != None
