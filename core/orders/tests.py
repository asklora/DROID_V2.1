from core.master.models import MasterOhlcvtr
from datetime import datetime

import pytest
from core.djangomodule.network.cloud import DroidDb
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.serializers import OrderCreateSerializer
from core.orders.services import sell_position_service
from core.user.models import Accountbalance, TransactionHistory, User
from dateutil.relativedelta import relativedelta
from django import setup
from django.conf import settings
from portfolio import classic_position_check, uno_position_check, ucdc_position_check
from rest_framework import exceptions


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

        side = "buy"
        ticker = "0780.HK"
        price = 1317
        user_id = 135
        bot_id = "STOCK_stock_0"

        order = Order.objects.create(
            amount=1317,
            bot_id=bot_id,
            price=price,
            side=side,
            ticker_id=ticker,
            user_id_id=user_id,
        )

        assert order.is_init == True
        assert order.placed == False
        assert order.status == "review"
        assert order.placed_at == None
        assert order.filled_at == None
        assert order.canceled_at == None
        assert order.amount == 1317
        assert order.price == 1317
        assert order.qty == 1  # from order.amount divided by order.price

    def test_should_create_new_buy_order_for_user(self) -> None:
        """
        A new BUY order should be created with empty setup
        """

        side = "buy"
        ticker = "3377.HK"
        qty = 1
        price = 1317
        user_id = 197
        bot_id = "STOCK_stock_0"

        order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side=side,
            ticker_id=ticker,
            user_id_id=user_id,
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

    def test_should_create_new_buy_order_for_classic_bot(self) -> None:
        """
        A new BUY order should be created with non-empty setup
        """

        side = "buy"
        ticker = "3377.HK"
        qty = 1
        price = 1317
        user_id = 197
        bot_id = "CLASSIC_classic_007692"

        order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side=side,
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

        side = "buy"
        ticker = "3377.HK"
        qty = 1
        price = 1317
        user_id = 197
        bot_id = "UNO_OTM_007692"

        order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side=side,
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

        side = "buy"
        ticker = "3377.HK"
        qty = 1
        price = 1317
        user_id = 197
        bot_id = "UCDC_ATM_007692"

        order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side=side,
            ticker_id=ticker,
            user_id_id=user_id,
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

        ticker = "6606.HK"
        qty = 1
        price = 1317
        user_id = 198
        bot_id = "STOCK_stock_0"

        # We create an order
        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
            price + 13, datetime.now(), position.position_uid
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
        ticker = "6606.HK"
        qty = 1
        price = 1317
        user_id = 198
        bot_id = "CLASSIC_classic_007692"

        # We create an order
        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
            price + 13, datetime.now(), position.position_uid
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
        ticker = "6606.HK"
        qty = 1
        price = 1317
        user_id = 198
        bot_id = "UNO_OTM_007692"

        # We create an order
        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
            price + 13, datetime.now(), position.position_uid
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
        ticker = "6606.HK"
        qty = 1
        price = 1317
        user_id = 198
        bot_id = "UCDC_ATM_007692"

        # We create an order
        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
            price + 13, datetime.now(), position.position_uid
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
        qty = 1
        user_id = 198
        bot_id = "CLASSIC_classic_007692"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day='2021-06-01',
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            created=log_time,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
        ticker = "6606.HK"
        qty = 1
        user_id = 198
        bot_id = "UNO_OTM_007692"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day='2021-06-01',
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            created=log_time,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
        ticker = "6606.HK"
        qty = 1
        user_id = 198
        bot_id = "UCDC_ATM_007692"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day='2021-06-01',
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = Order.objects.create(
            amount=price * qty,
            bot_id=bot_id,
            created=log_time,
            order_type="apps",
            price=price,
            qty=qty,
            side="buy",
            ticker_id=ticker,
            user_id_id=user_id,
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
        ticker = "6606.HK"
        qty = 1
        price = 1317
        user_id = 198
        bot_id = "STOCK_stock_0"

        user = User.objects.get(pk=user_id)

        # We create an order
        order_request = {
            "amount": qty * price,
            "bot_id": bot_id,
            "price": price,
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
