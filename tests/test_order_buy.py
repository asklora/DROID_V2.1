from datetime import datetime

import pytest
from core.orders.models import Order
from core.user.models import Accountbalance, TransactionHistory, User

from utils import create_buy_order


class TestBuy:
    pytestmark = pytest.mark.django_db

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
            margin=margin,
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