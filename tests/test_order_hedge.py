from datetime import datetime

import pytest
from core.master.models import MasterOhlcvtr
from core.orders.models import Order, OrderPosition, PositionPerformance
from portfolio import (classic_position_check, ucdc_position_check,
                       uno_position_check)
from django_pandas.io import read_frame
from utils import create_buy_order

@pytest.mark.django_db
class TestHedge:
    def test_should_create_hedge_order_for_classic_bot(self, user) -> None:
        # step 1: create a new order
        ticker = "6606.HK"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user.id,
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

    def test_should_create_hedge_order_for_uno_bot(self, user) -> None:
        # step 1: create a new order
        ticker = "3690.HK"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user.id,
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

    def test_should_create_hedge_order_for_ucdc_bot(self, user) -> None:
        # step 1: create a new order
        ticker = "2282.HK"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user.id,
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

    def test_should_create_hedge_order_for_ucdc_bot_with_margin(self, user) -> None:
        # step 1: create a new order
        ticker = "2282.HK"
        master = MasterOhlcvtr.objects.get(
            ticker=ticker,
            trading_day="2021-06-01",
        )
        price = master.close
        log_time = datetime.combine(master.trading_day, datetime.min.time())

        buy_order = create_buy_order(
            ticker=ticker,
            price=price,
            user_id=user.id,
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
        df =read_frame(performance)
        df.to_csv('hedge_margin.csv')
        print(performance.count())

        assert performance.exists() == True
        assert performance.count() > 1
