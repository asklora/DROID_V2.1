import pytest
from core.orders.serializers import OrderCreateSerializer
from core.user.models import User
from rest_framework import exceptions

pytestmark = pytest.mark.django_db(databases=['default','aurora_read','aurora_write'])



def test_should_create_new_buy_order_from_API(user) -> None:
    side = "buy"
    ticker = "0008.HK"
    qty = 2
    price = 1317
    bot_id = "STOCK_stock_0"

    user = User.objects.get(pk=user.id)

    # We create an order
    order_request = {
        "amount": price * qty,
        "bot_id": bot_id,
        "price": price,
        "qty": qty,
        "side": side,
        "ticker": ticker,
        "user": user.id,
    }
    serializer = OrderCreateSerializer(data=order_request)
    if serializer.is_valid(raise_exception=True):
        buy_order = serializer.save()

    assert buy_order.order_uid is not None


def test_should_fail_on_new_buy_order_from_API(user) -> None:
    # This line catches the exception, this is where the assertion happens
    with pytest.raises(exceptions.NotAcceptable):
        side = "buy"
        ticker = "0780.HK"
        price = 1317
        bot_id = "STOCK_stock_0"

        user = User.objects.get(pk=user.id)

        # We create an order
        order_request = {
            "amount": 300000,
            "bot_id": bot_id,
            "price": price,
            "side": side,
            "ticker": ticker,
            "user": user.id,
        }
        serializer = OrderCreateSerializer(data=order_request)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
