from datetime import datetime

from core.orders.models import Order
from core.user.models import User
from django.utils import timezone


def create_buy_order(
    price: float,
    ticker: str,
    amount: float = None,
    bot_id: str = "STOCK_stock_0",
    created: datetime = timezone.now(),
    margin: int = 1,
    qty: int = 100,
    user_id: int = None,
    user: User = None,
) -> Order:
    return Order.objects.create(
        amount=amount if amount is not None else price * qty,
        bot_id=bot_id,
        created=created,
        margin=margin,
        order_type="apps",  # to differentiate itself from FELS's orders
        price=price,
        qty=qty,
        side="buy",
        ticker_id=ticker,
        user_id_id=user_id,
        user_id=user,
    )


def confirm_order(
    order: Order,
    date: datetime = datetime.now(),
) -> None:
    if order:
        order.status = "placed"
        order.placed = True
        order.placed_at = date
        order.save()

        order.status = "filled"
        order.filled_at = date
        order.save()
