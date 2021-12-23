from dataclasses import dataclass
from datetime import datetime
from typing import Tuple, Union

from core.orders.factory.orderfactory import (
    OrderController,
    SellOrderProcessor,
)
from core.orders.models import (
    Feature,
    Order,
    OrderPosition,
    PositionPerformance,
)
from core.user.models import User
from django.test.client import Client
from django.utils import timezone


def get_position_performance(
    order: Order,
) -> Union[Tuple[OrderPosition, PositionPerformance], Tuple[None, None]]:
    try:
        performance: PositionPerformance = PositionPerformance.objects.get(
            order_uid_id=order.order_uid
        )

        position: OrderPosition = OrderPosition.objects.get(
            pk=performance.position_uid_id,
        )

        return position, performance
    except PositionPerformance.DoesNotExist:
        return None, None


def create_buy_order(
    price: float,
    ticker: str,
    amount: float = None,
    bot_id: str = "STOCK_stock_0",
    created: datetime = timezone.now(),
    margin: int = 1,
    qty: int = 10000,
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


def create_sell_order(order: Order) -> Order:
    latest_price: float = order.price + (order.price * 0.25)

    position, _ = get_position_performance(order)

    order_payload: dict = {
        "setup": {"position": position.position_uid},
        "side": "sell",
        "ticker": order.ticker,
        "user_id": order.user_id,
        "margin": order.margin,
    }

    controller: OrderController = OrderController()

    sell_order: Order = controller.process(
        SellOrderProcessor(
            order_payload,
            getterprice=MockGetterPrice(
                price=latest_price,
            ),
        ),
    )

    return sell_order


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


def confirm_order_api(
    order_uid: str,
    client: Client,
    authentication: dict,
):
    response = client.post(
        path="/api/order/action/",
        data={
            "order_uid": order_uid,
            "status": "placed",
            "firebase_token": "test",
        },
        **authentication,
    )

    if (
        response.status_code != 200
        or response.headers["Content-Type"] != "application/json"
    ):
        return None

    return response.json()


class MockGetterPrice:
    def __init__(self, price) -> None:
        self.price = price

    def get_price(self, tickers) -> float:
        return self.price


@dataclass
class FeatureManager:
    feature: Feature

    def __post_init__(self):
        print(
            f"{self.feature.name} feature is active"
            if self.feature.active
            else f"{self.feature.name} feature is NOT active"
        )

    @property
    def is_active(self) -> bool:
        return self.feature.active

    def activate(self):
        print(f"Activating {self.feature.name} feature")
        self.feature.active = True
        self.feature.save()

    def deactivate(self):
        print(f"Deactivating {self.feature.name} feature")
        self.feature.active = False
        self.feature.save()
