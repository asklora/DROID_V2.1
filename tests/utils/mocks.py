import json

from core.orders.factory.order_protocol import GetPriceProtocol, ValidatorProtocol
from core.orders.factory.orderfactory import (
    ActionProcessor,
    OrderController,
    order_executor,
)
from core.orders.factory.payload import ActionPayload
from core.orders.factory.validator import ActionValidator
from core.orders.models import Order, OrderPosition
from datasource.getterprice import RkdGetterPrice
from rest_framework import exceptions


class MockActionProcessor:
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if self.raw_payload["status"] == "cancel":
            self.raw_payload["side"] = "cancel"
        else:
            self.raw_payload["side"] = self.validator.order.side
        if getterprice:
            self.getter_price = getterprice

    def execute(self):

        task_payload: str = json.dumps(self.raw_payload)
        task = order_executor.apply(
            args=(task_payload,), task_id=self.payload.order_uid
        )
        self.response = {
            "action_id": task.id,
            "status": "executed",
            "order_uid": self.payload.order_uid,
        }


def mock_order_action_serializer(validated_data: dict):
    print("----action serializer is mocked----")
    controller = OrderController()
    return controller.process(MockActionProcessor(validated_data))


def mock_buy_validate(user, ticker, bot_id):
    print("----buy validation is mocked----")

    def is_order_exist():
        orders = Order.objects.filter(
            user_id=user,
            ticker=ticker,
            bot_id=bot_id,
            status="pending",
            side="buy",
        )

        if orders.exists():
            raise exceptions.NotAcceptable(
                {"detail": f"you already has order for {ticker} in current options"}
            )

    def is_portfolio_exist():
        portfolios = OrderPosition.objects.filter(
            user_id=user,
            ticker=ticker,
            bot_id=bot_id,
            is_live=True,
        )
        if portfolios.exists():
            raise exceptions.NotAcceptable(
                {
                    "detail": f"cannot have multiple position for {ticker} in current options"
                }
            )

    is_order_exist()
    is_portfolio_exist()


def mock_sell_validate(user, ticker, bot_id):
    def has_order():
        pending_order = Order.objects.filter(
            user_id=user,
            status="pending",
            bot_id=bot_id,
            ticker=ticker,
        )
        if pending_order.exists():
            last_order = pending_order.first()
            order_id = last_order.order_uid.hex
            raise exceptions.NotAcceptable(
                f"sell order already exists for this position, order id : {order_id}, current status pending"
            )

    def is_position_exists(self) -> OrderPosition:
        print(f">>>{self.payload.setup['position']}<<<")
        try:
            position = OrderPosition.objects.select_related(
                "user_id",
                "ticker",
            ).get(pk=self.payload.setup["position"])
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({"detail": "position not found error"})

        return position

    has_order()
    is_position_exists()
