import json

from core.orders.models import Order, OrderPosition
from core.orders.serializers import OrderActionSerializer
from core.user.models import User
from rest_framework import exceptions


def mock_order_serializer(validated_data: dict):
    try:
        instance = OrderActionSerializer.Meta.model.objects.get(
            order_uid=validated_data["order_uid"]
        )
    except Order.DoesNotExist:
        raise exceptions.NotFound({"detail": "order not found"})

    if validated_data["status"] == "cancel" and instance.status not in [
        "pending",
        "review",
    ]:
        raise exceptions.MethodNotAllowed(
            {"detail": f"cannot cancel order in {instance.status}"}
        )
    if instance.status == "filled":
        raise exceptions.MethodNotAllowed(
            {"detail": "order already filled, you cannot cancel / confirm"}
        )
    if instance.status == validated_data["status"]:
        raise exceptions.MethodNotAllowed(
            {"detail": f"order already {instance.status}"}
        )
    if not validated_data["status"] == "cancel":
        if instance.insufficient_balance():
            raise exceptions.MethodNotAllowed({"detail": "insufficient funds"})

    from core.services.order_services import order_executor

    payload = json.dumps(validated_data)
    print("Function is mocked")
    task = order_executor.apply(
        args=(payload,),
        task_id=validated_data["order_uid"],
    )
    data = {
        "action_id": task.id,
        "status": "executed in mock",
        "order_uid": validated_data["order_uid"],
    }
    return data


def mock_buy_validate(user, ticker, bot_id):
    print(f"----mocked----")

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
            orderId = last_order.order_uid.hex
            raise exceptions.NotAcceptable(
                f"sell order already exists for this position, order id : {orderId}, current status pending"
            )

    def is_position_exists(self) -> OrderPosition:
        print(f">>>{self.payload.setup['position']}<<<")
        try:
            position = OrderPosition.objects.select_related("user_id", "ticker").get(
                pk=self.payload.setup["position"]
            )
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({"detail": "position not found error"})

        return position

    has_order()
    is_position_exists()
