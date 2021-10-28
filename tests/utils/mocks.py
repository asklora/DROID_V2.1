import json

from core.orders.models import Order
from core.orders.serializers import OrderActionSerializer
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
