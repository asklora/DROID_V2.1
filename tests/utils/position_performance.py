from core.orders.models import OrderPosition, PositionPerformance


def get_position_performance(
    order_uid: str,
) -> [OrderPosition, PositionPerformance]:
    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid=order_uid,
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    if performance and position:
        return position, performance
