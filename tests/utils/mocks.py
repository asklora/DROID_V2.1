from core.orders.factory.orderfactory import (
    order_executor,
)
from core.orders.models import Order, OrderPosition
from rest_framework import exceptions


def mock_execute_task(payload):
    print('---task execution mocked---')
    return order_executor.apply(
        args=(payload,),
        task_id=payload.order_uid,
    )


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


def mock_sell_validate(user, ticker, position, bot_id):
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

    def is_position_exists(position) -> OrderPosition:
        print(f">>>{position}<<<")
        try:
            position = OrderPosition.objects.select_related(
                "user_id",
                "ticker",
            ).get(pk=position)
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({"detail": "position not found error"})

        return position

    has_order()
    is_position_exists(position)
