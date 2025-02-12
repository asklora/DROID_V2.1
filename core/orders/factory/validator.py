import asyncio
from core.bot.models import BotOptionType
from rest_framework import exceptions
from core.orders.models import OrderPosition
from .payload import ActionPayload, SellPayload, BuyPayload
from core.orders.models import Order, OrderPosition, Feature
from django.utils.translation import gettext as _


class SellValidator:

    position: OrderPosition

    def __init__(self, payload: SellPayload):
        self.payload = payload

    def is_sellable(self):
        try:
            feature = Feature.objects.get(name="prevent_instant_sell")
            if feature.active:
                if not self.position.sellable:
                    raise exceptions.NotAcceptable(
                        {"detail": _("Selling is available the next trading day")}
                    )
        except Feature.DoesNotExist:
            return

    async def is_user_position(self):
        if self.payload.user_id != self.position.user_id:
            raise exceptions.NotAcceptable(
                _("%(position_uid)s credentials error") % {"position_uid": self.position.position_uid}
            )

    def is_position_uid_valid(self):
        if self.payload.setup.get("position", None):
            return
        raise exceptions.NotAcceptable(
            {"detail": _("must provided the position uid for sell side")}
        )

    def is_position_exists(self) -> OrderPosition:
        try:
            position = OrderPosition.objects.select_related(
                "user_id", "ticker"
            ).get(position_uid=self.payload.setup["position"])
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({"detail": _("position not found error")})

        return position

    async def is_closed(self):
        if not self.position.is_live:
            raise exceptions.NotAcceptable(_("position has been closed"))
        return

    async def has_order(self):

        pending_order = await Order.objects.async_filter(
            user_id=self.position.user_id,
            status="pending",
            bot_id=self.position.bot_id,
            ticker=self.position.ticker,
        )
        if await pending_order.async_exists():
            last_order = await pending_order.async_first()
            orderId = last_order.order_uid.hex
            raise exceptions.NotAcceptable(
                _("sell order already exists for this position, order id : %(order_id)s, current status pending") % {"order_id": orderId}
            )

    async def validation_tasks(self):
        task = [
            asyncio.ensure_future(self.is_user_position()),
            asyncio.ensure_future(self.has_order()),
            asyncio.ensure_future(self.is_closed()),
        ]
        await asyncio.gather(*task)

    def validate(self):
        self.is_position_uid_valid
        self.position = self.is_position_exists()
        self.is_sellable()
        asyncio.run(self.validation_tasks())
        self.payload.margin = self.position.margin


class BuyValidator:
    def __init__(self, payload: BuyPayload):
        self.payload = payload
        self.user_amount = self.payload.user_id.user_balance.amount
        self.user_camount = self.payload.c_amount

    async def is_bot_exist(self):
        try:
            await BotOptionType.objects.async_get(bot_id=self.payload.bot_id)
        except BotOptionType.DoesNotExist:
            raise exceptions.NotFound({"detail": _("bot not found")})

    async def is_ticker_active(self):
        if not self.payload.ticker.is_active:
            raise exceptions.NotAcceptable(
                {"detail": _("%(ticker)s is not active") % {"ticker": self.payload.ticker.ticker}}
            )

    async def is_order_exist(self):
        orders = await Order.objects.async_filter(
            user_id=self.payload.user_id,
            ticker=self.payload.ticker,
            bot_id=self.payload.bot_id,
            status="pending",
            side="buy",
        )
        if await orders.async_exists():
            raise exceptions.NotAcceptable(
                {
                    "detail": _("you already has order for %(ticker)s in current options") % {"ticker": self.payload.ticker.ticker}
                }
            )

    async def is_portfolio_exist(self):
        portfolios = await OrderPosition.objects.async_filter(
            user_id=self.payload.user_id,
            ticker=self.payload.ticker,
            bot_id=self.payload.bot_id,
            is_live=True,
        )
        if await portfolios.async_exists():
            raise exceptions.NotAcceptable(
                {
                    "detail": _("cannot have multiple position for %(ticker)s in current options") % {"ticker": self.payload.ticker.ticker}
                }
            )

    async def is_below_one(self):
        return ((self.user_camount) / self.payload.price) < 1

    async def is_insufficient_funds(self):
        if self.payload.amount > self.user_amount or await self.is_below_one():
            raise exceptions.NotAcceptable({"detail": _("insufficient funds")})

    async def is_zero_amount(self):
        if self.payload.amount <= 0:
            raise exceptions.NotAcceptable({"detail": _("amount should not 0")})

    async def validation_tasks(self):
        tasks = [
            asyncio.ensure_future(self.is_bot_exist()),
            asyncio.ensure_future(self.is_ticker_active()),
            asyncio.ensure_future(self.is_order_exist()),
            asyncio.ensure_future(self.is_portfolio_exist()),
            asyncio.ensure_future(self.is_zero_amount()),
            asyncio.ensure_future(self.is_insufficient_funds()),
        ]
        await asyncio.gather(*tasks)

    def validate(self):
        asyncio.run(self.validation_tasks())


class ActionValidator:

    order: Order

    def __init__(self, payload: ActionPayload):
        self.payload = payload
        try:
            self.order = Order.objects.get(pk=self.payload.order_uid)
        except Order.DoesNotExist:
            raise exceptions.NotFound({"detail": _("order not found")})

    def is_actioned(self):
        if self.order.status == self.payload.status:
            raise exceptions.NotAcceptable(
                {"detail": _("order is already %(status)s") % {"status": self.order.status}}
            )

    def is_insufficient_funds(self):
        if not self.payload.status == "cancel":
            if self.order.insufficient_balance():
                raise exceptions.MethodNotAllowed(
                    {"detail": _("insufficient funds")}
                )

    def is_incorrect_status(self):
        if self.payload.status not in ["placed", "cancel"]:
            raise exceptions.MethodNotAllowed(
                {"detail": _("status should placed or cancel")}
            )

    def validate(self):
        self.is_incorrect_status()
        self.is_actioned()
        self.is_insufficient_funds()


class ExecutorValidator(ActionValidator):
    def is_actioned(self):
        if self.order.status == self.payload.status:
            raise Exception(_("order is already %(status)s") % {"status": self.order.status})

    def is_insufficient_funds(self):
        if not self.payload.status == "cancel":
            if self.order.insufficient_balance():
                Exception(_("insufficient funds"))


class CancelExecutorValidator(ExecutorValidator):
    def is_on_pending(self):
        if self.order.status != "pending":
            raise Exception(
                _("cannot cancel, for order with status %(status)s") % {"status": self.order.status}
            )

    def validate(self):
        super().validate()
        self.is_on_pending()
