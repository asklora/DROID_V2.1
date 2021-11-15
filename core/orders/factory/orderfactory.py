from dataclasses import asdict, dataclass
from core.universe.models import Universe
from core.user.models import User
from core.user.convert import ConvertMoney
from datasource.rkd import RkdData
from django.utils import timezone
from core.orders.models import Order, OrderPosition
from core.bot.models import BotOptionType
from .order_protocol import ValidatorProtocol, OrderProtocol
from rest_framework import exceptions
from django.db import transaction as db_transaction
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
)


@dataclass
class SellPayload:
    setup: dict
    side: str
    ticker: Universe
    user_id: User
    margin:int


@dataclass
class BuyPayload:
    amount: float
    bot_id: str
    price: float
    side: str
    ticker: Universe
    user_id: User
    margin: int

    @property
    def c_amount(self):
        converter = ConvertMoney(
            self.user_id.user_balance.currency_code, self.ticker.currency_code)
        return converter.convert(self.amount)


class SellValidator:
    
    position: OrderPosition=None

    def __init__(self, payload: SellPayload):
        self.payload = payload
    def is_user_position(self):
        if self.payload.user_id != self.position.user_id:
            raise exceptions.NotAcceptable(
                f"{self.position.position_uid} credentials error")
    
    
    def is_position_uid_valid(self):
        if self.payload.setup.get('position', None):
            return
        raise exceptions.NotAcceptable(
            {"detail": "must provided the position uid for sell side"})

    def is_position_exists(self) -> OrderPosition:
        try:
            position = OrderPosition.objects.select_related(
                'ticker').get(position_uid=self.payload.setup['position'])
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({'detail': 'position not found error'})

        return position

    def is_closed(self):
        if not self.position.is_live:
            raise exceptions.NotAcceptable(f"position, has been closed")
        return

    def has_order(self):
        pending_order = Order.objects.prefetch_related("ticker").filter(
            user_id=self.position.user_id,
            status='pending',
            bot_id=self.position.bot_id,
            ticker=self.position.ticker
        )
        if pending_order.exists():
            last_order = pending_order.first()
            orderId = last_order.order_uid.hex
            raise exceptions.NotAcceptable(
                f"sell order already exists for this position, order id : {orderId}, current status pending")

    def validate(self):
        self.is_position_uid_valid
        self.position = self.is_position_exists()
        self.is_user_position()
        self.is_closed()
        self.has_order()
        self.payload.margin = self.position.margin


class BuyValidator:

    def __init__(self, payload: BuyPayload):
        self.payload = payload
    
    def is_bot_exist(self):
        try:
            BotOptionType.objects.get(bot_id=self.payload.bot_id)
        except BotOptionType.DoesNotExist:
            raise exceptions.NotFound({"detail": "bot not found"})


    def is_ticker_active(self):
        if not self.payload.ticker.is_active:
            raise exceptions.NotAcceptable(
                {"detail": f"{self.payload.ticker.ticker} is not active"})
    
    
    def is_order_exist(self):
        orders = Order.objects.filter(user_id=self.payload.user_id, ticker=self.payload.ticker,
                                      bot_id=self.payload.bot_id, status='pending', side='buy')
        if orders.exists():
            raise exceptions.NotAcceptable(
                {"detail": f"you already has order for {self.payload.ticker.ticker} in current options"})

    def is_portfolio_exist(self):
        portfolios = OrderPosition.objects.filter(
            user_id=self.payload.user_id, ticker=self.payload.ticker, bot_id=self.payload.bot_id, is_live=True).prefetch_related('ticker')
        if portfolios.exists():
            raise exceptions.NotAcceptable(
                {"detail": f"cannot have multiple position for {self.payload.ticker.ticker} in current options"})

    def is_below_one(self):
        return (self.payload.c_amount / self.payload.price) < 1

    def is_insufficient_funds(self):
        if self.payload.amount > self.payload.user_id.user_balance.amount or self.is_below_one():
            raise exceptions.NotAcceptable({"detail": "insufficient funds"})

    def is_zero_amount(self):
        if self.payload.amount <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})

    def validate(self):
        self.is_bot_exist()
        self.is_ticker_active()
        self.is_order_exist()
        self.is_portfolio_exist()
        self.is_zero_amount()
        self.is_below_one()
        self.is_insufficient_funds()


class BaseOrderProcessor:
    response: Order = None

    def get_price(self, ticker:list)->float:
        rkd = RkdData()
        df = rkd.get_quote(
            ticker, save=True, df=True)
        df["latest_price"] = df["latest_price"].astype(float)
        return df.iloc[0]["latest_price"]


class SellOrderProcessor(BaseOrderProcessor):

    def __init__(self, payload: dict):
        self.payload = SellPayload(**payload)

        self.validator: ValidatorProtocol = SellValidator(self.payload)

    def execute(self):
        self.payload.price = self.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            position = self.validator.position
            bot = position.bot
            trading_day=timezone.now()
            if bot.is_ucdc():
                positions, self.response= ucdc_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_uno():
                positions, self.response=uno_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_classic():
                positions, self.response=classic_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_stock():
                positions, self.response=user_sell_position(self.payload.price, trading_day, position, apps=True)


class BuyOrderProcessor(BaseOrderProcessor):

    def __init__(self, payload: dict):
        self.payload = BuyPayload(**payload)
        self.validator: ValidatorProtocol = BuyValidator(self.payload)

    def execute(self):
        self.payload.price = self.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            self.response = Order.objects.create(
                **asdict(self.payload), order_type='apps', is_init=True)


class OrderController:

    def process(self, processor: OrderProtocol):
        processor.validator.validate()
        try:
            processor.execute()
        except Exception as e:
            raise exceptions.APIException({"detail": str(e)})
        return processor.response
