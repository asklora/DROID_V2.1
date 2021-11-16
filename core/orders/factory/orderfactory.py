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
import asyncio

class BaseOrderProcessor:
    response: Order = None

    def get_price(self, ticker:list)->float:
        rkd = RkdData(timeout=3)
        df = rkd.get_quote(
            ticker, save=True, df=True)
        df["latest_price"] = df["latest_price"].astype(float)
        return df.iloc[0]["latest_price"]


        
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
    
    async def is_user_position(self):
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
                'user_id','ticker').get(position_uid=self.payload.setup['position'])
        except OrderPosition.DoesNotExist:
            raise exceptions.NotFound({'detail': 'position not found error'})

        return position

    async def is_closed(self):
        if not self.position.is_live:
            raise exceptions.NotAcceptable(f"position, has been closed")
        return

    async def has_order(self):
        pending_order =await Order.objects.async_filter(
            user_id=self.position.user_id,
            status='pending',
            bot_id=self.position.bot_id,
            ticker=self.position.ticker
        )
        if await pending_order.async_exists():
            last_order = await pending_order.async_first()
            orderId = last_order.order_uid.hex
            raise exceptions.NotAcceptable(
                f"sell order already exists for this position, order id : {orderId}, current status pending")
    async def validation_tasks(self):
        task =[
            asyncio.ensure_future(self.is_user_position()),
            asyncio.ensure_future(self.has_order()),
            asyncio.ensure_future(self.is_closed()),
        ]
        await asyncio.gather(*task)
    
    def validate(self):
        self.is_position_uid_valid
        self.position = self.is_position_exists()
        asyncio.run(self.validation_tasks())
        # self.is_user_position()
        # self.is_closed()
        # self.has_order()
        self.payload.margin = self.position.margin

class BuyValidator:

    def __init__(self, payload: BuyPayload):
        self.payload = payload
        self.user_amount = self.payload.user_id.user_balance.amount
        self.user_camount =self.payload.c_amount
        
    
    async def is_bot_exist(self):
        try:
            await BotOptionType.objects.async_filter(bot_id=self.payload.bot_id)
        except BotOptionType.DoesNotExist:
            raise exceptions.NotFound({"detail": "bot not found"})


    async def is_ticker_active(self):
        if not self.payload.ticker.is_active:
            raise exceptions.NotAcceptable(
                {"detail": f"{self.payload.ticker.ticker} is not active"})
    
    async def is_order_exist(self):
        orders = await Order.objects.async_filter(user_id=self.payload.user_id, ticker=self.payload.ticker,
                                      bot_id=self.payload.bot_id, status='pending', side='buy')
        if await orders.async_exists():
            raise exceptions.NotAcceptable(
                {"detail": f"you already has order for {self.payload.ticker.ticker} in current options"})

    async def is_portfolio_exist(self):
        portfolios = await OrderPosition.objects.async_filter(
            user_id=self.payload.user_id, ticker=self.payload.ticker, bot_id=self.payload.bot_id, is_live=True)
        if await portfolios.async_exists():
            raise exceptions.NotAcceptable(
                {"detail": f"cannot have multiple position for {self.payload.ticker.ticker} in current options"})

    async def is_below_one(self):
        return ((self.user_camount) / self.payload.price) < 1

    async def is_insufficient_funds(self):
        if self.payload.amount > self.user_amount or await self.is_below_one():
            raise exceptions.NotAcceptable({"detail": "insufficient funds"})

    async def is_zero_amount(self):
        if self.payload.amount <= 0:
            raise exceptions.NotAcceptable({"detail": "amount should not 0"})
    
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
        # self.is_bot_exist()
        # self.is_ticker_active()
        # self.is_order_exist()
        # self.is_portfolio_exist()
        # self.is_zero_amount()
        # self.is_insufficient_funds()
        asyncio.run(self.validation_tasks())




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
        self.raw_payload=payload
        self.payload = BuyPayload(**payload)
        self.validator: ValidatorProtocol = BuyValidator(self.payload)

    def execute(self):
        self.raw_payload["price"] = self.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            self.response = Order.objects.create(
                **self.raw_payload, order_type='apps', is_init=True)


class OrderController:

    def process(self, processor: OrderProtocol):
        processor.validator.validate()
        try:
            processor.execute()
        except Exception as e:
            raise exceptions.APIException({"detail": str(e)})
        return processor.response
