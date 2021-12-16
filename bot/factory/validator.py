from dataclasses import dataclass
from core.universe.models import CurrencyCalendars, Universe
from datetime import datetime, timedelta
from core.bot.models import BotOptionType
from core.orders.models import OrderPosition


@dataclass
class BotCreateProps:
    ticker: str
    spot_date: datetime.date
    created:datetime
    expiry: datetime.date
    investment_amount: float
    price: float
    margin: int
    currency: str
    bot: BotOptionType
    bot_id: str

    def __init__(
        self,
        ticker: str,
        spot_date: datetime,
        investment_amount: float,
        price: float,
        bot_id: str,
        margin: int = 1,
    ):
        self.ticker = ticker
        self.spot_date = spot_date.date()
        self.created = spot_date
        self.investment_amount = investment_amount
        self.price = price
        self.bot_id = bot_id
        self.currency = self.get_ticker_currency()
        self.margin = margin

    def get_bot(self):
        try:
            self.bot = BotOptionType.objects.get(pk=self.bot_id)
        except BotOptionType.DoesNotExist:
            raise Exception("Bot does not exist")

    def get_ticker_currency(self) -> str:
        try:
            ticker = Universe.objects.get(ticker=self.ticker)
        except Universe.DoesNotExist:
            raise ValueError(f"Ticker {self.ticker} not found in Universe")
        return ticker.currency_code.currency_code

    def set_time_to_exp(self, time_to_exp: float):
        self.time_to_exp = time_to_exp

    def set_expiry_date(self):
        days = int(round((self.time_to_exp * 365), 0))
        expiry = self.spot_date + timedelta(days=days)
        currency: str = self.currency
        while expiry.weekday() != 5:
            expiry = expiry - timedelta(days=1)

        while True:
            holiday = False
            data = CurrencyCalendars.objects.filter(
                non_working_day=expiry, currency_code=currency
            ).distinct("non_working_day")
            if data:
                holiday = True
            if not holiday and expiry.weekday() < 5:
                break
            else:
                expiry = expiry - timedelta(days=1)
        self.expiry = expiry

    def validate(self):
        self.get_bot()
        self.set_time_to_exp(self.bot.time_to_exp)
        self.set_expiry_date()


@dataclass
class BotHedgerProps:
    sec_id: str
    bot: BotOptionType
    position: OrderPosition

    def __init__(self, sec_id: str):
        self.sec_id = sec_id

    def get_active_position(self):
        try:
            self.position = OrderPosition.objects.get(
                sec_id=self.sec_id, is_active=True
            )
        except OrderPosition.DoesNotExist:
            raise Exception("Position does not exist")

    def get_bot(self, bot_id):
        try:
            self.bot = BotOptionType.objects.get(pk=bot_id)
        except BotOptionType.DoesNotExist:
            raise Exception("Bot does not exist")

    def validate(self):
        self.get_active_position()
        self.get_bot(self.position.bot_id)


@dataclass
class BotStopperProps(BotHedgerProps):
    pass
