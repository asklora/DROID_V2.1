from dataclasses import dataclass
from core.universe.models import ExchangeMarket


@dataclass
class MarketManager:
    mic: str

    def __post_init__(self):
        self.market = ExchangeMarket.objects.get(
            mic=self.mic,
        )
        print(
            "market is initially open"
            if self.market.is_open
            else "market is initially closed"
        )

    @property
    def is_open(self) -> bool:
        return self.market.is_open

    def open(self):
        print("opening the market")
        self.market.is_open = True
        self.market.save()

    def close(self):
        print("closing the market")
        self.market.is_open = False
        self.market.save()
