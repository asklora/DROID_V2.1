from core.universe.models import ExchangeMarket


def check_market(mic: str) -> bool:
    market = ExchangeMarket.objects.get(mic=mic)
    print("market is initially open" if market.is_open else "market is initially closed")
    return market.is_open


def open_market(mic: str) -> None:
    print("opening the market")
    market = ExchangeMarket.objects.get(mic=mic)
    market.is_open = True
    market.save()


def close_market(mic: str) -> None:
    print("closing the market")
    market = ExchangeMarket.objects.get(mic=mic)
    market.is_open = False
    market.save()
