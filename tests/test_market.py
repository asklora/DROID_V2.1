import pytest
from core.universe.models import Universe
from datasource.rkd import RkdData

pytestmark = pytest.mark.django_db


def test_usd_market() -> None:
    tickers = Universe.objects.filter(
        currency_code="USD",
        is_active=True,
    ).values_list('ticker', flat=True)

    tickers_list = [str(elem) for elem in list(tickers)]

    rkd = RkdData()

    quotes = rkd.bulk_get_quote(ticker=tickers_list, df=True)

    print(tickers_list)
    print(quotes)

    assert tickers_list is not None
    assert quotes is not None
    assert len(tickers_list) == quotes.shape[0]
