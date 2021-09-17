import pytest
from core.universe.models import Universe
from datasource.rkd import RkdData

pytestmark = pytest.mark.django_db


def test_usd_market() -> None:
    # We get the tickers
    tickers = Universe.objects.filter(
        currency_code="USD",
        is_active=True,
    ).values_list("ticker", flat=True)

    # We turn them into list of tickers
    tickers_list = [str(elem) for elem in list(tickers)]

    # We initiate the rkd datasource class
    rkd = RkdData()

    # We get the quotes from the tickers above
    quotes = rkd.bulk_get_quote(ticker=tickers_list, df=True)

    # For debugging
    print(tickers_list)
    print(quotes)

    # We check if either the list of tickers or the quotes is empty
    assert tickers_list is not None
    assert quotes is not None

    # We then check if the length of returned quotes is correct
    assert len(tickers_list) == len(quotes.index)
