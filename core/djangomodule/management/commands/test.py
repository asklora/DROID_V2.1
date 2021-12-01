import random
from datetime import timedelta

from core.master.models import LatestPrice
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    def handle(self, *args, **options):
        yesterday = timezone.now().date() - timedelta(days=1)
        tickers = (
            LatestPrice.objects.filter(
                intraday_date=yesterday,
                ticker__currency_code="HKD",
            )
            .exclude(latest_price=None)
            .values_list(
                "ticker",
                "latest_price",
                named=True,
            )
        )

        tickers_list = list(tickers)

        random_tickers = random.sample(tickers_list, 5)

        print(random_tickers)

        for ticker in random_tickers:
            print(f"{ticker.ticker} - {ticker.latest_price}")
