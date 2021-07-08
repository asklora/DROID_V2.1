from datasource.rkd import Rkd
from core.universe.models import Universe
from django.core.management.base import BaseCommand
from datasource.rkd import RkdStream
"""
Global Variables
position : remote address / your IP

"""
rkd = RkdStream()

class Command(BaseCommand):

    HKD_universe = [ticker['ticker'] for ticker in Universe.objects.filter(
        currency_code__in=['HKD'], is_active=True)[:20].values('ticker')]
    rkd.ticker_data = HKD_universe
    quotes = rkd.stream()
