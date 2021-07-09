from datasource.rkd import Rkd
from core.universe.models import Universe
from django.core.management.base import BaseCommand
from datasource.rkd import RkdStream
"""
Global Variables
position : remote address / your IP

"""
in_list =['A',
'AA',
'AAL.O',
'AAP',
'AAPL.O',
'ABBV.K',
'ABT',
'ACN',
'ADBE.O',]

class Command(BaseCommand):
    def handle(self, *args, **options):
        rkd = RkdStream()
        # HKD_universe = [ticker['ticker'] for ticker in Universe.objects.filter(
        #     currency_code__in=['USD'], is_active=True).values('ticker')]
        rkd.ticker_data = in_list
        quotes = rkd.stream()
