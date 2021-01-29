from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.universe.models import Universe,UniverseConsolidated
from core.Clients.models import UniverseClient

class Command(BaseCommand):

    def handle(self, *args, **options):
        ticker = "JMIA.N"
        user  = User.objects.get(id=1)
        try:
            Universe.objects.get(ticker=ticker)
        except Universe.DoesNotExist:
            UniverseConsolidated.objects.create(origin_ticker=ticker,is_active=True)
            populate = UniverseConsolidated.ingestion_manager.get_isin_code(ticker=ticker)
            if populate:
                UniverseClient.objects.create(client_id=user.client_user.uid,ticker_id=ticker)
        return 'ok'
