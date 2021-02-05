from django.core.management.base import BaseCommand, CommandError
from core.services.pc4tasks import tasktest
from core.universe.models import Universe,UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        user = User.objects.get(id=1)
        a = UniverseClient.objects.filter(client=user.client_user.client_id,ticker='TRXC.K')
        print(a.count())