from core.services.tasks import send_csv_hanwha

class Command(BaseCommand):
    def handle(self, *args, **options):